#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-public-methods,too-many-arguments
"""
In short: put the checks from `MANIFEST.json` into a `releaser`-specified qloud app-environment.

Put checks from `MANIFEST.json`,
with specified configuration (required override in subclasses),
into Juggler,
using Qloud API,
and using `releaser-cli` semantics (file `./.release.hjson`, arguments `-e envname`, etcetera).

Warning: replaces all `active='PASSIVE'` checks configured.
"""

import os
import json
import logging
import urllib.parse

import click

import releaser.cli.utils as releaser_utils
from releaser.lib import qloud
from releaser.cli import options as releaser_options

from sbdutils.base import slstrip


def get_main_dir():
    try:
        import __main__
        return os.path.abspath(os.path.dirname(__main__.__file__))
    except Exception:  # pylint: disable=broad-except
        return None


MAIN_DIR = get_main_dir()


# For auto-registering the bundled checks;
# same format as `MANIFEST.json`.
COMMON_EXTRA_CHECKS = [
    {"check_script": "logrotate_hourly.sh",
     "run_always": True,
     "services": ["logrotate_hourly.service"],
     "timeout": 1800,
     "interval": 3600},
    {"check_script": "push_client.sh",
     "run_always": True,
     "services": ["push_client.service"],
     "timeout": 30,
     "interval": 180},
]


class QloudJugglerConfigurer:

    # component name -> check names.
    # If unset, all checks (in manifest) are applied to all components.
    component_checks = None

    # search paths for the manifest.
    manifest_paths = (
        # Make it possible to put the subclass-overrides script next to the
        # manifest itself and call it from wherever.
        os.path.join(MAIN_DIR, 'MANIFEST.json'),
        # Fallbacks.
        './juggler/app/MANIFEST.json',
        './MANIFEST.json',
    )

    ttl_multiplier = 10.0
    ttl_cap = 3600

    @staticmethod
    def get_extra_checks():
        """
        Overridable point: checks in addition to manifest, in the same format.

        Common example: `return qloud_juggler.COMMON_EXTRA_CHECKS`.
        """
        return []

    @staticmethod
    def get_aggregator_kwargs(**info):  # pylint: disable=unused-argument
        """
        `svc, component, ...` -> `aggregator_kwargs` service config.
        """
        return dict(
            limits=[
                dict(
                    crit='50%',
                    day_end=7,
                    day_start=1,
                    time_end=23,
                    time_start=0,
                    warn='1%',
                    # show_stats=True,
                ),
            ],
            # https://doc.qloud.yandex-team.ru/doc/api/juggler#modes
            # 'force_crit', 'force_ok', 'skip'
            nodata_mode='force_crit',
            # ok_desc="everything is ok",
            # info_desc="",
            # warn_desc="",
            # crit_desc="AA! Panic!!!,
        )

    @staticmethod
    def get_flaps_config(**info):  # pylint: disable=unused-argument
        return dict(
            boost_time=0,
            stable_time=600,
            critical_time=1200,
        )

    @staticmethod
    def get_notifications_config(**info):  # pylint: disable=unused-argument
        """
        ...

        Example:

            return [
                dict(
                    template_kwargs=dict(
                        day_end=1,
                        day_start=1,
                        time_start='00:00',
                        time_end='23:59',
                        method=['email'],
                        golem_responsible=False,
                        login=['robot-statbox-theta'],
                    ),
                    template_name='on_desc_change',
                ),
            ]
        """
        return []

    @staticmethod
    def postprocess_service(config, **info):  # pylint: disable=unused-argument
        return config

    # #######
    # ...
    # #######

    # Name **prefix** for a juggler check that is copied from the qloud's `ISS status hook`.
    # If empty, status hook checks will not be added.
    status_hook_check_names_leg = (
        'status_hook_check_copy_',
    )
    status_hook_check_name = 'isshook_'

    @staticmethod
    def get_token():
        return releaser_utils.get_oauth_token_or_panic()

    # @classmethod
    # def make_reqr(cls, oauth_token=None):
    #     reqr = APIRequester(session=None)
    #     reqr.headers["Authorization"] = "OAuth {}".format(oauth_token or cls.get_token())
    #     return reqr

    @classmethod
    def make_qloud_client(cls, qloud_instance, token, reqr=None):
        cli = qloud.QloudClient(qloud_instance, token)
        if reqr is not None:
            pass  # TODO: copy the Retry config.
        return cli

    def __init__(self, qloud_project, qloud_application, qloud_environment, qloud_instance='ext'):
        self.logger = logging.getLogger("qloud_juggler")
        token = self.get_token()
        self._auth_token = token
        self.qloud_project = qloud_project
        self.qloud_application = qloud_application
        self.qloud_environment = qloud_environment
        self.qloud_instance = qloud_instance
        # reqr = self.make_reqr(oauth_token=token)
        reqr = None
        self._reqr = reqr
        self.qloud_client = self.make_qloud_client(qloud_instance, token, reqr=reqr)
        qloud_locator = dict(
            project=self.qloud_project, application=self.qloud_application,
            environment=self.qloud_environment, component=None)
        self.qloud_locator = qloud_locator
        self.qloud_env_obj = qloud.QloudObject(**qloud_locator)

    def _req(self, uri, method='GET', timeout=33, **kwargs):
        cli = self.qloud_client
        url = urllib.parse.urljoin(cli.qloud_url, uri)
        resp = cli.session.request(
            method,
            url,
            timeout=timeout,
            **kwargs)
        if not resp.ok:
            self.logger.error(
                'Request error: %s %s %s | %r',
                resp.status_code, resp.request.method, resp.request.url, resp.content)
        resp.raise_for_status()
        return resp

    @property
    def _debug(self):
        return os.environ.get('QJS_DEBUG')

    @property
    def qloud_marker(self):
        if self.qloud_instance == 'ext':
            return 'qloud-ext'
        return 'qloud'

    def make_status_hook_one_name(self, base, status_check, idx=0):
        return base

    def make_status_hook_one_check(self, status_check, idx=0):
        if not self.status_hook_check_name:
            return None

        status_check_name = '{}{:02d}'.format(self.status_hook_check_name, idx)
        status_check_name = self.make_status_hook_one_name(
            status_check_name, status_check=status_check, idx=idx)

        status_check_timeout = status_check.get('timeout', 60000) / 1000.0

        status_check_cfg = dict(
            service=status_check_name,
            refresh_time=int(status_check_timeout * 1.05),
            ttl=int(status_check_timeout * 9),
        )

        if status_check.get('type') == 'http':
            status_check_cfg.update(
                active='http',
                active_kwargs=dict(
                    path=status_check['path'],
                    port=status_check['port'],
                    timeout=status_check['timeout'],

                    disable_ipv4=True,
                    ok_codes=[200],
                    warn_codes=[440],
                ),
            )
        else:
            # TODO: support them if/when needed.
            self.logger.warning("Unsupported status check type in %r", status_check)
            return None

        return status_check_cfg

    def make_status_hook_checks(self, component, qloud_env_dump, require=True, **kwargs):
        results = {}

        if not self.status_hook_check_name:
            return results

        component_data = next(
            (item for item in qloud_env_dump['components']
             if item['componentName'] == component),
            None)
        if component_data is None:
            if require:
                raise Exception("Component not found", dict(component=component))

        component_status_hooks = component_data['statusHookChecks']

        for idx, status_check in enumerate(component_status_hooks):
            status_check_cfg = self.make_status_hook_one_check(status_check, idx=idx, **kwargs)
            if status_check_cfg is None:
                continue
            results[status_check_cfg['service']] = status_check_cfg

        return results

    @classmethod
    def make_cli(cls):
        """
        Make a `click` command for the current class.
        """

        @click.command(
            help=(
                'Put the checks (from `MAINFEST.json`) and status hooks'
                ' to Juggler, via Qloud API'
            ),
        )
        @releaser_options.qloud_instance_option
        @releaser_options.project_option
        @releaser_options.application_option
        @releaser_options.environment_option
        @releaser_options.components_option
        def _run_cli(
                qloudinst, project, application, environment, components,
                cls=cls):
            mgr = cls(
                qloud_instance=qloudinst, qloud_project=project,
                qloud_application=application, qloud_environment=environment,
            )
            return mgr.run(components=components)

        return _run_cli

    @classmethod
    def run_cli(cls):
        """
        Execute the `run` with options from commandline/config through the `click`.

        See also: `make_cli`.

        Recommended entry point.
        """
        cli_func = cls.make_cli()
        return cli_func()  # pylint: disable=no-value-for-parameter

    def find_manifest(self):
        for path in self.manifest_paths:
            if os.path.exists(path):
                return path
        raise RuntimeError(
            "No `MANIFEST.json` file was found.",
            dict(manifest_paths=self.manifest_paths))

    def read_manifest(self):
        filename = self.find_manifest()
        with open(filename, 'rb') as fobj:
            result = json.load(fobj)
        return result

    def get_all_checks(self):
        manifest_data = self.read_manifest()
        manifest_checks = manifest_data['checks']
        all_checks = manifest_checks + self.get_extra_checks()
        return all_checks

    def run(self, components):
        """
        Main entry point.
        """
        logging.basicConfig(level=1)

        all_checks = self.get_all_checks()
        qloud_env_dump = None
        if self.status_hook_check_name:
            qloud_env_dump = self.qloud_client.get_environment_dump(self.qloud_env_obj)

        for component in components:
            self.process_component(component, all_checks=all_checks, qloud_env_dump=qloud_env_dump)

    def make_svc_config(
            self, service_name, check_config,
            svc_common=None, svc_params=None, common_info=None):
        check_script = check_config.get('check_script')
        if check_script == 'monrun_wrap':
            description = 'Wrapped monrun check {}:[{}]'.format(
                slstrip(check_config['args'][0], '/opt/app/etc/monrun/conf.d/', require=False),
                check_config['args'][1])
        elif check_script:
            description = "Passive check over script {!r}".format(check_script)
        else:
            description = '...'

        result = {}
        result.update(svc_common or {})
        result.update(
            service=service_name,
            description=description,
        )
        result.update(svc_params or {})

        interval = check_config.get('interval')
        if interval:
            ttl = interval * self.ttl_multiplier
            if ttl > self.ttl_cap:
                ttl = self.ttl_cap
            result.update(
                refresh_time=interval,
                ttl=int(ttl),
            )

        info = dict(
            common_info or {},
            service_name=service_name,
            check_config=check_config,
            service_config=result,
        )

        result.update(notifications=self.get_notifications_config(**info))
        result.update(aggregator_kwargs=self.get_aggregator_kwargs(**info))
        result.update(flaps=self.get_flaps_config(**info))
        result = self.postprocess_service(result, **info)
        return result

    def make_component_locator(self, component):
        full_locator = dict(self.qloud_locator, component=component)
        full_qobj = qloud.QloudObject(**full_locator)  # pylint: disable=repeated-keyword
        return str(full_qobj)

    def make_component_services(self, component, checks, status_hook_checks, qloud_env_dump=None):
        if not checks and not status_hook_checks:
            return {}

        qloud_marker = self.qloud_marker
        component_locator = self.make_component_locator(component)

        common_info = dict(
            all_checks=checks,
            component=component,
            qloud_env_dump=qloud_env_dump,
        )

        host_string = '{}.{}'.format(qloud_marker, component_locator)
        host_pieces = host_string.split('.')

        svc_common = dict(
            # Qloud/Juggler configuation
            active='PASSIVE',
            host=host_string,
            description="(managed using statcommons qloud_juggler.py)",

            # Juggler configuration
            aggregator_kwargs={},
            flaps={},
            notifications=[],
            tags=[
                '.'.join(host_pieces[:length])
                # 'qloud-ext.statbox', 'qloud-ext.statbox.xxx', 'qloud-ext.statbox.xxx.testing'
                for length in (2, 3, 4)],
        )

        def make_svc_config(
                service_name, check_config, svc_params=None,
                # pylint: disable=dangerous-default-value
                svc_common=svc_common, common_info=common_info):
            return self.make_svc_config(
                service_name=service_name, check_config=check_config,
                svc_params=svc_params, svc_common=svc_common,
                common_info=common_info)

        # qloud-format `name -> config`
        targets = {
            service_name: make_svc_config(service_name, check_config=check_config)
            for check_config in checks
            for service_name in check_config['services']
        }

        if status_hook_checks:
            # There's no manifest-formatted check config for these,
            # but common code for notifications and such should still be acceptable.
            # Note: `svc_common` will still be applied.
            status_hook_checks = {
                service_name: make_svc_config(
                    service_name, check_config={}, svc_params=service_config)
                for service_name, service_config in status_hook_checks.items()}
            targets.update(status_hook_checks)

        return targets

    def process_component(self, component, all_checks, qloud_env_dump=None):
        enabled_checks = None
        checks = all_checks
        if self.component_checks:
            enabled_checks = self.component_checks.get(component) or ()
            checks = [
                dict(
                    item,
                    services=list(
                        svcname for svcname in item['services']
                        if svcname in enabled_checks),
                )
                for item in checks]
            checks = list(
                item for item in checks
                if item['services'])
            # TODO: warn about not-found service names.

        status_hook_checks = self.make_status_hook_checks(
            component=component, qloud_env_dump=qloud_env_dump)

        targets = self.make_component_services(
            component=component,
            checks=checks,
            status_hook_checks=status_hook_checks,
            qloud_env_dump=qloud_env_dump)

        if self._debug:
            print("# ####### checks for component={component} #######".format(component=component))
            print({
                name: {key: val for key, val in cfg.items()
                       if key not in ('aggregator_kwargs', 'flaps', 'notifications', 'tags')}
                for name, cfg in targets.items()})

        return self.put_component_services(
            component=component,
            targets=targets)

    def put_component_services(self, component, targets):

        component_locator = self.make_component_locator(component)

        # Read the current state (for skip-if-the-same and create-if-nonexistent)
        currents = self._req(
            # https://doc.qloud.yandex-team.ru/doc/api/juggler#saved
            '/api/v1/juggler/checks/{componentId}/saved'.format(componentId=component_locator)
        ).json()

        names = (
            set(targets) |
            set(
                key for key, cfg in currents.items()
                # All passive checks should come from here.
                if cfg.get('active') == 'PASSIVE' or
                # Current and possible-previous status-hook copies.
                any(
                    cfg.get('service').startswith(prefix)
                    for prefix in (
                        tuple(self.status_hook_check_names_leg) +
                        (self.status_hook_check_name,)
                    ))
            )
        )

        for svcname in sorted(names):
            self.put_component_service(
                svcname,
                component_locator=component_locator,
                current=currents.get(svcname),
                target=targets.get(svcname),
            )

        return locals(), None

    def put_component_service(self, svcname, component_locator, current, target):
        if target is None:
            if current is not None:
                self._req(
                    '/api/v1/juggler/checks/{componentId}/remove'.format(
                        componentId=component_locator),
                    method='DELETE',
                    params=dict(service=svcname))
            return  # target is None, ensured `current is None`

        # else:  # elif target is not None:

        if not current:
            self._req(
                '/api/v1/juggler/checks/{componentId}/create'.format(
                    componentId=component_locator),
                method='PUT',
                params=dict(service=svcname, active=target['active']))

        # target is not None, ensured `current is not None`

        # Unused feature: poke the juggler API directly, if needed.
        # extra_juggler_config = dict(notifications=target['notifications'])
        extra_juggler_config = None

        # For things like `downtimeEnd`, enforce only known keys, and copy the rest:
        # current_from_listing = current
        current = self._req(
            '/api/v1/juggler/checks/{componentId}/get'.format(
                componentId=component_locator),
            params=dict(service=svcname),
        ).json()

        target = {**current, **target}

        if current != target:
            self._req(
                '/api/v1/juggler/checks/{componentId}/update'.format(
                    componentId=component_locator),
                method='POST',
                json=target)

        if extra_juggler_config:
            juggler_host = 'qloud-ext.{}'.format(component_locator)
            juggler_svc = svcname
            juggler_resp = self._req(
                'https://juggler-api.yandex-team.ru/api/checks/checks',
                params=dict(
                    host_name=juggler_host,
                    service_name=juggler_svc,
                    include_children='1',
                    include_notifications='1',
                    include_meta='1',
                    do='1'))
            juggler_cfg = dict(juggler_resp.json()[juggler_host][juggler_svc])
            clear_keys = ['flap_time', 'check_id', 'boost_time', 'creation_time', 'modification_time', 'critical_time', 'stable_time']
            for key in clear_keys:
                juggler_cfg.pop(key, None)
            juggler_cfg.update(
                flaps=target['flaps'],
                host=juggler_host,
                service=juggler_svc,
                methods=[],
            )
            juggler_cfg.update(extra_juggler_config)
            self._req(
                'https://juggler-api.yandex-team.ru/api/checks/add_or_update?do=1',
                method='POST',
                json=juggler_cfg)
