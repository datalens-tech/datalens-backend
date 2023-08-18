#!/usr/bin/env python3
"""
Autoconfiguration for https://sandbox.yandex-team.ru/schedulers
"""

from __future__ import division, absolute_import, print_function, unicode_literals

import os
import re
import logging
from statcommons.req import APIRequester


_sh_find_unsafe = re.compile(r'[^\w@%+=:,./-]').search


def sh_quote_prettier(s):
    r"""
    Quote a value for copypasteability in a posix commandline.

    A more readable version than the backported one.

    >>> sh_quote_prettier("'one's one'")
    "\\''one'\\''s one'\\'"
    """
    if not s:
        return "''"
    if _sh_find_unsafe(s) is None:
        return s

    # A shorter version: backslash-escaped single quote.
    result = "'" + s.replace("'", "'\\''") + "'"
    # Cleanup the empty excesses at the ends
    _overedge = "''"
    if result.startswith(_overedge):
        result = result[len(_overedge):]
    if result.endswith(_overedge):
        result = result[:-len(_overedge)]
    return result


def sh_join(args):
    return " ".join(sh_quote_prettier(val) for val in args)


class SandboxSchedulersConfigurer:

    # ### Required overrides ###

    # The actual tasks configs, dicts with keyword arguments to
    # `self.make_scheduler_config`
    schedulers = ()

    # environments to iterate on; qloud environment names, by default
    env_names = ("beta", "prod")
    env_names = ("beta",)
    # env_names = ("testing", "production")

    # qloud project name
    qloud_project = "statbox"
    # qloud application name
    app_name = None
    # qloud component name
    component = None

    author = "robot-statface-api"
    owner = "STATFACE"
    extra_tags = ()
    cmd_tpl = None

    def make_notifications(self, config, **kwargs):
        return []

    # ### Usable defaults ###

    env_to_tag = {"prod": "production", "beta": "testing"}
    # container = 870270439
    container = None
    # secfiles_vault = "STATFACE::statface-api-secfiles-key"
    secfiles_vault = None
    # qloud_oauth_vault = "STATFACE::robot-statface-api-qloud"
    qloud_oauth_vault = None
    # registry_vault = "robot-statface-api::STATFACE::robot-statface-api-qloud"
    registry_vault = None

    base_url = "https://sandbox.yandex-team.ru/api/v1.0/scheduler/"
    # face_list_url = "https://sandbox.yandex-team.ru/schedulers"
    face_url_tpl = "https://sandbox.yandex-team.ru/scheduler/{id}/view"

    @staticmethod
    def get_token():
        with open(os.path.expanduser("~/.ya_sandbox_oauth")) as fobj:
            token = fobj.read()
        token = token.strip()
        return token

    @classmethod
    def make_reqr(cls):
        reqr = APIRequester(base_url=cls.base_url, session=None)
        reqr.headers["Authorization"] = "OAuth {}".format(cls.get_token())
        return reqr

    def __init__(self):
        self.reqr = self.make_reqr()
        self.logger = logging.getLogger("sandbox_schedulers_configurer")
        self.known_scheduler_ids = {}

    def req(self, uri, **kwargs):
        return self.reqr(uri, **kwargs)

    # Was used as a template:
    # https://sandbox.yandex-team.ru/scheduler/12091/view
    def make_scheduler(self, base_scheduler_id=12091):
        return self.req("", data=dict(source=base_scheduler_id))

    # pylint: disable=too-many-branches
    def make_scheduler_config(
            self,
            name,  # command
            env_name,  # "prod"|"beta"
            next_time="2018-12-14T09:00:00.000Z",
            interval=None,
            comment="",
            # ...
            app_name=None,
            author=None,
            extra_tags=None,
            component=None,
            cmd=None,
            cmd_tpl=None,
            # ...
            semaphores=True,
            ram_gib=2.0,
            disk_gib=50.0,
            kill_timeout=10800,
            description=None,
            scheduler_id=None,
    ):
        assert interval or next_time
        if app_name is None:
            app_name = self.app_name
        assert app_name
        if author is None:
            author = self.author
        assert author
        if extra_tags is None:
            extra_tags = self.extra_tags
        assert extra_tags is not None
        if component is None:
            component = self.component
        assert component
        component_full = "{project}.{app}.{env}.{component}".format(
            project=self.qloud_project, app=app_name,
            env=env_name, component=component)
        # order is somewhat important (visually)
        tags = (
            [
                app_name.upper().replace("-", "_"),  # "QC_API",
                (self.env_to_tag.get(env_name) or env_name).upper(),  # "PRODUCTION",
                "PERIODIC",
            ] + list(tag.upper() for tag in extra_tags) + [
                name.upper()  # "send_hung_nodes_report",
            ])
        if not description:
            description = "{base} {name} {env}{comment}".format(
                base=app_name, name=name, env=env_name,
                comment=" ({})".format(comment) if comment else "",
            )
        if semaphores is True:
            semaphores = {
                # "release": ["EXECUTE"],
                "acquires": [
                    {
                        "name": "{}/{}/{}".format(app_name, env_name, name),
                        # "weight": None,
                        "capacity": 1,
                    }
                ],
            }
        semaphores = semaphores or {}
        if next_time:
            repetition = {"weekly": [0, 1, 2, 3, 4, 5, 6]}
        else:
            assert interval
            repetition = {"interval": interval}
        if not cmd:
            if cmd_tpl is None:
                cmd_tpl = self.cmd_tpl
            assert cmd_tpl
            cmd = cmd_tpl.format(**locals())
        assert cmd

        custom_fields = [
            {"name": "Command", "value": cmd},
            {"name": "Component", "value": component_full},
        ]
        if self.container is not None:
            custom_fields.append({"name": "Container", "value": self.container})
        if self.qloud_oauth_vault is not None:
            custom_fields.append({"name": "OAuthVault", "value": self.qloud_oauth_vault})
        if self.secfiles_vault is not None:
            custom_fields.append({"name": "SecfilesKeyVault", "value": self.secfiles_vault})
        if self.registry_vault is not None:
            custom_fields.append({"name": "RegistryAuth", "value": self.registry_vault})

        config = {
            "id": scheduler_id,
            "time": {
                "next": next_time,
                # "last": "2018-12-13T15:02:52.404Z",
                # "created": "2018-12-13T15:03:14.395Z",
                # "updated": "2018-12-13T15:02:36.056Z",
            },
            "owner": self.owner,
            "author": author,
            "status": "STOPPED",
            # "rights": "write",
            "schedule": {
                "repetition": repetition,
                "start_time": next_time,
                "retry": {"ignore": True},
                "sequential_run": False,
                "fail_on_error": False,
            },
            "description": "",
            "task": {
                # "time": None,
                "type": "STATFACE_TASK_RUNNER",
                "owner": self.owner,
                # "queue": [],
                # "author": "",
                # "status": "",
                # "rights": [],
                # "scores": -1,
                # "se_tag": None,
                # "parent": {},
                # "se_tags": [],
                # "reports": [],
                # "children": {},
                # "lock_host": "",
                "priority": {"class": "BACKGROUND", "subclass": "LOW"},
                # "tasks_archive_resource": None,
                "requirements": {
                    "semaphores": semaphores,
                    # "client_tags": "GENERIC",
                    "ram": int(ram_gib * 2**30),
                    # "cpu_model": "",
                    # "host": "",
                    "disk_space": int(disk_gib * 2**30),
                    # "platform": "",
                    # "dns": "dns64",
                    # "ramdrive": None,
                    # "cores": 1,
                    # "privileged": True,
                },
                # "hidden": False,
                # "important": False,
                # "execution": {"current": 0, "estimated": 0},
                "kill_timeout": kill_timeout,
                "description": description,
                "fail_on_any_error": False,
                # "relatedHosts": [],
                "tags": tags,
                "notifications": None,
                "custom_fields": custom_fields,
                # "sdk_version": 2,
            },
        }
        full_context = dict(locals())
        full_context.pop('self', None)
        config["task"]["notifications"] = self.make_notifications(**full_context)
        return config

    def find_scheduler_id(self, config, extra_filtered=False):
        filter_params = dict(
            all_tags=True,
            tags=",".join(config["task"]["tags"]),
            limit=3,
            offset=0,
            order="-updated",
        )
        if extra_filtered:
            filter_params.update(
                owner=config["owner"],
                author=config["author"],
                task_type=config["task"]["type"],
            )
        resp = self.req(
            "",
            params=filter_params,
        )
        items = resp.json()["items"]
        if not items:
            return None
        if len(items) > 1:
            raise Exception(
                "Multiple schedulers were found",
                dict(filter_params=filter_params, items=items, url=resp.url))
        return items[0]["id"]

    def configure_scheduler_by_id(self, scheduler_id, config):
        data = dict(
            config,
            scheduler_id=scheduler_id,
        )
        resp = self.req(
            "{}".format(scheduler_id),
            method="put",
            data=data,
        )
        return resp

    def start_scheduler(self, scheduler_id):
        return self.req(
            "/api/v1.0/batch/schedulers/start",
            method="put",
            data=[scheduler_id])

    # def upsert_scheduler_leg(self, env_name, conf):
    #     identifier = (conf["name"], env_name)
    #     print(identifier)
    #     if identifier not in results:
    #         resp0 = make_scheduler()
    #         scheduler_id = resp0.json()["id"]
    #         results[identifier] = scheduler_id
    #     else:
    #         resp0 = None
    #         scheduler_id = results[identifier]

    #     print(self.face_url_tpl.format(id=scheduler_id))

    #     config = self.make_scheduler_config(
    #         scheduler_id=scheduler_id, env_name=env_name, **conf)
    #     resp1 = self.configure_scheduler_by_id(
    #         scheduler_id=scheduler_id, config=config)
    #     resp2 = self.start_scheduler(
    #         scheduler_id)
    #     print(resp0, resp1, resp2)

    def upsert_scheduler(self, env_name, conf):
        identifier = (conf["name"], env_name)
        print(identifier)

        config = self.make_scheduler_config(
            env_name=env_name, **conf)
        scheduler_id = self.find_scheduler_id(config)
        resp0 = None
        if scheduler_id is None:
            resp0 = self.make_scheduler()
            scheduler_id = resp0.json()["id"]

        self.known_scheduler_ids[identifier] = scheduler_id
        print(self.face_url_tpl.format(id=scheduler_id))

        resp1 = self.configure_scheduler_by_id(scheduler_id=scheduler_id, config=config)
        resp2 = self.start_scheduler(scheduler_id)
        print(resp0, resp1, resp2)

    def main(self):
        for conf in self.schedulers:
            for env_name in self.env_names:
                try:
                    self.upsert_scheduler(conf=conf, env_name=env_name)
                except Exception:
                    raise

        # Make a wiki page source piece:
        page = []
        for conf in self.schedulers:
            page.append("* {name} ({comment}):".format(**conf))
            for env_name in self.env_names:
                identifier = (conf["name"], env_name)
                scheduler_id = self.known_scheduler_ids[identifier]
                page.append("  (({} {}))".format(
                    self.face_url_tpl.format(id=scheduler_id),
                    env_name,
                ))
            page.append("\n")
        print("".join(page))


# Usage example:
# https://github.yandex-team.ru/statbox/statface-api-v4/blob/master/_aux/sandbox_schedulers.py
