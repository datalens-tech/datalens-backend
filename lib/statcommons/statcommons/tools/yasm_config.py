#!/usr/bin/env python3
# pylint: disable=fixme,line-too-long
"""
Requires the juggler scope on the token in `~/.release.hjson`

One way to get it:
https://oauth.yandex-team.ru/authorize?response_type=token&client_id=2d1760be683244f4a91559d09cec6140
"""


import os
import re
import logging
from collections import defaultdict
# import urllib.parse
# from copy import deepcopy
# import simplejson as json
import hjson
# import requests
from statcommons.req import APIRequester


def normalize_signals(data, defaults=None):
    defaults = defaults or {}
    for identifier, cfg in data.items():
        cfg = cfg.copy()
        cfg.setdefault('title', identifier)
        for def_key, def_value in defaults.items():
            cfg.setdefault(def_key, def_value)
        # TODO?: hash-based auto-color?
        data[identifier] = cfg
    return data


def drop_keys(data, keys):
    data = data.copy()
    for key in keys:
        data.pop(key, None)
    return data


class YASMConfigurer:

    # #######
    # Likely-required overrides
    # #######

    # qloud project name; e.g. "statbox.statface-api.prod"
    prj = ""

    # qloud routers names;  e.g. ("statface-api-prod", "statface-upload-prod")
    routers = ()
    # mdb postgresql/... databases: `{cid: {"title": ..., "db_type": "postgres", ...}}`
    # (Warning: might become legacy; yc.y-t shows graphs using charts.y-t -> solomon.y-t, not yasm.y-t)
    # https://st.yandex-team.ru/MDB-4356
    databases = {}
    # qloud component names;  e.g. ("sf4", "sf4celery", ...)
    components = ()
    # component names that need "workers" signals;  e.g. ("sf4-*",)
    components_with_workers = ()
    # yasm panel user; see url;  e.g. "hhell"
    panel_user = ""
    # yasm panel id; see url;  e.g. "_this_project_panel"
    panel_key = ""
    # ...
    panel_editors = ()
    panel_abc = None
    # ...
    panel_title = ""
    # `(row, ...)`; `row`: `(column, ...)`; `column`: graph name;
    # e.g. (("sf4", "sf4celery"), ("problems",))
    panel_layout = ()

    # {line_title: yasm_alert_options, ...}
    line_title_to_alert_config = {}

    def alert_notifications(self, **kwargs):
        # juggler alert notifications config; example:
        # [
        #     {"template_name": "on_status_change",
        #      "template_kwargs": {
        #          "method": ["email"],
        #          "login": ["hhell", "asnytin"],
        #          "status": ["CRIT"],
        #      }},
        # ]
        return []

    # #######
    # Usable defaults
    # #######

    base_url = "https://yasm.yandex-team.ru"
    panel_default_graph_width = 2
    panel_default_graph_height = 2

    def get_alert_config(self, title, signal=None, **kwargs):
        return self.line_title_to_alert_config.get(title)

    def make_panel_graph_config(self, name, **kwargs):
        """
        Given an item from `self.panel_layout`, make a complete chart configuration.
        """

        if not isinstance(name, str):
            return name

        result = dict(
            title=name,
        )

        # Special-ish cases:
        if name == 'net_all':
            return dict(
                result,
                signals=self.signals_for_everything(
                    signals=[
                        value
                        for key, value in self.container_resources_extra.items()
                        if key.startswith('net_')],
                    kinds=('resources',),
                    mdb_default_itype='mdbdom0',
                ),
            )
        if name in self.known_kinds:
            kinds = (name,)
            return dict(
                result,
                signals=self.make_qloud_signals(kinds=kinds),
            )
        _suffix = '_all'
        if name.endswith('_all'):
            name_base = name[:-len(_suffix)]
            if name_base in self.known_kinds:
                kinds = (name_base,)
                if name_base == 'resources':
                    result.update(
                        # Cap with some padding.
                        minValue=-0.1,
                        maxValue=1.1,
                    )
                return dict(
                    result,
                    signals=self.signals_for_all_components(
                        lambda component, **kwargs: self.make_qloud_signals(
                            kinds=kinds, component=component, const=False, **kwargs)),
                )

        name_pieces = name.rsplit('__', 1)
        # if len(name_pieces) == 1:
        #     name_pieces = name.rsplit('_', 1)
        if len(name_pieces) == 2:
            name_base, name_suffix = name_pieces
            if name_suffix == '*':
                kinds = self.known_kinds
            else:
                kinds = name_suffix.split(',')
            assert all(kind in self.known_kinds for kind in kinds), (kinds, self.known_kinds)
        else:  # not very convenient, more of a just-in-case catch.
            name_base = name
            name_suffix = ""
            # kinds = self.known_kinds
            kinds = ('resources',)

        if name_base in self.components:
            component = name_base
            return dict(
                result,
                signals=self.make_qloud_signals(kinds=kinds, component="{}-*".format(component), **kwargs),
                # minValue=0,
                # maxValue=1,
            )
        if name_base in self.routers:
            return dict(
                result,
                signals=self.make_qloud_router_signals(kinds=kinds, name=name_base, **kwargs),
            )
        db_cfg = self.databases_all.get(name_base)
        if db_cfg:
            return dict(
                result,
                signals=self.make_db_signals(kinds=kinds, **{**kwargs, **db_cfg}),
            )

        raise Exception("Unknown panel graph", dict(name=name, name_base=name_base, name_suffix=name_suffix))

    def common_alert_config(self, name, component=None, juggler_name=None, prj=None, **kwargs):
        if prj is None:
            prj = self.prj
        name_base = name
        if component:
            name = "{}_{}".format(component, name)
        full_name = "{}{}".format(self.alert_prefix, name)
        notifications = self.alert_notifications(name=name, component=component, **kwargs)
        host = "qloud-ext.{}".format(prj)
        if component:
            host = "{}.{}".format(host, component)
        return {
            "name": full_name,
            # "signal": ...,
            "mgroups": ["QLOUD"],
            "description": "...",
            "tags": {
                # # Hard to override, should come entirely from the signals.
                # "ctype": ["unknown"],
                # "itype": ["qloud"],
                # "prj": [prj],
            },
            "juggler_check": {
                "namespace": "yasm.ambry.simple",
                "host": host,
                "service": juggler_name or name_base,  # `"yasm_" + name`?
                "ttl": 900,
                "refresh_time": 5,
                "notifications": notifications,
                "children": [
                    {"host": "yasm_alert",
                     "service": full_name}],
                "aggregator": "logic_or",
                "aggregator_kwargs": {
                    "unreach_service": [{"check": "yasm_alert:virtual-meta"}],
                    "unreach_mode": "force_ok",
                    "nodata_mode": "force_crit",
                },
                "tags": ["a_itype_qloud", "a_prj_{}".format(prj)],
                "responsible": [],
            },
            "value_modify": {"window": 60, "type": "aver"},
            # Usable as default but should probably be changed.
            "warn": [0.05, 0.2],
            "crit": [None, 0.05],
            # # Hopefully unnecessary:
            # "updated": "2019-02-27T14:45:59.587000",
        }

    # #######
    # Structures with all involved signals
    # #######

    known_kinds = ('resources', 'resourceusage', 'problems', 'misc')

    disk_free_frac_name_tpl = 'diff(1,div(push-disk-used_bytes_{0}_vmmv, push-disk-total_bytes_{0}_vmmv))'
    disk_usage_pct_name_tpl = 'perc(push-disk-used_bytes_{0}_vmmv, push-disk-total_bytes_{0}_vmmv)'

    container_resources = normalize_signals(defaults=dict(kind='resources'), data=dict(
        mem_free_frac=dict(color='#f5e', name='diff(1,div(portoinst-anon_usage_gb_txxx, div(portoinst-anon_limit_tmmv, counter-instance_tmmv)))'),
        # # This one time-aggregates by average rather than max:
        # mem_free_frac=dict(color='#f5e', name='diff(1,div(portoinst-anon_usage_gb_tmmv, portoinst-anon_limit_tmmv))'),
        # # This one includes page (disk) caches:
        # mem_free_frac=dict(color='#f5e', 'diff(1,div(portoinst-memory_usage_gb_txxx,min(portoinst-memory_limit_gb_thhh)))'),

        cpu_free_frac=dict(color='#1a3', name='diff(1,div(portoinst-cpu_usage_cores_txxx,min(portoinst-cpu_limit_cores_thhh)))'),

        # TODO: consistent colors.

        disk_free_frac=dict(name='diff(1,div(unistat-auto_disk_rootfs_usage_bytes_axxx,unistat-auto_disk_rootfs_total_bytes_axxx))'),

        # TODO: i/o:
        # https://yasm.yandex-team.ru/chart/hosts=QLOUD;itype=qloud;graphs=%7Bportoinst-io_read_fs_bytes_tmmv,portoinst-io_write_fs_bytes_tmmv,portoinst-io_limit_bytes_tmmv%7D;prj=statbox.statface-api.prod;ctype=unknown/?chart=4
    ))

    container_resources_extra = normalize_signals(defaults=dict(kind='resources'), data=dict(
        # # Not very useful, jumping up to -30 all the time.
        # net_free_frac=dict(name='diff(1,div(portoinst-net_mb_summ, portoinst-net_guarantee_mb_summ))'),
        # # Slightly more informative, but still can jump up and down without good reason.
        net_free_guarantee_frac=dict(name='max(0, diff(1, div(portoinst-net_mb_summ, portoinst-net_guarantee_mb_summ)))'),
        net_free_limit_frac=dict(name='max(0, diff(1, div(portoinst-net_mb_summ, portoinst-net_limit_mb_summ)))'),
    ))

    container_resourceusage = normalize_signals(defaults=dict(kind='resourceusage'), data=dict(
        # As percent usage:
        cpu_usage_pct=dict(name='perc(portoinst-cpu_usage_cores_tmmv,portoinst-cpu_guarantee_cores_tmmv)'),
        mem_usage_pct=dict(name='perc(portoinst-memory_usage_gb_tmmv,portoinst-memory_guarantee_gb_tmmv)'),
        # net_usage_pct=dict(name='perc(portoinst-net_mb_summ, portoinst-net_guarantee_mb_summ)'),
        disk_usage_pct=dict(name='perc(unistat-auto_disk_rootfs_usage_bytes_axxx,unistat-auto_disk_rootfs_total_bytes_axxx)'),
    ))

    container_problems = normalize_signals(defaults=dict(kind='problems'), data=dict(
        tcp_drops_sum=dict(color='#ee9', name='sum(hsum(unistat-net_tcp_backlog_drop_dhhh), hsum(unistat-net_tcp_listen_drops_dhhh), hsum(unistat-net_tcp_listen_overflows_dhhh))'),
        tcp_retransmits_sum=dict(kind='problems_ext', color='#9e9', name='hsum(unistat-net_tcp_retrans_segs_dhhh)'),
        cpu_throttled_cores=dict(color='#eee', name='portoinst-cpu_throttled_cores_tmmv'),
        cpu_wait_cores=dict(color='#eee', name='portoinst-cpu_wait_cores_tmmv'),
    ))

    container_signals = normalize_signals(defaults=dict(), data=dict(
        {**container_resources, **container_resourceusage, **container_problems},
        # ...
    ))

    # `router_tag = 'itype=qloudrouter;ctype={};{}'.format(routers_str, tag_suffix)`; tag_suffix: 'prj={prj}' | 'prj={prj};tier={components}'
    qloudrouter_signals = normalize_signals(defaults=dict(host='QLOUD', itype='qloudrouter'), data=dict(
        proxy_upstream_errors_sum=dict(kind='problems', itype='qloudrouter', color='#37bff2', name='push-proxy_errors_summ'),
        backend_5xx_responses_sum=dict(kind='problems', itype='qloudrouter', color='#169833', name='push-response_5xx_summ'),
        # # dead signal?..
        # dead_instances_max=dict(kind='problems', itype='qloudrouter', color='#f6ab31', name='max(unistat-qloud-upstream-instance-down_ahhh)'),
        # quantile of dead instances (default on the qloud dashboard)
        # dead_instances_q95=dict(kind='problems', itype='qloudrouter', color='#c95edd', name='quant(unistat-qloud-upstream-instance-down_ahhh,95)'),
    ))

    # `logger_tag = 'itype=qloudlogger;ctype=unknown;{}'.format(tag_suffix)`; tag_suffix: 'prj={prj}' | 'prj={prj};tier={components}'
    qloudlogger_signals = normalize_signals(defaults=dict(host='QLOUD', itype='qloudlogger'), data=dict(
        dropped_log_messages_sum=dict(kind='problems', color='#999', name='push-drop_summ'),

    ))

    # Required values: itype -> tag.
    # `main_tag = 'itype=qloud;ctype=unknown;{}'.format(tag_suffix)`; tag_suffix: 'prj={prj}' | 'prj={prj};tier={components}'
    qloud_extra_signals = normalize_signals(defaults=dict(host='QLOUD', itype='qloud'), data=dict(
        {**qloudrouter_signals, **qloudlogger_signals},

        # ...
    ))
    worker_signals = normalize_signals(defaults=dict(kind='resources'), data=dict(
        workers_free_total_frac=dict(color='#55c', name='diff(1,div(max(unistat-workers_busy_ahhh),min(unistat-workers_total_ahhh)))'),
        workers_free_active_frac=dict(color='#77f', name='diff(1,div(unistat-workers_busy_ammx,sum(unistat-workers_busy_ammx,unistat-workers_idle_ammn)))'),
    ))

    # Have to add some fake host+tag to make the panel work.
    const_signals = normalize_signals(defaults=dict(kind='resources'), data=dict(
        empty=dict(color='#b33', name='const(0)'),
        full=dict(color='#3b3', name='const(1)'),
    ))

    # Required additional parameters: `tag='itype={itype};ctype={cid}'
    mdb_signals = normalize_signals(defaults=dict(itype='mdbdom0', host='CON'), data=dict(
        # Copy workable signals explicitly, with added defaults and fixes:

        # Note: different aggregations available from `container_signals`.
        cpu_free_frac=dict(container_signals['cpu_free_frac'], name='diff(1,div(portoinst-cpu_usage_cores_tmmv,portoinst-cpu_guarantee_cores_tmmv))'),
        mem_free_frac=dict(container_signals['mem_free_frac'], name='diff(1,div(portoinst-anon_usage_gb_tmmv,portoinst-anon_limit_tmmv))'),
        # net_free_frac=dict(container_signals['net_free_frac']),
        # per-subtype: disk_free_frac

        cpu_usage_pct=dict(container_signals['cpu_usage_pct'], name='perc(portoinst-cpu_usage_cores_tmmv,portoinst-cpu_guarantee_cores_tmmv)'),
        mem_usage_pct=dict(container_signals['mem_usage_pct'], name='perc(portoinst-anon_usage_gb_tmmv,portoinst-anon_limit_tmmv)'),
        # net_usage_pct=dict(container_signals['net_usage_pct']),
        # per-subtype: disk_usage_pct

        cpu_throttled_cores=dict(container_signals['cpu_throttled_cores']),
        cpu_wait_cores=dict(container_signals['cpu_wait_cores']),
    ))

    # Required additional parameters: `tag='itype={itype};ctype={cid}'
    postgresql_signals = normalize_signals(defaults=dict(itype='mailpostgresql', host='CON'), data=dict(
        mdb_signals,

        disk_free_frac=dict(container_signals['disk_free_frac'], name=disk_free_frac_name_tpl.format('/var/lib/postgresql')),
        disk_usage_pct=dict(container_signals['disk_usage_pct'], name=disk_usage_pct_name_tpl.format('/var/lib/postgresql')),

        # considering queries below 100ms as non-problematic.
        query_time_beyond_100ms=dict(kind='problems', name='diff(max(100, or(push-pooler-avg_query_time_vmmv, 0)), 100)'),
        # considering transactions below 200ms as non-problematic.
        tx_time_beyond_200ms=dict(kind='problems', name='diff(max(200, or(push-pooler-avg_xact_time_vmmv, 0)), 200)'),

        replication_lag=dict(kind='problems', name='div(push-postgres-replication_lag_vmmv, push-postgres-is_replica_vmmv)'),

        # Warning: this one seems somewhat weird.
        tcp_drops_sum=dict(container_signals['tcp_drops_sum'], name='sum(netstat-drops_summ,netstat-tcpext_listen_drops_summ,netstat-tcpext_backlog_drop_summ)'),
        tcp_retransmits_sum=dict(container_signals['tcp_retransmits_sum'], name='netstat-tcp_retrans_segs_summ'),
    ))

    redis_signals = normalize_signals(defaults=dict(itype='mdbredis'), data=dict(
        mdb_signals,

        disk_free_frac=dict(container_signals['disk_free_frac'], name=disk_free_frac_name_tpl.format('/var/lib/redis')),
        disk_usage_pct=dict(container_signals['disk_usage_pct'], name=disk_usage_pct_name_tpl.format('/var/lib/redis')),
        # push-redis_used_memory_vmmv

        # See also:
        instances=dict(kind='misc', name='counter-instance_tmmv'),
        instances_alive=dict(kind='misc', name='push-redis_is_alive_vmmv'),
        instances_master=dict(kind='misc', name='push-redis_is_master_vmmv'),
        # push-redis_role_vmmv
        connected_client=dict(kind='misc', name='push-redis_connected_clients_vmmv'),
        hit_rate=dict(kind='misc', name='push-redis_hit_rate_vmmv'),
        rps=dict(kind='misc', name='push-redis_instantaneous_ops_per_sec_vmmv'),
        mem_fragmentation=dict(kind='misc', name='push-redis_mem_fragmentation_ratio_vmmv'),

        master_io_lag=dict(kind='problems', name='push-redis_master_last_io_seconds_ago_vmmv'),

        # Warning: this one seems somewhat weird.
        tcp_drops_sum=dict(container_signals['tcp_drops_sum'], name='sum(netstat-drops_summ,netstat-tcpext_listen_drops_summ,netstat-tcpext_backlog_drop_summ)'),
        tcp_retransmits_sum=dict(container_signals['tcp_retransmits_sum'], name='netstat-tcp_retrans_segs_summ'),
    ))

    def get_all_known_signals(self):
        return dict(
            container=self.container_signals,
            container_resources_extra=self.container_resources_extra,
            worker=self.worker_signals,
            const=self.const_signals,
            mdb=self.mdb_signals,
            postgresql=self.postgresql_signals,
            redis=self.redis_signals,
        )

    # #######
    # Signals generators
    # #######

    def _postprocess_signals(self, signals, common=None, defaults=None, kinds=None, **kwargs):
        if kinds is not None:
            signals = [
                item
                for item in signals
                if item['kind'] in kinds]

        if common is not None or defaults is not None:
            common = common or {}
            defaults = defaults or {}
            signals = [
                {
                    **defaults,
                    **item,
                    **common,
                }
                for item in signals]

        signals = [
            drop_keys(item, ['kind'])
            for item in signals]

        return signals

    def process_container_signals(self, signals, **kwargs):
        return self._postprocess_signals(signals, **kwargs)

    def maybe_add_const_signals(self, signals, const='auto', kinds=None):
        if const == 'auto':
            const = kinds and 'resources' in kinds
        if not const or not signals:
            return signals
        const_signals = self.get_all_known_signals()['const'].values()
        const_signals = self._postprocess_signals(const_signals, defaults=signals[0])
        return list(signals) + list(const_signals)

    def make_container_signals_base(self, kinds, common=None, workers=None, const=True, **kwargs):
        all_known_signals = self.get_all_known_signals()

        signals = list(all_known_signals['container'].values())

        if workers:
            signals += list(all_known_signals['worker'].values())

        result = self.process_container_signals(signals, kinds=kinds, common=common, **kwargs)
        result = self.maybe_add_const_signals(result, const=const, kinds=kinds)
        return result

    def process_qloud_container_signals(self, signals, prj=None, component=None, **kwargs):
        if prj is None:
            prj = self.prj
        tag = 'itype=qloud;ctype=unknown;prj={}'.format(prj)
        if component:
            tag = '{};tier={}'.format(tag, component)
        common = dict(host='QLOUD', tag=tag)
        # return self._postprocess_signals(signals, common=common, **kwargs)
        return self.process_container_signals(signals, common=common, **kwargs)

    def make_qloud_container_signals(self, kinds, prj=None, component=None, workers=None, **kwargs):
        if component:
            if workers is None:
                workers = component in self.components_with_workers

        signals = self.make_container_signals_base(kinds=kinds, workers=workers, **kwargs)
        return self.process_qloud_container_signals(signals, prj=prj, component=component)

    def process_qloud_extra_signals(self, signals, prj=None, component=None, routers=None, **kwargs):
        if prj is None:
            prj = self.prj
        if routers is None:
            routers = self.routers
        tag_suffix = 'prj={}'.format(prj)
        if component:
            tag_suffix = '{};tier={}'.format(tag_suffix, component)
        routers_str = ','.join(routers)

        itype_to_tag = dict(
            qloudrouter='itype=qloudrouter;ctype={};{}'.format(routers_str, tag_suffix),
            qloudlogger='itype=qloudlogger;ctype=unknown;{}'.format(tag_suffix),
            qloud='itype=qloud;ctype=unknown;{}'.format(tag_suffix),
        )

        # Pre-copy for inplace editing:
        signals = [signal.copy() for signal in signals]

        # Convert itype -> tag
        for signal in signals:
            itype = signal.pop('itype', None)
            if itype is not None:
                signal['tag'] = itype_to_tag[itype]

        return self._postprocess_signals(signals, **kwargs)

    def make_qloud_extra_signals(self, kinds, prj=None, component=None, **kwargs):
        """ Non-porto qloud signals """
        signals = self.qloud_extra_signals.values()

        return self.process_qloud_extra_signals(signals, prj=prj, component=component, kinds=kinds, **kwargs)

    def make_qloud_signals(self, kinds, prj=None, component=None, **kwargs):
        signals = list(self.make_qloud_container_signals(kinds=kinds, prj=prj, component=component, **kwargs))
        signals += list(self.make_qloud_extra_signals(kinds=kinds, prj=prj, component=component, **kwargs))
        return signals

    def process_qloud_router_signals(self, signals, name, **kwargs):
        tag = 'itype=qloudrouter;ctype={}'.format(name)
        common = dict(host='QLOUD', tag=tag)
        return self._postprocess_signals(signals, common=common, **kwargs)

    def make_qloud_router_signals(self, kinds, name, **kwargs):
        """
        Router (l7) specific signals.

        NOTE: this involves a special itype rather than l7's component.
        """
        signals = self.make_container_signals_base(kinds=kinds, **kwargs)
        return self.process_qloud_router_signals(signals, name=name)

    def process_db_signals(self, signals, cid, default_itype=None, default_host='CON', **kwargs):
        signals = [signal.copy() for signal in signals]
        for signal in signals:
            itype = signal.pop('itype', default_itype)
            if itype is None:
                raise ValueError("Neither signal itype not default_itype were specified")
            if default_host is not None:
                signal.setdefault('host', default_host)
            signal['tag'] = 'itype={itype};ctype={cid}'.format(itype=itype, cid=cid)
        return self._postprocess_signals(signals, **kwargs)

    def make_db_signals(self, kinds, cid, db_type=None, const='auto', **kwargs):
        """
        mdb database specific signals.
        """
        # # Example itype=mdbdom0:
        # https://yasm.yandex-team.ru/chart/hosts=CON;itype=mdbdom0;ctype=594c8ca1-0d86-4d87-b5a0-449cbdbdb75e;graphs=%7Bperc(portoinst-cpu_usage_cores_tmmv,portoinst-cpu_guarantee_cores_tmmv),perc(portoinst-memory_usage_gb_tmmv,portoinst-memory_guarantee_gb_tmmv),perc(portoinst-net_mb_summ,portoinst-net_guarantee_mb_summ),portoinst-cpu_wait_cores_tmmv,*%7D/
        # # Example itype=mailpostgresql
        # https://yasm.yandex-team.ru/chart/hosts=CON;itype=mailpostgresql;ctype=594c8ca1-0d86-4d87-b5a0-449cbdbdb75e;graphs=%7Bor(push-pooler-avg_query_time_vmmv,0),or(push-pooler-avg_xact_time_vmmv,0),div(push-postgres-replication_lag_vmmv,push-postgres-is_replica_vmmv),*%7D/
        if db_type in ('pg', 'postgres', 'postgresql'):
            signals = self.postgresql_signals.values()
        elif db_type in ('redis',):
            signals = self.redis_signals.values()
        else:
            signals = self.mdb_signals.values()

        result = self.process_db_signals(signals, kinds=kinds, cid=cid, **kwargs)
        result = self.maybe_add_const_signals(result, const=const, kinds=kinds)
        return result

    # #######
    # self-utils
    # #######

    def name_to_guid(self, name, **kwargs):
        """
        Fake GUID from a name.

        For stable URLs... probably.
        """
        key = "{}{}__{}".format(self.panel_user, self.panel_key, name)
        return string_to_guid(key)

    # TODO: memoizex
    @property
    def databases_all(self):
        databases = self.databases
        result_items = [
            dict(
                cfg,
                cid=cid,
                title=cfg.get('title') or cid,
            ) for cid, cfg in databases.items()]
        result = {
            item['title']: item
            for item in result_items}
        result.update({
            item['cid']: item
            for item in result_items})
        return result

    @property
    def prj_underscored(self):
        return self.prj.replace(".", "_").replace("-", "_")

    @property
    def alert_prefix_base(self):
        return self.prj_underscored

    @property
    def alert_prefix(self):
        return self.alert_prefix_base + "."

    @staticmethod
    def get_token():
        with open(os.path.expanduser("~/.release.hjson")) as fobj:
            data = hjson.load(fobj)
        return data["oauth_token"]

    @classmethod
    def make_reqr(cls):
        reqr = APIRequester(base_url=cls.base_url, session=None)
        reqr.headers["Authorization"] = "OAuth {}".format(cls.get_token())
        return reqr

    def __init__(self):
        self.reqr = self.make_reqr()
        self.logger = logging.getLogger("yasm_configurer")

    def req(self, uri, **kwargs):
        return self.reqr(uri, **kwargs)

    def make_alert(self, signal, get_alert_config=None, prj=None, chart=None, **kwargs):
        if get_alert_config is None:
            get_alert_config = self.get_alert_config

        tag = signal["tag"]
        tags = self.parse_tag(tag, lists=True)

        if prj is None:
            prj = (tags.get('prj') or tags.get('ctype') or [''])[0]

        component = (tags.get("tier") or [""])[0]
        component = re.sub(r"-[0-9*]$", "", component)  # "sf4-*" -> "sf4"
        component = component or None

        title = signal.get("_identifier") or signal["title"]
        cfg = get_alert_config(title=title, signal=signal, chart=chart, prj=prj, **kwargs)
        if cfg is None:
            return None

        name = title
        if chart is not None:
            name = "{}_{}".format(chart["title"].replace("-", "_"), name)
        alert = self.common_alert_config(
            name=name,
            prj=prj,
            component=component,
        )
        alert.update({
            "signal": signal["name"],
            "tags": {
                **alert["tags"],
                **tags}})
        alert.update(cfg)

        return alert

    def alerts_configs(self, panel=None):
        result = []
        known = {}

        # Use the same lines as the ones on the panel.
        # TODO?: make both from a single common pre-config.
        if panel is None:
            panel = self.make_panel_config()

        for chart in panel["values"]["charts"]:
            # chart_title_base = chart["title"]  # e.g. "sf4-*"
            # chart_title = chart_title_base.replace("-", "_").replace("*", "all")
            for signal in chart["signals"]:
                alert = self.make_alert(signal, chart=chart)
                if not alert:
                    continue

                # Deduplication:
                known_alert = known.get(alert["name"])
                if known_alert is not None:
                    if known_alert["signal"] != alert["signal"] or known_alert["tags"] != alert["tags"]:
                        raise Exception("Non-unique alert name", alert["name"], alert, known_alert)
                    continue  # skip duplicates
                known[alert["name"]] = alert

                # ...
                result.append(alert)

        return result

    # def _alert_upsert_hax(self, config):
    #     name = config["name"]
    #     self.req(
    #         "/srvambry/alerts/create",
    #         data=config,
    #         require=False)
    #     return self.req(
    #         "/srvambry/alerts/update",
    #         params=dict(name=name),
    #         data=config)

    def alerts_upsert(self, alerts, prefix=None, bind=True):
        if not alerts:
            return None
        if prefix is None:
            prefix = self.alert_prefix_base
        resp = self.req(
            "/srvambry/alerts/replace",
            data=dict(
                prefix=prefix,
                alerts=alerts))
        if bind:
            self.alerts_bind(alerts)
        return resp

    def alerts_bind(self, alerts):
        for alert in alerts:
            self.req(
                "/srvambry/alerts/bind_check",
                params=dict(
                    alert_name=alert["name"],
                    check_host=alert["juggler_check"]["host"],
                    check_service=alert["juggler_check"]["service"]),
                method="POST",
                require=False)

    def get_current_alerts(self):
        resp = self.req(
            "/srvambry/alerts/list",
            params=dict(name_prefix=self.alert_prefix_base))
        return resp.json()

    @staticmethod
    def parse_tag(tag, lists=True):
        result = {}
        for piece in tag.split(";"):
            if not piece:
                continue
            piece = piece.split("=", 1)
            if len(piece) != 2:
                continue
            key, value = piece
            if lists:
                value = value.split(",")
            result[key] = value
        return result

    def signals_for_all_components(self, func, components=None):
        """
        Helper to make a sorted bunch of graph signals separated by component.
        """
        if components is None:
            components = self.components
        result = []
        for component in components:
            for signal in func(component="{}-*".format(component)):
                signal["_title_base"] = signal["title"]
                signal["title"] = "{}__{}".format(signal["title"], component)
                result.append(signal)
        result.sort(key=lambda item: item.get("title"))
        return result

    def signals_for_everything(
            self, signals, components_split=True, prj=None, kinds=None,
            components=None, mdb_default_itype=None, **kwargs):
        result = []

        common_kwargs = dict(
            kwargs,
            signals=signals,
            prj=prj,
            kinds=kinds,
        )
        if components_split:
            signals_one = self.signals_for_all_components(
                lambda component, common_kwargs=common_kwargs, **kwargs: self.process_qloud_container_signals(
                    component=component, const=False, **common_kwargs, **kwargs),
                components=components)
        else:
            signals_one = self.process_qloud_container_signals(
                const=False, **common_kwargs)
        result.extend(signals_one)

        for router in self.routers:
            signals_one = self.process_qloud_router_signals(name=router, **common_kwargs)
            signals_one = self._map_title(signals_one, lambda title: '{}__{}'.format(title, router))
            result.extend(signals_one)

        for db_cfg in self.databases_all.values():
            signals_one = self.process_db_signals(
                default_itype=mdb_default_itype,
                **common_kwargs,
                **db_cfg)
            signals_one = self._map_title(signals_one, lambda title: '{}__{}'.format(title, db_cfg['title']))
            result.extend(signals_one)

        return result

    def _map_title(self, signals, mapper, bk_key='_title_prev'):
        signals = [signal.copy() for signal in signals]
        for signal in signals:
            if bk_key is not None:
                signal[bk_key] = signal['title']
            signal['title'] = mapper(signal['title'])
        return signals

    def postprocess_panel_graph_config(self, config, name=None, validate=True, **kwargs):
        config.setdefault("type", "graphic")
        if isinstance(name, str):
            config.setdefault("title", name)
        config.setdefault("consts", [])
        config.setdefault("width", self.panel_default_graph_width)
        config.setdefault("height", self.panel_default_graph_height)
        if not config.get("id") and isinstance(name, str):
            config["id"] = self.name_to_guid(name, _config=config, **kwargs)
        if validate:
            # If this does not pass, the panel would undebuggably fail to load:
            for signal in config['signals']:
                assert signal.get('host'), signal
                assert signal.get('tag'), signal
                assert signal.get('name'), signal
                assert signal.get('title'), signal
        return config

    def resolve_layout(self, yield_flat=True):
        """
        Treat each row as an upper-line grouper.
        Bottom line, at the same time, is non-binding.
        Example:

           (("a", "b", "c"),
            ("d", "e"))

        with tall "c" it will result in:

            a b c
            d e c
                c

        but with wide "d" at the same time:

            a b c
                c
                c
            d d e
        """
        col_to_row_pos = defaultdict(lambda: 1)  # col -> row
        for row_items in self.panel_layout:
            current_col = 1
            row_results = []

            for source_item in row_items:
                config = self.make_panel_graph_config(source_item)
                config = self.postprocess_panel_graph_config(config, name=source_item)
                config["col"] = current_col  # cumulative(item["width"] for item in row_results)
                current_col += config["width"]
                row_results.append(config)

            if not row_results:
                continue
            row_columns = range(1, current_col)
            row_pos = max(col_to_row_pos[col] for col in row_columns)
            for res_item in row_results:
                res_item["row"] = row_pos
                end_row = row_pos + res_item["height"]
                for column in range(res_item["col"], res_item["col"] + res_item["width"]):
                    col_to_row_pos[column] = end_row

            if yield_flat:
                for res_item in row_results:
                    yield res_item
            else:
                yield row_results

    def make_panel_config(self):
        charts = self.resolve_layout(yield_flat=True)
        charts = list(charts)
        result = {
            "keys": {"user": self.panel_user, "key": self.panel_key},
            "values": {
                "user": self.panel_user,
                "title": self.panel_title,
                "type": "panel",
                "editors": list(self.panel_editors),
                "charts": charts,
            },
        }
        if self.panel_abc:
            result['values']['abc'] = self.panel_abc
        return result

    def panel_upsert(self, config):
        return self.req(
            "/srvambry/upsert",
            data=config)

    # #######
    # Main entry point
    # #######

    def run(self, put_panel=True, put_alerts=True, bind_alerts=True, print_alerts=True):
        logging.basicConfig(level=1)
        panel = self.make_panel_config()
        debug_print("panel", panel)
        if put_panel:
            self.panel_upsert(panel)
        if put_alerts:
            alerts = self.alerts_configs(panel=panel)
            debug_print("alerts", alerts)
            self.alerts_upsert(alerts, bind=bind_alerts)
        if print_alerts:
            current_alerts = self.get_current_alerts()
            debug_print("current_alerts", current_alerts)


def debug_print(name, value):
    print("\n ======= {} =======".format(name))
    print(value)


def to_bytes(value, default=None, encoding='utf-8', errors='strict'):
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode(encoding, errors)
    if default is not None:
        return default(value)
    return value


def pair_window(iterable):
    iterable = iter(iterable)
    try:
        prev_value = next(iterable)
    except StopIteration:
        return
    for item in iterable:
        yield prev_value, item
        prev_value = item


def cumsum(iterable):
    iterable = iter(iterable)
    try:
        value = next(iterable)
    except StopIteration:
        return
    yield value
    for item in iterable:
        value += item
        yield value


def string_to_guid(string):
    import hashlib
    string = to_bytes(string)
    hexdigest = hashlib.sha256(string).hexdigest()
    uuid_lengths = (8, 4, 4, 4, 12)  # "ab6741e0-715a-c4d7-df9b-7d9a896a9ca6"
    return "-".join(
        hexdigest[pos0:pos1]
        for pos0, pos1 in pair_window(cumsum((0,) + uuid_lengths)))


# Usage example:
# https://github.yandex-team.ru/statbox/statface-api-v4/blob/master/_aux/yasm_config.py
