#!/usr/bin/env python3
"""
Requires the juggler scope on the token in `~/.release.hjson`.

One way to get it:
https://oauth.yandex-team.ru/authorize?response_type=token&client_id=2d1760be683244f4a91559d09cec6140

Overall documentation about `~/.release.hjson`:
https://github.yandex-team.ru/tools/releaser
"""

from __future__ import annotations

from multiprocessing.pool import ThreadPool
import sys

from statcommons.tools.yasm_config import YASMConfigurer

# # https://qloud-ext.yandex-team.ru/projects/datalens/balancers
# dl-ch-ybc
# dl-ext-control-prod
# dl-ext-control-test
# dl-ext-prod-ch
# dl-int-back-prod
# dl-int-back-test
# dl-int-upload-prod
# dl-int-upload-test
# ext-prod


ENVIRONMENTS = dict(
    int_testing=dict(
        prjs=dict(
            back="datalens.bi.int-testing",  # https://qloud-ext.yandex-team.ru/projects/datalens/bi/int-testing  # dataset-api 0.413.0  materializer-api 0.195.0  materializer-worker 0.195.0  mssql latest  oracle 12.2.0.1  # back.datalens-beta.yandex-team.ru
            converter="datalens.bi-converter.int-testing",  # https://qloud-ext.yandex-team.ru/projects/datalens/bi-converter/int-testing  # api 0.59.0  # upload.datalens-beta.yandex-team.ru
            dls="stat.dls.beta",  # https://qloud-ext.yandex-team.ru/projects/stat/dls/beta  # backend 1.87  backend-tasks 1.87  # dls-int-beta.yandex.net
            # maybe_front='stat.statface-front.testing',  # https://qloud-ext.yandex-team.ru/projects/stat/statface-front/testing  # statface 2.303.0  dataset 0.30.32-260  # stat-beta.yandex-team.ru
            # dead_front_02='stat.datalens.testing',  # https://qloud-ext.yandex-team.ru/projects/stat/datalens/testing  # dataset 0.30.3-237  gateway 1.0.3-113  wizard 0.3.3-40  # datalens-beta.yandex-team.ru
        ),
        # routers=dict(
        #     back='dl-int-back-test',
        #     converter='statface-upload-beta',
        #     converter_self='dl-int-upload-test',
        #     dls='b-dls-int-beta',
        #     maybe_front='statface-beta',
        #     # dead_front_02='datalens-beta',
        # ),
        postgres=dict(
            us="f236a33e-7a8e-4410-871a-bfc920843089",  # https://yc.yandex-team.ru/folders/foo9uad1ur2be9ici0gb/managed-postgresql/cluster/f236a33e-7a8e-4410-871a-bfc920843089
            dls="6db4cc46-3f10-4532-b097-21bc56777867",  # https://yc.yandex-team.ru/folders/foo867ak5sdnsd3f9a56/managed-postgresql/cluster/6db4cc46-3f10-4532-b097-21bc56777867
        ),
        clickhouse=dict(
            chmain="mdbrk9btv5g0rvrhlndk",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/mdbrk9btv5g0rvrhlndk
        ),
        redis=dict(
            caches="mdb1k25sl14082b09eum",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-redis/cluster/mdb1k25sl14082b09eum
            redismain="mdblh8n8eh01ru2dgbms",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-redis/cluster/mdblh8n8eh01ru2dgbms
        ),
        panel_key="._bi_int_testing",
    ),
    int_production=dict(
        prjs=dict(
            back="datalens.bi.int-production",  # https://qloud-ext.yandex-team.ru/projects/datalens/bi/int-production  # dataset-api 0.407.0  materializer-api 0.183.0  materializer-worker 0.184.0
            converter="datalens.bi-converter.int-production",  # https://qloud-ext.yandex-team.ru/projects/datalens/bi-converter/int-production
            clickhouse_balancer="datalens.clickhouse-balancer.ya-bi-cluster",  # https://qloud-ext.yandex-team.ru/projects/datalens/clickhouse-balancer/ya-bi-cluster
            dls="stat.dls.prod",  # https://qloud-ext.yandex-team.ru/projects/stat/dls/prod
            united_storage="stat.united-storage.production",  # https://qloud-ext.yandex-team.ru/projects/stat/united-storage/production  # united-storage 1.0.0-163  # united-storage.yandex-team.ru  # 20rps
            front="stat.statface-front.production",  # https://qloud-ext.yandex-team.ru/projects/stat/statface-front/production  # statface 2.303.0  dataset 0.30.32-260  # stat.yandex-team.ru  # ...
            # dead_front_02='stat.datalens.production',  # https://qloud-ext.yandex-team.ru/projects/stat/datalens/production  # dataset 0.30.0-90-freeze-fix-1  wizard 0.1.1-52  # datalens.yandex-team.ru
            # something_charts='stat.charts.production',  # https://qloud-ext.yandex-team.ru/projects/stat/charts/production  # ui 2.235.0  api 5.0.0-114  scr 1.78.0  sync 0.1.0-4  gateway 1.1.7-121  wizard 0.3.27-60
        ),
        # routers=dict(
        #     back='dl-int-back-prod',
        #     converter='statface-upload-prod',
        #     clickhouse_balancer='dl-ch-ybc',
        #     dls='b-dls-int-prod',
        #     united_storage='charts',
        #     front='statface',
        #     # dead_front_02='datalens',
        #     # something_charts='charts',
        # ),
        postgres=dict(
            us="992fd8ce-b5dc-4637-b61f-d3c2d5f5f949",  # https://yc.yandex-team.ru/folders/foo9uad1ur2be9ici0gb/managed-postgresql/cluster/992fd8ce-b5dc-4637-b61f-d3c2d5f5f949
            dls="4fbff674-1ac6-4645-8baf-e352679f430c",  # https://yc.yandex-team.ru/folders/foo867ak5sdnsd3f9a56/managed-postgresql/cluster/4fbff674-1ac6-4645-8baf-e352679f430c
        ),
        clickhouse=dict(
            ya_bi_cluster="21376c94-8b92-4df5-8eac-a91fca05eaba",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/21376c94-8b92-4df5-8eac-a91fca05eaba?section=monitoring
            # ya_bi_analytics=...,  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/7ce87c2f-0701-4a5c-989a-d4faf6220c77?section=monitoring
        ),
        redis=dict(
            caches="mdbo68it8jpiennovbb0",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-redis/cluster/mdbo68it8jpiennovbb0
            redismain="mdbijrui7vdtmqu78k86",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-redis/cluster/mdbijrui7vdtmqu78k86
        ),
        panel_key="._bi_int_production",
    ),
    ext_testing=dict(
        prjs=dict(
            back="datalens.bi.testing",  # https://qloud-ext.yandex-team.ru/projects/datalens/bi/testing  # dataset-api 0.412.0  setup-folder 0.19.0  materializer-api 0.197.0  materializer-worker 0.197.0
            converter="datalens.bi-converter.testing",  # https://qloud-ext.yandex-team.ru/projects/datalens/bi-converter/testing  # api 0.61.0
            # charts='datalens.charts.testing',  # https://qloud-ext.yandex-team.ru/projects/datalens/charts/testing  # api 5.0.0-116
            control="datalens.control.testing",  # https://qloud-ext.yandex-team.ru/projects/datalens/control/testing  # api 1.1.0-6
            dls="datalens.dls.testing",  # https://qloud-ext.yandex-team.ru/projects/datalens/dls/testing  # backend 1.87  backend-tasks 1.87
            united_storage="datalens.united-storage.testing",  # https://qloud-ext.yandex-team.ru/projects/datalens/united-storage/testing  # united-storage 1.0.0-163
            front="datalens.front.testing",  # https://qloud-ext.yandex-team.ru/projects/datalens/front/testing  # dataset 0.30.33-262  dash 1.0.30-84  gateway 1.1.7-121  wizard 0.3.24-59
        ),
        # routers=dict(
        #     back='dl-ext-back-test',
        #     converter='dl-ext-upload-test',
        #     charts='dl-ext-infra-test',
        #     dls='dl-ext-infra-test',
        #     united_storage='dl-ext-infra-test',
        #     control='dl-ext-control-test',
        #     front='dl-ext-front-test',
        # ),
        postgres=dict(
            us="49530da1-dd15-46ba-94eb-b2ba67487cc8",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-postgresql/cluster/49530da1-dd15-46ba-94eb-b2ba67487cc8
            dls="0fc7e853-8c48-4898-bb75-a971ccd92a11",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-postgresql/cluster/0fc7e853-8c48-4898-bb75-a971ccd92a11
            # an all-environments mess:
            bi_pg_cluster="1bab1c2d-7a0e-4c31-b068-bdbd1812da5f",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-postgresql/cluster/1bab1c2d-7a0e-4c31-b068-bdbd1812da5f
            dl_prod_celery_back="71fbabab-7284-48d9-82ba-723db6fd2038",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-postgresql/cluster/71fbabab-7284-48d9-82ba-723db6fd2038
            mat_status="mdb94vte9sjonpl5f3tj",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-postgresql/cluster/mdb94vte9sjonpl5f3tj
            # dl_ext_prod_setup_folder='mdb1v0r9u64rpi4iersa',  #  https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-postgresql/cluster/mdb1v0r9u64rpi4iersa
        ),
        clickhouse=dict(
            chmain="mdbp15neri2887qmke3q",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/mdbp15neri2887qmke3q
        ),
        redis=dict(
            caches="mdbb2913iig62n1dlmgv",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-redis/cluster/mdbb2913iig62n1dlmgv
            redismain="mdb2ptmk7r999bqokqre",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-redis/cluster/mdb2ptmk7r999bqokqre
        ),
        panel_key="._bi_ext_testing",
    ),
    ext_production=dict(
        prjs=dict(
            back="datalens.bi.production",  # https://qloud-ext.yandex-team.ru/projects/datalens/bi/production
            converter="datalens.bi-converter.production",  # https://qloud-ext.yandex-team.ru/projects/datalens/bi-converter/production
            # charts='datalens.charts.production',  # https://qloud-ext.yandex-team.ru/projects/datalens/charts/production
            clickhouse_balancer="datalens.clickhouse-balancer.dl-ext-prod-ch-1",  # https://qloud-ext.yandex-team.ru/projects/datalens/clickhouse-balancer/dl-ext-prod-ch-1
            control="datalens.control.production",  # https://qloud-ext.yandex-team.ru/projects/datalens/control/production
            dls="datalens.dls.production",  # https://qloud-ext.yandex-team.ru/projects/datalens/dls/production
            united_storage="datalens.united-storage.production",  # https://qloud-ext.yandex-team.ru/projects/datalens/united-storage/production
            front="datalens.front.production",  # https://qloud-ext.yandex-team.ru/projects/datalens/front/production
        ),
        # routers=dict(
        #     back='dl-ext-back-prod',
        #     converter='dl-ext-upload-prod',
        #     charts='dl-ext-infra-prod',
        #     dls='dl-ext-infra-prod',
        #     united_storage='dl-ext-infra-prod',
        #     clickhouse_balancer='dl-ext-prod-ch',
        #     control='dl-ext-control-prod',
        #     front='dl-ext-front-prod',
        # ),
        postgres=dict(
            us="e33cfe18-192a-4ea4-a3a1-0b5b4fa57dbe",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-postgresql/cluster/e33cfe18-192a-4ea4-a3a1-0b5b4fa57dbe
            dls="dc4dba83-7de8-41d4-849e-a09aaf08a829",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-postgresql/cluster/dc4dba83-7de8-41d4-849e-a09aaf08a829
            # an all-environments mess:
            bi_pg_cluster="1bab1c2d-7a0e-4c31-b068-bdbd1812da5f",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-postgresql/cluster/1bab1c2d-7a0e-4c31-b068-bdbd1812da5f
            dl_prod_celery_back="71fbabab-7284-48d9-82ba-723db6fd2038",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-postgresql/cluster/71fbabab-7284-48d9-82ba-723db6fd2038
            mat_status="mdb94vte9sjonpl5f3tj",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-postgresql/cluster/mdb94vte9sjonpl5f3tj
            # dl_ext_prod_setup_folder='mdb1v0r9u64rpi4iersa',  #  https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-postgresql/cluster/mdb1v0r9u64rpi4iersa
        ),
        clickhouse=dict(
            # CH
            datalens_bi_production_ext_1="mdbeihd7np2vfptuvail",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/mdbeihd7np2vfptuvail
            datalens_bi_production_ext_2="mdb1ekvqhsb3c4skl4lm",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/mdb1ekvqhsb3c4skl4lm
            datalens_bi_production_ext_3="mdbaui51mt2dpcouerok",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/mdbaui51mt2dpcouerok
        ),
        redis=dict(
            caches="mdb78lhldpc86244jal1",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-redis/cluster/mdb78lhldpc86244jal1
            redismain="mdb8ns50khdqekga1np7",  # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-redis/cluster/mdb8ns50khdqekga1np7
        ),
        panel_key="._bi_ext_production",
    ),
)


class YASM_BI(YASMConfigurer):
    panel_abc = "yandexbi"

    def __init__(self, environment="ext_testing"):
        self.environment = environment
        super().__init__()

    @property
    def envcfg(self):
        return ENVIRONMENTS[self.environment]

    @property
    def databases(self):
        envcfg = self.envcfg
        result = {}
        for key in ("postgres", "clickhouse", "redis"):
            result.update({cid: dict(title=name, cid=cid, db_type=key) for name, cid in envcfg.get(key, {}).items()})
        return result

    @property
    def routers(self):
        return self.envcfg["routers_flat"]

    @property
    def prj(self):
        raise Exception("No fallback here")

    @property
    def components(self):
        raise Exception("No fallback here")

    @property
    def panel_key(self):
        return self.envcfg["panel_key"]

    panel_user = "hhell"
    panel_editors = ()

    @property
    def panel_title(self):
        return "bi {}".format(self.environment)

    components_with_workers = (
        "dataset-api-*",
        "dataset-data-api-*",
    )

    @property
    def panel_layout(self):
        envcfg = self.envcfg

        envcfg = self.resolve_components(envcfg)

        # Essentially make the entire configuration in here

        _resources_common = dict(minValue=-0.1, maxValue=1.1)
        _problems_common = dict(
            # # Best-case: hide the zero-value lines completely.
            # # This, however, merely makes the entire graph background red.
            # minValue=0.0001,
        )
        resources_all = dict(_resources_common, title="resources_all", signals=[])
        problems_all = dict(_problems_common, title="problems_all", signals=[])
        problems = dict(_problems_common, title="problems", signals=[])
        router_resources_all = dict(_resources_common, title="router_resources_all", signals=[])
        router_problems_all = dict(_problems_common, title="router_problems_all", signals=[])
        db_resources_all = dict(_resources_common, title="db_resources_all", signals=[])
        db_problems_all = dict(_problems_common, title="db_problems_all", signals=[])
        net_all = dict(_resources_common, title="network_resources_all", signals=[])
        misc_all = dict(title="misc_all", signals=[])

        net_signals_base = dict(
            signals=[value for key, value in self.container_resources_extra.items() if key.startswith("net_")],
            kinds=("resources",),
        )
        http_5xx_all = dict(_problems_common, title="http_5xx_all", signals=[])

        for name, prj in envcfg["prjs"].items():
            components = envcfg["prj_components"][name]
            for kind, chart in (("resources", resources_all), ("problems", problems_all), ("misc", misc_all)):
                signals = self.signals_for_all_components(
                    lambda component, _kind=kind, _prj=prj, **kwargs: self.make_qloud_signals(
                        kinds=(_kind,), prj=_prj, component=component, const=False, **kwargs
                    ),
                    components=components,
                )
                signals = self._add_title(signals, name)
                chart["signals"].extend(signals)

            signals = self.make_qloud_signals(kinds=("problems",), prj=prj)
            signals = self._add_title(signals, name)
            problems["signals"].extend(signals)

            signals = self.signals_for_all_components(
                lambda component, _prj=prj, **kwargs: self.process_qloud_container_signals(
                    prj=_prj, component=component, const=False, **net_signals_base, **kwargs
                ),
                components=components,
            )
            signals = self._add_title(signals, name)
            net_all["signals"].extend(signals)

        for router in envcfg["routers_flat"]:
            for kind, chart in (
                ("resources", router_resources_all),
                ("problems", router_problems_all),
                ("misc", misc_all),
            ):
                signals = self.make_qloud_router_signals(kinds=(kind,), name=router, const=False)
                signals = self._add_title(signals, router)
                chart["signals"].extend(signals)

            signals = self.process_qloud_router_signals(name=router, **net_signals_base)
            signals = self._add_title(signals, router)
            net_all["signals"].extend(signals)

        # for name, cid in envcfg['dbs'].items():
        for db_cfg in self.databases.values():
            for kind, chart in (("resources", db_resources_all), ("problems", db_problems_all), ("misc", misc_all)):
                signals = self.make_db_signals(kinds=(kind,), const=False, **db_cfg)
                signals = self._add_title(signals, db_cfg["title"])
                chart["signals"].extend(signals)

            signals = self.process_db_signals(default_itype="mdbdom0", **net_signals_base, **db_cfg)
            signals = self._add_title(signals, db_cfg["title"])
            net_all["signals"].extend(signals)

        http_5xx_all["signals"] = [item for item in problems_all["signals"] if "5xx_responses_sum" in item["title"]]
        return (
            (resources_all, problems_all),
            (problems, net_all),
            (
                router_resources_all,
                router_problems_all,
            ),
            (
                db_resources_all,
                db_problems_all,
            ),
            (http_5xx_all, misc_all),
        )

    def _add_title(self, signals, name):
        # return self._map_title(signals, lambda val: '{}__{}'.format(name, val), bk_key='_title_mid')
        for signal in signals:
            signal["_title_mid"] = signal["title"]
            signal["title"] = "{}__{}".format(name, signal["title"])
        return signals

    def resolve_components(self, envcfg):
        prjs = envcfg["prjs"]
        envcfg["prj_configs"] = {}
        envcfg["prj_components"] = {}
        envcfg["prj_routers"] = {}

        def proc_one(prj):
            name, addr = prj
            resp = self.req("https://qloud-ext.yandex-team.ru/api/v1/environment/stable/{}".format(addr))
            cfg = resp.json()
            envcfg["prj_configs"][name] = cfg
            envcfg["prj_routers"][name] = [item["name"] for item in cfg["routers"]]
            envcfg["prj_components"][name] = list(cfg["components"].keys())

        pool = ThreadPool(4)
        for _ in pool.imap_unordered(proc_one, prjs.items()):
            pass

        envcfg["routers_flat"] = sorted(set(name for routers in envcfg["prj_routers"].values() for name in routers))
        return envcfg

    @property
    def alert_prefix_base(self):
        return "datalens__{}__".format(self.environment)

    def alert_notifications(self, **kwargs):
        if self.environment.endswith("_testing"):
            return []
        return [
            {
                "template_name": "on_status_change",
                "template_kwargs": {
                    "method": ["email"],
                    "login": [
                        "datalens-mon"
                    ],  # XXX: not exactly a login. https://ml.yandex-team.ru/lists/datalens-mon/
                    "status": ["CRIT"],
                },
            },
            {
                "template_name": "on_status_change",
                "template_kwargs": {
                    "method": ["telegram"],
                    "login": ["DataLensDuty"],  # DataLensDuty Telegram Chat https://nda.ya.ru/t/vRI2z2wc6W2tPD
                    "status": ["CRIT", "WARN", "OK"],
                },
            },
        ]

    _minute_avg = {"window": 60, "type": "aver"}
    _5minute_avg = {"window": 60 * 5, "type": "aver"}

    line_title_to_alert_config = {
        "cpu_free_frac": {
            # # monitorado defaults:
            # "crit": [None, 0.15], "warn": [0.15, 0.25],
            "crit": [None, 0.05],
            "warn": [0.05, 0.15],
            "value_modify": _5minute_avg,
        },
        "mem_free_frac": {
            # # monitorado defaults:
            # "crit": [None, 0.15], "warn": [0.15, 0.25],
            "crit": [None, 0.02],
            "warn": [0.02, 0.2],  # "ok": [0.2, None],
            "value_modify": _minute_avg,
        },
        # "cpu_free_frac": ...
        "workers_free_total_frac": {
            "crit": [None, 0.15],
            "warn": [0.15, 0.3],  # "ok": [0.3, None],
            "value_modify": _minute_avg,
        },
        # "workers_free_active_frac": ...
        "proxy_upstream_errors_sum": {
            "warn": [2, 18],
            "crit": [18, None],  # "ok": [None, 2],
            "value_modify": _minute_avg,
        },
        "backend_5xx_responses_sum": {
            # "ok": [None, 2],
            "warn": [1, 3],
            "crit": [3, None],
            "value_modify": _minute_avg,
        },
        # TODO: "http_5xx_frac": {"warn": [0.01, 0.02], "crit": [0.02, None]},
        # TODO: "http_4xx_frac": {"warn": [0.2, 0.3], "crit": [0.3, None]},
        # TODO: "http_499_frac": {"warn": [0.005, 0.2], "crit": [0.2, None]},
        # TODO: "timing_q95": {"warn": [0.5, 1], "crit": [1, None]},
        # TODO: "timing_q99": {"warn": [0.5, 1], "crit": [1, None]},
        # TODO: "net_free_frac": {"crit": [None, 0.15], "warn": [0.15, 0.25]},
        # TODO: "disk_free_frac": {"crit": [None, 0.15], "warn": [0.15, 0.3]},
        # TODO: database "cpu_wait_cores": {"warn": [2, 3], "crit": [3, None]},
        # TODO: database "replication_lag_sec_avg": {"warn": [2, 5], "crit": [5, None]},
        # TODO: database "statement_sec_avg": {"warn": [0.1, 0.2], "crit": [0.2, None]},
        # TODO: database "tx_sec_avg": {"warn": [0.2, 0.3], "crit": [0.3, None]},
        # TODO: ensure database's "cpu_free_frac", "net_free_frac", "disk_free_frac".
        # # "dead_instances__max": ...
        "dead_instances_q95": {
            # "ok": [None, 1],
            "warn": [1, 2],
            "crit": [2, None],
            # "value_modify": _minute_avg,
        },
        "dropped_log_messages_sum": {
            # "ok": [None, 1],
            # "warn": [1, 100],
            "crit": [10000, None],
            "value_modify": _minute_avg,
        },
    }

    def get_alert_config(self, title, signal=None, **kwargs):
        signal = signal or {}
        # TODO: add context values (qloud component and such) to the signals.
        if title.startswith("clickhouse_balancer__cpu_") or title.startswith("clickhouse_balancer__mem_"):
            # Special-ish case: a component that is actually just a
            # plalceholder to configure the L7's nginx.
            # Obviously, has no resources to speak of.
            # TODO: remove the chart line itself too.
            return None
        if title.startswith("united_storage__"):
            # Not relevant to the notifications.
            return None
        return self.line_title_to_alert_config.get(title) or self.line_title_to_alert_config.get(
            signal.get("_title_base")
        )

    def common_alert_config(self, *args, **kwargs):
        result = super().common_alert_config(*args, **kwargs)
        result["juggler_check"]["host"] = "datalens_{}".format(self.environment)
        return result


ENVS = ("ext_testing", "int_testing", "ext_production", "int_production")
# ENVS = ('ext_production',)
# ENVS = ('int_production',)
ENVS = ("int_testing",)


def main():
    alerts = "--no-alerts" not in sys.argv
    for environment in ENVS:
        YASM_BI(environment).run(put_alerts=alerts, print_alerts=alerts)


if __name__ == "__main__":
    main()
