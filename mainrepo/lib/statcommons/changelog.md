0.28.0
------

* [dmifedorov](http://staff/dmifedorov)

 * BI-1717: metrics for dataset-api  [ https://a.yandex-team.ru/arc/commit/7231403 ]

* [hhell](http://staff/hhell)

 * minor stylistic change for an easier further hack  [ https://a.yandex-team.ru/arc/commit/7223171 ]
 * statcommons: fix import tests with no celery       [ https://a.yandex-team.ru/arc/commit/7222856 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-03 15:20:45+03:00

0.27.0
------

* [kchupin](http://staff/kchupin)

 * [BI-1121] Common requirements in remote debug docker-container  [ https://a.yandex-team.ru/arc/commit/7153842 ]

* [hhell](http://staff/hhell)

 * better buildability           [ https://a.yandex-team.ru/arc/commit/7017360 ]
 * recurse for libraries         [ https://a.yandex-team.ru/arc/commit/6980795 ]
 * build-and-release in readmes  [ https://a.yandex-team.ru/arc/commit/6974155 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-05 16:38:37+03:00

0.26.0
------

* [hhell](http://staff/hhell)

 * [BI-1540] statcommons over releaser                                                                               [ https://a.yandex-team.ru/arc/commit/6974093 ]
 * 0.25.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927087 ]
 * [BI-1548] dependencies cleanup                                                                                    [ https://a.yandex-team.ru/arc/commit/6927086 ]
 * 0.24.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927085 ]
 * Minor fix                                                                                                         [ https://a.yandex-team.ru/arc/commit/6927084 ]
 * 0.23.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927083 ]
 * Minor refactoring                                                                                                 [ https://a.yandex-team.ru/arc/commit/6927081 ]
 * 0.22.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927080 ]
 * More logging tuning                                                                                               [ https://a.yandex-team.ru/arc/commit/6927079 ]
 * Corrected base logging config                                                                                     [ https://a.yandex-team.ru/arc/commit/6927078 ]
 * 0.21.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927077 ]
 * 0.20.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927076 ]
 * log_config: cleanup of an irrelevant line                                                                         [ https://a.yandex-team.ru/arc/commit/6927075 ]
 * 0.19.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927074 ]
 * Common logging configuration thingies                                                                             [ https://a.yandex-team.ru/arc/commit/6927072 ]
 * 0.18.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927071 ]
 * logs: minor improvements                                                                                          [ https://a.yandex-team.ru/arc/commit/6927070 ]
 * logmappers are actually logmutators                                                                               [ https://a.yandex-team.ru/arc/commit/6927068 ]
 * logs: RecordMappers: a convenience logging factory to apply a list of mappers on each record                      [ https://a.yandex-team.ru/arc/commit/6927067 ]
 * 0.17.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927066 ]
 * logs: do not die on mutilated exc_info                                                                            [ https://a.yandex-team.ru/arc/commit/6927065 ]
 * 0.16.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927064 ]
 * logs: some tuning                                                                                                 [ https://a.yandex-team.ru/arc/commit/6927063 ]
 * 0.15.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927062 ]
 * logs: timestampns                                                                                                 [ https://a.yandex-team.ru/arc/commit/6927061 ]
 * remove JsonExtCompatFormatter; use annotator-filters or logging factories instead                                 [ https://a.yandex-team.ru/arc/commit/6927060 ]
 * logs: some tuning to account for ylog                                                                             [ https://a.yandex-team.ru/arc/commit/6927059 ]
 * logs: TaggedSysLogHandler                                                                                         [ https://a.yandex-team.ru/arc/commit/6927058 ]
 * 0.14.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927057 ]
 * statcommons.logs: ylog × py-data-logging. py3-only                                                                [ https://a.yandex-team.ru/arc/commit/6927056 ]
 * yasm_config: token notice                                                                                         [ https://a.yandex-team.ru/arc/commit/6927055 ]
 * celery_ping: experimental: thread-lock the ping-check to try to avoid extra weird errors                          [ https://a.yandex-team.ru/arc/commit/6927054 ]
 * 0.13.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927053 ]
 * celery_ping force_ok_flag_file                                                                                    [ https://a.yandex-team.ru/arc/commit/6927052 ]
 * qloud_juggler: minor unused feature: directly edit the juggler check config                                       [ https://a.yandex-team.ru/arc/commit/6927051 ]
 * yasm_config: panel_abc                                                                                            [ https://a.yandex-team.ru/arc/commit/6927047 ]
 * yasm_config mdb redis support                                                                                     [ https://a.yandex-team.ru/arc/commit/6927046 ]
 * 0.11.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927045 ]
 * Copy of the django_pgaas.hosts for a no-deps usage                                                                [ https://a.yandex-team.ru/arc/commit/6927044 ]
 * qloud_juggler: easier status hook name configurability, shorter default status hook name prefix                   [ https://a.yandex-team.ru/arc/commit/6927043 ]
 * qloud_juggler: status hook check copy: consider 400 a 'CRIT' status, add a custom HTTP 440 for the 'WARN' status  [ https://a.yandex-team.ru/arc/commit/6927042 ]
 * 0.10.0                                                                                                            [ https://a.yandex-team.ru/arc/commit/6927041 ]
 * [BI-393] qloud_juggler enconfigurer script                                                                        [ https://a.yandex-team.ru/arc/commit/6927040 ]
 * 0.9.0                                                                                                             [ https://a.yandex-team.ru/arc/commit/6927039 ]
 * Fix celery_ping ipv6 support on python2                                                                           [ https://a.yandex-team.ru/arc/commit/6927038 ]
 * 0.8.0                                                                                                             [ https://a.yandex-team.ru/arc/commit/6927037 ]
 * [BI-589] celery unistat support, included in celery_ping                                                          [ https://a.yandex-team.ru/arc/commit/6927036 ]
 * yasm_config: remove some currently dead checks (of dead instances)                                                [ https://a.yandex-team.ru/arc/commit/6927035 ]
 * 0.7.0                                                                                                             [ https://a.yandex-team.ru/arc/commit/6927033 ]
 * celery_ping: logging, py3 fix                                                                                     [ https://a.yandex-team.ru/arc/commit/6927032 ]
 * yasm_config: move the generally unhelpful tcp_retransmits to a non-default kind                                   [ https://a.yandex-team.ru/arc/commit/6927031 ]
 * yasm_config: signals_for_everything, net_all                                                                      [ https://a.yandex-team.ru/arc/commit/6927030 ]
 * yasm_config make_db_signals 'kinds' fix                                                                           [ https://a.yandex-team.ru/arc/commit/6927029 ]
 * yasm_config: refactored version to make it easier to add custom signals                                           [ https://a.yandex-team.ru/arc/commit/6927028 ]
 * yasm_config: fix the mixed up cpu/mem lines                                                                       [ https://a.yandex-team.ru/arc/commit/6927027 ]
 * yasm_config: further multi-project-panel support                                                                  [ https://a.yandex-team.ru/arc/commit/6927026 ]
 * yasm_config: signals for all components _title_base                                                               [ https://a.yandex-team.ru/arc/commit/6927025 ]
 * yasm_config: do not process an empty list of alerts                                                               [ https://a.yandex-team.ru/arc/commit/6927024 ]
 * 0.6.0                                                                                                             [ https://a.yandex-team.ru/arc/commit/6927023 ]
 * unistat: apparently, unknown tags break the unistatting completely                                                [ https://a.yandex-team.ru/arc/commit/6927022 ]
 * 0.5.0                                                                                                             [ https://a.yandex-team.ru/arc/commit/6927021 ]
 * version bump helper script fix                                                                                    [ https://a.yandex-team.ru/arc/commit/6927020 ]
 * version bump helper script                                                                                        [ https://a.yandex-team.ru/arc/commit/6927019 ]
 * 0.4.0                                                                                                             [ https://a.yandex-team.ru/arc/commit/6927018 ]
 * unistat: fix, again                                                                                               [ https://a.yandex-team.ru/arc/commit/6927017 ]
 * yasm_config: Remove network usage lines as it currently makes the eyes bleed too much                             [ https://a.yandex-team.ru/arc/commit/6927016 ]
 * yasm_config: slightly more overridability                                                                         [ https://a.yandex-team.ru/arc/commit/6927015 ]
 * unistat: common stream-serialization and fixes                                                                    [ https://a.yandex-team.ru/arc/commit/6927014 ]
 * yasm_config: container disk usage: correct source                                                                 [ https://a.yandex-team.ru/arc/commit/6927013 ]
 * yasm_config: major refactoring, additional mdb postgresql support                                                 [ https://a.yandex-team.ru/arc/commit/6927012 ]
 * Minor comment fix                                                                                                 [ https://a.yandex-team.ru/arc/commit/6927011 ]
 * 0.3.0                                                                                                             [ https://a.yandex-team.ru/arc/commit/6927010 ]
 * sandbox_schedulers: skip uncustomized parameters                                                                  [ https://a.yandex-team.ru/arc/commit/6927009 ]
 * tools/sandbox_schedulers: configurable disk_space                                                                 [ https://a.yandex-team.ru/arc/commit/6927008 ]
 * sandbox_schedulers: some fixes                                                                                    [ https://a.yandex-team.ru/arc/commit/6927007 ]
 * applib/celery_ping: celery aliveness status http daemon                                                           [ https://a.yandex-team.ru/arc/commit/6927006 ]
 * tools/sandbox_schedulers: updated default container resource                                                      [ https://a.yandex-team.ru/arc/commit/6927005 ]
 * yasm_config: memory max usage instead of average                                                                  [ https://a.yandex-team.ru/arc/commit/6927004 ]
 * yasm_config: fix balancers signals and alerts                                                                     [ https://a.yandex-team.ru/arc/commit/6927003 ]
 * yasm_config: parenthesis fix                                                                                      [ https://a.yandex-team.ru/arc/commit/6927002 ]
 * yasm_config: balancer-components charts                                                                           [ https://a.yandex-team.ru/arc/commit/6927001 ]
 * yasm_config: free memory excluding the caches                                                                     [ https://a.yandex-team.ru/arc/commit/6927000 ]
 * 0.2.0                                                                                                             [ https://a.yandex-team.ru/arc/commit/6926999 ]
 * verify_unistat tool copy for easier access                                                                        [ https://a.yandex-team.ru/arc/commit/6926997 ]
 * uwsgi unistat, sandbox schedulers configurer, yasm configurer                                                     [ https://a.yandex-team.ru/arc/commit/6926996 ]

* [nslus](http://staff/nslus)

 * ARCADIA-2258 [migration] bb/STATBOX/commons  [ https://a.yandex-team.ru/arc/commit/6927161 ]

* [asnytin](http://staff/asnytin)

 * 0.12.0                         [ https://a.yandex-team.ru/arc/commit/6927050 ]
 * [BI-960] get_yc_service_token  [ https://a.yandex-team.ru/arc/commit/6927048 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-15 17:47:31+03:00

0.25.0
---

Releaser-based versioning.

[Anton Vasilyev](http://staff/hhell@yandex-team.ru) 2020-06-15 17:30:10+03:00

