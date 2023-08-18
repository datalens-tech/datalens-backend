12.99.0
-------

* [hhell](http://staff/hhell)

 * BI-1511: NativeType transition step 2 [run large tests]                                                                                                            [ https://a.yandex-team.ru/arc/commit/7284516 ]
 * DEVTOOLSSUPPORT-2799: bi large tests on sandbox+SSD [run large tests]                                                                                              [ https://a.yandex-team.ru/arc/commit/7274279 ]
 * Minor debug convenience improvement                                                                                                                                [ https://a.yandex-team.ru/arc/commit/7269890 ]
 * BI-1511: NativeType dict-valued primitive [run large test]                                                                                                         [ https://a.yandex-team.ru/arc/commit/7236846 ]
 * DEVTOOLSSUPPORT-2799: docker client timeout: 60 -> 300 [run large tests]                                                                                           [ https://a.yandex-team.ru/arc/commit/7236833 ]
 * fix bi-api-sync swagger                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/7232571 ]
 * tier1 tests: bi-common common test image [run large tests]                                                                                                         [ https://a.yandex-team.ru/arc/commit/7227514 ]
 * minor dataset.add_data_source refactoring [run large tests]                                                                                                        [ https://a.yandex-team.ru/arc/commit/7225042 ]
 * DEVTOOLSSUPPORT-2799: PGCTLTIMEOUT everywhere because apparently sandbox environment is sloooooooooow and the default 60 seconds is not enough to stop a postgres  [ https://a.yandex-team.ru/arc/commit/7221482 ]
 * BI-1511: native type classes in bi-api and bi-uploads [run large tests]                                                                                            [ https://a.yandex-team.ru/arc/commit/7219091 ]
 * clean up tox markers to reduce the disparity with tier0 tests                                                                                                      [ https://a.yandex-team.ru/arc/commit/7214206 ]
 * BI-1121: bi-billing tier0 tests [run large tests]                                                                                                                  [ https://a.yandex-team.ru/arc/commit/7210643 ]
 * assorted fixes                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/7206805 ]
 * makefiles cleanup and commonize                                                                                                                                    [ https://a.yandex-team.ru/arc/commit/7206735 ]
 * BI-1511: NativeType split-class [run large tests]                                                                                                                  [ https://a.yandex-team.ru/arc/commit/7206294 ]
 * minor fixes                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/7185059 ]
 * switching to py38 in tier1 tests                                                                                                                                   [ https://a.yandex-team.ru/arc/commit/7178546 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-1635: disable managed network for service clickhouses                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/7279479 ]
 * CLOUDSUPPORT-45259: fix raw_schema eq check when ntypes are None                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/7271409 ]
 * CLOUDSUPPORT-44194 DLHELP-890: increase rqe timeouts for materialization (hard'n'scary way)

по мотивам рассказа kchupin@

мне всё же не очень нравится идея в ConnectOptions класть штуки не про непосредственно БД  [ https://a.yandex-team.ru/arc/commit/7254513 ]
 * BI-1718: metrics for aiohttp and flask apps                                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/7233256 ]
 * BI-1608: ext-testing: use cloud us, dls and uploads                                                                                                                                                                   [ https://a.yandex-team.ru/arc/commit/7220085 ]

* [kchupin](http://staff/kchupin)

 * [BI-1461] Dedicated package for common testing utils                   [ https://a.yandex-team.ru/arc/commit/7278640 ]
 * [BI-1461] Testing utils deduplication and refactoring                  [ https://a.yandex-team.ru/arc/commit/7223101 ]
 * [BI-1212] Load available connectors in runtime instead of import-time  [ https://a.yandex-team.ru/arc/commit/7208196 ]
 * [BI-1212] commonize flask test client                                  [ https://a.yandex-team.ru/arc/commit/7207749 ]
 * [BI-1212] associated_svc_acct_id field for base provider connection    [ https://a.yandex-team.ru/arc/commit/7207478 ]
 * [BI-1212] SA controller idempotency & instantiation                    [ https://a.yandex-team.ru/arc/commit/7206708 ]
 * [BI-1212] Service account keys list/get/delete methods wrappers        [ https://a.yandex-team.ru/arc/commit/7188412 ]
 * [BI-1212] Service account creation                                     [ https://a.yandex-team.ru/arc/commit/7181747 ]

* [hans](http://staff/hans)

 * BI-1616 BI-1638 Allow albato and loginom in productio  [ https://a.yandex-team.ru/arc/commit/7265677 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added a field remapping mechanism, BI-974                                                                                                                   [ https://a.yandex-team.ru/arc/commit/7265161 ]
 * Refactored feature API                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/7238949 ]
 * PR from branch users/gstatsenko/feature-base-classes

WIP

Moved source features to a separate folder

Moved source features to a separate folder           [ https://a.yandex-team.ru/arc/commit/7236806 ]
 * Added DivisionByZero exception                                                                                                                              [ https://a.yandex-team.ru/arc/commit/7232567 ]
 * Moved source features to a separate folder                                                                                                                  [ https://a.yandex-team.ru/arc/commit/7214979 ]
 * Refactored and fixed generation of cache keys, SUBBOTNIK-5797                                                                                               [ https://a.yandex-team.ru/arc/commit/7214137 ]
 * Added DatasetCapabilities.supports_materialization method; added supports_manual_materialization, supports_scheduled_materialization option flags, BI-1501  [ https://a.yandex-team.ru/arc/commit/7182300 ]

* [asnytin](http://staff/asnytin)

 * CLOUDSUPPORT-43882: more csv export timeouts             [ https://a.yandex-team.ru/arc/commit/7215719 ]
 * BI-1553: geo layers + billing checker small refactoring  [ https://a.yandex-team.ru/arc/commit/7207131 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-03 15:20:51+03:00

12.98.0
-------

* [hhell](http://staff/hhell)

 * minor style changes and extra checks            [ https://a.yandex-team.ru/arc/commit/7178058 ]
 * bi-billing tier1 tests in the common container  [ https://a.yandex-team.ru/arc/commit/7175758 ]

* [gstatsenko](http://staff/gstatsenko)

 * Implemented mat manager's method that checks if materialization of a given type is required, BI-1501  [ https://a.yandex-team.ru/arc/commit/7177362 ]
 * Added CH code 159 for SourceTimeout error, DLHELP-943                                                 [ https://a.yandex-team.ru/arc/commit/7177015 ]

* [asnytin](http://staff/asnytin)

 * fixed yc_auth test, broken after https://a.yandex-team.ru/review/1370229

сломал тест в https://a.yandex-team.ru/review/1370229 когда оборачивал 500-ку в 401  [ https://a.yandex-team.ru/arc/commit/7177195 ]
 * lighten metrika tests to avoid flaps                                                                                                                           [ https://a.yandex-team.ru/arc/commit/7177057 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-05 16:38:05+03:00

12.97.0
-------

* [hhell](http://staff/hhell)

 * realistic install_requires                                             [ https://a.yandex-team.ru/arc/commit/7175743 ]
 * test_aio_event_loop_middleware: extend the test, reduce the flakiness  [ https://a.yandex-team.ru/arc/commit/7169911 ]
 * test_aio_event_loop_middleware: more debug details                     [ https://a.yandex-team.ru/arc/commit/7164622 ]
 * BI-1121: bi-materializer tier0 fixes                                   [ https://a.yandex-team.ru/arc/commit/7158101 ]
 * Further fix                                                            [ https://a.yandex-team.ru/arc/commit/7157763 ]
 * un-overridden rqe async service in bi-api                              [ https://a.yandex-team.ru/arc/commit/7157722 ]
 * BI-1121: bimat tier0 build                                             [ https://a.yandex-team.ru/arc/commit/7157330 ]
 * Fix chydb blackbox test [run large tests]                              [ https://a.yandex-team.ru/arc/commit/7154551 ]
 * Fix for bi-uploads tests                                               [ https://a.yandex-team.ru/arc/commit/7150399 ]
 * Tests-support reorganization [run large tests]                         [ https://a.yandex-team.ru/arc/commit/7150321 ]
 * bicommon/testing/environment: make docker dependency more optional     [ https://a.yandex-team.ru/arc/commit/7149068 ]
 * extras_require "all"                                                   [ https://a.yandex-team.ru/arc/commit/7148859 ]
 * bi-uploads ext-tests for manual run [run large tests]                  [ https://a.yandex-team.ru/arc/commit/7143677 ]
 * Remove migrations from the main library                                [ https://a.yandex-team.ru/arc/commit/7132548 ]
 * BI-1121: commonize hooked configure_logging                            [ https://a.yandex-team.ru/arc/commit/7112519 ]
 * BI-1121: yet another tests fix                                         [ https://a.yandex-team.ru/arc/commit/7105699 ]

* [hans](http://staff/hans)

 * BI-1675 PR from branch users/hans/upload_int_to_float

cast values to float on upload

add test  [ https://a.yandex-team.ru/arc/commit/7171027 ]
 * BI-1616 BI-1638 fix class registrations                                                          [ https://a.yandex-team.ru/arc/commit/7156372 ]
 * BI-1616 BI-1638 Add Albato and Loginom connectors                                                [ https://a.yandex-team.ru/arc/commit/7152480 ]

* [asnytin](http://staff/asnytin)

 * Геослои + флаг запрета скачивания данных

BI-1666: bi-api tests for data_export_forbidden flag and geolayers

BI-1666: data_export_forbidden flag

BI-1553: geolayers connection allowed tables

BI-1553: geolayers datasource materialization properties

BI-1553: geolayers: tests and fixes

BI-1553: geolayers: get purchased layers with BillingChecker

BI-1553: geolayers connection, filters draft  [ https://a.yandex-team.ru/arc/commit/7170873 ]
 * BI-1661: require billing checker                                                                                                                                                                                                                                                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/7168769 ]
 * yc_auth_401_on_missing_header                                                                                                                                                                                                                                                                                                                                                                               [ https://a.yandex-team.ru/arc/commit/7165207 ]
 * metrika tests: a bit lighter requests

чтобы тесты не флапали                                                                                                                                                                                                                                                                                                                                               [ https://a.yandex-team.ru/arc/commit/7158668 ]
 * BI-1667: ch err code 349                                                                                                                                                                                                                                                                                                                                                                                    [ https://a.yandex-team.ru/arc/commit/7158667 ]
 * BI-1661: BillingChecker service in ServiceRegistry                                                                                                                                                                                                                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/7142224 ]
 * BI-1070: connection without host test fix                                                                                                                                                                                                                                                                                                                                                                   [ https://a.yandex-team.ru/arc/commit/7111989 ]

* [gstatsenko](http://staff/gstatsenko)

 * Separated materialization management from the Dataset class, BI-1501                                                                                                                         [ https://a.yandex-team.ru/arc/commit/7170541 ]
 * Implemented reporting for data processors, BI-1477                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/7170523 ]
 * PR from branch users/gstatsenko/dataset-legacy-prop-cleanup

Removed several legacy attributes from the Dataset class

Separated materialization management from the Dataset class, BI-1501  [ https://a.yandex-team.ru/arc/commit/7161651 ]
 * Added default access mode for all source types, BI-1501                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/7158669 ]
 * Change test_compeng_cache.py                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/7150110 ]
 * Some internal refactoring for the implementation of data processor reporting, BI-1477                                                                                                        [ https://a.yandex-team.ru/arc/commit/7106114 ]
 * Separated report profiler from reporting registry, BI-1477                                                                                                                                   [ https://a.yandex-team.ru/arc/commit/7106110 ]

* [kchupin](http://staff/kchupin)

 * [BI-1121] Manual for running tests from pycharm                              [ https://a.yandex-team.ru/arc/commit/7155819 ]
 * [BI-1629] Fix cache TTL override for Metrica API connection                  [ https://a.yandex-team.ru/arc/commit/7153851 ]
 * [BI-1121] Common requirements in remote debug docker-container               [ https://a.yandex-team.ru/arc/commit/7153842 ]
 * [BI-1629] Selectors now take in account connection-level cache TTL override  [ https://a.yandex-team.ru/arc/commit/7144308 ]
 * [BI-1629] Cache TTL override field in connection data                        [ https://a.yandex-team.ru/arc/commit/7140265 ]
 * [BI-1484] Implicit cache TTL configuration/calculation                       [ https://a.yandex-team.ru/arc/commit/7139176 ]
 * [BI-1125] Materialized dataset copy                                          [ https://a.yandex-team.ru/arc/commit/7107764 ]

* [seray](http://staff/seray)

 * [BI-1384] fix tariff_type typing                       [ https://a.yandex-team.ru/arc/commit/7139504 ]
 * [BI-1384] adding BillingAdditionalInfoReportingRecord  [ https://a.yandex-team.ru/arc/commit/7112804 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-04 22:03:18+03:00

12.96.0
-------

* [asnytin](http://staff/asnytin)

 * BI-1636: AppMetricaLogsAPI: fixed partitions count calculation per insert  [ https://a.yandex-team.ru/arc/commit/7102400 ]
 * BI-1070: __system/dl_ch_config files multihost migration                   [ https://a.yandex-team.ru/arc/commit/7101472 ]

* [hhell](http://staff/hhell)

 * Attemot to fix bi-common db tests: include testenv-common data [RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/7099839 ]
 * tests: enable wait-for-us-pg [RUN LARGE TESTS]                                    [ https://a.yandex-team.ru/arc/commit/7097575 ]

* [kchupin](http://staff/kchupin)

 * [BI-1125] Testing US was updated. Usage was generalized in DC in bi-common & bi-api  [ https://a.yandex-team.ru/arc/commit/7098717 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-11 14:01:46+03:00

12.95.0
-------

* [hhell](http://staff/hhell)

 * tests: disable wait-for-us-pg for now      [ https://a.yandex-team.ru/arc/commit/7096809 ]
 * actualize [RUN LARGE TESTS]                [ https://a.yandex-team.ru/arc/commit/7096803 ]
 * Minor reorganization                       [ https://a.yandex-team.ru/arc/commit/7095390 ]
 * Auxiliary: make_clean_test: arc-usage fix  [ https://a.yandex-team.ru/arc/commit/7087620 ]

* [asnytin](http://staff/asnytin)

 * BI-1070: multihost connection executors

BI-1070: multihost connection executors

BI-1070: bi-api connections schema: comma-separated multiple hosts  [ https://a.yandex-team.ru/arc/commit/7096632 ]

* [gstatsenko](http://staff/gstatsenko)

 * Fixed test_integration_compeng_configured_case and test_compeng_cache                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/7095685 ]
 * Removed data_context from DatasetView

Removed data_context from DatasetView

Added TODO to SR factory

Switched to using data processor factories, BI-1477

Fixed ya.make

Added data processor factory, BI-1477  [ https://a.yandex-team.ru/arc/commit/7090275 ]
 * Added cache for data processor/compeng

Implemented operation processor cache, BI-1477

Switched to using data processor factories, BI-1477

Fixed ya.make

Added data processor factory, BI-1477                  [ https://a.yandex-team.ru/arc/commit/7089757 ]
 * Raised Dataset version to 11                                                                                                                                                                                       [ https://a.yandex-team.ru/arc/commit/7089742 ]

* [kchupin](http://staff/kchupin)

 * PyCharm integration for monorepo format  [ https://a.yandex-team.ru/arc/commit/7095427 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-09 21:03:45+03:00

12.94.0
-------

* [hhell](http://staff/hhell)

 * Support sqlalchemy 1.2, minor dependencies tuning  [ https://a.yandex-team.ru/arc/commit/7087434 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-07 21:42:48+03:00

12.93.0
-------

* [hhell](http://staff/hhell)

 * unpin marshmallow  [ https://a.yandex-team.ru/arc/commit/7087207 ]

* [gstatsenko](http://staff/gstatsenko)

 * Switched to using data processor factories, BI-1477    [ https://a.yandex-team.ru/arc/commit/7087105 ]
 * Added ignoreing of db_name if it is None in cache key  [ https://a.yandex-team.ru/arc/commit/7085418 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-07 19:25:57+03:00

12.92.0
-------

* [hhell](http://staff/hhell)

 * bi-common/tests/db: return retries for now                                         [ https://a.yandex-team.ru/arc/commit/7081346 ]
 * bicommon ext tests [RUN LARGE TESTS]                                               [ https://a.yandex-team.ru/arc/commit/7076534 ]
 * BI-1121: working db tests                                                          [ https://a.yandex-team.ru/arc/commit/7074111 ]
 * eqe_secret_key from environ through a central point                                [ https://a.yandex-team.ru/arc/commit/7072859 ]
 * BI-1121: tier0-compatible docker-compose config, test environment in tests.config  [ https://a.yandex-team.ru/arc/commit/7072011 ]
 * BI-1121: Return the oracle hackaround for old sqlalchemy                           [ https://a.yandex-team.ru/arc/commit/7072001 ]
 * minor fix for tests                                                                [ https://a.yandex-team.ru/arc/commit/7067064 ]
 * tests: more version pinning and fixes                                              [ https://a.yandex-team.ru/arc/commit/7051659 ]
 * BI-1121: reorganized tests                                                         [ https://a.yandex-team.ru/arc/commit/7051310 ]

* [asnytin](http://staff/asnytin)

 * BI-1636: LogsAPI dsrc: sort data rows by partition key field  [ https://a.yandex-team.ru/arc/commit/7081160 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added LazyCachedDatasetDataSelectorAsync, BI-1477  [ https://a.yandex-team.ru/arc/commit/7073580 ]
 * Fixed cache key creation                           [ https://a.yandex-team.ru/arc/commit/7067247 ]

* [shadchin](http://staff/shadchin)

 * Fix datalens with Python 3.8

В Python 3.8 исключения вынесли в отдельный модуль  [ https://a.yandex-team.ru/arc/commit/7070005 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-06 12:56:50+03:00

12.91.0
-------

* [dmifedorov](http://staff/dmifedorov)

 * BI-1622: do not select * in preview subselect

fix tests

use raw_schema

BI-1622: do not select * in preview subselect  [ https://a.yandex-team.ru/arc/commit/7049491 ]

* [hhell](http://staff/hhell)

 * preliminary tests reorganization         [ https://a.yandex-team.ru/arc/commit/7039795 ]
 * chyt-sqlalchemy un-hack                  [ https://a.yandex-team.ru/arc/commit/7038997 ]
 * return bicommon/bin to tier0             [ https://a.yandex-team.ru/arc/commit/7037276 ]
 * BI-1121: register_sa_dialects for tier0  [ https://a.yandex-team.ru/arc/commit/7036074 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-29 11:24:01+03:00

12.90.0
-------

* [hhell](http://staff/hhell)

 * fixes                                                         [ https://a.yandex-team.ru/arc/commit/7031929 ]
 * DEVTOOLSSUPPORT-1829: Attempt to fix timeouts on stylechecks  [ https://a.yandex-team.ru/arc/commit/7031602 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-24 20:54:58+03:00

12.89.0
-------

* [kchupin](http://staff/kchupin)

 * [BI-1621] Sentry before_send preprocessor to cleanup secret local vars in stacktrace frames  [ https://a.yandex-team.ru/arc/commit/7031091 ]
 * Some .arcignore                                                                              [ https://a.yandex-team.ru/arc/commit/7006929 ]

* [hhell](http://staff/hhell)

 * install_requires to peerdir  [ https://a.yandex-team.ru/arc/commit/7030800 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved BI_TYPE_AGGREGATIONS to bi-api and added count aggregation for all data types  [ https://a.yandex-team.ru/arc/commit/7009745 ]
 * Added minor fixes for cache keys                                                     [ https://a.yandex-team.ru/arc/commit/7009412 ]
 * Updated global dataLens/backend .arcignore                                           [ https://a.yandex-team.ru/arc/commit/7008125 ]

* [dmifedorov](http://staff/dmifedorov)

 * CLOUD-47530: wrap async billing checks in sync  [ https://a.yandex-team.ru/arc/commit/7009649 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-24 16:44:10+03:00

12.88.0
-------

* [dmifedorov](http://staff/dmifedorov)

 * CLOUD-47530: add check_connector_is_available_sync  [ https://a.yandex-team.ru/arc/commit/7006630 ]

* [hhell](http://staff/hhell)

 * [BI-1121] bi-common recurse + linting  [ https://a.yandex-team.ru/arc/commit/7002960 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-22 21:34:35+03:00

12.87.0
-------

* [hhell](http://staff/hhell)

 * fix                                                                                                                                                                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/7002664 ]
 * fix                                                                                                                                                                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/7002623 ]
 * [BI-1540] bi-common arcadia tier1                                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/7002616 ]
 * 12.86.0                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6994175 ]
 * Merge pull request #1025 in STATBOX/bi-common from hhell/stuff04 to master

\* commit 'bddb1f5b69828d88eebd5533a186f8d0713441f2':
  [BI-1614] RQE + table-func-over-sqlalchemy-text fix                                                                                                         [ https://a.yandex-team.ru/arc/commit/6994174 ]
 * 12.85.0                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6994171 ]
 * some TODOs and more precise test-skipping                                                                                                                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/6994168 ]
 * CHYDB async: actually run the tests                                                                                                                                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/6994167 ]
 * CHYDB async                                                                                                                                                                                                                                                                                    [ https://a.yandex-team.ru/arc/commit/6994166 ]
 * [BI-1598] CHYT/CHYDB: pass all errors' db_message to the user                                                                                                                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/6994160 ]
 * test_pg_op_exec_adapter: better active_queries_query                                                                                                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/6994127 ]
 * Tests: fix data_processing op_runner tests                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6994105 ]
 * 12.76.0                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6994104 ]
 * [BI-1561] CHYDB extra-parameters fix                                                                                                                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/6994101 ]
 * 12.75.0                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6994100 ]
 * Merge pull request #986 in STATBOX/bi-common from hhell/stuff02 to master

\* commit 'a6d8602039495fb487cdd3ce10b30b77e5cc7409':
  [BI-1516] rls all_values renamed to pattern_type for future extensibility
  [BI-1516] rls all_values / all_subjects support                                  [ https://a.yandex-team.ru/arc/commit/6994096 ]
 * Merge pull request #991 in STATBOX/bi-common from hhell/stuff05 to master

\* commit '57dcc4c824b72355d0aed54dd40bda9df2105adc':
  [BI-1561] CHYDB: ydb_cluster / ydb_database parameters                                                                                                       [ https://a.yandex-team.ru/arc/commit/6994095 ]
 * Merge pull request #996 in STATBOX/bi-common from hhell/stuff06 to master

\* commit '568a22afad77ced7b018b24ed766093e53071ab5':
  [BI-1547] more legacy cleanup
  [BI-1547] Minor notes
  [BI-1547] Remove some obsolete gauthling mentions
  [BI-1547] Remove apispec-oneofschema dependency  [ https://a.yandex-team.ru/arc/commit/6994084 ]
 * [BI-1547] more legacy cleanup                                                                                                                                                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/6994079 ]
 * [BI-1547] Minor notes                                                                                                                                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/6994078 ]
 * [BI-1547] Remove some obsolete gauthling mentions                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6994077 ]
 * [BI-1547] Remove apispec-oneofschema dependency                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994076 ]
 * Merge pull request #941 in STATBOX/bi-common from hhell/stuff04 to master

\* commit '7d473c1092d6919a78eb999a72b57b8cad2d8841':
  flask ContextVarMiddleware: force fresh context to avoid problems. Use a global dict for inter-request context (see statcommons).                            [ https://a.yandex-team.ru/arc/commit/6994066 ]
 * [BI-1547] More deps cleanup, tests fix                                                                                                                                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/6994036 ]
 * [BI-1547] Minor preliminary dependency cleanup                                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/6994035 ]
 * Minor doc update                                                                                                                                                                                                                                                                               [ https://a.yandex-team.ru/arc/commit/6994011 ]
 * Merge pull request #966 in STATBOX/bi-common from hhell/stuff06 to master

\* commit '067df78e91347abf1be74eaf8f322f4093bae46e':
  [BI-1478] preliminary fix for tz-aware datetimes over asyncpg compeng                                                                                        [ https://a.yandex-team.ru/arc/commit/6993974 ]

* [nslus](http://staff/nslus)

 * ARCADIA-2273 [migration] bb/STATBOX/bi-common  [ https://a.yandex-team.ru/arc/commit/7001276 ]

* [gstatsenko](http://staff/gstatsenko)

 * Merge pull request #1023 in STATBOX/bi-common from field-autoaggregated-fix to master

\* commit 'ddce47b5972c7d0ad89d8fc711f5c164c7580702':
  Refactored the logic of the BIField.autoaggregated property, BI-1593                                                         [ https://a.yandex-team.ru/arc/commit/6994173 ]
 * Merge pull request #1022 in STATBOX/bi-common from formula-has-auto-aggregation to master

\* commit 'c838a888f57460f4846f75e756c6aed66ab96f7a':
  Minor PR refactoring
  Added migration for has_auto_aggregation attribute of formula fields, BI-1593                     [ https://a.yandex-team.ru/arc/commit/6994170 ]
 * Merge pull request #1019 in STATBOX/bi-common from cache-key-refactoring to master

\* commit 'ee24def3dc252b2fd2646e3a01e898615d37be30':
  Added make_data_select_cache_key
  Minor PR fix
  Refactored building and usage of cache keys, BI-1477                          [ https://a.yandex-team.ru/arc/commit/6994164 ]
 * Merge pull request #1018 in STATBOX/bi-common from cache-engine-factory to master

\* commit '5c4a5f13df89b9c396d69bf70a66a2eb8b42db3a':
  Moved cache engine creation to CacheEngineFactory, BI-1477                                                                       [ https://a.yandex-team.ru/arc/commit/6994152 ]
 * Merge pull request #994 in STATBOX/bi-common from lazy-async-chunked to master

\* commit 'e3da4e8ceb43ecc84d0a4287302e58d2de254c47':
  Made finalizer async in LazyAsyncChunked
  Implemented LazyAsyncChunked for use in lazy data selector, BI-1477                      [ https://a.yandex-team.ru/arc/commit/6994149 ]
 * Merge pull request #1006 in STATBOX/bi-common from table-recreation-test to master

\* commit '22c51c6737ab1b0c4eb8218628ed3bc4b8943e8e':
  Fixed tests
  Minor fixes
  test_pg_op_exec_adapter: better active_queries_query
  Added test_create_same_name_tables, BI-1477  [ https://a.yandex-team.ru/arc/commit/6994148 ]
 * Fixed tests                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994145 ]
 * Minor fixes                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994131 ]
 * Added test_create_same_name_tables, BI-1477                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994109 ]
 * Merged master                                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6994091 ]
 * Merge pull request #993 in STATBOX/bi-common from legacy-cleanups to master

\* commit '25564c95e47acc725c17127d6728b5dd95164d2c':
  Seveeral minor fixes and legacy cleanups                                                                                               [ https://a.yandex-team.ru/arc/commit/6994075 ]
 * Fixed imports in tests                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6994051 ]
 * Merge pull request #982 in STATBOX/bi-common from new-query-exceptions to master

\* commit '266328e3d8a4181052bb79bef265c6404d3d8c80':
  Added JoinColumnTypeMismatch and NoSpaceLeft exceptions, BI-1552                                                                  [ https://a.yandex-team.ru/arc/commit/6994033 ]
 * 12.68.0                                                                                                                                                                                                                                                                    [ https://a.yandex-team.ru/arc/commit/6994022 ]
 * Merge pull request #981 in STATBOX/bi-common from connection-replacement-compatibility-checks to master

\* commit '60b9ca55138d927e65f62aaa7dc33c1db68f72a8':
  Added connection replacement compatibility checks, BI-1525                                                 [ https://a.yandex-team.ru/arc/commit/6994021 ]
 * Merge pull request #973 in STATBOX/bi-common from capabilities-clean-legacy to master

\* commit 'd6ee417153c3a8692def0b13e672469259ff074a':
  Fixed test
  Cleaned up legacy capability methods in us_dataset, BI-1525                                                     [ https://a.yandex-team.ru/arc/commit/6994015 ]
 * 12.63.0                                                                                                                                                                                                                                                                    [ https://a.yandex-team.ru/arc/commit/6993986 ]
 * Separated DatasetCapabilities from Dataset                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/6993983 ]
 * Merge pull request #951 in STATBOX/bi-common from execution-report to master

\* commit '8b82ddd2e8bd1d3d2eb6fd8330fcd2e02cfba9fc':
  Removed QueryExecutionProfilerReport.source
  Fixed PR comments
  Added execution report class, BI-1503                               [ https://a.yandex-team.ru/arc/commit/6993976 ]

* [asnytin](http://staff/asnytin)

 * 12.84.0                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6994159 ]
 * [BI-1070] FORCE_RQE_MODE flag                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6994155 ]
 * 12.83.0                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6994153 ]
 * Merge pull request #1011 in STATBOX/bi-common from BI-1513 to master

\* commit 'd352664058ea8008c38acecc7a7b31d907bd87db':
  [BI-1513] disable_rls param in geocache datasource                                                                      [ https://a.yandex-team.ru/arc/commit/6994151 ]
 * Merge pull request #1000 in STATBOX/bi-common from BI-1576 to master

\* commit 'ff749ce61f8900a7ac9779fd6dadbc2915970ef5':
  [BI-1576] deleted fields ym:*:params from LogsAPI                                                                       [ https://a.yandex-team.ru/arc/commit/6994097 ]
 * 12.66.0                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6993997 ]
 * Merge pull request #975 in STATBOX/bi-common from BI-1432 to master

\* commit 'f0606690c2680ab5869f52d367e898c9c2a0debb':
  [BI-1432] yc_session check: fixes, errors processing, logging
  [BI-1432] yc_session cookie check in IAM SessionService  [ https://a.yandex-team.ru/arc/commit/6993996 ]
 * 12.65.0                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6993993 ]
 * fixed iam_token fetching from flask.g                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6993991 ]
 * 12.62.0                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6993972 ]
 * yc_auth iam_token by cookies fetching fix                                                                                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/6993968 ]

* [kchupin](http://staff/kchupin)

 * [BI-1508] MyPy fixes + connection_type fix in query report logging fix                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/6994098 ]
 * 12.72.0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994073 ]
 * [BI-1508] Fix SelectorFactory interface                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994071 ]
 * 12.71.0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994070 ]
 * Merge pull request #992 in STATBOX/bi-common from kchupin/mypy-simple-fixes to master

\* commit '366b862909e6972b27b3adc5ba52f6918c96c169':
  More MyPy fixes for data selectors
  More MyPy fixes for YC autth                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6994069 ]
 * [BI-1508] Some naming fixes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/6994061 ]
 * [BI-1508] allow_cache_usage argument for SelectorFactory get_dataset_selector()                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6994059 ]
 * [BI-1508] Cache attributes of data selector clarification                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6994058 ]
 * Flask yc_auth partial MyPy fixes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       [ https://a.yandex-team.ru/arc/commit/6994049 ]
 * MyPy fixes: ignore_missing_imports = True for libraries                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994048 ]
 * CH async DBA MyPy fixes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994047 ]
 * Base data processing MyPy fixes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6994046 ]
 * Flask blackbox_auth.py MyPy fixes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/6994045 ]
 * aio_event_loop_middleware.py MyPy fixes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994044 ]
 * Upload robot controller MyPy fixes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6994043 ]
 * Dataset cache engine MyPy fixes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6994042 ]
 * 12.70.0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994041 ]
 * Merge pull request #985 in STATBOX/bi-common from kchupin/stuff to master

\* commit 'a6705ac1382b0168efea2e7ba1f97b6ac837d18e':
  [BI-1508] DataSelector.close() default implementation
  [BI-1508] DataSelector.close() method redesign
  [BI-1508] SelectorType enum was moved to enums.py
  [BI-1508] Naming fixes
  [BI-1508] Selectors factory in services registry
  [BI-1508] Close method for dataset selectors                                                                                                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994040 ]
 * Merge pull request #984 in STATBOX/bi-common from kchupin/stuff03 to master

\* commit '96a68750ea652ab5fe0424b3c498634d12ce084c':
  [BI-1508] Typing fixes
  [BI-1508] Service registry backref                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6994029 ]
 * [BI-1508] Typing fixes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/6994028 ]
 * [BI-1508] Service registry backref                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6994024 ]
 * Merge pull request #983 in STATBOX/bi-common from kchupin/stuff02 to master

\* commit 'c4efb65f691bdd5d6442a69adbb8de9e751502d4':
  [BI-1508] DefaultServicesRegistry creation code in conftest deduplication
  [BI-1508] Some TODOs
  [BI-1508] allow_pure_async_ch_dba=True by default to use in tests pure async CH DBA
  [BI-1508] DirectOnlyInsecureConnExecutorFactory and IntRQEOnlyInsecureConnExecutorFactory was removed. Now all tests use DefaultConnExecutorFactory.
  [BI-1508] RCI field for service registry                                                                                                                                                                                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/6994023 ]
 * [BI-1508] Naming fixes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/6994009 ]
 * [BI-1508] Some interfaces refactoring                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/6994008 ]
 * [BI-1508] Naming fixes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/6994006 ]
 * [BI-1508] Naming fixes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/6994005 ]
 * [BI-1508] Report processing was moved to dedicated package                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             [ https://a.yandex-team.ru/arc/commit/6994004 ]
 * [BI-1508] Reports registry introduction                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994003 ]
 * 12.67.0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6994002 ]
 * [BI-1491] Services registry property usage fix in dataset selectors & explicit creating of CE in selector                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6994000 ]
 * Merge pull request #976 in STATBOX/bi-common from kchupin/stuff to master

\* commit '3b46c0549f5b3c060c58fe1981268d4755486081':
  [BI-1491] Test for connection executor caching at factory level
  [BI-1491] Connection executor caching at factory level                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             [ https://a.yandex-team.ru/arc/commit/6993999 ]
 * 12.64.0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6993989 ]
 * Merge pull request #970 in STATBOX/bi-common from kchupin/stuff to master

\* commit '0c470e83454a9cbd62bf1c89cd54e36b180173b5':
  [BI-1491] Remove useless aioredis dependency in async US manager
  [BI-1491] Support of Callable[[RequestContextInfo] as SR factory was removed in SR middleware
  [BI-1491] Fix missing DefaultServicesRegistry.caches_redis_client_factory argument in DefaultSRFactory.make_service_registry(). Docs improvement.
  [BI-1491] Dataset & US manager dependencies on DatasetCacheEngine was removed
  [BI-1491] Dataset cache engine now is created in cache selector. Services registry now provides redis client instead of dataset cache engine.
  [BI-1491] US manager become public property of US entry
  [BI-1491] Interface for service registry factory. Caches Redis client factory in service registry.  [ https://a.yandex-team.ru/arc/commit/6993988 ]
 * Merge pull request #969 in STATBOX/bi-common from kchupin/stuff to master

\* commit '01822dd74a25e57d9db4beaed93d9d4f5b822a15':
  [BI-1491] Dataset cache engine was moved to data selectors package
  [BI-1491] Sync version of dataset cache engine was removed                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/6993971 ]
 * [BI-1491] Remove all sync data selectors code                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/6993962 ]
 * [BI-1491] Fix after rebase on master                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   [ https://a.yandex-team.ru/arc/commit/6993961 ]
 * [BI-1491] Caches dataset integration tests partially restored                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/6993960 ]
 * [BI-1491] Most part of tests now does not use sync data selectors                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/6993959 ]
 * [BI-1491] Use services registry in data selectors to create connection executors/dataset cache engines                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/6993958 ]
 * [BI-1491] Dataset cache engine factory in services registry                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/6993957 ]
 * [BI-1491] Sync wrapper for AsyncDataSelector                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/6993956 ]
 * [BI-1491] aio util to_sync_iterable + mypy aio utils cleanup                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/6993955 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-21 17:50:15+03:00

12.86.0
---

Releaser-based versioning.

[Anton Vasilyev](http://staff/hhell@yandex-team.ru) 2020-06-21 16:10:06+03:00

