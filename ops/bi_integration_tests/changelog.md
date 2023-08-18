0.11.0
-----

[dmifedorov](http://staff/dmifedorov) 2023-07-04 10:20:55+02:00

 * use certs in test's aiohttp clients

0.3.0
-----

[dmifedorov](http://staff/dmifedorov) 2023-06-30 16:20:55+02:00

0.2.0
-----

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4655 Fix DLS issue in RLS integration test for yc-preprod                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/11806413 ]
 * BI-4259 Use workbook API in intergration tests.

BI-4259 Account credentials in integration tests refactoring

BI-4259 Rework integration tests fixtures.

BI-4259 Refactor the use of request executors in integration tests.

BI-4259 Add test_file_api test data to ya.make

BI-4259 Improve logging in integration tests.

BI-4259 Add variables for workbook management.

BI-4259 Feature a request executor for workbook management in integration tests.

BI-4259 Add add_wb_sa_access_binding method to workbook client.

BI-4259 Feature terraform module for service accounts.

BI-4259 update int tests version. wip

BI-4259 Remove materialization remains from integration tests.

BI-4259 Pass workbook_id to US API in order to create connections and datasets.

BI-4259 Fixture for workbook creation and teardown.

BI-4259 Fixture in integration tests for us_workbook_cmd_client.

BI-4259 Feature create_tenant factory function.

BI-4259 Fix color escape sequences for zsh in ycdl_profile.sh.

BI-4259 Add US properties to integration test config.  [ https://a.yandex-team.ru/arc/commit/11661278 ]
 * BI-4421 Fix file path error in sandbox intergration-test runs.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   [ https://a.yandex-team.ru/arc/commit/11365628 ]
 * BI-4260 Fix rls integration test.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/11018746 ]
 * BI-4260 Use special rls config syntax for service account rls subjects.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/11000199 ]
 * BI-4281 feature bi_api_commons package                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/10951763 ]
 * BI-4289 Create iam_rm_client lazily                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/10943869 ]
 * BI-4141 Use service accounts for auth in integration tests.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/10870992 ]
 * BI-4084 Feature DLEnv for running integration tests with mounted secrets.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/10776978 ]
 * BI-4030 Feature docker-container and makefile for running standalone integration tests                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/10476788 ]
 * BI-4001 Change dataset used in test_window_functions.py from CSV to PG.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/10307403 ]
 * Revert "BI-4001 Change dataset used in test_window_functions.py from CSV to PG."                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/10301819 ]
 * BI-4001 Change dataset used in test_window_functions.py from CSV to PG.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/10300370 ]

* [vgol](http://staff/vgol)

 * BI-4434: fix yield fixtures                                                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/11410769 ]
 * BI-4434: EA updated to detect workbook access denied errors and convert them into a simple common error. GRPC proxy descriminate such error and provides grpc status code PERMISSION DENIED  [ https://a.yandex-team.ru/arc/commit/11361054 ]

* [ovsds](http://staff/ovsds)

 * BI-4291: add integration tests for file upload  [ https://a.yandex-team.ru/arc/commit/11064655 ]

* [konstasa](http://staff/konstasa)

 * Fix edit connection data in integration tests                  [ https://a.yandex-team.ru/arc/commit/11050639 ]
 * Removed converter_scenario from cloud access integration test  [ https://a.yandex-team.ru/arc/commit/9779520 ]

* [thenno](http://staff/thenno)

 * Remove checking source_features in tests  [ https://a.yandex-team.ru/arc/commit/10511658 ]
 * BI-3569: fix integration tests            [ https://a.yandex-team.ru/arc/commit/9610868 ]

* [shadchin](http://staff/shadchin)

 * Move asynctest in contrib/deprecated

Пакет больше [не развивается](https://github.com/Martiusweb/asynctest/issues/158) и имеет проблемы с совместимостью с новыми версиями Python 3.

Родной unittests.mock научился в асинронность, его должно хватать.

Make precommit hook happy: IGNIETFERRO-2000.  [ https://a.yandex-team.ru/arc/commit/10370687 ]

* [shashkin](http://staff/shashkin)

 * Fix error response headers  [ https://a.yandex-team.ru/arc/commit/9983140 ]

* [gstatsenko](http://staff/gstatsenko)

 * Fixed Konstantin\'s last name                                     [ https://a.yandex-team.ru/arc/commit/9654637 ]
 * Moved most of the enums from bi_core.enums to bi_constants.enums  [ https://a.yandex-team.ru/arc/commit/8561234 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-3154: remove uploads-api from integr tests              [ https://a.yandex-team.ru/arc/commit/9214866 ]
 * biint: remove uploads-api usage                            [ https://a.yandex-team.ru/arc/commit/8632930 ]
 * CLOUDSUPPORT-94550: increase mat timeout for integr tests  [ https://a.yandex-team.ru/arc/commit/8607498 ]
 * fix integr tests (dataset version mismatch)                [ https://a.yandex-team.ru/arc/commit/8597125 ]
 * bi-integr-tests: option for mat-dep tests skipping         [ https://a.yandex-team.ru/arc/commit/8596146 ]
 * BI-1009: fix bi_integration_tests                          [ https://a.yandex-team.ru/arc/commit/8055921 ]

* [kchupin](http://staff/kchupin)

 * [BI-2945] Fetching all connections from pseudo-workbook                                         [ https://a.yandex-team.ru/arc/commit/8922263 ]
 * [BI-2663] Remove sample generation in `DatasetResource.enforce_materialization_and_cleanup()`   [ https://a.yandex-team.ru/arc/commit/8457868 ]
 * [BI-2495] IAM fixtures for DataCloud integration tests                                          [ https://a.yandex-team.ru/arc/commit/8440295 ]
 * [BI-2258] YC API exceptions clarification in uploads robot creation process                     [ https://a.yandex-team.ru/arc/commit/7932362 ]
 * [BI-2161] Use direct IAM/RM GRPC channels instead of API adapter                                [ https://a.yandex-team.ru/arc/commit/7922913 ]
 * [BI-2200] Migration from yndx-datalens to yndx-datalens-back-1/2 in bi_cloud_integration_tests  [ https://a.yandex-team.ru/arc/commit/7901076 ]
 * [BI-1849] pycharm structure fix                                                                 [ https://a.yandex-team.ru/arc/commit/7729491 ]
 * [BI-1461] Integration tests regularization                                                      [ https://a.yandex-team.ru/arc/commit/7364301 ]
 * [BI-1461] Fix run of async tests                                                                [ https://a.yandex-team.ru/arc/commit/7362812 ]
 * [BI-1461] Multi-env integration tests [run large tests]                                         [ https://a.yandex-team.ru/arc/commit/7353182 ]
 * [BI-1461] Fix peerdir for shortuuid                                                             [ https://a.yandex-team.ru/arc/commit/7345101 ]
 * [BI-1461] Access control integration tests initial implementation                               [ https://a.yandex-team.ru/arc/commit/7340801 ]
 * [BI-1461] Initial structure of integration testing module [run large tests]                     [ https://a.yandex-team.ru/arc/commit/7302774 ]

* [tsimafeip](http://staff/tsimafeip)

 * BI-2692 Fix minor validation bug.                         [ https://a.yandex-team.ru/arc/commit/8521088 ]
 * BI-2692 Run integration tests for internal installations  [ https://a.yandex-team.ru/arc/commit/8519437 ]
 * BI-2643 Extend integration tests for window functions.    [ https://a.yandex-team.ru/arc/commit/8490396 ]
 * BI-2643 Add integration tests for window functions.       [ https://a.yandex-team.ru/arc/commit/8446698 ]
 * BI-2574 Add integration test for geocoding functions.     [ https://a.yandex-team.ru/arc/commit/8360533 ]
 * BI-2558 Add public API access integration test.           [ https://a.yandex-team.ru/arc/commit/8326432 ]
 * BI-2554 RLS integration test added, code refactoring.     [ https://a.yandex-team.ru/arc/commit/8318793 ]

* [hhell](http://staff/hhell)

 * BI-2246: assorted tier1 python3.9-related fixes                   [ https://a.yandex-team.ru/arc/commit/7920895 ]
 * from __future__ import annotations                                [ https://a.yandex-team.ru/arc/commit/7886187 ]
 * BI-1849: ops                                                      [ https://a.yandex-team.ru/arc/commit/7728016 ]
 * BI-1849: tier1 deps fix, bi_configs dep to bi_cloud_integrations  [ https://a.yandex-team.ru/arc/commit/7724517 ]
 * BI-1849: reorganize into lib/                                     [ https://a.yandex-team.ru/arc/commit/7724253 ]
 * BI-2058: YC GRPC client helpers                                   [ https://a.yandex-team.ru/arc/commit/7723385 ]

[dmifedorov](http://staff/dmifedorov) 2023-06-30 14:47:15+02:00

0.1.78
-----

[Alexander Ushakov](http://staff/alex-ushakov@yandex-team.ru) 2022-12-22 08:31:45+03:00

* BI-4655 Fix DLS issue in RLS integration test for yc-preprod




0.1.77
-----

[Alexander Ushakov](http://staff/alex-ushakov@yandex-team.ru) 2022-12-22 08:31:45+03:00

* BI-4376 Use sa created in terraform for integration tests



0.1.29
-----

[Alexander Ushakov](http://staff/alex-ushakov@yandex-team.ru) 2022-12-22 08:31:45+03:00

* BI-4259 Create and destroy workbooks in integration tests.



0.1.23
-----

[Alexander Ushakov](http://staff/alex-ushakov@yandex-team.ru) 2022-12-22 08:31:45+03:00

* BI-4260 Use special rls config syntax for sa subjects


0.1.17
-----

[Alexander Ushakov](http://staff/alex-ushakov@yandex-team.ru) 2022-12-22 08:31:45+03:00

* BI-4084 Feature DLS_ENABLED for toggling dls node creation in integration tests


0.1.16
-----

[Alexander Ushakov](http://staff/alex-ushakov@yandex-team.ru) 2022-12-22 08:31:45+03:00

* BI-4084 Remove obsolete user properties for dynamic env from integration test secrets


0.1.15
-----

[Alexander Ushakov](http://staff/alex-ushakov@yandex-team.ru) 2022-12-22 08:31:45+03:00

* BI-4084 Change auth method to service accounts for integration tests


0.1.11
-----

[Alexander Ushakov](http://staff/alex-ushakov@yandex-team.ru) 2022-11-24 08:31:45+03:00

* BI-4084 Feature configuration for reading secrets from a mounted file


0.1.0
-----

[Alexander Ushakov](http://staff/alex-ushakov@yandex-team.ru) 2022-11-24 08:31:45+03:00

* BI-4030 Feature docker-container and makefile for running standalone integration tests
