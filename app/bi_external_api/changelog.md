0.80.0
------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4616 Dedicated external-api for Ya-team  [ https://a.yandex-team.ru/arc/commit/12097943 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-04 12:28:38+00:00

0.79.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 21:35:01+00:00

0.78.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4623] Fix auth MWs list in ext-api for Nebius  [ https://a.yandex-team.ru/arc/commit/12089882 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 15:57:58+00:00

0.77.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 14:17:12+00:00

0.76.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 20:01:55+00:00

0.75.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4687] Add missing GRPC entrypoint to external API package  [ https://a.yandex-team.ru/arc/commit/12068901 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-3367 Fix grpc acceptance tests for bi-ext-api.  [ https://a.yandex-team.ru/arc/commit/12068464 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-01 17:38:11+00:00

0.74.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4687] Store generated proto stubs for DC API  [ https://a.yandex-team.ru/arc/commit/12067967 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed direct usage of get_secret from bi_external_api_tests  [ https://a.yandex-team.ru/arc/commit/12052985 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4684 Add workbook basic info to DCOpWorkbookGetResponse.  [ https://a.yandex-team.ru/arc/commit/12034931 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-01 16:06:34+00:00

0.73.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4718] New convention for docker context in bake  [ https://a.yandex-team.ru/arc/commit/12026607 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-26 23:27:57+00:00

0.72.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4718] Use base image bake target context in Dockerfile FROM  [ https://a.yandex-team.ru/arc/commit/12022471 ]

* [gstatsenko](http://staff/gstatsenko)

 * Fixed container host resolution in bi_external_api_tests  [ https://a.yandex-team.ru/arc/commit/12018180 ]

* [ovsds](http://staff/ovsds)

 * BI-4766: fix PyYaml dependencies  [ https://a.yandex-team.ru/arc/commit/12011297 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-26 15:46:59+00:00

0.71.0
------

* [konstasa](http://staff/konstasa)

 * BI-4727 Make RQE URL scheme configurable via env  [ https://a.yandex-team.ru/arc/commit/11988597 ]

* [vgol](http://staff/vgol)

 * BI-4726: More fixes + modified CI workflow to split tests between multiple jobs  [ https://a.yandex-team.ru/arc/commit/11984828 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-3367 Support chart filters in external API                         [ https://a.yandex-team.ru/arc/commit/11979831 ]
 * BI-4683 Add protospec for ListWorkbooksRequest/ListWorkbooksResponse  [ https://a.yandex-team.ru/arc/commit/11975942 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved testing folders from bi_core to bi_core_testing  [ https://a.yandex-team.ru/arc/commit/11977106 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-25 10:00:44+00:00

0.70.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-19 19:13:42+00:00

0.69.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-19 14:57:12+00:00

0.68.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-19 14:47:23+00:00

0.67.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-19 13:13:02+00:00

0.66.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4718] Generate proto specs & download to local machine & tier-1 for EXT API      [ https://a.yandex-team.ru/arc/commit/11953078 ]
 * [BI-4687] Generate antlr in Dockerfile.tier1                                         [ https://a.yandex-team.ru/arc/commit/11918817 ]
 * [BI-4623] External API in IL: initial bootstrap of app & schema                      [ https://a.yandex-team.ru/arc/commit/11771561 ]
 * [BI-4632] Extract DLRequestBase to bi_api_commons                                    [ https://a.yandex-team.ru/arc/commit/11730197 ]
 * [BI-4616] Extract interface for ParticularAPIOperationTranslator                     [ https://a.yandex-team.ru/arc/commit/11658118 ]
 * [BI-4542] Workaround to prevent saving ID formulas with title field refs: style fix  [ https://a.yandex-team.ru/arc/commit/11456820 ]

* [vgol](http://staff/vgol)

 * PR from branch users/vgol/BI-4549_rename_tests_dirs_ext_api

BI-4549: bi_external_api 2/2

BI-4549: bi_external_api 1/2  [ https://a.yandex-team.ru/arc/commit/11940575 ]
 * BI-4592: updated .release.hjson to use pyproject.toml instead of setup.py                                                [ https://a.yandex-team.ru/arc/commit/11909029 ]
 * BI-4592: re using old local dev, but supplying deps from pyproject toml files                                            [ https://a.yandex-team.ru/arc/commit/11908104 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-4593: Removed most of the 'secrets' fixtures               [ https://a.yandex-team.ru/arc/commit/11884899 ]
 * BI-4601: Removed US preparation from common_pytest_configure  [ https://a.yandex-team.ru/arc/commit/11830608 ]
 * Removed us container name from all tests and configs          [ https://a.yandex-team.ru/arc/commit/11809819 ]
 * Moved DynamicEnum to a separate library                       [ https://a.yandex-team.ru/arc/commit/11681164 ]

* [konstasa](http://staff/konstasa)

 * BI-4693 Remove materializer client, materialization enums, dataset-api materializer handlers      [ https://a.yandex-team.ru/arc/commit/11867004 ]
 * BI-4393 Remove access_mode, AccessModeManager, dataset_mode, MaterializationManager               [ https://a.yandex-team.ru/arc/commit/11849226 ]
 * BI-4630 BI-4249 Connection sections; CHYT group; remove ENABLED_BACKEND_DRIVEN_FORMS app setting  [ https://a.yandex-team.ru/arc/commit/11783814 ]

* [mcpn](http://staff/mcpn)

 * BI-4540: move RequestBootstrap and related middlewares to bi_api_commons  [ https://a.yandex-team.ru/arc/commit/11860977 ]
 * BI-4540: move some more aio middlewares to bi_api_commons                 [ https://a.yandex-team.ru/arc/commit/11840996 ]
 * BI-4540: move yc auth middlewares to bi_api_commons_ya_cloud              [ https://a.yandex-team.ru/arc/commit/11820848 ]
 * BI-4540: move blackbox middlewares to bi_api_commons_ya_team              [ https://a.yandex-team.ru/arc/commit/11797588 ]

* [dmifedorov](http://staff/dmifedorov)

 * hosts and ports from docker-compose in tests  [ https://a.yandex-team.ru/arc/commit/11774830 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-18 17:42:55+00:00

0.65.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4542] Workaround to prevent saving ID formulas with title field refs  [ https://a.yandex-team.ru/arc/commit/11429578 ]

* [gstatsenko](http://staff/gstatsenko)

 * CreateDSFrom as DynamicEnum; Implemented DynamicEnumField  [ https://a.yandex-team.ru/arc/commit/11425957 ]

* [konstasa](http://staff/konstasa)

 * BI-4389 Remove ConnectionInternalCH, leaving InternalMaterializationConnectionRef  [ https://a.yandex-team.ru/arc/commit/11377220 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-11 12:31:06+00:00

0.64.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-27 09:48:14+00:00

0.63.0
------

* [vgol](http://staff/vgol)

 * ORION-3023: modified nginx conf to support grpc proxy  [ https://a.yandex-team.ru/arc/commit/11370897 ]

* [kchupin](http://staff/kchupin)

 * [BI-4434] EA fix broken common exception handling pipeline       [ https://a.yandex-team.ru/arc/commit/11362006 ]
 * [BI-4508] Adopt to accidental int version field in chart config  [ https://a.yandex-team.ru/arc/commit/11361901 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-26 17:40:35+00:00

0.62.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-25 16:00:39+00:00

0.61.0
------

* [vgol](http://staff/vgol)

 * BI-4435: [EA] respect app config do_add_exc_message in WorkbookOperationErrorHandler  [ https://a.yandex-team.ru/arc/commit/11358605 ]
 * BI-4437: [EA] GRPC proxy to use ErrWorkbookOp structure in the grpc error details     [ https://a.yandex-team.ru/arc/commit/11336197 ]
 * BI-4436: [EA] Store request_id in ErrWorkbookOp                                       [ https://a.yandex-team.ru/arc/commit/11318000 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-25 15:54:20+00:00

0.60.0
------

* [vgol](http://staff/vgol)

 * BI-4418: [EA] allow empty connection secret, write logs on exceptions in the grpc service  [ https://a.yandex-team.ru/arc/commit/11233060 ]

* [kchupin](http://staff/kchupin)

 * [BI-4235] Hide CHYT data sources in DC API                                             [ https://a.yandex-team.ru/arc/commit/11211393 ]
 * [ORION-2810] Add fallbacks for service descriptions                                    [ https://a.yandex-team.ru/arc/commit/11185779 ]
 * [BI-4113] Integration test to check if there is no exc messages in errors in prod env  [ https://a.yandex-team.ru/arc/commit/11185658 ]
 * [BI-4408] ext-api DC prod integration tests                                            [ https://a.yandex-team.ru/arc/commit/11184641 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-18 15:14:15+00:00

0.59.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4342] Prettify request ID and Authorization header handling in GRPC proxy  [ https://a.yandex-team.ru/arc/commit/11182475 ]
 * [ORION-2921] Update DC preprod SA/project ID & add it for DC prod              [ https://a.yandex-team.ru/arc/commit/11179327 ]
 * [ORION-2810] Fix headers level in specs & text for service group index         [ https://a.yandex-team.ru/arc/commit/11175683 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-31 10:47:06+00:00

0.58.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4342] Fix RPC call logging `rpc_req_kind` field                                 [ https://a.yandex-team.ru/arc/commit/11170112 ]
 * [BI-4342] Validate workbook ID on tenant resolution phase to prevet path-traversal  [ https://a.yandex-team.ru/arc/commit/11170060 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-29 23:11:11+00:00

0.57.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3383] Handle unknown field IDs in charts                                       [ https://a.yandex-team.ru/arc/commit/11169540 ]
 * [BI-3383] Add all supported visualizations to complex workbook test                [ https://a.yandex-team.ru/arc/commit/11169312 ]
 * [BI-3383] Complex workbook example with ID formulas & most types of visualization  [ https://a.yandex-team.ru/arc/commit/11166946 ]
 * [ORION-2810] Integrate proto doc json generation into CLI tool for doc gen         [ https://a.yandex-team.ru/arc/commit/11166504 ]
 * [BI-4185] Fix bug with Chart.extraSettings deserialization                         [ https://a.yandex-team.ru/arc/commit/11160615 ]
 * [BI-4342] Fix error message for not existing workbooks                             [ https://a.yandex-team.ru/arc/commit/11160470 ]
 * [ORION-2810] CLI tool to generate docs                                             [ https://a.yandex-team.ru/arc/commit/11160420 ]
 * [ORION-2810] Add connection of visualization config & GRPC reference               [ https://a.yandex-team.ru/arc/commit/11154096 ]
 * [ORION-2810] Add index pages for service group to be refereced by API ref index    [ https://a.yandex-team.ru/arc/commit/11152952 ]
 * [BI-4235] Documentation for dash grid                                              [ https://a.yandex-team.ru/arc/commit/11147643 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4398 Move TenantDef and AuthData implementations to bi_api_commons.  [ https://a.yandex-team.ru/arc/commit/11155774 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-29 19:49:19+00:00

0.56.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4342] Do not set error in successfull operations  [ https://a.yandex-team.ru/arc/commit/11147247 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-27 15:07:31+00:00

0.55.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4235] Initial integration of JSON models docs into DC GRPC docs                                       [ https://a.yandex-team.ru/arc/commit/11146330 ]
 * [BI-3383] Switch DC to ID-only formulas & cut field ID gen suffix & ID formula conversion symmetry fixes  [ https://a.yandex-team.ru/arc/commit/11145858 ]
 * [BI-3383] Preliminary tests refactoring                                                                   [ https://a.yandex-team.ru/arc/commit/11145387 ]
 * [BI-3383] Fix tier-1 dependencies for local debug                                                         [ https://a.yandex-team.ru/arc/commit/11137185 ]
 * [BI-3383] Support guid_formula in the external domain                                                     [ https://a.yandex-team.ru/arc/commit/11137184 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-27 13:45:33+00:00

0.54.0
------

* [kchupin](http://staff/kchupin)

 * [ORION-2810] Fix unhandled `repeated` label                                [ https://a.yandex-team.ru/arc/commit/11126548 ]
 * [ORION-2810] API ref by GRPC specs: externalize models & add all services  [ https://a.yandex-team.ru/arc/commit/11092519 ]
 * [ORION-2810] Tool to generate API ref by GRPC specs                        [ https://a.yandex-team.ru/arc/commit/11067962 ]

* [mcpn](http://staff/mcpn)

 * BI-4377: Moved Clickhouse formula and db testing connectors to a separate library  [ https://a.yandex-team.ru/arc/commit/11122003 ]

* [vgol](http://staff/vgol)

 * BI-4342: [EA] Uniform err handling in the grpc proxy  [ https://a.yandex-team.ru/arc/commit/11037521 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added db_testing connector abstraction  [ https://a.yandex-team.ru/arc/commit/11025618 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4270 Move us_workbook_cmd_client to bi_us_client package.  [ https://a.yandex-team.ru/arc/commit/11018744 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-24 13:38:27+00:00

0.53.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4282] Resolve project ID by workbook ID in operation body instead of obligatory HTTP header  [ https://a.yandex-team.ru/arc/commit/11005967 ]

* [vgol](http://staff/vgol)

 * BI-4282: api fixes, mypy fixes  [ https://a.yandex-team.ru/arc/commit/11003970 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-08 14:18:23+00:00

0.52.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4282] Prettify EA GRPC proxy launcher  [ https://a.yandex-team.ru/arc/commit/10982201 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-03 17:52:03+00:00

0.51.0
------

* [vgol](http://staff/vgol)

 * BI-4282: fix run script: chmod +x, port env var name  [ https://a.yandex-team.ru/arc/commit/10979023 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-03 12:24:16+00:00

0.50.0
------

* [vgol](http://staff/vgol)

 * BI-4282: EA: Grpc proxy                                            [ https://a.yandex-team.ru/arc/commit/10973950 ]
 * BI-4237: EA integration tests without dep on bi_external_api code  [ https://a.yandex-team.ru/arc/commit/10884354 ]

* [kchupin](http://staff/kchupin)

 * [BI-4282] Tier 0 & 1 GRPC stubs generation for external API  [ https://a.yandex-team.ru/arc/commit/10968634 ]
 * [BI-4017] DoubleCloud GRPC API initial proposal              [ https://a.yandex-team.ru/arc/commit/10964223 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4281 feature bi_api_commons package  [ https://a.yandex-team.ru/arc/commit/10951763 ]

* [konstasa](http://staff/konstasa)

 * Ignore typing in external api settings  [ https://a.yandex-team.ru/arc/commit/10887820 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-4195: bi_api_lib pt 2  [ https://a.yandex-team.ru/arc/commit/10855847 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-02 18:19:57+00:00

0.49.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-10 10:11:43+00:00

0.48.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4222] Add option for force closing HTTP connections in internal API clients  [ https://a.yandex-team.ru/arc/commit/10829315 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-08 18:49:46+00:00

0.47.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4222] Fix local fixture for WorkbookOpsFacade                                        [ https://a.yandex-team.ru/arc/commit/10819960 ]
 * [BI-4103] Add description field for whole models & add it for unsupported visualization  [ https://a.yandex-team.ru/arc/commit/10788733 ]
 * [BI-4103] Remove CHYT connection types from DC model mapper                              [ https://a.yandex-team.ru/arc/commit/10786269 ]
 * [BI-4106] Fix redundant newlines & typos in operation descriptions.                      [ https://a.yandex-team.ru/arc/commit/10781591 ]

* [vgol](http://staff/vgol)

 * BI-4222: EA new connection creation op, DC true workbook support and fixtures rework  [ https://a.yandex-team.ru/arc/commit/10819726 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-07 19:45:40+00:00

0.46.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4106] Docs for rest of DC operations  [ https://a.yandex-team.ru/arc/commit/10780515 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-01 16:22:58+00:00

0.45.0
------

* [kchupin](http://staff/kchupin)

 * [BI-4106] Fixes of doc format according docs team recomedations                                      [ https://a.yandex-team.ru/arc/commit/10779728 ]
 * [BI-4185] Chart.colorsConfig = undefined if no config & required Chart.extraSettings                 [ https://a.yandex-team.ru/arc/commit/10778094 ]
 * [BI-4112] Ability to run docs generation locally                                                     [ https://a.yandex-team.ru/arc/commit/10772757 ]
 * [BI-4112] Update & fix tier-1 dependencies versions according A to make local installation possible  [ https://a.yandex-team.ru/arc/commit/10772550 ]
 * [BI-4112] Dedicated documentation for DC                                                             [ https://a.yandex-team.ru/arc/commit/10772001 ]
 * [BI-4112] Adopt RPC view for multiple operations set incl. DC                                        [ https://a.yandex-team.ru/arc/commit/10758328 ]
 * [BI-4113] Tune tests to ignore exc_message field in errors & add missing unit tests to ya.make       [ https://a.yandex-team.ru/arc/commit/10753377 ]
 * [BI-4112] Dedicated set of operations for DC                                                         [ https://a.yandex-team.ru/arc/commit/10735898 ]

* [asnytin](http://staff/asnytin)

 * BI-4225: remove bi-billing usages from bi-api  [ https://a.yandex-team.ru/arc/commit/10773596 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-01 14:58:26+00:00

0.44.0
------

* [vgol](http://staff/vgol)

 * BI-4113: Removed settings factory in ExternalAPISettings.DO_ADD_EXC_MESSAGE, now using False as fallback       [ https://a.yandex-team.ru/arc/commit/10713635 ]
 * BI-4113: do add exc message flag for ext api, disable for DC                                                   [ https://a.yandex-team.ru/arc/commit/10706291 ]
 * BI-4103 fix Settings class bi_external_api.settings.ExternalAPISettings has more than one app_type attributes  [ https://a.yandex-team.ru/arc/commit/10698903 ]

* [kchupin](http://staff/kchupin)

 * [DCDOCS-305] Fix tests after error messages correction                        [ https://a.yandex-team.ru/arc/commit/10705350 ]
 * [BI-4103] Default ext API type to CORE and require exact match for env value  [ https://a.yandex-team.ru/arc/commit/10704932 ]
 * [BI-4185] Clarifying errors for unsupported types of visualizations           [ https://a.yandex-team.ru/arc/commit/10704338 ]

* [annisu](http://staff/annisu)

 * DCDOCS-305 Update Doc strings for API

Translations for API descriptions.  [ https://a.yandex-team.ru/arc/commit/10702471 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-24 10:32:43+00:00

0.43.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-19 19:08:37+00:00

0.42.0
------

* [vgol](http://staff/vgol)

 * BI-4191:[EA] skip fields with none values  [ https://a.yandex-team.ru/arc/commit/10690002 ]
 * BI-4103: exp api model mapper per env      [ https://a.yandex-team.ru/arc/commit/10660422 ]

* [kchupin](http://staff/kchupin)

 * [BI-4185] Ability to run RO-tests against int preprod & fix int API CLI to send extra-headers  [ https://a.yandex-team.ru/arc/commit/10660360 ]
 * [BI-4185] Adopt missing acceptableValues in dash-api ManualControlSourceSelect                 [ https://a.yandex-team.ru/arc/commit/10643094 ]
 * [BI-4185] Optional acceptableValues in ManualControlSourceDate & new fields in dash root       [ https://a.yandex-team.ru/arc/commit/10642728 ]
 * [BI-4093] Fix EntrySummary generation in int API clients for true workbooks                    [ https://a.yandex-team.ru/arc/commit/10539672 ]
 * [BI-4092] ext-api app configuration & tests for IAM auth in DC                                 [ https://a.yandex-team.ru/arc/commit/10486583 ]
 * [BI-4098] Multilanguage description for models/fields/operations                               [ https://a.yandex-team.ru/arc/commit/10481893 ]

* [shadchin](http://staff/shadchin)

 * IGNIETFERRO-1154 Rename uwsgi to uWSGI  [ https://a.yandex-team.ru/arc/commit/10610666 ]

[robot-statinfra](http://staff/robot-statinfra) 2023-01-19 20:45:16+03:00

0.41.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3378] Generalising & parametrizing acceptance tests for different connection types  [ https://a.yandex-team.ru/arc/commit/10453980 ]
 * [BI-4093] Add support of true workbooks processing in WBOpsFacade                       [ https://a.yandex-team.ru/arc/commit/10451025 ]
 * [BI-4092] [EXT-API] YC auth middleware for AppType.DATA_CLOUD                           [ https://a.yandex-team.ru/arc/commit/10429870 ]
 * [BI-4092] Switch EA to new base HTTP client with support of auth in all envs            [ https://a.yandex-team.ru/arc/commit/10428490 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-08 15:53:50+00:00

0.40.0
------

* [vgol](http://staff/vgol)

 * BI-3811: External API: Support ignored connections in dashboard tabs  [ https://a.yandex-team.ru/arc/commit/10406542 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-28 12:34:13+03:00

0.39.0
------

* [mcpn](http://staff/mcpn)

 * BI-3731: add proper pickling for lambdas in clickhouse-sqlalchemy  [ https://a.yandex-team.ru/arc/commit/10386254 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-25 17:53:03+03:00

0.38.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-31 16:53:56+03:00

0.37.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-26 14:17:56+03:00

0.36.0
------

* [vgol](http://staff/vgol)

 * BI-3929: Fix dash models naming according FE team agreements

DashElementKind.widget_container -> DashElementKind.charts_container
DashWidgetContainer -> DashChartsContainer
DashWidgetContainer.default_widget_id -> DashChartsContainer.default_active_chart_tab_id  [ https://a.yandex-team.ru/arc/commit/10171789 ]
 * BI-3379: Add separated defaulter to dash converter

BI-3379: Separated defaulting step for Dashboard converter                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/10170073 ]

* [kchupin](http://staff/kchupin)

 * [BI-MAINTENANCE] External API docs for workbook clusterization operation  [ https://a.yandex-team.ru/arc/commit/10095738 ]

* [asnytin](http://staff/asnytin)

 * fixed typo in RawSQLLevel enum value  [ https://a.yandex-team.ru/arc/commit/10089214 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-19 15:58:30+03:00

0.35.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-28 16:30:16+03:00

0.34.0
------

* [gstatsenko](http://staff/gstatsenko)

 * Added core connector for Oracle  [ https://a.yandex-team.ru/arc/commit/10029840 ]

* [kchupin](http://staff/kchupin)

 * [BI-MAINTENANCE] Make-target for acceptance tests run in external API & fix unclosed session  [ https://a.yandex-team.ru/arc/commit/9999227 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-28 16:21:43+03:00

0.33.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-09 15:06:48+03:00

0.32.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-09 13:17:19+03:00

0.31.0
------

* [shadchin](http://staff/shadchin)

 * CONTRIB-756 Put sentry-sdk under yamaker pypi

<!-- add_enigma_sqs_monitoring_address_to_description:2909280 BEGIN -->
<a href="https://solomon.yandex-team.ru/?project=kikimr&cluster=sqs&service=kikimr_sqs&host=cluster&dashboard=SQS&l.user=edu-infra-ci-dev&l.queue=development-2909280&b=30m">YA SQS Monitoring</a>
<!-- add_enigma_sqs_monitoring_address_to_description:2909280 END -->

<!-- enigma-stand:2909280 BEGIN -->
<a href=https://arc-2909280.stands.enigma.education.yandex.net/swagger><img src=https://badger.education.yandex.net/custom/[enigma-stand:2909280][lightgrey]/[success][green]/badge.svg/></a> <a href=https://yd.yandex-team.ru/stages/arc-2909280-enigma-stand><img src=https://badger.education.yandex.net/custom/[deploy][grey]/badge.svg/></a>
<!-- enigma-stand:2909280 END -->
<!-- enigma-it-stand:2909280 BEGIN -->
<a href=https://yndx-edu.slack.com/archives/C021V5CSE8L><img src=https://badger.education.yandex.net/custom/[enigma-it-stand:2909280][lightgrey]/[deleted][ffaf80]/badge.svg/></a>
<!-- enigma-it-stand:2909280 END -->  [ https://a.yandex-team.ru/arc/commit/9953365 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-06 15:08:12+03:00

0.30.0
------

* [vgol](http://staff/vgol)

 * BI-3799: Added WorkbookNotFound    [ https://a.yandex-team.ru/arc/commit/9946839 ]
 * BI-3792: Dataset field validation  [ https://a.yandex-team.ru/arc/commit/9942800 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-31 21:29:38+03:00

0.29.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3696] Fix datasource type resolution ext->int for CHYT table range/list  [ https://a.yandex-team.ru/arc/commit/9933717 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-29 21:05:40+03:00

0.28.0
------

* [vgol](http://staff/vgol)

 * BI-3373: Added support for more chart types.                                                          [ https://a.yandex-team.ru/arc/commit/9905398 ]
 * BI-3750: Add type discriminator aliases for model descriptor and consider it during deserialization.  [ https://a.yandex-team.ru/arc/commit/9877832 ]

* [kchupin](http://staff/kchupin)

 * [BI-3373] Add highchartsId for visualizations where .id != .highchartsId (frontend set it only in wizard code)  [ https://a.yandex-team.ru/arc/commit/9893124 ]
 * [BI-3373] Add test for copying reference workbook (skipped by default)                                          [ https://a.yandex-team.ru/arc/commit/9893104 ]

* [mcpn](http://staff/mcpn)

 * BI-3686: use POST /private/getTenantsList  [ https://a.yandex-team.ru/arc/commit/9881477 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-24 16:11:38+03:00

0.27.0
------

* [vgol](http://staff/vgol)

 * BI-3365: Handle dataset validation error to make them more readable  [ https://a.yandex-team.ru/arc/commit/9853336 ]
 * BI-3371: Add support for treemap charts to ext api convertors.       [ https://a.yandex-team.ru/arc/commit/9852033 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-12 14:16:12+03:00

0.26.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3382] Handle missing default_value in old charts updates  [ https://a.yandex-team.ru/arc/commit/9827277 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-05 20:45:32+03:00

0.25.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3374] Fix non-specified type of default value in date_range/text_input selector  [ https://a.yandex-team.ru/arc/commit/9826924 ]
 * [BI-3382] Dataset parameters converter                                               [ https://a.yandex-team.ru/arc/commit/9825495 ]
 * [BI-3382] Internal domain model for dataset parameters                               [ https://a.yandex-team.ru/arc/commit/9823612 ]
 * [BI-3374] Ignore all undeclared keys in TabItem                                      [ https://a.yandex-team.ru/arc/commit/9812325 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-05 19:28:25+03:00

0.24.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3696] Support for CHYT table list/range  [ https://a.yandex-team.ru/arc/commit/9809053 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-02 21:31:58+03:00

0.23.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3364] Limit __repr__ of some errors to prevent logs blow-up        [ https://a.yandex-team.ru/arc/commit/9807368 ]
 * [BI-3374] `showTitle` in internal dash control source become optional  [ https://a.yandex-team.ru/arc/commit/9805283 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-02 17:23:01+03:00

0.22.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3374] datasetFieldType & fieldType in internal dash control source become optional  [ https://a.yandex-team.ru/arc/commit/9802682 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-01 20:55:28+03:00

0.21.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3692] Support name map in clusterization responses/errors                                                                   [ https://a.yandex-team.ru/arc/commit/9802237 ]
 * [BI-3692] WorkbookContextLoader.gather_workbook_by_dash() now returns original entry summaries in addition to WB ctx            [ https://a.yandex-team.ru/arc/commit/9800988 ]
 * [BI-3692] Workbook clusterization: ability to scan US folders for dashboards & WB entry name normalization to avoid duplicates  [ https://a.yandex-team.ru/arc/commit/9800409 ]
 * [BI-3692] Method to search all dashboards in US folders hierarchy                                                               [ https://a.yandex-team.ru/arc/commit/9798315 ]
 * [BI-3692] MiniUSClient.get_folder_content()                                                                                     [ https://a.yandex-team.ru/arc/commit/9798150 ]
 * [BI-3692] Fix preprod acceptance tests: client did not set secret for connection in `wb_create`                                 [ https://a.yandex-team.ru/arc/commit/9798148 ]
 * [BI-3374] Ignore `layout` key in TabItem                                                                                        [ https://a.yandex-team.ru/arc/commit/9798147 ]
 * [BI-3364] Simplify & harder operation root error handler                                                                        [ https://a.yandex-team.ru/arc/commit/9798144 ]

* [mcpn](http://staff/mcpn)

 * BI-3249: add ERR.DS_API prefix to errors from the error_registry  [ https://a.yandex-team.ru/arc/commit/9798595 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-01 19:38:07+03:00

0.20.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3692] DLSudo/DLAllowSuperUser headers passthrow to internal API clients                                                 [ https://a.yandex-team.ru/arc/commit/9792892 ]
 * [BI-3692] Initial implementation of US entry clusterization operation                                                       [ https://a.yandex-team.ru/arc/commit/9792434 ]
 * [BI-3364] Support of legacy charts without `datasetIds` prop & MM processor fix for None sequences & asserts clarification  [ https://a.yandex-team.ru/arc/commit/9790185 ]
 * [BI-3364] Fix status code for YAML content type response & allow unicode in YAML                                            [ https://a.yandex-team.ru/arc/commit/9789520 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-29 17:15:23+03:00

0.19.0
------

* [shashkin](http://staff/shashkin)

 * BI-3636: Change jaeger service names on testing                                 [ https://a.yandex-team.ru/arc/commit/9782112 ]
 * BI-3397: Rename DATETIMENAIVE to GENERICDATETIME and add tz changing functions  [ https://a.yandex-team.ru/arc/commit/9735405 ]
 * BI-3397: Introduce naive datetime type                                          [ https://a.yandex-team.ru/arc/commit/9668851 ]

* [kchupin](http://staff/kchupin)

 * [BI-3366] Fixes in measures coloring conversion: DRY & float thresholds support                                [ https://a.yandex-team.ru/arc/commit/9772036 ]
 * [BI-3366] Shapes support in charts                                                                             [ https://a.yandex-team.ru/arc/commit/9768769 ]
 * [BI-3366] Colors support in charts                                                                             [ https://a.yandex-team.ru/arc/commit/9768346 ]
 * [BI-3366] Resolve ad-hoc fields in charts before conversion instead of post-processing                         [ https://a.yandex-team.ru/arc/commit/9767956 ]
 * [BI-3366] Fix models for charts coloring                                                                       [ https://a.yandex-team.ru/arc/commit/9765762 ]
 * [BI-3366] Partial support of string mappings in MM field processor                                             [ https://a.yandex-team.ru/arc/commit/9765753 ]
 * [BI-3366] External & internal domain model for colors/shapes                                                   [ https://a.yandex-team.ru/arc/commit/9753966 ]
 * [BI-3366] Use field processor to set new data type/field type in chart ad-hoc fields                           [ https://a.yandex-team.ru/arc/commit/9743019 ]
 * [BI-3366] Support of dict fields in model mapper                                                               [ https://a.yandex-team.ru/arc/commit/9741628 ]
 * [BI-3374] Dash text input controls support                                                                     [ https://a.yandex-team.ru/arc/commit/9734301 ]
 * [BI-3375] Apply common WB context entry handling scheme for connections                                        [ https://a.yandex-team.ru/arc/commit/9734293 ]
 * [BI-3374] Support for custom comparison operations in dashboard controls                                       [ https://a.yandex-team.ru/arc/commit/9727602 ]
 * [BI-3374] Internal dash domain model: manual inputs & operations & full set of selector base field attributes  [ https://a.yandex-team.ru/arc/commit/9722515 ]
 * [BI-3370] Pivot tables support                                                                                 [ https://a.yandex-team.ru/arc/commit/9586466 ]
 * [BI-3364] Workbook validation errors processing in operation facade                                            [ https://a.yandex-team.ru/arc/commit/9453996 ]

* [konstasa](http://staff/konstasa)

 * BI-2372: Dataset field limit  [ https://a.yandex-team.ru/arc/commit/9596180 ]

* [asnytin](http://staff/asnytin)

 * BI-3499: connectors_data refactoring  [ https://a.yandex-team.ru/arc/commit/9492671 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-27 22:06:50+03:00

0.18.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3381] Support for title/text on dashboards                                                   [ https://a.yandex-team.ru/arc/commit/9443166 ]
 * [BI-3364] Preliminary refactoring: move converter-specific exceptions from base exc definitions  [ https://a.yandex-team.ru/arc/commit/9405943 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-11 19:29:09+03:00

0.17.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3364] Docs texts edits from TW                                                                                                [ https://a.yandex-team.ru/arc/commit/9383876 ]
 * [BI-3364] WIP: mechanism to store converions errors in workbook context                                                           [ https://a.yandex-team.ru/arc/commit/9358806 ]
 * [BI-3364] Preliminary refactoring: generalize in-memory plan apply procedure for chart/dash/dataset                               [ https://a.yandex-team.ru/arc/commit/9358307 ]
 * [BI-3364] First attempt to implement postponed errors in chart                                                                    [ https://a.yandex-team.ru/arc/commit/9342678 ]
 * [BI-3364] Converters error context control tooling & it initial usage in dash converter                                           [ https://a.yandex-team.ru/arc/commit/9336627 ]
 * [BI-3278] Docs for dataset refs in chart model                                                                                    [ https://a.yandex-team.ru/arc/commit/9323524 ]
 * [BI-3278] Builders for all ext entry types & whole workbook example in docs                                                       [ https://a.yandex-team.ru/arc/commit/9322488 ]
 * [BI-3278] Doc examples for all operation except workbook modify & read                                                            [ https://a.yandex-team.ru/arc/commit/9310725 ]
 * [BI-3278] Examples capabilities for AMM docs                                                                                      [ https://a.yandex-team.ru/arc/commit/9310011 ]
 * [BI-3278] Preliminary refactoring for examples in docs                                                                            [ https://a.yandex-team.ru/arc/commit/9309109 ]
 * [BI-3278] AMM field documentation capabilities                                                                                    [ https://a.yandex-team.ru/arc/commit/9307880 ]
 * [BI-3278] AMM documentation generation tooling & initial docs generation                                                          [ https://a.yandex-team.ru/arc/commit/9307606 ]
 * [BI-3278] AMM schema model (generic data model DSL) for documentation generation                                                  [ https://a.yandex-team.ru/arc/commit/9307497 ]
 * [BI-3278] AMM model descriptor refactoring                                                                                        [ https://a.yandex-team.ru/arc/commit/9306309 ]
 * [BI-3278] Acceptance tests against deployed preprod & fix missing secret value in WorkbookOpsClient client request serialization  [ https://a.yandex-team.ru/arc/commit/9306296 ]

* [shadchin](http://staff/shadchin)

 * IGNIETFERRO-1816 Switch on collections.abc  [ https://a.yandex-team.ru/arc/commit/9346792 ]

* [shashkin](http://staff/shashkin)

 * BI-3207: Add guid_formula  [ https://a.yandex-team.ru/arc/commit/9335165 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-26 13:12:40+03:00

0.16.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-03-31 20:24:32+03:00

0.15.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3278] Charts sort support & bug fixes: dash strict bool in range/multiselect; MyPy fixes  [ https://a.yandex-team.ru/arc/commit/9297619 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-31 19:57:13+03:00

0.14.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3278] Model bug fixes & logging improvements & load_only flag for secret data in requests  [ https://a.yandex-team.ru/arc/commit/9296814 ]
 * [BI-3278] Models & converter stubs for chart sorting/colors                                    [ https://a.yandex-team.ru/arc/commit/9296065 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-31 17:38:10+03:00

0.13.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3278] Remove useless className in ChartField internal model                            [ https://a.yandex-team.ru/arc/commit/9295232 ]
 * [BI-3278] id -> workbook_id in RPC RQ/RS                                                   [ https://a.yandex-team.ru/arc/commit/9295230 ]
 * [BI-3278] Support for measure names/measure values & chart/dash API bugfixes/bug-adoption  [ https://a.yandex-team.ru/arc/commit/9294806 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-31 14:00:27+03:00

0.12.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3278] Ignore all exceptions during workbook loading & fix ordering of load_fail_info in WB context  [ https://a.yandex-team.ru/arc/commit/9292622 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-30 20:00:08+03:00

0.11.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3278] Internal API clients error handling                                    [ https://a.yandex-team.ru/arc/commit/9292539 ]
 * [BI-3278] Field calc spec in external domain model to prevent repeatable one-of  [ https://a.yandex-team.ru/arc/commit/9291787 ]
 * [BI-3278] Datasource spec in external domain model to prevent repeatable one-of  [ https://a.yandex-team.ru/arc/commit/9291385 ]
 * [BI-3278] Store workbook loading exceptions                                      [ https://a.yandex-team.ru/arc/commit/9290737 ]
 * [BI-3278] Logging model mapper target class instantiation fails                  [ https://a.yandex-team.ru/arc/commit/9290315 ]
 * [BI-3278] ClickHouse support                                                     [ https://a.yandex-team.ru/arc/commit/9290006 ]
 * [BI-3278] Connection & workbook creation handles                                 [ https://a.yandex-team.ru/arc/commit/9289796 ]
 * [BI-3278] Add US mini client                                                     [ https://a.yandex-team.ru/arc/commit/9288226 ]
 * [BI-3278] Advise fields hanlde implementation                                    [ https://a.yandex-team.ru/arc/commit/9286850 ]
 * [BI-3278] Handling dataset validation errors at dataset API client level         [ https://a.yandex-team.ru/arc/commit/9286460 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-30 19:37:00+03:00

0.10.0
------

* [kchupin](http://staff/kchupin)

 * [BI-3278] Fix 500 on request scheme violation                                      [ https://a.yandex-team.ru/arc/commit/9284349 ]
 * [BI-3278] Add pass-throw of missing default in attrs model to MA field `required`  [ https://a.yandex-team.ru/arc/commit/9284073 ]
 * [BI-3278] Fix ColumnDiagram type discriminator                                     [ https://a.yandex-team.ru/arc/commit/9283534 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-29 11:00:53+03:00

0.9.0
-----

* [kchupin](http://staff/kchupin)

 * [BI-3278] Force rewrite mode support & bug fixes  [ https://a.yandex-team.ru/arc/commit/9282993 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-28 21:04:25+03:00

0.8.0
-----

* [kchupin](http://staff/kchupin)

 * [BI-3278] Grand selector refactoring: support for all dataset-based selectors                  [ https://a.yandex-team.ru/arc/commit/9280001 ]
 * [BI-3278] SingleOrMultiString struct                                                           [ https://a.yandex-team.ru/arc/commit/9278578 ]
 * [BI-3278] Debug mode for exception composer                                                    [ https://a.yandex-team.ru/arc/commit/9277148 ]
 * [BI-3278] Clarifying selector defaults validation                                              [ https://a.yandex-team.ru/arc/commit/9277119 ]
 * [BI-3278] Charts converter generalization & add support for charts: column, linear, indicator  [ https://a.yandex-team.ru/arc/commit/9276979 ]
 * [BI-3278] Light-weight way to launch acceptance tests                                          [ https://a.yandex-team.ru/arc/commit/9274367 ]
 * [BI-3278] CHYT user auth sources and some CHYT tests                                           [ https://a.yandex-team.ru/arc/commit/9273274 ]
 * [BI-3278] Integrate deep error handling mechanism into web app & fix some bugs in it           [ https://a.yandex-team.ru/arc/commit/9270667 ]
 * [BI-3278] Deep error handling mechanism                                                        [ https://a.yandex-team.ru/arc/commit/9270091 ]
 * [BI-3278] Error handling during workbook context loading                                       [ https://a.yandex-team.ru/arc/commit/9269442 ]
 * [BI-3278] Remove legacy workbook designer                                                      [ https://a.yandex-team.ru/arc/commit/9265035 ]
 * [BI-3278] Dashboards processing in ops facade                                                  [ https://a.yandex-team.ru/arc/commit/9264263 ]

* [shadchin](http://staff/shadchin)

 * IGNIETFERRO-1816 Switch on collections.abc  [ https://a.yandex-team.ru/arc/commit/9273606 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-28 13:34:54+03:00

0.7.0
-----

* [kchupin](http://staff/kchupin)

 * [BI-3278] Fix dataset creation result schema oredering issue & add modification plan to RPC model            [ https://a.yandex-team.ru/arc/commit/9261326 ]
 * [BI-3278] Passing revision ID to APIClientBIBackControlPlane.modify_dataset() from initially loaded dataset  [ https://a.yandex-team.ru/arc/commit/9259629 ]
 * [BI-3278] Preserving DTO class fields ordering in serialized API responses                                   [ https://a.yandex-team.ru/arc/commit/9259445 ]
 * [BI-3278] Entry summary instantiation simplification                                                         [ https://a.yandex-team.ru/arc/commit/9258917 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-22 19:59:29+03:00

0.6.0
-----

* [kchupin](http://staff/kchupin)

 * [BI-3278] Add endpoint codes for OpenTracing  [ https://a.yandex-team.ru/arc/commit/9258145 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-22 00:56:30+03:00

0.5.0
-----

* [kchupin](http://staff/kchupin)

 * [BI-3278] Enable Jaeger tracer  [ https://a.yandex-team.ru/arc/commit/9258051 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-21 23:43:04+03:00

0.4.0
-----

* [kchupin](http://staff/kchupin)

 * [BI-3278] Tracing & req ID passthroughs  [ https://a.yandex-team.ru/arc/commit/9258007 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-21 23:19:58+03:00

0.3.0
-----

* [kchupin](http://staff/kchupin)

 * [BI-3278] Remove required authorization in ping handle & add deploy opts to releaser config  [ https://a.yandex-team.ru/arc/commit/9257398 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-21 19:44:07+03:00

0.2.0
-----

* [kchupin](http://staff/kchupin)

 * [BI-3278] App package bootstrap                                                                                                                     [ https://a.yandex-team.ru/arc/commit/9256366 ]
 * [BI-3278] Add RPC style handle for workbook ops facade                                                                                              [ https://a.yandex-team.ru/arc/commit/9255381 ]
 * [BI-3278] Workbook ops facade ready for dataset & charts                                                                                            [ https://a.yandex-team.ru/arc/commit/9253543 ]
 * [BI-3278] WorkbookContext mutation methods                                                                                                          [ https://a.yandex-team.ru/arc/commit/9253118 ]
 * [BI-3278] Dataset API client PUT/DELETE methods                                                                                                     [ https://a.yandex-team.ru/arc/commit/9253092 ]
 * [BI-3278] Model fields processor initial implementation                                                                                             [ https://a.yandex-team.ru/arc/commit/9253088 ]
 * [BI-3278] Web app initial construction: workbook get handle + application middlewares                                                               [ https://a.yandex-team.ru/arc/commit/9241788 ]
 * [BI-3278] Mapping for external domain model                                                                                                         [ https://a.yandex-team.ru/arc/commit/9240967 ]
 * [BI-3226] Internal API clients refactoring                                                                                                          [ https://a.yandex-team.ru/arc/commit/9237322 ]
 * [BI-3226] Adding dashboards to workbook context                                                                                                     [ https://a.yandex-team.ru/arc/commit/9227920 ]
 * [BI-3136] Converting charts int to ext                                                                                                              [ https://a.yandex-team.ru/arc/commit/9227657 ]
 * [BI-3226] Dashboards int/ext domain model & converter                                                                                               [ https://a.yandex-team.ru/arc/commit/9225023 ]
 * [BI-3226] Model mapper: add ability to set children_type_discriminator_attr_name not only on root classes                                           [ https://a.yandex-team.ru/arc/commit/9221279 ]
 * [BI-3226] Add charts to WB context & froze + cleanup internal domain model of charts                                                                [ https://a.yandex-team.ru/arc/commit/9168236 ]
 * [BI-3226] Fix trailing comma for tuples in pretty repr                                                                                              [ https://a.yandex-team.ru/arc/commit/9168115 ]
 * [BI-3226] Model mapper: add support of optional list/list of optionals                                                                              [ https://a.yandex-team.ru/arc/commit/9167654 ]
 * [BI-3226] Model mapper: pretty repr                                                                                                                 [ https://a.yandex-team.ru/arc/commit/9167575 ]
 * [BI-3226] Initial adding of charts & dashboards to workbook context                                                                                 [ https://a.yandex-team.ru/arc/commit/9161373 ]
 * [BI-3226] Support serialization key overrides in model mapper (due to `from` keys in dash API)                                                      [ https://a.yandex-team.ru/arc/commit/9149762 ]
 * [BI-3226] Domain model & marshmallow field: mapping string -> (string|list[string]). Required for dashboard control item defaults.                  [ https://a.yandex-team.ru/arc/commit/9148529 ]
 * [BI-3136] Re-use InternalTestingInstallation.DATALENS_API_LB_MAIN_BASE_URL in bi_ext_api_int_preprod_bi_api_control_plane_client                    [ https://a.yandex-team.ru/arc/commit/9148359 ]
 * [BI-3136] Add defaulting to WorkbookDesigner.create_chart() & add tests real stand tests                                                            [ https://a.yandex-team.ru/arc/commit/9148080 ]
 * [BI-3136] Internal & external domain models for charts. External to internal charts converters initial implementation.                              [ https://a.yandex-team.ru/arc/commit/9143997 ]
 * [BI-3136] Add composition of Dataset & EntrySummary: DatasetInstance                                                                                [ https://a.yandex-team.ru/arc/commit/9127100 ]
 * [BI-3136] Internal dataset-api domain model packaging refactoring                                                                                   [ https://a.yandex-team.ru/arc/commit/9122245 ]
 * [BI-3136] Add ability to perform actions to existing datasets for dataset-api client                                                                [ https://a.yandex-team.ru/arc/commit/9121188 ]
 * [BI-3136] Froze all internal domain models & add field resolution methods for internal dataset                                                      [ https://a.yandex-team.ru/arc/commit/9118840 ]
 * [BI-3136] Add support for optional fields in marshmallow model mapper                                                                               [ https://a.yandex-team.ru/arc/commit/9116098 ]
 * [BI-3136] Add datasets to workbook context                                                                                                          [ https://a.yandex-team.ru/arc/commit/9115674 ]
 * [BI-3136] Add support for collections.abc.Sequence in model mapper                                                                                  [ https://a.yandex-team.ru/arc/commit/9105901 ]
 * [BI-3136] Preliminary refactoring of external dataset fields model                                                                                  [ https://a.yandex-team.ru/arc/commit/9102692 ]
 * [BI-3136] Model mapper: support enums serialization by value & containers nesting                                                                   [ https://a.yandex-team.ru/arc/commit/9066022 ]
 * [BI-2945] High-level workbook designer prototype                                                                                                    [ https://a.yandex-team.ru/arc/commit/9049955 ]
 * [BI-2945] Initial version of external domain model & round-trip converter to internal representation                                                [ https://a.yandex-team.ru/arc/commit/9032513 ]
 * [BI-2945] Ability to add source avatar without updating direct fields in result schema                                                              [ https://a.yandex-team.ru/arc/commit/8945300 ]
 * [BI-2945] Dedicated models for editable & read-only versions of internal field model                                                                [ https://a.yandex-team.ru/arc/commit/8944650 ]
 * [BI-2945] Initial implementation of internal dataset API client & models                                                                            [ https://a.yandex-team.ru/arc/commit/8925723 ]
 * [BI-2945] Introducing workbook context for external api & internal API client improvements                                                          [ https://a.yandex-team.ru/arc/commit/8922897 ]
 * [BI-2945] RQE fixtures for external API tests                                                                                                       [ https://a.yandex-team.ru/arc/commit/8922350 ]
 * [BI-2945] Model mapper: base class to declare pre_load logic in models & improved interface for models registration                                 [ https://a.yandex-team.ru/arc/commit/8922283 ]
 * [BI-2945] Fetching all connections from pseudo-workbook                                                                                             [ https://a.yandex-team.ru/arc/commit/8922263 ]
 * [BI-2945] Model mapper: bool fields support & preventing schemas linking with non-abstract parent classes & serialized type discriminator name fix  [ https://a.yandex-team.ru/arc/commit/8922254 ]
 * [BI-2945] Bootstrapping external API aiohttp app                                                                                                    [ https://a.yandex-team.ru/arc/commit/8879141 ]
 * [BI-2945] External API US-required tests bootstrap                                                                                                  [ https://a.yandex-team.ru/arc/commit/8871299 ]
 * [BI-2945] ModelMapper support of type discriminator in model class vars                                                                             [ https://a.yandex-team.ru/arc/commit/8859660 ]
 * [BI-2945] Naive implementation of tool for marshmallow scheme auto-generation from attrs classes                                                    [ https://a.yandex-team.ru/arc/commit/8844267 ]
 * [BI-2945] BI external API project bootstrap                                                                                                         [ https://a.yandex-team.ru/arc/commit/8825394 ]

* [konstasa](http://staff/konstasa)

 * [BI-3245] Combine req id and error handling middlewares in the RequestBootstrap and log ERR_CODE  [ https://a.yandex-team.ru/arc/commit/9224180 ]

* [shashkin](http://staff/shashkin)

 * BI-3259: Add CalculationSpec into BIField

WIP

Implemented CalculationSpec inside BIField  [ https://a.yandex-team.ru/arc/commit/9223724 ]
 * BI-3092: Add parameter field type                                                           [ https://a.yandex-team.ru/arc/commit/9071754 ]

* [shadchin](http://staff/shadchin)

 * IGNIETFERRO-1816 Switch on collections.abc  [ https://a.yandex-team.ru/arc/commit/9157895 ]
 * IGNIETFERRO-1816 Switch on collections.abc  [ https://a.yandex-team.ru/arc/commit/9145245 ]

* [asnytin](http://staff/asnytin)

 * BI-3148: bi_file_uploader app settings  [ https://a.yandex-team.ru/arc/commit/9145830 ]
 * BI-3148: bi_file_uploader app           [ https://a.yandex-team.ru/arc/commit/9101127 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-2962: Added tree str data type  [ https://a.yandex-team.ru/arc/commit/9123352 ]

* [dim-gonch](http://staff/dim-gonch)

 * MyPy | Remove fixtures call

В новой версии Pytest вызов фикстур явно - запрещен https://docs.pytest.org/en/latest/deprecations.html#calling-fixtures-directly
CROWDFUNDING-15  [ https://a.yandex-team.ru/arc/commit/8969717 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-21 16:42:05+03:00

0.1.0
-----

* [kchupin](http://staff/kchupin)

 * [BI-3278] Gunicorn entry point                                                                                                                      [ https://a.yandex-team.ru/arc_vcs/commit/e82f4920ba9730ba086da8e467fb6da377527789 ]
 * [BI-3278] Add RPC style handle for workbook ops facade                                                                                              [ https://a.yandex-team.ru/arc/commit/9255381 ]
 * [BI-3278] Workbook ops facade ready for dataset & charts                                                                                            [ https://a.yandex-team.ru/arc/commit/9253543 ]
 * [BI-3278] WorkbookContext mutation methods                                                                                                          [ https://a.yandex-team.ru/arc/commit/9253118 ]
 * [BI-3278] Dataset API client PUT/DELETE methods                                                                                                     [ https://a.yandex-team.ru/arc/commit/9253092 ]
 * [BI-3278] Model fields processor initial implementation                                                                                             [ https://a.yandex-team.ru/arc/commit/9253088 ]
 * [BI-3278] Web app initial construction: workbook get handle + application middlewares                                                               [ https://a.yandex-team.ru/arc/commit/9241788 ]
 * [BI-3278] Mapping for external domain model                                                                                                         [ https://a.yandex-team.ru/arc/commit/9240967 ]
 * [BI-3226] Internal API clients refactoring                                                                                                          [ https://a.yandex-team.ru/arc/commit/9237322 ]
 * [BI-3226] Adding dashboards to workbook context                                                                                                     [ https://a.yandex-team.ru/arc/commit/9227920 ]
 * [BI-3136] Converting charts int to ext                                                                                                              [ https://a.yandex-team.ru/arc/commit/9227657 ]
 * [BI-3226] Dashboards int/ext domain model & converter                                                                                               [ https://a.yandex-team.ru/arc/commit/9225023 ]
 * [BI-3226] Model mapper: add ability to set children_type_discriminator_attr_name not only on root classes                                           [ https://a.yandex-team.ru/arc/commit/9221279 ]
 * [BI-3226] Add charts to WB context & froze + cleanup internal domain model of charts                                                                [ https://a.yandex-team.ru/arc/commit/9168236 ]
 * [BI-3226] Fix trailing comma for tuples in pretty repr                                                                                              [ https://a.yandex-team.ru/arc/commit/9168115 ]
 * [BI-3226] Model mapper: add support of optional list/list of optionals                                                                              [ https://a.yandex-team.ru/arc/commit/9167654 ]
 * [BI-3226] Model mapper: pretty repr                                                                                                                 [ https://a.yandex-team.ru/arc/commit/9167575 ]
 * [BI-3226] Initial adding of charts & dashboards to workbook context                                                                                 [ https://a.yandex-team.ru/arc/commit/9161373 ]
 * [BI-3226] Support serialization key overrides in model mapper (due to `from` keys in dash API)                                                      [ https://a.yandex-team.ru/arc/commit/9149762 ]
 * [BI-3226] Domain model & marshmallow field: mapping string -> (string|list[string]). Required for dashboard control item defaults.                  [ https://a.yandex-team.ru/arc/commit/9148529 ]
 * [BI-3136] Re-use InternalTestingInstallation.DATALENS_API_LB_MAIN_BASE_URL in bi_ext_api_int_preprod_bi_api_control_plane_client                    [ https://a.yandex-team.ru/arc/commit/9148359 ]
 * [BI-3136] Add defaulting to WorkbookDesigner.create_chart() & add tests real stand tests                                                            [ https://a.yandex-team.ru/arc/commit/9148080 ]
 * [BI-3136] Internal & external domain models for charts. External to internal charts converters initial implementation.                              [ https://a.yandex-team.ru/arc/commit/9143997 ]
 * [BI-3136] Add composition of Dataset & EntrySummary: DatasetInstance                                                                                [ https://a.yandex-team.ru/arc/commit/9127100 ]
 * [BI-3136] Internal dataset-api domain model packaging refactoring                                                                                   [ https://a.yandex-team.ru/arc/commit/9122245 ]
 * [BI-3136] Add ability to perform actions to existing datasets for dataset-api client                                                                [ https://a.yandex-team.ru/arc/commit/9121188 ]
 * [BI-3136] Froze all internal domain models & add field resolution methods for internal dataset                                                      [ https://a.yandex-team.ru/arc/commit/9118840 ]
 * [BI-3136] Add support for optional fields in marshmallow model mapper                                                                               [ https://a.yandex-team.ru/arc/commit/9116098 ]
 * [BI-3136] Add datasets to workbook context                                                                                                          [ https://a.yandex-team.ru/arc/commit/9115674 ]
 * [BI-3136] Add support for collections.abc.Sequence in model mapper                                                                                  [ https://a.yandex-team.ru/arc/commit/9105901 ]
 * [BI-3136] Preliminary refactoring of external dataset fields model                                                                                  [ https://a.yandex-team.ru/arc/commit/9102692 ]
 * [BI-3136] Model mapper: support enums serialization by value & containers nesting                                                                   [ https://a.yandex-team.ru/arc/commit/9066022 ]
 * [BI-2945] High-level workbook designer prototype                                                                                                    [ https://a.yandex-team.ru/arc/commit/9049955 ]
 * [BI-2945] Initial version of external domain model & round-trip converter to internal representation                                                [ https://a.yandex-team.ru/arc/commit/9032513 ]
 * [BI-2945] Ability to add source avatar without updating direct fields in result schema                                                              [ https://a.yandex-team.ru/arc/commit/8945300 ]
 * [BI-2945] Dedicated models for editable & read-only versions of internal field model                                                                [ https://a.yandex-team.ru/arc/commit/8944650 ]
 * [BI-2945] Initial implementation of internal dataset API client & models                                                                            [ https://a.yandex-team.ru/arc/commit/8925723 ]
 * [BI-2945] Introducing workbook context for external api & internal API client improvements                                                          [ https://a.yandex-team.ru/arc/commit/8922897 ]
 * [BI-2945] RQE fixtures for external API tests                                                                                                       [ https://a.yandex-team.ru/arc/commit/8922350 ]
 * [BI-2945] Model mapper: base class to declare pre_load logic in models & improved interface for models registration                                 [ https://a.yandex-team.ru/arc/commit/8922283 ]
 * [BI-2945] Fetching all connections from pseudo-workbook                                                                                             [ https://a.yandex-team.ru/arc/commit/8922263 ]
 * [BI-2945] Model mapper: bool fields support & preventing schemas linking with non-abstract parent classes & serialized type discriminator name fix  [ https://a.yandex-team.ru/arc/commit/8922254 ]
 * [BI-2945] Bootstrapping external API aiohttp app                                                                                                    [ https://a.yandex-team.ru/arc/commit/8879141 ]
 * [BI-2945] External API US-required tests bootstrap                                                                                                  [ https://a.yandex-team.ru/arc/commit/8871299 ]
 * [BI-2945] ModelMapper support of type discriminator in model class vars                                                                             [ https://a.yandex-team.ru/arc/commit/8859660 ]
 * [BI-2945] Naive implementation of tool for marshmallow scheme auto-generation from attrs classes                                                    [ https://a.yandex-team.ru/arc/commit/8844267 ]
 * [BI-2945] BI external API project bootstrap                                                                                                         [ https://a.yandex-team.ru/arc/commit/8825394 ]

* [konstasa](http://staff/konstasa)

 * [BI-3245] Combine req id and error handling middlewares in the RequestBootstrap and log ERR_CODE  [ https://a.yandex-team.ru/arc/commit/9224180 ]

* [shashkin](http://staff/shashkin)

 * BI-3259: Add CalculationSpec into BIField

WIP

Implemented CalculationSpec inside BIField  [ https://a.yandex-team.ru/arc/commit/9223724 ]
 * BI-3092: Add parameter field type                                                           [ https://a.yandex-team.ru/arc/commit/9071754 ]

* [shadchin](http://staff/shadchin)

 * IGNIETFERRO-1816 Switch on collections.abc  [ https://a.yandex-team.ru/arc/commit/9157895 ]
 * IGNIETFERRO-1816 Switch on collections.abc  [ https://a.yandex-team.ru/arc/commit/9145245 ]

* [asnytin](http://staff/asnytin)

 * BI-3148: bi_file_uploader app settings  [ https://a.yandex-team.ru/arc/commit/9145830 ]
 * BI-3148: bi_file_uploader app           [ https://a.yandex-team.ru/arc/commit/9101127 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-2962: Added tree str data type  [ https://a.yandex-team.ru/arc/commit/9123352 ]

* [dim-gonch](http://staff/dim-gonch)

 * MyPy | Remove fixtures call

В новой версии Pytest вызов фикстур явно - запрещен https://docs.pytest.org/en/latest/deprecations.html#calling-fixtures-directly
CROWDFUNDING-15  [ https://a.yandex-team.ru/arc/commit/8969717 ]

[kchupin](http://staff/kchupin) 2022-03-21 15:27:23+03:00

