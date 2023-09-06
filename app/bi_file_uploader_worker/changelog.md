0.142.0
-------

* [Sergei Borodin](http://staff/seray@yandex-team.ru)

 * BI-4428 fix file-worker deps (#366)  [ https://github.com/datalens-tech/datalens-backend-private/commit/db51dd62 ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * some more mypy stubs and fixes for mypy sync tool (#362)  [ https://github.com/datalens-tech/datalens-backend-private/commit/c98cd2ac ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Taskfile cleanup (#359)  [ https://github.com/datalens-tech/datalens-backend-private/commit/7ca2daec ]

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version app/bi_file_secure_reader 0.14.0 (#363)  [ https://github.com/datalens-tech/datalens-backend-private/commit/246f33b6 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-06 15:19:59+00:00

0.141.0
-------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * move ACCESS_SERVICE_PERMISSIONS_CHECK_DELAY (#357)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/def5dfa2 ]
 * mypy fixes vol. 6 (#351)                                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/d4aa169e ]
 * move tvm.py from bi_core (#343)                                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/63170213 ]
 * BI-4852: postgresql mdb-specific bi api connector (#277)                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/2a6649a4 ]
 * BI-4860: CONNECTORS_DATA -> CONNECTORS (#340)                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/d6a0ca42 ]
 * cleanup enums (#331)                                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/c199b8df ]
 * unskip test_dataset_revision_id (#323)                                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/70646a0c ]
 * move metrica form rows to the connector package (#321)                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/d0cbea49 ]
 * BI-4834: move the yql bi api connectors to a separate package (#247)                      [ https://github.com/datalens-tech/datalens-backend-private/commit/8323adc8 ]
 * BI-4834: move the solomon bi api connector to a separate package (#243)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/12f30a09 ]
 * BI-4834: move the file bi api connectors to a separate package (#242)                     [ https://github.com/datalens-tech/datalens-backend-private/commit/8097203d ]
 * BI-4860: allow empty connector settings (#275)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/7ec4d370 ]
 * more mypy fixes for bi_api_lib (#255)                                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/ff09d700 ]
 * BI-4852: implement connectors whitelist (#251)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/3cb7ff52 ]
 * add grpc stubs to the mypy image (#252)                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/ac0fffaf ]
 * move the core solomon connector to a separate package (#232)                              [ https://github.com/datalens-tech/datalens-backend-private/commit/3579a91a ]
 * add more stubs packages (#235)                                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/5ed9bef3 ]
 * BI-4860: switch to the new connectors settings loading schema (#201)                      [ https://github.com/datalens-tech/datalens-backend-private/commit/e1ef68aa ]
 * BI-4834: move the postgresql and greenplum bi api connectors to separate packages (#219)  [ https://github.com/datalens-tech/datalens-backend-private/commit/624d2bde ]
 * BI-4834: move the clickhouse bi api connector to a separate library (#178)                [ https://github.com/datalens-tech/datalens-backend-private/commit/e24a7c8e ]
 * Move the clickhouse core connector to a separate package (#177)                           [ https://github.com/datalens-tech/datalens-backend-private/commit/d3949db0 ]
 * move sa_creds.py to bi_cloud_integration (#188)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/c5ddc5bf ]
 * Move the greenplum core connector to a separate package (#181)                            [ https://github.com/datalens-tech/datalens-backend-private/commit/feb99c5a ]
 * BI-4852: clean up comments in the postgresql adapter (#185)                               [ https://github.com/datalens-tech/datalens-backend-private/commit/4ff0e29e ]
 * BI-4860: load connectors settings using a dynamically generated class (#150)              [ https://github.com/datalens-tech/datalens-backend-private/commit/d0a942d9 ]
 * some mypy fixes in connectors (#161)                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/434d93c1 ]
 * even more mypy fixes (#154)                                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/07e33e4b ]
 * BI-4664: remove dls client from a common SR (#130)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/1b2990ab ]
 * remove explain query (#128)                                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/d2c373d5 ]
 * BI-4860: connector settings fallback registry (#119)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/48f0f80b ]
 * mark failing oracle test (#110)                                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/cc953d25 ]
 * move metrica connector aliases to the connector package (#111)                            [ https://github.com/datalens-tech/datalens-backend-private/commit/d7bf72e6 ]
 * BI-4860: connector settings registry (#80)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/ae48ac53 ]
 * some more mypy fixes (#93)                                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/96fa4426 ]
 * BI-4860: generate ConnectorsSettingsByType by settings registry (#88)                     [ https://github.com/datalens-tech/datalens-backend-private/commit/c531841a ]
 * fix typing (#91)                                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/0d95eb34 ]
 * remove old bitrix conncetor settings (#76)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/197ff7d8 ]
 * BI-4834: move the gsheets bi api connector to a separate package (#71)                    [ https://github.com/datalens-tech/datalens-backend-private/commit/7c8eb901 ]
 * BI-4834: move chyt bi api connectors to separate packages (#50)                           [ https://github.com/datalens-tech/datalens-backend-private/commit/da598318 ]
 * fix mypy in the monitoring connector (#59)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/d1270b43 ]
 * replace most do_xxx classvars with regulated marks in core tests (#36)                    [ https://github.com/datalens-tech/datalens-backend-private/commit/e9a04870 ]
 * remove unused file (#51)                                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/9592af64 ]
 * remove method_not_implemented (#48)                                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/312cd5c0 ]
 * BI-4834: move the bitrix bi api connector to a separate package (#46)                     [ https://github.com/datalens-tech/datalens-backend-private/commit/088d08a8 ]
 * enable more tests in ci (#41)                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/2598b2b0 ]
 * remove remaining setup pys (#42)                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/8b23500d ]
 * small fixes for mypy (#37)                                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/9981e20c ]
 * BI-4834: move metrica bi api connectors to a separate package (#27)                       [ https://github.com/datalens-tech/datalens-backend-private/commit/ba21b479 ]
 * BI-4817: move some dashsql tests from bi api to connectors packages (#31)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/8b2af588 ]
 * BI-4834: move the monitoring bi api connector to a separate package (#23)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/a20b8724 ]

* [Sergei Borodin](http://staff/seray@yandex-team.ru)

 * BI-4428 adding an ability to connect to security-reader via TCP (#346)  [ https://github.com/datalens-tech/datalens-backend-private/commit/1be37cd6 ]
 * BI-4884 rootless yc-dls (#257)                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/4b419ad0 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Added bake for doc generation (#353)                                                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/4c5cb1e4 ]
 * Some renamings in dl-repmanager (#355)                                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/13da9dd3 ]
 * Correct generation of localizations for all packages (#352)                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/54969d19 ]
 * Removed env-dependent scopes from formula_ref (#338)                                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/726e0b59 ]
 * Created empty package for formula testing tools (#246)                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/e72b3057 ]
 * Normalized dependencies for connectors and some other libraries (#290)                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/e9fdf9c4 ]
 * Moved diff_utils to bi_maintenance (#330)                                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/b0b35d6f ]
 * Moved maintenance tools and crawlers to bi_maintenance (#293)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/1072a661 ]
 * Partailly normalized bi_core dependencies (#291)                                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/62db91ba ]
 * Added a simple test to dl_repmanager (#308)                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/971790c2 ]
 * Removed ArcadiaFileLoader from bi_testing (#294)                                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/f8373c2a ]
 * Implemented virtual FS editor and dry run for repmanager (#287)                                         [ https://github.com/datalens-tech/datalens-backend-private/commit/78f6aa8d ]
 * Switching to fully managed FS access in repmanager (#278)                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/f326b39a ]
 * Fixes for connectorization of bi_formula_ref (#267)                                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/3f5c290c ]
 * Normalized bi_api_lib's requirements (but without test deps) (#274)                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/e36bc968 ]
 * Normalized bi_api_commons' requirements (#270)                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/6805eb30 ]
 * Normalized bi_api_connector requirements by moving ConnectionFormTestBase to bi_api_lib_testing (#271)  [ https://github.com/datalens-tech/datalens-backend-private/commit/b3a71a13 ]
 * Added path validation to fs editor (#259)                                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/b11a2583 ]
 * Normalized bi_api_client's requirements (#269)                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/db39ebcb ]
 * Switched to prproject.toml-compatible format in requirement check (#273)                                [ https://github.com/datalens-tech/datalens-backend-private/commit/fb1ae2dd ]
 * Implemented renaming of packages (#244)                                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/532d804b ]
 * Removed custom test dependency sections (#248)                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/d1aee372 ]
 * Switched back to git in repmanager, but without the cp command (#237)                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/1cf900f1 ]
 * Cleaned up some legacy stuff in makefiles (#228)                                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/6280b7f0 ]
 * Removed dist-info from some packages (#236)                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/eba1d5c6 ]
 * Switched dl-repmanager to default fs editor (#229)                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/d833b265 ]
 * Moved testenv-common to mainrepo (#227)                                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/a49028b9 ]
 * BI-4894: Moved libraries to mainrepo (#223)                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/b36037b4 ]
 * Moved bi_connector_postgresql to mainrepo (#215)                                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/7acd0aeb ]
 * Fix package moving (#214)                                                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/e7160c91 ]
 * Fixed unregistration of packages (#209)                                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/86d147bc ]
 * Added ch-package-type to repmanager (#198)                                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/8c6ae180 ]
 * Fully connectorized bi_formula_ref and reversed its dependencies (#183)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/eb377759 ]
 * Removed empty modules from bi_testing (#172)                                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/73b57b5a ]
 * Moved TestClientConverterAiohttpToFlask from bi_testing_ya to bi_api_lib_testing (#175)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/15d0d59a ]
 * BI-4150: Implemented optimization of IF and CASE functions with constant conditions (#148)              [ https://github.com/datalens-tech/datalens-backend-private/commit/bb1bdbf1 ]
 * BI-4703: Partial implementation of data source migration for connection replacement (#19)               [ https://github.com/datalens-tech/datalens-backend-private/commit/d14823c7 ]
 * Moved dialect support settings to formula-ref configs (#143)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/0fb32cc0 ]
 * Removed and refactored some legacy core data source tests (#53)                                         [ https://github.com/datalens-tech/datalens-backend-private/commit/7a7df160 ]
 * Connectorized DialectName (#147)                                                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/34332734 ]
 * Added recursive config discovery to dl-repomanager (#171)                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/6ebc5ed0 ]
 * Mypy fixes in bi_formula (#159)                                                                         [ https://github.com/datalens-tech/datalens-backend-private/commit/ad10f867 ]
 * Moved remaining PG constant declarations to connector (#145)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/de502795 ]
 * Added creation of an empty table in oracle db to fix template test (#122)                               [ https://github.com/datalens-tech/datalens-backend-private/commit/cf7e241c ]
 * Some fixes for mypy (#129)                                                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/940857b9 ]
 * Moved formula_ref configs to separate packages (#133)                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/996b3d81 ]
 * Switched to short locales in formula_ref and fixed locales in clickhouse connector (#90)                [ https://github.com/datalens-tech/datalens-backend-private/commit/f71f6b75 ]
 * Added ConvertBlocksToFunctionsMutation for converting IF and CASE blocks to functions (#24)             [ https://github.com/datalens-tech/datalens-backend-private/commit/66adc9db ]
 * Added mypy task (#125)                                                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/9f56a555 ]
 * Added docker-remote and docker-reinstall commands to taskfile (#106)                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/83bd071b ]
 * Moved the boilerplate package to mainrepo (#102)                                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/4051ebae ]
 * Introduced taskfiles (#86)                                                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/d04e8219 ]
 * Moved dl_repmanager to mainrepo (#92)                                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/687c59f5 ]
 * BI-4863: Added clickhouse formula_ref plugin (#74)                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/2ade1400 ]
 * Fixed bake targets for i18n (#75)                                                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/d15fa6f6 ]
 * Added --no-location option to .po generator (#83)                                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/54020f42 ]
 * Refactored update-po and msfmt common makefile targets (#67)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/45db6d0d ]
 * Added make remote-shell command (#49)                                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/321c8af8 ]
 * Added registrations to formula_ref plugins and switched to new localization logic (#34)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/4cfe5077 ]
 * Removed empty bi_core/i18n/localizer_base.py (#35)                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/6b38614f ]
 * Added the mainrepo dir (#32)                                                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/08cc8dab ]
 * Some mypy fixes for bi_i18n,  bi_core and connectors (#28)                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/7927363f ]
 * git-related fixes for repmanager (#30)                                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/7898fcf1 ]
 * Fixed i18n import in monitoring connector (#29)                                                         [ https://github.com/datalens-tech/datalens-backend-private/commit/6804cdc1 ]
 * Removed some unused attributes and methods from USEntry (#14)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/82ca37eb ]
 * Added the option of using whitelists in connector registration (#18)                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/cbbcf252 ]
 * Removed __init__ from the root dir (#26)                                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/ad1f2f72 ]
 * Added formula_ref plugins (#20)                                                                         [ https://github.com/datalens-tech/datalens-backend-private/commit/c1fa53df ]
 * Moved some modules from bi_testing to bi_testing_ya (#22)                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/808304f0 ]
 * Implemented regulated test mechanism (#21)                                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/a3e18162 ]
 * Partially moved some more constants to connectors (#12)                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/84579681 ]
 * Added bi_i18n package (#10)                                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/21fcbf82 ]
 * Added .mo files to bi_formula_ref package (#15)                                                         [ https://github.com/datalens-tech/datalens-backend-private/commit/af79cd6d ]
 * Added __pycache__ to .gitignore (#13)                                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/864e9cb6 ]

* [Max Zuev](http://staff/mail@thenno.me)

 * BI-4359: move bi_api_lib tests to legacy bundle (#322)          [ https://github.com/datalens-tech/datalens-backend-private/commit/205b6109 ]
 * Enable tests in legacy bundle (#326)                            [ https://github.com/datalens-tech/datalens-backend-private/commit/76e7240d ]
 * BI-4359: fast fix app configs (it should fix the build) (#262)  [ https://github.com/datalens-tech/datalens-backend-private/commit/78711a8a ]
 * Remove bi_test_project_task_interface (#205)                    [ https://github.com/datalens-tech/datalens-backend-private/commit/78cb3ebb ]
 * BI-4359: don't use defaults from libs (#182)                    [ https://github.com/datalens-tech/datalens-backend-private/commit/da152ccf ]
 * BI-4359: return back nebius defaults to bi-configs (#164)       [ https://github.com/datalens-tech/datalens-backend-private/commit/4714d557 ]
 * BI-4359: delete old tooling and defaults (#155)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/6af6ee18 ]
 * BI-4359: move old defaults to new package (#99)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/f0b18311 ]
 * Try to fix ci tests (delete terrarium from root level)          [ https://github.com/datalens-tech/datalens-backend-private/commit/7053fc54 ]

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * BI-4905 Split bi_api_commons into public and private parts (#311)                         [ https://github.com/datalens-tech/datalens-backend-private/commit/d5fc8a87 ]
 * Fix import in chyt internal connector tests (#334)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/96a27451 ]
 * BI-4898 Separate base bi-api app factories (#288)                                         [ https://github.com/datalens-tech/datalens-backend-private/commit/ca3234ca ]
 * BI-4901 Separate file-uploader-* base app settings (#299)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/43eeff3a ]
 * Add missing dependencies to app/bi_api (#283)                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/74c75467 ]
 * Update old connector availability configs (#280)                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/2ceaead3 ]
 * BI-4899 Tags instead of APP_TYPE to control function availability (#264)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/9c96e0a3 ]
 * BI-4901 Separate base app settings for file-uploader-api (#240)                           [ https://github.com/datalens-tech/datalens-backend-private/commit/026ab893 ]
 * Remove some private stuff from base connection forms (#263)                               [ https://github.com/datalens-tech/datalens-backend-private/commit/b1b30379 ]
 * Remove hardcoded relative env file path in OsEnvParamGetter (#239)                        [ https://github.com/datalens-tech/datalens-backend-private/commit/cb820a52 ]
 * BI-4229 Move connector availability into configs (#156)                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/88a7aec2 ]
 * BI-4898 Separate base app settings for bi-api (#238)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/312acf24 ]
 * Fix .po generation for non-connnector packages (#254)                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/98dee9a5 ]
 * BI-4800 Remove some usages of YENV_TYPE (#114)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/86ba0002 ]
 * Always use env in MDBDomainManager instead of a hardcoded config (#192)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/40faee4d ]
 * Remove preprod ig configs (#170)                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/534386e9 ]
 * BI-4801 WIP Remove some usages of YENV_NAME (#117)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/dc04f01a ]
 * Add dev-machine secrets sync to the taskfile (#127)                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/ece8e9e0 ]
 * Sync secrets from yav to dev machine on secrets-update for local ext tests runs (#123)    [ https://github.com/datalens-tech/datalens-backend-private/commit/5236bc0f ]
 * Enable bitrix ext tests in CI (#77)                                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/d7b7cf15 ]
 * BI-4692 Comment out unconfigured ext tests instead of a whitelist in test splitter (#69)  [ https://github.com/datalens-tech/datalens-backend-private/commit/00af47fd ]
 * Remove YC file-uploader[-worker] meta packages (#68)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/790186a3 ]
 * Remove OS meta packages (#63)                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/7766dbd6 ]
 * Remove YC bi-api meta packages (#60)                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/d59f021d ]
 * BI-4692 GitHub Actions secrets for ext tests (#44)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/4271639f ]
 * Fix bitrix_gds bi-api-connector dependency (#66)                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/a1f35b9d ]
 * Update Kontur.Market texts; fix translations generation (#47)                             [ https://github.com/datalens-tech/datalens-backend-private/commit/338074a2 ]
 * Remove the remaining gen-parser prerequisites from Makefiles                              [ https://github.com/datalens-tech/datalens-backend-private/commit/380fbd87 ]

* [Konstantin Chupin](http://staff/91148200+ya-kc@users.noreply.github.com)

 * [BI-4902] Move `bi-connector-solomon` from bi-api-lib to bi-api (path fix) (#350)             [ https://github.com/datalens-tech/datalens-backend-private/commit/0c6b5efe ]
 * [BI-4902] Move `bi-connector-solomon` from bi-api-lib to bi-api (#349)                        [ https://github.com/datalens-tech/datalens-backend-private/commit/8602265a ]
 * [BI-4902] Tool to compare resolved depdendencies for apps (#345)                              [ https://github.com/datalens-tech/datalens-backend-private/commit/196cedfa ]
 * [BI-4902] Move OS data API to main repo (#319)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/3c1d36fe ]
 * [BI-4902] Move OS control API to main repo (#318)                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/cb29e57e ]
 * [BI-4902] Cleanup metapkg scope-shrinker & fix transition to pathlib (#310)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/19f09dda ]
 * [BI-4902] Move antlr code-gen to main repo (#301)                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/b638276a ]
 * [BI-4902] Initial implementation of OS metapkg sync (#265)                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/ae74f540 ]
 * [BI-4902] Cleanup BQ crunches in meta pkg (#260)                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/8d748410 ]
 * [BI-4902] Sort dependencies in meta-package by groups according to policy (#258)              [ https://github.com/datalens-tech/datalens-backend-private/commit/22b92ab8 ]
 * [BI-4830] Remove manual installation of BQ (#221)                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/caed181d ]
 * [BI-4894] Yet another fix of testenv-common/images (#233)                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/b9ce8dd3 ]
 * [BI-4894] Sync local dev 3rd party deps (#230)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/0d6eab5f ]
 * [BI-4894] Fix version bump actions (#226)                                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/12e48d76 ]
 * [BI-4830] Fix BQ dependencies (#213)                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/205fb59d ]
 * [BI-4830] Fix BQ listing in all_packages.lst (#210)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/fafe5f3b ]
 * [BI-4830] Moved bi_connector_bigquery to mainrepo (#200)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/99e4793f ]
 * [BI-4830] Remove tier-0 build rudiments (entry points) (#199)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/5fc27056 ]
 * [BI-4830] Remove tier-0 build rudiments (#196)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/cc681a89 ]
 * [BI-4830] Cleanup bi-api image before moving libs to mainrepo (#194)                          [ https://github.com/datalens-tech/datalens-backend-private/commit/059aae55 ]
 * [BI-4632] Externalize tenant resolution in public US workaround middleware & FU tasks (#158)  [ https://github.com/datalens-tech/datalens-backend-private/commit/b799e05e ]
 * [BI-4830] Adopt mainrepo/libs to all Docker builds (#105)                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/44459fff ]
 * [BI-4830] Parametrize base rootless image at bake level (#98)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/6dfdb3c7 ]
 * [BI-4830] Version bump PR auto-merge (#84)                                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/363f45f1 ]
 * [BI-4830] Version bump for integration tests fix (#57)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/c4188eef ]
 * [BI-4830] Version bump for integration tests (#56)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/56fcecee ]
 * [BI-4830] Fix release PR branch name (#52)                                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/24eeb5d2 ]
 * [BI-4830] Add dedicated dir for CI artifacts to .gitignore (#43)                              [ https://github.com/datalens-tech/datalens-backend-private/commit/a787a30e ]
 * [BI-4830] Fix make pycharm (#11)                                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/bc7a3ab6 ]
 * [BI-4830] Remove GH sync tools (#9)                                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/88c518a7 ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * Removing workaround  for different docker-compose in arc ci and gh ci. (#341)                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/490493ed ]
 * GH_CI_mypy_job_fix (#316)                                                                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/e468d967 ]
 * pathlib-in-dl-repmanager instead of plain str (#317)                                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/607f4eb8 ]
 * fix_mypy_bi_ci (#320)                                                                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/1eda379b ]
 * GH_CI up_gh_ci_actions_checkout (#309)                                                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/3ecafe1d ]
 * GH_CI_router_and_split_on_light_runners (#305)                                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/59da4c1b ]
 * Modified GH CI test targets discovery logic to also search in mainrepo/terrarium (#307)                             [ https://github.com/datalens-tech/datalens-backend-private/commit/adc314a1 ]
 * single mypy (#296)                                                                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/57787881 ]
 * added script to distribute common mypy settings across repo (#286)                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/4b345470 ]
 * replaced plain str with pathlib.Path in dl_repmanager (#261)                                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/e8cc05bf ]
 * custom runner for mypy, repsects [datalens.meta.mypy] > targets section in the sub project's pyproject.toml (#253)  [ https://github.com/datalens-tech/datalens-backend-private/commit/f51fd03b ]
 * fix Path wrap (#245)                                                                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/90f21e7a ]
 * Added script to sync mypy annotation requirements to 3rd party requirements. (#234)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/add228a4 ]
 * gh-ci-cancel-in-progress-on-pr-update (#204)                                                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/c4402639 ]
 * build did image (#176)                                                                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/6f04fc33 ]
 * Moving from shell hell to bake new world (#120)                                                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/194c8cd0 ]
 * temporary disable ext private jobs, sunce runners are offline (#162)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/659ade9d ]
 * workaround (#160)                                                                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/8324a4a7 ]
 * split ext tests into private and public (#153)                                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/c1929610 ]
 * workaround to have working gha builder image (#152)                                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/44d40064 ]
 * dedicated img for mypy (#139)                                                                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/cc7cd018 ]
 * minor mypy fix (#136)                                                                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/32e8f8db ]
 * try more mypy on gh build step (#135)                                                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/5cc954fb ]
 * fix_main_flow_git_fetch_trunk (#112)                                                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/0c4a3702 ]
 * moved terrarium/bi_ci into mainrepo/ (#82)                                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/1dc7e253 ]
 * enable_ext_tag_in_workflow (#79)                                                                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/8b5e3b5b ]
 * gh ci option to run only mypy tests (#55)                                                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/b0d84f36 ]
 * GH CI fixes: Skip publish if not executed pytests (#33)                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/94232ef4 ]
 * Detect which files was changed between pr branch and base (#25)                                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/412a2e04 ]
 * placeholder workflow for simpler creation of new flow and avoid confusion in running flows list (#17)               [ https://github.com/datalens-tech/datalens-backend-private/commit/90d8b6a9 ]

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version mainrepo/lib/bi_api_lib 0.1989.0 (#339)         [ https://github.com/datalens-tech/datalens-backend-private/commit/c477e28d ]
 * releasing version mainrepo/lib/bi_api_lib 0.1988.0 (#306)         [ https://github.com/datalens-tech/datalens-backend-private/commit/511665c5 ]
 * releasing version mainrepo/lib/bi_api_lib 0.1987.0 (#304)         [ https://github.com/datalens-tech/datalens-backend-private/commit/97649be2 ]
 * releasing version mainrepo/lib/bi_api_lib 0.1986.0 (#302)         [ https://github.com/datalens-tech/datalens-backend-private/commit/cea91b7c ]
 * releasing version mainrepo/lib/bi_api_lib 0.1985.0 (#298)         [ https://github.com/datalens-tech/datalens-backend-private/commit/9ad4288a ]
 * releasing version mainrepo/lib/bi_api_lib 0.1984.0 (#292)         [ https://github.com/datalens-tech/datalens-backend-private/commit/74808e53 ]
 * releasing version mainrepo/lib/bi_api_lib 0.1983.0 (#282)         [ https://github.com/datalens-tech/datalens-backend-private/commit/addea43b ]
 * releasing version mainrepo/lib/bi_api_lib 0.1982.0 (#276)         [ https://github.com/datalens-tech/datalens-backend-private/commit/22bc332b ]
 * releasing version mainrepo/lib/bi_api_lib 0.1981.0 (#268)         [ https://github.com/datalens-tech/datalens-backend-private/commit/d2793c02 ]
 * releasing version mainrepo/lib/bi_api_lib 0.1980.0 (#250)         [ https://github.com/datalens-tech/datalens-backend-private/commit/4b5e4d64 ]
 * releasing version lib/bi_file_uploader_worker_lib 0.140.0 (#208)  [ https://github.com/datalens-tech/datalens-backend-private/commit/03f15ebc ]
 * releasing version lib/bi_file_uploader_worker_lib 0.139.0 (#190)  [ https://github.com/datalens-tech/datalens-backend-private/commit/117cb4a4 ]
 * releasing version lib/bi_file_uploader_worker_lib 0.138.0 (#186)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e362e09b ]
 * releasing version lib/bi_file_uploader_api_lib 0.115.0 (#184)     [ https://github.com/datalens-tech/datalens-backend-private/commit/c5ee7694 ]
 * releasing version lib/bi_file_uploader_worker_lib 0.137.0 (#174)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e01d0a10 ]
 * releasing version lib/bi_file_uploader_api_lib 0.114.0 (#173)     [ https://github.com/datalens-tech/datalens-backend-private/commit/5fd20e93 ]
 * releasing version app/bi_file_secure_reader 0.13.0 (#167)         [ https://github.com/datalens-tech/datalens-backend-private/commit/a7446079 ]
 * releasing version lib/bi_file_uploader_worker_lib 0.136.0 (#166)  [ https://github.com/datalens-tech/datalens-backend-private/commit/6a012d79 ]
 * releasing version lib/bi_file_uploader_api_lib 0.113.0 (#165)     [ https://github.com/datalens-tech/datalens-backend-private/commit/db4174af ]
 * releasing version lib/bi_api_lib 0.1979.0 (#132)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/74689b23 ]
 * releasing version lib/bi_api_lib 0.1978.0 (#124)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/f5571771 ]
 * releasing version ops/bi_integration_tests 0.20.0 (#118)          [ https://github.com/datalens-tech/datalens-backend-private/commit/b896c799 ]
 * releasing version lib/bi_api_lib 0.1977.0 (#116)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/4a28df17 ]
 * releasing version lib/bi_api_lib 0.1976.0 (#115)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/7dd0c10f ]
 * releasing version ops/bi_integration_tests 0.19.0 (#113)          [ https://github.com/datalens-tech/datalens-backend-private/commit/e6f21fea ]
 * releasing version lib/bi_api_lib 0.1975.0 (#109)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/0e10952b ]
 * releasing version lib/bi_api_lib 0.1974.0 (#107)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/180b8a88 ]
 * releasing version ops/bi_integration_tests 0.18.0 (#101)          [ https://github.com/datalens-tech/datalens-backend-private/commit/9d66cbd4 ]
 * releasing version ops/bi_integration_tests 0.17.0 (#97)           [ https://github.com/datalens-tech/datalens-backend-private/commit/55a217bd ]
 * releasing version lib/bi_api_lib 0.1973.0 (#94)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/bb14b670 ]
 * releasing version lib/bi_api_lib 0.1972.0 (#89)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/643bfaf4 ]
 * releasing version ops/bi_integration_tests 0.16.0 (#85)           [ https://github.com/datalens-tech/datalens-backend-private/commit/038232fd ]
 * releasing version lib/bi_api_lib 0.1971.0 (#81)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/23f04eb8 ]
 * releasing version ops/bi_integration_tests 0.15.0 (#78)           [ https://github.com/datalens-tech/datalens-backend-private/commit/5a1933ef ]
 * releasing version ops/bi_integration_tests 0.14.0 (#73)           [ https://github.com/datalens-tech/datalens-backend-private/commit/986cbc88 ]
 * releasing version ops/bi_integration_tests 0.13.0 (#64)           [ https://github.com/datalens-tech/datalens-backend-private/commit/9fdff6a2 ]
 * releasing version ops/bi_integration_tests 0.12.0 (#58)           [ https://github.com/datalens-tech/datalens-backend-private/commit/cf0b7ac6 ]
 * releasing version lib/bi_api_lib 0.1970.0 (#40)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/3c2ed13e ]

* [vallbull](http://staff/33630435+vallbull@users.noreply.github.com)

 * BI-4803: Fix full date filtering (#312)                [ https://github.com/datalens-tech/datalens-backend-private/commit/7b9a852c ]
 * BI-4758: Get rid of read_only=True (#303)              [ https://github.com/datalens-tech/datalens-backend-private/commit/363e2cd6 ]
 * BI-4791: Delete port check (#295)                      [ https://github.com/datalens-tech/datalens-backend-private/commit/3c616d15 ]
 * BI-4776: Fix typo in code (#300)                       [ https://github.com/datalens-tech/datalens-backend-private/commit/f7892a5d ]
 * BI-4776: Chyt forms (#256)                             [ https://github.com/datalens-tech/datalens-backend-private/commit/01ecf70a ]
 * BI-4803: Fix data filtering in selectors (#179)        [ https://github.com/datalens-tech/datalens-backend-private/commit/d4f4a5f4 ]
 * BI-4624: Use anyascii from pypi (#206)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/3c5403ce ]
 * BI-4626: Add transfer, source and new log-group (#70)  [ https://github.com/datalens-tech/datalens-backend-private/commit/aac8c56e ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * removed bi_core/_aux (#284)                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/21a15c8f ]
 * delete bi_sqlalchemy_mysql dependency, drop legacy maintenance script, fix deps (#279)  [ https://github.com/datalens-tech/datalens-backend-private/commit/372eb59a ]
 * BI-4385: cleanup (#272)                                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/8f4f9c02 ]
 * Replace EnumField to native marshmallow Enum field (#195)                               [ https://github.com/datalens-tech/datalens-backend-private/commit/6736c94e ]
 * remove ya-team specific apps from github (#197)                                         [ https://github.com/datalens-tech/datalens-backend-private/commit/e752fa67 ]
 * mypy fixes (#163)                                                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/d2c6403e ]
 * Removed DLS from github repo (#149)                                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/91f8be9b ]
 * BI-4385: exception classes cleanup (#144)                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/9fc86f9d ]
 * BI-4385: utils cleanup (#137)                                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/b5034d08 ]
 * BI-4385: drop unnecessary conn_opts mutation (#138)                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/266d4213 ]
 * BI-3546: rewrite mdb domains in YaTeam (#104)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/9f3ee913 ]
 * BI-4855: added MDB protos to yc_apis_proto_stubs (#72)                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/ae189873 ]

* [dmi-feo](http://staff/fdi1992@gmail.com)

 * remove unused stuff from statcommons + run tests on file removals (#266)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e9a2985c ]
 * BI-4904: mount internal cert in int-rqe containers (#281)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/70ea817d ]
 * BI-4835: increase cloudlogging retention_period (#241)                    [ https://github.com/datalens-tech/datalens-backend-private/commit/59423045 ]
 * BI-4835: common message key (#225)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/08838fb1 ]
 * BI-4835: formatted cloudlogging logs (#224)                               [ https://github.com/datalens-tech/datalens-backend-private/commit/1d9734db ]
 * sentry: add sec group + up version (#218)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/90c49fe7 ]
 * update left stands (#217)                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/07b9d501 ]
 * upload_to_ycr: use yc-prod build folder repo (#211)                       [ https://github.com/datalens-tech/datalens-backend-private/commit/c54ff858 ]
 * fluentbit: trigger helm upgrade on config change (#212)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/4458f29f ]
 * return USE_IAM_SUBJECT_RESOLVER (#216)                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/4cfe1929 ]
 * BI-4750: monitoring connection for istrael and nemax (#65)                [ https://github.com/datalens-tech/datalens-backend-private/commit/a1bf019f ]

* [Sergei Borodin](http://staff/serayborodin@gmail.com)

 * BI-4478 rootless file* apps (#193)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/1a4a959c ]
 * BI-4876 rename entrypoint file-worker (#207)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/31f12ca6 ]
 * BI-4876 fix factory (#189)                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/745dbd2f ]
 * BI-4883 file-secure-reader tier1 (#157)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/4a08e4e3 ]
 * BI-4875 BI-4876 new file-uploader* images (#151)                             [ https://github.com/datalens-tech/datalens-backend-private/commit/5d580b92 ]
 * BI-4478 k8s deployment with rootless bi-api* (#146)                          [ https://github.com/datalens-tech/datalens-backend-private/commit/cddae8eb ]
 * BI-4873 BI-4874 yc-public-data-api yc-embedded-data-api docker tier1 (#131)  [ https://github.com/datalens-tech/datalens-backend-private/commit/71caa7d1 ]
 * BI-4872 yc-data-api docker tier1 (#126)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/adb226e6 ]
 * Up integration-tests version (#121)                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/1cabd36c ]
 * integration-tests tier1 only (#108)                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/6a425226 ]
 * phusion base image for integration-tests (#62)                               [ https://github.com/datalens-tech/datalens-backend-private/commit/5a458a65 ]
 * integration-tests with bake (#54)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/fb99f601 ]
 * BI-4809 control-api yc-preprod (#45)                                         [ https://github.com/datalens-tech/datalens-backend-private/commit/ccc60445 ]

* [Dmitry Nadein](http://staff/pr45dima@mail.ru)

 * BI-4749: Update sentry dsn env vars (#38)  [ https://github.com/datalens-tech/datalens-backend-private/commit/f55e0140 ]

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * SYNC On branch trunk aabaf57ac056cd9d5ab8541280330f76bedb90d6  [ https://github.com/datalens-tech/datalens-backend-private/commit/f47b6469 ]
 * SYNC On branch trunk f9b55cf0bbe5880d1f49b17ad9615c7e010f2b55  [ https://github.com/datalens-tech/datalens-backend-private/commit/6e5572b6 ]
 * SYNC On branch trunk b62b8da33d58eb7fb22395578156db63e1549606  [ https://github.com/datalens-tech/datalens-backend-private/commit/5274b302 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-06 12:46:41+00:00

0.140.0
-------

* [Sergei Borodin](http://staff/serayborodin@gmail.com)

 * BI-4876 rename entrypoint file-worker (#207)  [ https://github.com/datalens-tech/datalens-backend-private/commit/31f12ca ]

* [vallbull](http://staff/33630435+vallbull@users.noreply.github.com)

 * BI-4803: Fix data filtering in selectors (#179)  [ https://github.com/datalens-tech/datalens-backend-private/commit/d4f4a5f ]
 * BI-4624: Use anyascii from pypi (#206)           [ https://github.com/datalens-tech/datalens-backend-private/commit/3c5403c ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Removed empty modules from bi_testing (#172)                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/73b57b5 ]
 * Moved TestClientConverterAiohttpToFlask from bi_testing_ya to bi_api_lib_testing (#175)  [ https://github.com/datalens-tech/datalens-backend-private/commit/15d0d59 ]

* [Max Zuev](http://staff/mail@thenno.me)

 * Remove bi_test_project_task_interface (#205)  [ https://github.com/datalens-tech/datalens-backend-private/commit/78cb3eb ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * gh-ci-cancel-in-progress-on-pr-update (#204)  [ https://github.com/datalens-tech/datalens-backend-private/commit/c440263 ]
 * build did image (#176)                        [ https://github.com/datalens-tech/datalens-backend-private/commit/6f04fc3 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * remove ya-team specific apps from github (#197)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e752fa6 ]

* [Konstantin Chupin](http://staff/91148200+ya-kc@users.noreply.github.com)

 * [BI-4830] Remove tier-0 build rudiments (entry points) (#199)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/5fc2705 ]
 * [BI-4830] Remove tier-0 build rudiments (#196)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/cc681a8 ]
 * [BI-4830] Cleanup bi-api image before moving libs to mainrepo (#194)                          [ https://github.com/datalens-tech/datalens-backend-private/commit/059aae5 ]
 * [BI-4632] Externalize tenant resolution in public US workaround middleware & FU tasks (#158)  [ https://github.com/datalens-tech/datalens-backend-private/commit/b799e05 ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * move sa_creds.py to bi_cloud_integration (#188)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/c5ddc5b ]
 * Move the greenplum core connector to a separate package (#181)  [ https://github.com/datalens-tech/datalens-backend-private/commit/feb99c5 ]
 * BI-4852: clean up comments in the postgresql adapter (#185)     [ https://github.com/datalens-tech/datalens-backend-private/commit/4ff0e29 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-30 08:47:15+00:00

0.139.0
-------

* [Sergei Borodin](http://staff/serayborodin@gmail.com)

 * BI-4876 fix factory (#189)  [ https://github.com/datalens-tech/datalens-backend-private/commit/745dbd2 ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * Moving from shell hell to bake new world (#120)  [ https://github.com/datalens-tech/datalens-backend-private/commit/194c8cd ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-29 14:41:58+00:00

0.138.0
-------

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version lib/bi_file_uploader_api_lib 0.115.0 (#184)  [ https://github.com/datalens-tech/datalens-backend-private/commit/c5ee769 ]

* [Max Zuev](http://staff/mail@thenno.me)

 * BI-4359: don't use defaults from libs (#182)               [ https://github.com/datalens-tech/datalens-backend-private/commit/da152cc ]
 * BI-4359: return back nebius defaults to bi-configs (#164)  [ https://github.com/datalens-tech/datalens-backend-private/commit/4714d55 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * BI-4150: Implemented optimization of IF and CASE functions with constant conditions (#148)  [ https://github.com/datalens-tech/datalens-backend-private/commit/bb1bdbf ]
 * BI-4703: Partial implementation of data source migration for connection replacement (#19)   [ https://github.com/datalens-tech/datalens-backend-private/commit/d14823c ]
 * Moved dialect support settings to formula-ref configs (#143)                                [ https://github.com/datalens-tech/datalens-backend-private/commit/0fb32cc ]
 * Removed and refactored some legacy core data source tests (#53)                             [ https://github.com/datalens-tech/datalens-backend-private/commit/7a7df16 ]
 * Connectorized DialectName (#147)                                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/3433273 ]
 * Added recursive config discovery to dl-repomanager (#171)                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/6ebc5ed ]

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * Remove preprod ig configs (#170)  [ https://github.com/datalens-tech/datalens-backend-private/commit/534386e ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-29 13:50:58+00:00

0.137.0
-------

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version lib/bi_file_uploader_api_lib 0.114.0 (#173)  [ https://github.com/datalens-tech/datalens-backend-private/commit/5fd20e9 ]
 * releasing version app/bi_file_secure_reader 0.13.0 (#167)      [ https://github.com/datalens-tech/datalens-backend-private/commit/a744607 ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * BI-4860: load connectors settings using a dynamically generated class (#150)  [ https://github.com/datalens-tech/datalens-backend-private/commit/d0a942d ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-29 10:01:24+00:00

0.136.0
-------

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version lib/bi_file_uploader_api_lib 0.113.0 (#165)  [ https://github.com/datalens-tech/datalens-backend-private/commit/db4174a ]
 * releasing version lib/bi_api_lib 0.1979.0 (#132)               [ https://github.com/datalens-tech/datalens-backend-private/commit/74689b2 ]
 * releasing version lib/bi_api_lib 0.1978.0 (#124)               [ https://github.com/datalens-tech/datalens-backend-private/commit/f557177 ]
 * releasing version ops/bi_integration_tests 0.20.0 (#118)       [ https://github.com/datalens-tech/datalens-backend-private/commit/b896c79 ]
 * releasing version lib/bi_api_lib 0.1977.0 (#116)               [ https://github.com/datalens-tech/datalens-backend-private/commit/4a28df1 ]
 * releasing version lib/bi_api_lib 0.1976.0 (#115)               [ https://github.com/datalens-tech/datalens-backend-private/commit/7dd0c10 ]
 * releasing version ops/bi_integration_tests 0.19.0 (#113)       [ https://github.com/datalens-tech/datalens-backend-private/commit/e6f21fe ]
 * releasing version lib/bi_api_lib 0.1975.0 (#109)               [ https://github.com/datalens-tech/datalens-backend-private/commit/0e10952 ]
 * releasing version lib/bi_api_lib 0.1974.0 (#107)               [ https://github.com/datalens-tech/datalens-backend-private/commit/180b8a8 ]
 * releasing version ops/bi_integration_tests 0.18.0 (#101)       [ https://github.com/datalens-tech/datalens-backend-private/commit/9d66cbd ]
 * releasing version ops/bi_integration_tests 0.17.0 (#97)        [ https://github.com/datalens-tech/datalens-backend-private/commit/55a217b ]
 * releasing version lib/bi_api_lib 0.1973.0 (#94)                [ https://github.com/datalens-tech/datalens-backend-private/commit/bb14b67 ]
 * releasing version lib/bi_api_lib 0.1972.0 (#89)                [ https://github.com/datalens-tech/datalens-backend-private/commit/643bfaf ]
 * releasing version ops/bi_integration_tests 0.16.0 (#85)        [ https://github.com/datalens-tech/datalens-backend-private/commit/038232f ]
 * releasing version lib/bi_api_lib 0.1971.0 (#81)                [ https://github.com/datalens-tech/datalens-backend-private/commit/23f04eb ]
 * releasing version ops/bi_integration_tests 0.15.0 (#78)        [ https://github.com/datalens-tech/datalens-backend-private/commit/5a1933e ]
 * releasing version ops/bi_integration_tests 0.14.0 (#73)        [ https://github.com/datalens-tech/datalens-backend-private/commit/986cbc8 ]
 * releasing version ops/bi_integration_tests 0.13.0 (#64)        [ https://github.com/datalens-tech/datalens-backend-private/commit/9fdff6a ]
 * releasing version ops/bi_integration_tests 0.12.0 (#58)        [ https://github.com/datalens-tech/datalens-backend-private/commit/cf0b7ac ]
 * releasing version lib/bi_api_lib 0.1970.0 (#40)                [ https://github.com/datalens-tech/datalens-backend-private/commit/3c2ed13 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * mypy fixes (#163)                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/d2c6403 ]
 * Removed DLS from github repo (#149)                     [ https://github.com/datalens-tech/datalens-backend-private/commit/91f8be9 ]
 * BI-4385: exception classes cleanup (#144)               [ https://github.com/datalens-tech/datalens-backend-private/commit/9fc86f9 ]
 * BI-4385: utils cleanup (#137)                           [ https://github.com/datalens-tech/datalens-backend-private/commit/b5034d0 ]
 * BI-4385: drop unnecessary conn_opts mutation (#138)     [ https://github.com/datalens-tech/datalens-backend-private/commit/266d421 ]
 * BI-3546: rewrite mdb domains in YaTeam (#104)           [ https://github.com/datalens-tech/datalens-backend-private/commit/9f3ee91 ]
 * BI-4855: added MDB protos to yc_apis_proto_stubs (#72)  [ https://github.com/datalens-tech/datalens-backend-private/commit/ae18987 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Mypy fixes in bi_formula (#159)                                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/ad10f86 ]
 * Moved remaining PG constant declarations to connector (#145)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/de50279 ]
 * Added creation of an empty table in oracle db to fix template test (#122)                    [ https://github.com/datalens-tech/datalens-backend-private/commit/cf7e241 ]
 * Some fixes for mypy (#129)                                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/940857b ]
 * Moved formula_ref configs to separate packages (#133)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/996b3d8 ]
 * Switched to short locales in formula_ref and fixed locales in clickhouse connector (#90)     [ https://github.com/datalens-tech/datalens-backend-private/commit/f71f6b7 ]
 * Added ConvertBlocksToFunctionsMutation for converting IF and CASE blocks to functions (#24)  [ https://github.com/datalens-tech/datalens-backend-private/commit/66adc9d ]
 * Added mypy task (#125)                                                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/9f56a55 ]
 * Added docker-remote and docker-reinstall commands to taskfile (#106)                         [ https://github.com/datalens-tech/datalens-backend-private/commit/83bd071 ]
 * Moved the boilerplate package to mainrepo (#102)                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/4051eba ]
 * Introduced taskfiles (#86)                                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/d04e821 ]
 * Moved dl_repmanager to mainrepo (#92)                                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/687c59f ]
 * BI-4863: Added clickhouse formula_ref plugin (#74)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/2ade140 ]
 * Fixed bake targets for i18n (#75)                                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/d15fa6f ]
 * Added --no-location option to .po generator (#83)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/54020f4 ]
 * Refactored update-po and msfmt common makefile targets (#67)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/45db6d0 ]
 * Added make remote-shell command (#49)                                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/321c8af ]
 * Added registrations to formula_ref plugins and switched to new localization logic (#34)      [ https://github.com/datalens-tech/datalens-backend-private/commit/4cfe507 ]
 * Removed empty bi_core/i18n/localizer_base.py (#35)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/6b38614 ]
 * Added the mainrepo dir (#32)                                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/08cc8da ]
 * Some mypy fixes for bi_i18n,  bi_core and connectors (#28)                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/7927363 ]
 * git-related fixes for repmanager (#30)                                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/7898fcf ]
 * Fixed i18n import in monitoring connector (#29)                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/6804cdc ]
 * Removed some unused attributes and methods from USEntry (#14)                                [ https://github.com/datalens-tech/datalens-backend-private/commit/82ca37e ]
 * Added the option of using whitelists in connector registration (#18)                         [ https://github.com/datalens-tech/datalens-backend-private/commit/cbbcf25 ]
 * Removed __init__ from the root dir (#26)                                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/ad1f2f7 ]
 * Added formula_ref plugins (#20)                                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/c1fa53d ]
 * Moved some modules from bi_testing to bi_testing_ya (#22)                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/808304f ]
 * Implemented regulated test mechanism (#21)                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/a3e1816 ]
 * Partially moved some more constants to connectors (#12)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/8457968 ]
 * Added bi_i18n package (#10)                                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/21fcbf8 ]
 * Added .mo files to bi_formula_ref package (#15)                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/af79cd6 ]
 * Added __pycache__ to .gitignore (#13)                                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/864e9cb ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * some mypy fixes in connectors (#161)                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/434d93c ]
 * even more mypy fixes (#154)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/07e33e4 ]
 * BI-4664: remove dls client from a common SR (#130)                         [ https://github.com/datalens-tech/datalens-backend-private/commit/1b2990a ]
 * remove explain query (#128)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/d2c373d ]
 * BI-4860: connector settings fallback registry (#119)                       [ https://github.com/datalens-tech/datalens-backend-private/commit/48f0f80 ]
 * mark failing oracle test (#110)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/cc953d2 ]
 * move metrica connector aliases to the connector package (#111)             [ https://github.com/datalens-tech/datalens-backend-private/commit/d7bf72e ]
 * BI-4860: connector settings registry (#80)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/ae48ac5 ]
 * some more mypy fixes (#93)                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/96fa442 ]
 * BI-4860: generate ConnectorsSettingsByType by settings registry (#88)      [ https://github.com/datalens-tech/datalens-backend-private/commit/c531841 ]
 * fix typing (#91)                                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/0d95eb3 ]
 * remove old bitrix conncetor settings (#76)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/197ff7d ]
 * BI-4834: move the gsheets bi api connector to a separate package (#71)     [ https://github.com/datalens-tech/datalens-backend-private/commit/7c8eb90 ]
 * BI-4834: move chyt bi api connectors to separate packages (#50)            [ https://github.com/datalens-tech/datalens-backend-private/commit/da59831 ]
 * fix mypy in the monitoring connector (#59)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/d1270b4 ]
 * replace most do_xxx classvars with regulated marks in core tests (#36)     [ https://github.com/datalens-tech/datalens-backend-private/commit/e9a0487 ]
 * remove unused file (#51)                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/9592af6 ]
 * remove method_not_implemented (#48)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/312cd5c ]
 * BI-4834: move the bitrix bi api connector to a separate package (#46)      [ https://github.com/datalens-tech/datalens-backend-private/commit/088d08a ]
 * enable more tests in ci (#41)                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/2598b2b ]
 * remove remaining setup pys (#42)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/8b23500 ]
 * small fixes for mypy (#37)                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/9981e20 ]
 * BI-4834: move metrica bi api connectors to a separate package (#27)        [ https://github.com/datalens-tech/datalens-backend-private/commit/ba21b47 ]
 * BI-4817: move some dashsql tests from bi api to connectors packages (#31)  [ https://github.com/datalens-tech/datalens-backend-private/commit/8b2af58 ]
 * BI-4834: move the monitoring bi api connector to a separate package (#23)  [ https://github.com/datalens-tech/datalens-backend-private/commit/a20b872 ]

* [Max Zuev](http://staff/mail@thenno.me)

 * BI-4359: delete old tooling and defaults (#155)         [ https://github.com/datalens-tech/datalens-backend-private/commit/6af6ee1 ]
 * BI-4359: move old defaults to new package (#99)         [ https://github.com/datalens-tech/datalens-backend-private/commit/f0b1831 ]
 * Try to fix ci tests (delete terrarium from root level)  [ https://github.com/datalens-tech/datalens-backend-private/commit/7053fc5 ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * temporary disable ext private jobs, sunce runners are offline (#162)                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/659ade9 ]
 * workaround (#160)                                                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/8324a4a ]
 * split ext tests into private and public (#153)                                                         [ https://github.com/datalens-tech/datalens-backend-private/commit/c192961 ]
 * workaround to have working gha builder image (#152)                                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/44d4006 ]
 * dedicated img for mypy (#139)                                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/cc7cd01 ]
 * minor mypy fix (#136)                                                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/32e8f8d ]
 * try more mypy on gh build step (#135)                                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/5cc954f ]
 * fix_main_flow_git_fetch_trunk (#112)                                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/0c4a370 ]
 * moved terrarium/bi_ci into mainrepo/ (#82)                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/1dc7e25 ]
 * enable_ext_tag_in_workflow (#79)                                                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/8b5e3b5 ]
 * gh ci option to run only mypy tests (#55)                                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/b0d84f3 ]
 * GH CI fixes: Skip publish if not executed pytests (#33)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/94232ef ]
 * Detect which files was changed between pr branch and base (#25)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/412a2e0 ]
 * placeholder workflow for simpler creation of new flow and avoid confusion in running flows list (#17)  [ https://github.com/datalens-tech/datalens-backend-private/commit/90d8b6a ]

* [Sergei Borodin](http://staff/serayborodin@gmail.com)

 * BI-4883 file-secure-reader tier1 (#157)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/4a08e4e ]
 * BI-4875 BI-4876 new file-uploader* images (#151)                             [ https://github.com/datalens-tech/datalens-backend-private/commit/5d580b9 ]
 * BI-4478 k8s deployment with rootless bi-api* (#146)                          [ https://github.com/datalens-tech/datalens-backend-private/commit/cddae8e ]
 * BI-4873 BI-4874 yc-public-data-api yc-embedded-data-api docker tier1 (#131)  [ https://github.com/datalens-tech/datalens-backend-private/commit/71caa7d ]
 * BI-4872 yc-data-api docker tier1 (#126)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/adb226e ]
 * Up integration-tests version (#121)                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/1cabd36 ]
 * integration-tests tier1 only (#108)                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/6a42522 ]
 * phusion base image for integration-tests (#62)                               [ https://github.com/datalens-tech/datalens-backend-private/commit/5a458a6 ]
 * integration-tests with bake (#54)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/fb99f60 ]
 * BI-4809 control-api yc-preprod (#45)                                         [ https://github.com/datalens-tech/datalens-backend-private/commit/ccc6044 ]

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * BI-4801 WIP Remove some usages of YENV_NAME (#117)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/dc04f01 ]
 * Add dev-machine secrets sync to the taskfile (#127)                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/ece8e9e ]
 * Sync secrets from yav to dev machine on secrets-update for local ext tests runs (#123)    [ https://github.com/datalens-tech/datalens-backend-private/commit/5236bc0 ]
 * Enable bitrix ext tests in CI (#77)                                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/d7b7cf1 ]
 * BI-4692 Comment out unconfigured ext tests instead of a whitelist in test splitter (#69)  [ https://github.com/datalens-tech/datalens-backend-private/commit/00af47f ]
 * Remove YC file-uploader[-worker] meta packages (#68)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/790186a ]
 * Remove OS meta packages (#63)                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/7766dbd ]
 * Remove YC bi-api meta packages (#60)                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/d59f021 ]
 * BI-4692 GitHub Actions secrets for ext tests (#44)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/4271639 ]
 * Fix bitrix_gds bi-api-connector dependency (#66)                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/a1f35b9 ]
 * Update Kontur.Market texts; fix translations generation (#47)                             [ https://github.com/datalens-tech/datalens-backend-private/commit/338074a ]
 * Remove the remaining gen-parser prerequisites from Makefiles                              [ https://github.com/datalens-tech/datalens-backend-private/commit/380fbd8 ]

* [Konstantin Chupin](http://staff/91148200+ya-kc@users.noreply.github.com)

 * [BI-4830] Adopt mainrepo/libs to all Docker builds (#105)         [ https://github.com/datalens-tech/datalens-backend-private/commit/44459ff ]
 * [BI-4830] Parametrize base rootless image at bake level (#98)     [ https://github.com/datalens-tech/datalens-backend-private/commit/6dfdb3c ]
 * [BI-4830] Version bump PR auto-merge (#84)                        [ https://github.com/datalens-tech/datalens-backend-private/commit/363f45f ]
 * [BI-4830] Version bump for integration tests fix (#57)            [ https://github.com/datalens-tech/datalens-backend-private/commit/c4188ee ]
 * [BI-4830] Version bump for integration tests (#56)                [ https://github.com/datalens-tech/datalens-backend-private/commit/56fcece ]
 * [BI-4830] Fix release PR branch name (#52)                        [ https://github.com/datalens-tech/datalens-backend-private/commit/24eeb5d ]
 * [BI-4830] Add dedicated dir for CI artifacts to .gitignore (#43)  [ https://github.com/datalens-tech/datalens-backend-private/commit/a787a30 ]
 * [BI-4830] Fix make pycharm (#11)                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/bc7a3ab ]
 * [BI-4830] Remove GH sync tools (#9)                               [ https://github.com/datalens-tech/datalens-backend-private/commit/88c518a ]

* [Dmitry Nadein](http://staff/pr45dima@mail.ru)

 * BI-4749: Update sentry dsn env vars (#38)  [ https://github.com/datalens-tech/datalens-backend-private/commit/f55e014 ]

* [dmi-feo](http://staff/fdi1992@gmail.com)

 * BI-4750: monitoring connection for istrael and nemax (#65)  [ https://github.com/datalens-tech/datalens-backend-private/commit/a1bf019 ]

* [vallbull](http://staff/33630435+vallbull@users.noreply.github.com)

 * BI-4626: Add transfer, source and new log-group (#70)  [ https://github.com/datalens-tech/datalens-backend-private/commit/aac8c56 ]

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * SYNC On branch trunk aabaf57ac056cd9d5ab8541280330f76bedb90d6  [ https://github.com/datalens-tech/datalens-backend-private/commit/f47b646 ]
 * SYNC On branch trunk f9b55cf0bbe5880d1f49b17ad9615c7e010f2b55  [ https://github.com/datalens-tech/datalens-backend-private/commit/6e5572b ]
 * SYNC On branch trunk b62b8da33d58eb7fb22395578156db63e1549606  [ https://github.com/datalens-tech/datalens-backend-private/commit/5274b30 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-29 08:58:40+00:00

0.135.0
-------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4833 Add file-uploader-worker health check poetry script  [ https://a.yandex-team.ru/arc/commit/12135568 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-09 18:33:40+00:00

0.134.0
-------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4833 Add gunicorn dep to bi_file_uploader_worker  [ https://a.yandex-team.ru/arc/commit/12134707 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-09 16:33:18+00:00

0.133.0
-------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4833 Add file-uploader-worker and gunicorn as poetry scripts to bi_file_uploader_worker  [ https://a.yandex-team.ru/arc/commit/12133390 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-09 14:37:04+00:00

0.132.0
-------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4833 Add tier-1 Dockerfile for bi-file-uploader-worker  [ https://a.yandex-team.ru/arc/commit/12131537 ]
 * BI-4836 Extract bi_file_secure_reader_lib                  [ https://a.yandex-team.ru/arc/commit/12130114 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-09 12:40:21+00:00

0.131.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-28 16:39:17+00:00

0.130.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-28 08:34:58+00:00

0.129.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 09:07:31+00:00

0.128.0
-------

* [vgol](http://staff/vgol)

 * BI-4592: updated .release.hjson to use pyproject.toml instead of setup.py      [ https://a.yandex-team.ru/arc/commit/11909029 ]
 * BI-4592: re using old local dev, but supplying deps from pyproject toml files  [ https://a.yandex-team.ru/arc/commit/11908104 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 08:46:49+00:00

0.127.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-06 07:10:51+00:00

0.125.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4356 Introduce bi_file_uploader_worker_lib                        [ https://a.yandex-team.ru/arc/commit/11814784 ]
 * BI-4356 Move common file-uploader modules into bi_file_uploader_lib  [ https://a.yandex-team.ru/arc/commit/11767670 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed us container name from all tests and configs  [ https://a.yandex-team.ru/arc/commit/11809819 ]

* [dmifedorov](http://staff/dmifedorov)

 * replace aioredis with fresh redis             [ https://a.yandex-team.ru/arc/commit/11777084 ]
 * hosts and ports from docker-compose in tests  [ https://a.yandex-team.ru/arc/commit/11774830 ]

* [vgol](http://staff/vgol)

 * BI-4592: Added script to generate pyproject.toml from setup.py + ya.make, with reference versions taken from prod container  [ https://a.yandex-team.ru/arc/commit/11774366 ]

* [kchupin](http://staff/kchupin)

 * [BI-4632] Generalize USCrawlerBase & US client/manager tenant control: generalize crawler scope limit, remove ability to set tenant from entry config in crawler, generalize tenancy related methods in USM/C  [ https://a.yandex-team.ru/arc/commit/11761805 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-28 13:39:12+00:00

0.124.0
-------

* [kchupin](http://staff/kchupin)

 * [BI-4632] Extract DLRequestBase to bi_api_commons                                                                                  [ https://a.yandex-team.ru/arc/commit/11730197 ]
 * [BI-4631] Remove `yenv` usage in file uploader worker. Was used to determine if lauched in tests. Replaced with flag in settings.  [ https://a.yandex-team.ru/arc/commit/11700552 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-20 18:46:31+00:00

0.123.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4610 Do not delete data and throw errors on exec for errors not caused by the user on gsheets autoupdate  [ https://a.yandex-team.ru/arc/commit/11685256 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-13 07:06:13+00:00

0.122.0
-------

* [konstasa](http://staff/konstasa)

 * CLOUDLINETWO-3598 Save CSV parsing error into the DataFile  [ https://a.yandex-team.ru/arc/commit/11650486 ]
 * BI-4609 Add quota exceeded errors for gsheets               [ https://a.yandex-team.ru/arc/commit/11647137 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-06 14:31:18+00:00

0.121.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4599 Add request-id to error details on gsheets autoupdate  [ https://a.yandex-team.ru/arc/commit/11625245 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-02 14:42:42+00:00

0.120.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4557 Fail download when too few rows received; release source update lock on manual data update  [ https://a.yandex-team.ru/arc/commit/11565971 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved CHS3 core connectors to a separate package  [ https://a.yandex-team.ru/arc/commit/11509091 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-26 09:34:06+00:00

0.119.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4447 Non-ref sources in file based connections                                  [ https://a.yandex-team.ru/arc/commit/11427853 ]
 * BI-4389 Remove ConnectionInternalCH, leaving InternalMaterializationConnectionRef  [ https://a.yandex-team.ru/arc/commit/11377220 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4398 Move TenantDef and AuthData implementations to bi_api_commons.  [ https://a.yandex-team.ru/arc/commit/11155774 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-05 11:24:04+00:00

0.118.0
-------

* [asnytin](http://staff/asnytin)

 * BI-4286: RenameTenantFilesTask - fix gsheets connections loading  [ https://a.yandex-team.ru/arc/commit/11110450 ]

* [gstatsenko](http://staff/gstatsenko)

 * Replaced some usages of us_manager with us_entry_buffer  [ https://a.yandex-team.ru/arc/commit/11093787 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-22 14:27:04+00:00

0.117.0
-------

* [asnytin](http://staff/asnytin)

 * BI-4286: rename tenant files task - tenant_id fix  [ https://a.yandex-team.ru/arc/commit/11067683 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-16 19:45:43+00:00

0.116.0
-------

* [asnytin](http://staff/asnytin)

 * BI-4286: added file-upl rename tenant files api handler and task  [ https://a.yandex-team.ru/arc/commit/11051658 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-15 09:57:13+00:00

0.115.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-13 14:46:48+00:00

0.114.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-13 13:21:40+00:00

0.113.0
-------

* [seray](http://staff/seray)

 * BI-4094 fix assert expression  [ https://a.yandex-team.ru/arc/commit/11003434 ]

* [thenno](http://staff/thenno)

 * Use arq from contrib (and drop some mat-tests)  [ https://a.yandex-team.ru/arc/commit/10991557 ]

* [konstasa](http://staff/konstasa)

 * Remove connector settings set to None in tests  [ https://a.yandex-team.ru/arc/commit/10968577 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-07 17:01:38+00:00

0.112.0
-------

* [seray](http://staff/seray)

 * BI-4275 secure_reader deploy isolation  [ https://a.yandex-team.ru/arc/commit/10963816 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4056 Restore Feature Date32 for clickhouse connections. Convert user date type to Date32.                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/10960390 ]
 * Revert "BI-4056 Feature Date32 for clickhouse connections. Convert user date type to Date32."

This reverts commit b65d02f2548784d3b12f72b4602774362dc96749, reversing
changes made to 4a4ed7ae678b93240b64a3ca00389df61033e4ba.  [ https://a.yandex-team.ru/arc/commit/10957507 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-01 16:58:26+00:00

0.111.0
-------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4281 feature bi_api_commons package  [ https://a.yandex-team.ru/arc/commit/10951763 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-28 15:58:28+00:00

0.110.0
-------

* [seray](http://staff/seray)

 * BI-4275 additional secure_reader entry point  [ https://a.yandex-team.ru/arc/commit/10914811 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-21 13:35:58+00:00

0.109.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4276 Fix unclosed usm client session in download gsheet & parse tasks; refactor gsheets task a bit  [ https://a.yandex-team.ru/arc/commit/10889099 ]
 * BI-4244 Equeo partner connector                                                                        [ https://a.yandex-team.ru/arc/commit/10871053 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4056 Feature Date32 for clickhouse connections. Convert user date type to Date32.  [ https://a.yandex-team.ru/arc/commit/10880983 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-21 09:49:51+00:00

0.108.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4257 Fix forward slash encoding in gsheets api request urls  [ https://a.yandex-team.ru/arc/commit/10870118 ]
 * Recreate test gsheets with new permissions and disclaimers      [ https://a.yandex-team.ru/arc/commit/10743408 ]

* [seray](http://staff/seray)

 * BI-4095 bi_file_secure_reader microservice  [ https://a.yandex-team.ru/arc/commit/10833539 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-15 08:43:20+00:00

0.107.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4013 BI-4116 Store original errors instead if INVALID_SOURCE on data update failure; store the whole error in DataFile, not just code; refactor connection source failing  [ https://a.yandex-team.ru/arc/commit/10667127 ]

* [seray](http://staff/seray)

 * YCDOCS-7328 test for long line whith None  [ https://a.yandex-team.ru/arc/commit/10656236 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-16 10:51:02+00:00

0.106.0
-------

* [seray](http://staff/seray)

 * YCDOCS-7328 skip None value in preview reduce  [ https://a.yandex-team.ru/arc/commit/10643981 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-11 18:02:06+00:00

0.105.0
-------

* [seray](http://staff/seray)

 * BI-4094 xlsx file datetime repr  [ https://a.yandex-team.ru/arc/commit/10596389 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-29 13:31:36+00:00

0.104.0
-------

* [seray](http://staff/seray)

 * BI-4094 xlsx file support  [ https://a.yandex-team.ru/arc/commit/10586650 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-28 10:20:38+00:00

0.103.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4013 Do not remove raw_schema in failed sources during data update to let them load in dataset; move connection error raise back into datasource  [ https://a.yandex-team.ru/arc/commit/10572555 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-23 15:26:19+00:00

0.102.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4013 Fix the issue with unknown field errors occuring before connection component errors; allow filling file source params with None values to fail them properly during data update  [ https://a.yandex-team.ru/arc/commit/10568355 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-22 20:45:39+00:00

0.101.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4152 Wrap sync work in download task and gsheets client in TPE                                                      [ https://a.yandex-team.ru/arc/commit/10562475 ]
 * BI-4154 Respond with 400 instead of 500 when can not find source by id in file based connection                        [ https://a.yandex-team.ru/arc/commit/10562180 ]
 * BI-4116 Handle too many columns and too large file in file uploader properly: do not fail, just save error into dfile  [ https://a.yandex-team.ru/arc/commit/10561446 ]
 * BI-4013 Store errors that occur during data update in file-based connections error registry and throw them on execute  [ https://a.yandex-team.ru/arc/commit/10559993 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-22 10:21:21+00:00

0.100.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4114 Fix missing column naming; other minor fixes  [ https://a.yandex-team.ru/arc/commit/10523020 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-15 11:57:29+00:00

0.99.0
------

* [asnytin](http://staff/asnytin)

 * BI-4046: file-uploader-worker health check API  [ https://a.yandex-team.ru/arc/commit/10500687 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-12 19:55:31+00:00

0.98.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-12 14:02:25+00:00

0.97.0
------

* [konstasa](http://staff/konstasa)

 * BI-4114 Generate missing titles and column names for gsheets using alphabet notation  [ https://a.yandex-team.ru/arc/commit/10493401 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-11 22:51:49+00:00

0.96.0
------

* [konstasa](http://staff/konstasa)

 * Fix parsing gsheets with no header; improve & refactor gsheets tests in fuw  [ https://a.yandex-team.ru/arc/commit/10489272 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-09 16:58:27+00:00

0.95.0
------

* [konstasa](http://staff/konstasa)

 * BI-4115 GSheets fix save by fixing data sink for download + better logging  [ https://a.yandex-team.ru/arc/commit/10484741 ]
 * BI-4078 Use spreadsheets.values.get in chunk loading to speed it up         [ https://a.yandex-team.ru/arc/commit/10481532 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-09 08:39:45+00:00

0.94.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-05 09:40:04+00:00

0.93.0
------

* [konstasa](http://staff/konstasa)

 * BI-4078 Load gsheets in chunks to support big spreadsheets; exponential backoff to avoid quota limit errors  [ https://a.yandex-team.ru/arc/commit/10443427 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-02 13:22:48+00:00

0.92.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-02 12:46:47+00:00

0.91.0
------

* [konstasa](http://staff/konstasa)

 * Fix bool parsing for gsheets  [ https://a.yandex-team.ru/arc/commit/10437726 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-01 17:06:57+00:00

0.90.0
------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-30 17:48:10+00:00

0.89.0
------

* [konstasa](http://staff/konstasa)

 * BI-4054 Fix broken data update when one sheet is added multiple times                           [ https://a.yandex-team.ru/arc/commit/10357020 ]
 * Allow file source polling by returning file_id from connection and syncing DataFile on replace  [ https://a.yandex-team.ru/arc/commit/10355287 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-17 16:18:48+00:00

0.88.0
------

* [konstasa](http://staff/konstasa)

 * Make S3 filename just a shortuuid with tenant_id on save to avoid collision when retrying or updating gsheets data  [ https://a.yandex-team.ru/arc/commit/10345619 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-16 09:02:35+00:00

0.87.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-11-14 20:24:56+03:00

0.86.0
------

* [konstasa](http://staff/konstasa)

 * BI-3980 Cleanup old files from S3 on data update  [ https://a.yandex-team.ru/arc/commit/10330955 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-14 11:58:41+00:00

0.85.0
------

* [asnytin](http://staff/asnytin)

 * require only file-connector related stuff in file-uploader settings  [ https://a.yandex-team.ru/arc/commit/10319684 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-10 21:31:51+03:00

0.84.0
------

* [konstasa](http://staff/konstasa)

 * Token refreshing for gsheets  [ https://a.yandex-team.ru/arc/commit/10293280 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-07 13:36:30+03:00

0.83.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-11-02 20:05:24+03:00

0.82.0
------

* [konstasa](http://staff/konstasa)

 * BI-4012 New authorization flow in GSheets                                      [ https://a.yandex-team.ru/arc/commit/10265843 ]
 * BI-3921 GSheets one-time update: release source lock when parsing is finished  [ https://a.yandex-team.ru/arc/commit/10234141 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-01 14:15:50+03:00

0.81.0
------

* [konstasa](http://staff/konstasa)

 * Make US lock timings in SaveSourceTask more reasonable                                  [ https://a.yandex-team.ru/arc/commit/10219965 ]
 * BI-3922 Update status codes in links api (not found and permission denied are now 400)  [ https://a.yandex-team.ru/arc/commit/10219964 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-24 12:47:37+03:00

0.80.0
------

* [konstasa](http://staff/konstasa)

 * BI-3901 GSheets autoupdate: use UTC time everywhere  [ https://a.yandex-team.ru/arc/commit/10213268 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-21 18:00:39+03:00

0.79.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-21 16:35:42+03:00

0.78.0
------

* [asnytin](http://staff/asnytin)

 * BI-3986: genericdatetime in file-connector  [ https://a.yandex-team.ru/arc/commit/10211553 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-21 15:33:03+03:00

0.77.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-20 18:50:07+03:00

0.76.0
------

* [konstasa](http://staff/konstasa)

 * Fix raw schema calculation for gsheets                       [ https://a.yandex-team.ru/arc/commit/10184931 ]
 * BI-3922 Improve error handling for gsheets in file uploader  [ https://a.yandex-team.ru/arc/commit/10184877 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-18 10:54:55+03:00

0.75.0
------

* [konstasa](http://staff/konstasa)

 * BI-3921 GSheets one-time update  [ https://a.yandex-team.ru/arc/commit/10169420 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-14 13:19:37+03:00

0.74.0
------

* [konstasa](http://staff/konstasa)

 * BI-3920 Encryption in file-uploader(-worker) + use token for GSheets  [ https://a.yandex-team.ru/arc/commit/10157386 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-12 17:15:59+03:00

0.73.0
------

* [konstasa](http://staff/konstasa)

 * BI-3901 GSheets autoupdate; step 2: tasks & notification  [ https://a.yandex-team.ru/arc/commit/10134513 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-07 18:09:14+03:00

0.72.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-07 15:09:21+03:00

0.71.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-07 13:24:52+03:00

0.70.0
------

* [konstasa](http://staff/konstasa)

 * BI-3915 Convert datetimes to UTC in file type caster when tz is specified  [ https://a.yandex-team.ru/arc/commit/10124152 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-06 10:43:28+03:00

0.69.0
------

* [konstasa](http://staff/konstasa)

 * BI-3901 GSheets autoupdate; step 1: API  [ https://a.yandex-team.ru/arc/commit/10105838 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-03 16:00:15+03:00

0.68.0
------

* [konstasa](http://staff/konstasa)

 * Reorganized tasks in bi_file_uploader_worker          [ https://a.yandex-team.ru/arc/commit/10055000 ]
 * BI-3784 Add source errors to file-uploader responses  [ https://a.yandex-team.ru/arc/commit/10052712 ]
 * BI-3784 Remove column types overrides for gsheets     [ https://a.yandex-team.ru/arc/commit/10051038 ]
 * Move GSheetsClient into bi_file_uploader              [ https://a.yandex-team.ru/arc/commit/10047947 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-28 16:22:22+03:00

0.67.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-20 13:27:57+03:00

0.66.0
------

* [konstasa](http://staff/konstasa)

 * BI-3784 Remove re-guessing logic from file parser for gsheets  [ https://a.yandex-team.ru/arc/commit/10043919 ]
 * BI-3784 Switch to aiogoogle in GSheetsClient                   [ https://a.yandex-team.ru/arc/commit/10014820 ]
 * BI-3867 Use local task processor in tests                      [ https://a.yandex-team.ru/arc/commit/9993902 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-20 13:23:03+03:00

0.65.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-09 15:06:59+03:00

0.64.0
------

* [konstasa](http://staff/konstasa)

 * BI-3790 GSheets CHS3 connector  [ https://a.yandex-team.ru/arc/commit/9980495 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-07 19:11:22+03:00

0.63.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-06 15:06:40+03:00

0.62.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-01 17:07:21+03:00

0.61.0
------

* [asnytin](http://staff/asnytin)

 * BI-3816: use rci tenant_id as s3 file prefix in file-connector  [ https://a.yandex-team.ru/arc/commit/9935411 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-30 12:11:17+03:00

0.60.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-08-26 15:50:44+03:00

0.59.0
------

* [konstasa](http://staff/konstasa)

 * BI-3784 GSheets processing in file-uploader-worker: download and parse              [ https://a.yandex-team.ru/arc/commit/9919711 ]
 * BI-3628 Add file type diversity into DataFile; add links endpoint to file-uploader  [ https://a.yandex-team.ru/arc/commit/9879129 ]

* [mcpn](http://staff/mcpn)

 * BI-3686: use POST /private/getTenantsList  [ https://a.yandex-team.ru/arc/commit/9881477 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-26 12:41:19+03:00

0.58.0
------

* [thenno](http://staff/thenno)

 * BI-3404: add request_id to TaskProcessor  [ https://a.yandex-team.ru/arc/commit/9875189 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3627: Moved a whole lot of CH* stuff to connector folders        [ https://a.yandex-team.ru/arc/commit/9856630 ]
 * BI-3627: Moved adapter modules to the respective connector folders  [ https://a.yandex-team.ru/arc/commit/9847278 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-17 14:52:24+03:00

0.57.0
------

* [konstasa](http://staff/konstasa)

 * BI-3691 Cleanup file previews on tenant deletion  [ https://a.yandex-team.ru/arc/commit/9844484 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-10 12:56:48+03:00

0.56.0
------

* [konstasa](http://staff/konstasa)

 * BI-3519 Fix cron schedule and logging in CleanS3LifecycleRulesTask  [ https://a.yandex-team.ru/arc/commit/9767423 ]
 * BI-3595 Add new fields (dash id, chart kind) to reporting           [ https://a.yandex-team.ru/arc/commit/9757889 ]
 * BI-3519 Update LC rule to trigger in the future                     [ https://a.yandex-team.ru/arc/commit/9752013 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3653: Made SR required in USM  [ https://a.yandex-team.ru/arc/commit/9753654 ]
 * BI-3653: Fixed USM in public API  [ https://a.yandex-team.ru/arc/commit/9741965 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-09 13:33:34+03:00

0.55.0
------

* [konstasa](http://staff/konstasa)

 * BI-3589 Shorter dialect detection retry timeout  [ https://a.yandex-team.ru/arc/commit/9718035 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-15 16:22:35+03:00

0.54.0
------

* [konstasa](http://staff/konstasa)

 * BI-3589 Better dialect detection timeout  [ https://a.yandex-team.ru/arc/commit/9714312 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3341: Removed lifecycle (save, delete, publish, etc.) methods from USEntry  [ https://a.yandex-team.ru/arc/commit/9711221 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-13 13:58:42+03:00

0.53.0
------

* [konstasa](http://staff/konstasa)

 * BI-3598 Use DateTime64 in the FileTypeTransformer  [ https://a.yandex-team.ru/arc/commit/9702462 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-11 17:57:38+03:00

0.52.0
------

* [konstasa](http://staff/konstasa)

 * BI-3589 Set timeout on csv dialect detection  [ https://a.yandex-team.ru/arc/commit/9689722 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-07 17:41:37+03:00

0.51.0
------

* [konstasa](http://staff/konstasa)

 * BI-3519 Disable regular bucket lifecycle cleanup in ya-team  [ https://a.yandex-team.ru/arc/commit/9653198 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-06 21:12:22+03:00

0.50.0
------

* [konstasa](http://staff/konstasa)

 * BI-3589 Reduce sample size in ParseFileTask  [ https://a.yandex-team.ru/arc/commit/9653113 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-29 23:14:00+03:00

0.49.0
------

* [asnytin](http://staff/asnytin)

 * BI-3579: file-uploader-worker services registry fix  [ https://a.yandex-team.ru/arc/commit/9648050 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-29 11:03:27+03:00

0.48.0
------

* [konstasa](http://staff/konstasa)

 * BI-3519 Cleanup files in s3 on org/folder deletion  [ https://a.yandex-team.ru/arc/commit/9632918 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-27 16:40:18+03:00

0.47.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-23 16:33:07+03:00

0.46.0
------

* [asnytin](http://staff/asnytin)

 * BI-3412: DataCloudEnvManagerFactory  [ https://a.yandex-team.ru/arc/commit/9624080 ]

* [konstasa](http://staff/konstasa)

 * BI-3558 Market Couriers connector  [ https://a.yandex-team.ru/arc/commit/9620826 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-23 08:37:04+03:00

0.45.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-22 11:00:13+03:00

0.44.0
------

* [asnytin](http://staff/asnytin)

 * BI-3412: fu-worker services registry datacloud fix  [ https://a.yandex-team.ru/arc/commit/9617618 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-21 23:25:10+03:00

0.43.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-21 19:43:08+03:00

0.42.0
------

* [konstasa](http://staff/konstasa)

 * BI-3502 Remove conditions related to preview_id backward compatibility after migration  [ https://a.yandex-team.ru/arc/commit/9616934 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-21 19:36:19+03:00

0.41.0
------

* [konstasa](http://staff/konstasa)

 * BI-3502 Update SaveSourceTask to handle replaced sources properly  [ https://a.yandex-team.ru/arc/commit/9608414 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-20 13:16:19+03:00

0.40.0
------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3341: Removed data source interfaces from connections  [ https://a.yandex-team.ru/arc/commit/9572243 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-10 02:23:19+03:00

0.39.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-04 14:15:56+03:00

0.38.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-04 10:03:31+03:00

0.37.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-04 09:56:00+03:00

0.36.0
------

* [konstasa](http://staff/konstasa)

 * BI-3502: File conn: Use preview_id instead of source_id to manage previews; Source replacement  [ https://a.yandex-team.ru/arc/commit/9544700 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-03 16:16:31+03:00

0.35.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-02 14:01:09+03:00

0.34.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-31 00:06:23+03:00

0.33.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-26 14:05:37+03:00

0.32.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-25 17:33:26+03:00

0.31.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-24 23:36:57+03:00

0.30.0
------

* [asnytin](http://staff/asnytin)

 * BI-3499: connectors_data refactoring  [ https://a.yandex-team.ru/arc/commit/9492671 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-24 17:57:20+03:00

0.29.0
------

* [asnytin](http://staff/asnytin)

 * BI-3395: fill file conn raw_schema before save tasks scheduling  [ https://a.yandex-team.ru/arc/commit/9487322 ]

* [konstasa](http://staff/konstasa)

 * Fix typo in file-uploader-worker logging  [ https://a.yandex-team.ru/arc/commit/9440672 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-23 22:05:37+03:00

0.28.0
------

* [asnytin](http://staff/asnytin)

 * fixed bi_file_uploader_worker package version  [ https://a.yandex-team.ru/arc/commit/9395277 ]

* [konstasa](http://staff/konstasa)

 * BI-3396 S3 file deletion task  [ https://a.yandex-team.ru/arc/commit/9383282 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-04 20:20:39+03:00

0.27.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-19 21:26:51+03:00

0.26.0
------

* [asnytin](http://staff/asnytin)

 * file uploader fixes  [ https://a.yandex-team.ru/arc/commit/9367093 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-19 10:36:44+03:00

0.25.0
------

* [konstasa](http://staff/konstasa)

 * Single quotes in s3 table function  [ https://a.yandex-team.ru/arc/commit/9353856 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-19 02:25:44+03:00

0.24.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150: file uploader fixes  [ https://a.yandex-team.ru/arc/commit/9327761 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-08 16:18:43+03:00

0.23.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150: fail file status on parsing error  [ https://a.yandex-team.ru/arc/commit/9326805 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-08 13:04:37+03:00

0.22.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150: file uploader s3 bucket settings  [ https://a.yandex-team.ru/arc/commit/9323655 ]

* [thenno](http://staff/thenno)

 * BI-3346: move profiling and ylog from bi_core to bi_app_tools  [ https://a.yandex-team.ru/arc/commit/9321275 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-07 17:33:28+03:00

0.21.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: file uploader redis model authorization by user_id  [ https://a.yandex-team.ru/arc/commit/9321116 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-07 11:50:48+03:00

0.20.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: file connection preview in bi_api  [ https://a.yandex-team.ru/arc/commit/9317225 ]

* [konstasa](http://staff/konstasa)

 * BI-3099: Fixed distincts with file connection  [ https://a.yandex-team.ru/arc/commit/9316211 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-06 20:53:24+03:00

0.19.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150: Fixed s3 batch size calculation  [ https://a.yandex-team.ru/arc/commit/9313215 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-05 16:25:27+03:00

0.18.0
------

* [asnytin](http://staff/asnytin)

 * file uploader task debug logging  [ https://a.yandex-team.ru/arc/commit/9312383 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-05 13:45:41+03:00

0.17.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: file_uploader fixes                        [ https://a.yandex-team.ru/arc/commit/9311199 ]
 * fixed file_uploader sentry settings                 [ https://a.yandex-team.ru/arc/commit/9311144 ]
 * BI-3099: file_uploader apply_settings api and task  [ https://a.yandex-team.ru/arc/commit/9310687 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-05 10:21:32+03:00

0.16.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 22:24:08+03:00

0.15.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 21:44:03+03:00

0.14.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 21:26:46+03:00

0.13.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150: file_uploader s3 keys quoting fix  [ https://a.yandex-team.ru/arc/commit/9309991 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 20:07:39+03:00

0.12.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150: file_uploader tasks: fixed BIType load  [ https://a.yandex-team.ru/arc/commit/9309654 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 19:10:31+03:00

0.11.0
------

* [thenno](http://staff/thenno)

 * BI-3328: change healthcheck command  [ https://a.yandex-team.ru/arc/commit/9308832 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 17:16:37+03:00

0.10.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150 s3 config fix  [ https://a.yandex-team.ru/arc/commit/9306920 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 12:38:28+03:00

0.9.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3150: file_uploader save source task - part 2  [ https://a.yandex-team.ru/arc/commit/9304809 ]

* [thenno](http://staff/thenno)

 * BI-3328: add task processor health check  [ https://a.yandex-team.ru/arc/commit/9303297 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 10:57:32+03:00

0.8.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3150: forgotten dockerfile in file uploader tests  [ https://a.yandex-team.ru/arc/commit/9301310 ]
 * BI-3150: file uploader source save task - part 1      [ https://a.yandex-team.ru/arc/commit/9301170 ]

* [thenno](http://staff/thenno)

 * BI-3327: add sentry to task processor projects  [ https://a.yandex-team.ru/arc/commit/9299806 ]
 * BI-3325: add common task processor cli          [ https://a.yandex-team.ru/arc/commit/9294702 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-01 17:00:09+03:00

0.7.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3099: file uploader file/source status API handler  [ https://a.yandex-team.ru/arc/commit/9289220 ]

* [thenno](http://staff/thenno)

 * BI-3284: add retries and tests to task processor  [ https://a.yandex-team.ru/arc/commit/9288439 ]

* [konstasa](http://staff/konstasa)

 * BI-3151 ConnExecutor and DBAdapter for CHS3  [ https://a.yandex-team.ru/arc/commit/9286381 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-30 11:05:48+03:00

0.6.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3099: file uploader: preview generation and source info api  [ https://a.yandex-team.ru/arc/commit/9284335 ]
 * bi_file_uploader init-db for s3                                 [ https://a.yandex-team.ru/arc/commit/9280307 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-29 11:02:29+03:00

0.5.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3099: arq redis service  [ https://a.yandex-team.ru/arc/commit/9278766 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-28 13:05:44+03:00

0.4.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3099: file_uploader parse_file task  [ https://a.yandex-team.ru/arc/commit/9274613 ]

* [thenno](http://staff/thenno)

 * BI-3231 remove tp adapter, add utils for redis, and cli to file uploader  [ https://a.yandex-team.ru/arc/commit/9271135 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-26 17:40:43+03:00

0.3.0
-----

* [konstasa](http://staff/konstasa)

 * File uploader worker copy/paste consequences  [ https://a.yandex-team.ru/arc/commit/9270098 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-24 17:18:59+03:00

0.2.0
-----

* [konstasa](http://staff/konstasa)

 * Correct deploy units IDs for file uploader and worker  [ https://a.yandex-team.ru/arc/commit/9269473 ]

* [thenno](http://staff/thenno)

 * BI-3276: use common logger context             [ https://a.yandex-team.ru/arc/commit/9264705 ]
 * BI-3287: add ArqWorker wrapper                 [ https://a.yandex-team.ru/arc/commit/9261547 ]
 * BI-3276: add common logger for task processor  [ https://a.yandex-team.ru/arc/commit/9246719 ]
 * BI-3125: rename _LOGGER -> LOGGER              [ https://a.yandex-team.ru/arc/commit/9234075 ]

* [asnytin](http://staff/asnytin)

 * BI-3150: bi_file_uploader_worker setup                                      [ https://a.yandex-team.ru/arc/commit/9242320 ]
 * BI-3149: file uploader bi_core connector                                    [ https://a.yandex-team.ru/arc/commit/9239438 ]
 * BI-3148: added bi_file_uploader_worker and bi_file_uploader_task_interface  [ https://a.yandex-team.ru/arc/commit/9227262 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-24 15:50:22+03:00

0.1.0
-----
 * BI-3148: added bi_file_uploader_worker and bi_file_uploader_task_interface  [ https://a.yandex-team.ru/arc_vcs/commit/e915235c96e58c89e030c3cdb010b88f46f8be10 ]

[asnytin](http://staff/asnytin) 2022-03-12 15:55:23+04:00

