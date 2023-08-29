0.115.0
-------

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

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version lib/bi_file_uploader_worker_lib 0.137.0 (#174)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e01d0a1 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-29 13:43:00+00:00

0.114.0
-------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * BI-4860: load connectors settings using a dynamically generated class (#150)  [ https://github.com/datalens-tech/datalens-backend-private/commit/d0a942d ]

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version app/bi_file_secure_reader 0.13.0 (#167)         [ https://github.com/datalens-tech/datalens-backend-private/commit/a744607 ]
 * releasing version lib/bi_file_uploader_worker_lib 0.136.0 (#166)  [ https://github.com/datalens-tech/datalens-backend-private/commit/6a012d7 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-29 09:48:20+00:00

0.113.0
-------

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

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version lib/bi_api_lib 0.1979.0 (#132)          [ https://github.com/datalens-tech/datalens-backend-private/commit/74689b2 ]
 * releasing version lib/bi_api_lib 0.1978.0 (#124)          [ https://github.com/datalens-tech/datalens-backend-private/commit/f557177 ]
 * releasing version ops/bi_integration_tests 0.20.0 (#118)  [ https://github.com/datalens-tech/datalens-backend-private/commit/b896c79 ]
 * releasing version lib/bi_api_lib 0.1977.0 (#116)          [ https://github.com/datalens-tech/datalens-backend-private/commit/4a28df1 ]
 * releasing version lib/bi_api_lib 0.1976.0 (#115)          [ https://github.com/datalens-tech/datalens-backend-private/commit/7dd0c10 ]
 * releasing version ops/bi_integration_tests 0.19.0 (#113)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e6f21fe ]
 * releasing version lib/bi_api_lib 0.1975.0 (#109)          [ https://github.com/datalens-tech/datalens-backend-private/commit/0e10952 ]
 * releasing version lib/bi_api_lib 0.1974.0 (#107)          [ https://github.com/datalens-tech/datalens-backend-private/commit/180b8a8 ]
 * releasing version ops/bi_integration_tests 0.18.0 (#101)  [ https://github.com/datalens-tech/datalens-backend-private/commit/9d66cbd ]
 * releasing version ops/bi_integration_tests 0.17.0 (#97)   [ https://github.com/datalens-tech/datalens-backend-private/commit/55a217b ]
 * releasing version lib/bi_api_lib 0.1973.0 (#94)           [ https://github.com/datalens-tech/datalens-backend-private/commit/bb14b67 ]
 * releasing version lib/bi_api_lib 0.1972.0 (#89)           [ https://github.com/datalens-tech/datalens-backend-private/commit/643bfaf ]
 * releasing version ops/bi_integration_tests 0.16.0 (#85)   [ https://github.com/datalens-tech/datalens-backend-private/commit/038232f ]
 * releasing version lib/bi_api_lib 0.1971.0 (#81)           [ https://github.com/datalens-tech/datalens-backend-private/commit/23f04eb ]
 * releasing version ops/bi_integration_tests 0.15.0 (#78)   [ https://github.com/datalens-tech/datalens-backend-private/commit/5a1933e ]
 * releasing version ops/bi_integration_tests 0.14.0 (#73)   [ https://github.com/datalens-tech/datalens-backend-private/commit/986cbc8 ]
 * releasing version ops/bi_integration_tests 0.13.0 (#64)   [ https://github.com/datalens-tech/datalens-backend-private/commit/9fdff6a ]
 * releasing version ops/bi_integration_tests 0.12.0 (#58)   [ https://github.com/datalens-tech/datalens-backend-private/commit/cf0b7ac ]
 * releasing version lib/bi_api_lib 0.1970.0 (#40)           [ https://github.com/datalens-tech/datalens-backend-private/commit/3c2ed13 ]

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-29 08:57:28+00:00

0.112.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-08 09:59:40+00:00

0.111.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-07 15:57:21+00:00

0.110.0
-------

* [vgol](http://staff/vgol)

 * moved app_yc one level up & fixed bad local dep  [ https://a.yandex-team.ru/arc/commit/12044054 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-28 16:39:20+00:00

0.109.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-28 08:34:50+00:00

0.108.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 09:30:30+00:00

0.107.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4478 Unique package names for YC apps  [ https://a.yandex-team.ru/arc/commit/12024949 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 08:57:32+00:00

0.106.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4478 YC apps  [ https://a.yandex-team.ru/arc/commit/12022965 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-26 16:15:34+00:00

0.105.0
-------

* Sync version
