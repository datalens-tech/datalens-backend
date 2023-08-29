0.114.0
-------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * BI-4860: load connectors settings using a dynamically generated class (#150)  [ https://github.com/datalens-tech/datalens-backend-private/commit/d0a942d ]

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version app/bi_file_secure_reader 0.13.0 (#167)         [ https://github.com/datalens-tech/datalens-backend-private/commit/a744607 ]
 * releasing version lib/bi_file_uploader_worker_lib 0.136.0 (#166)  [ https://github.com/datalens-tech/datalens-backend-private/commit/6a012d7 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-29 09:48:19+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-29 08:57:27+00:00

0.112.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-08 09:59:36+00:00

0.111.0
-------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4824 Add tier-1 Dockerfile for bi-file-uploader  [ https://a.yandex-team.ru/arc/commit/12113138 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-07 15:57:17+00:00

0.110.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-28 16:39:16+00:00

0.109.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-28 08:34:46+00:00

0.108.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 09:30:26+00:00

0.107.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 08:57:28+00:00

0.106.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-26 16:15:31+00:00

0.105.0
-------

* [vgol](http://staff/vgol)

 * BI-4592: updated .release.hjson to use pyproject.toml instead of setup.py      [ https://a.yandex-team.ru/arc/commit/11909029 ]
 * BI-4592: re using old local dev, but supplying deps from pyproject toml files  [ https://a.yandex-team.ru/arc/commit/11908104 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-14 11:45:11+00:00

0.104.0
-------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-11 09:48:14+00:00

0.103.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4356 Introduce bi_file_uploader_api_lib                           [ https://a.yandex-team.ru/arc/commit/11801543 ]
 * BI-4356 Move common file-uploader modules into bi_file_uploader_lib  [ https://a.yandex-team.ru/arc/commit/11767670 ]

* [mcpn](http://staff/mcpn)

 * BI-4540: move blackbox middlewares to bi_api_commons_ya_team  [ https://a.yandex-team.ru/arc/commit/11797588 ]

* [dmifedorov](http://staff/dmifedorov)

 * replace aioredis with fresh redis             [ https://a.yandex-team.ru/arc/commit/11777084 ]
 * hosts and ports from docker-compose in tests  [ https://a.yandex-team.ru/arc/commit/11774830 ]

* [vgol](http://staff/vgol)

 * BI-4592: Added script to generate pyproject.toml from setup.py + ya.make, with reference versions taken from prod container  [ https://a.yandex-team.ru/arc/commit/11774366 ]

* [kchupin](http://staff/kchupin)

 * [BI-4632] Extract DLRequestBase to bi_api_commons                                                                                  [ https://a.yandex-team.ru/arc/commit/11730197 ]
 * [BI-4631] Remove `yenv` usage in file uploader worker. Was used to determine if lauched in tests. Replaced with flag in settings.  [ https://a.yandex-team.ru/arc/commit/11700552 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-27 16:13:52+00:00

0.102.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4610 Do not delete data and throw errors on exec for errors not caused by the user on gsheets autoupdate  [ https://a.yandex-team.ru/arc/commit/11685256 ]
 * BI-4611 Improve (and increase time) retries for gsheets api                                                  [ https://a.yandex-team.ru/arc/commit/11668720 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-13 07:06:08+00:00

0.101.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4609 Add quota exceeded errors for gsheets                  [ https://a.yandex-team.ru/arc/commit/11647137 ]
 * BI-4599 Add request-id to error details on gsheets autoupdate  [ https://a.yandex-team.ru/arc/commit/11625245 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-06 14:18:43+00:00

0.100.0
-------

* [konstasa](http://staff/konstasa)

 * BI-4557 Fail download when too few rows received; release source update lock on manual data update  [ https://a.yandex-team.ru/arc/commit/11565971 ]
 * BI-4557 Remove redundant multipart upload creation request from the /files view                     [ https://a.yandex-team.ru/arc/commit/11565970 ]
 * BI-4389 Remove ConnectionInternalCH, leaving InternalMaterializationConnectionRef                   [ https://a.yandex-team.ru/arc/commit/11377220 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved CHS3 core connectors to a separate package                                       [ https://a.yandex-team.ru/arc/commit/11509091 ]
 * Replaced missing and default with load_default and dump_default in marshmallow fields  [ https://a.yandex-team.ru/arc/commit/11465506 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4398 Move TenantDef and AuthData implementations to bi_api_commons.  [ https://a.yandex-team.ru/arc/commit/11155774 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-26 09:33:57+00:00

0.99.0
------

* [asnytin](http://staff/asnytin)

 * BI-4286: added file-upl rename tenant files api handler and task  [ https://a.yandex-team.ru/arc/commit/11051658 ]

* [konstasa](http://staff/konstasa)

 * BI-4257 Update aiogoogle & remove url safe chars workaround  [ https://a.yandex-team.ru/arc/commit/11037476 ]
 * BI-4258 Move api key from query params to headers            [ https://a.yandex-team.ru/arc/commit/11036079 ]
 * Remove connector settings set to None in tests               [ https://a.yandex-team.ru/arc/commit/10968577 ]
 * Pad years <1000 properly when parsing gsheets                [ https://a.yandex-team.ru/arc/commit/10954291 ]

* [thenno](http://staff/thenno)

 * Use arq from contrib (and drop some mat-tests)  [ https://a.yandex-team.ru/arc/commit/10991557 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4056 Restore Feature Date32 for clickhouse connections. Convert user date type to Date32.                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/10960390 ]
 * Revert "BI-4056 Feature Date32 for clickhouse connections. Convert user date type to Date32."

This reverts commit b65d02f2548784d3b12f72b4602774362dc96749, reversing
changes made to 4a4ed7ae678b93240b64a3ca00389df61033e4ba.  [ https://a.yandex-team.ru/arc/commit/10957507 ]
 * BI-4281 feature bi_api_commons package                                                                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/10951763 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-15 09:56:43+00:00

0.98.0
------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4056 Feature Date32 for clickhouse connections. Convert user date type to Date32.  [ https://a.yandex-team.ru/arc/commit/10880983 ]

* [konstasa](http://staff/konstasa)

 * BI-4244 Equeo partner connector                                 [ https://a.yandex-team.ru/arc/commit/10871053 ]
 * BI-4257 Fix forward slash encoding in gsheets api request urls  [ https://a.yandex-team.ru/arc/commit/10870118 ]
 * Recreate test gsheets with new permissions and disclaimers      [ https://a.yandex-team.ru/arc/commit/10743408 ]

* [seray](http://staff/seray)

 * BI-4095 bi_file_secure_reader microservice  [ https://a.yandex-team.ru/arc/commit/10833539 ]

* [kchupin](http://staff/kchupin)

 * [BI-4112] Update & fix tier-1 dependencies versions according A to make local installation possible  [ https://a.yandex-team.ru/arc/commit/10772550 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-21 09:49:45+00:00

0.97.0
------

* [konstasa](http://staff/konstasa)

 * BI-4013 BI-4116 Store original errors instead if INVALID_SOURCE on data update failure; store the whole error in DataFile, not just code; refactor connection source failing  [ https://a.yandex-team.ru/arc/commit/10667127 ]

* [seray](http://staff/seray)

 * BI-4094 return None for empty options      [ https://a.yandex-team.ru/arc/commit/10660069 ]
 * YCDOCS-7328 test for long line whith None  [ https://a.yandex-team.ru/arc/commit/10656236 ]

* [shadchin](http://staff/shadchin)

 * IGNIETFERRO-1154 Rename uwsgi to uWSGI  [ https://a.yandex-team.ru/arc/commit/10610666 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-16 10:50:30+00:00

0.96.0
------

* [seray](http://staff/seray)

 * BI-4094 xlsx in resp scheme      [ https://a.yandex-team.ru/arc/commit/10602485 ]
 * BI-4094 xlsx file datetime repr  [ https://a.yandex-team.ru/arc/commit/10596389 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-30 15:17:25+00:00

0.95.0
------

* [seray](http://staff/seray)

 * BI-4094 xlsx file support  [ https://a.yandex-team.ru/arc/commit/10586650 ]

* [konstasa](http://staff/konstasa)

 * BI-4013 Do not remove raw_schema in failed sources during data update to let them load in dataset; move connection error raise back into datasource                                      [ https://a.yandex-team.ru/arc/commit/10572555 ]
 * BI-4013 Fix the issue with unknown field errors occuring before connection component errors; allow filling file source params with None values to fail them properly during data update  [ https://a.yandex-team.ru/arc/commit/10568355 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-28 10:20:31+00:00

0.94.0
------

* [konstasa](http://staff/konstasa)

 * BI-4152 Wrap sync work in download task and gsheets client in TPE                                                      [ https://a.yandex-team.ru/arc/commit/10562475 ]
 * BI-4116 Handle too many columns and too large file in file uploader properly: do not fail, just save error into dfile  [ https://a.yandex-team.ru/arc/commit/10561446 ]
 * BI-4013 Store errors that occur during data update in file-based connections error registry and throw them on execute  [ https://a.yandex-team.ru/arc/commit/10559993 ]
 * BI-4114 Fix missing column naming; other minor fixes                                                                   [ https://a.yandex-team.ru/arc/commit/10523020 ]

* [thenno](http://staff/thenno)

 * Make mypy tests large in bi_file_uploader, bi_file_uploader_task_interface and bi_task_processor  [ https://a.yandex-team.ru/arc/commit/10551230 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-22 10:21:23+00:00

0.93.0
------

* [konstasa](http://staff/konstasa)

 * GSheets autoupdate: make update source lock non-blocking  [ https://a.yandex-team.ru/arc/commit/10504874 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-13 12:10:40+00:00

0.92.0
------

* [konstasa](http://staff/konstasa)

 * Fix gsheets autoupdate for the third time by explicitly sending tenant_id  [ https://a.yandex-team.ru/arc/commit/10499254 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-12 16:17:06+00:00

0.91.0
------

* [konstasa](http://staff/konstasa)

 * Fix gsheets autoupdate in public once again by creating DataFile owned by system      [ https://a.yandex-team.ru/arc/commit/10497660 ]
 * BI-4114 Generate missing titles and column names for gsheets using alphabet notation  [ https://a.yandex-team.ru/arc/commit/10493401 ]
 * Fix parsing gsheets with no header; improve & refactor gsheets tests in fuw           [ https://a.yandex-team.ru/arc/commit/10489272 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-12 14:02:30+00:00

0.90.0
------

* [konstasa](http://staff/konstasa)

 * BI-4115 GSheets fix save by fixing data sink for download + better logging                                [ https://a.yandex-team.ru/arc/commit/10484741 ]
 * BI-4078 Use spreadsheets.values.get in chunk loading to speed it up                                       [ https://a.yandex-team.ru/arc/commit/10481532 ]
 * Add another endpoint for update_connection_data with auth skip                                            [ https://a.yandex-team.ru/arc/commit/10463625 ]
 * BI-4090 Parse dates & datetimes in gsheets by their internal gsheets format and not the formatted string  [ https://a.yandex-team.ru/arc/commit/10451898 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-09 08:39:59+00:00

0.89.0
------

* [konstasa](http://staff/konstasa)

 * BI-4078 Load gsheets in chunks to support big spreadsheets; exponential backoff to avoid quota limit errors  [ https://a.yandex-team.ru/arc/commit/10443427 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-02 13:23:04+00:00

0.88.0
------

* [konstasa](http://staff/konstasa)

 * Stringify preview data in file-uploader responses  [ https://a.yandex-team.ru/arc/commit/10443033 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-02 12:46:50+00:00

0.87.0
------

* [konstasa](http://staff/konstasa)

 * Fix bool parsing for gsheets                                                                                                [ https://a.yandex-team.ru/arc/commit/10437726 ]
 * BI-4068 Move task processor task schedulling from dataset-api views to lifecycle manager; autouse use_local_task_processor  [ https://a.yandex-team.ru/arc/commit/10429680 ]

* [mcpn](http://staff/mcpn)

 * BI-3731: add proper pickling for lambdas in clickhouse-sqlalchemy  [ https://a.yandex-team.ru/arc/commit/10386254 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-01 17:11:49+00:00

0.86.0
------

* [konstasa](http://staff/konstasa)

 * BI-4054 Fix broken data update when one sheet is added multiple times                           [ https://a.yandex-team.ru/arc/commit/10357020 ]
 * BI-3921 Better response when sources lack fields needed to update them                          [ https://a.yandex-team.ru/arc/commit/10357012 ]
 * Allow file source polling by returning file_id from connection and syncing DataFile on replace  [ https://a.yandex-team.ru/arc/commit/10355287 ]
 * BI-3922 Add error field to the source status response                                           [ https://a.yandex-team.ru/arc/commit/10355278 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-17 16:19:20+00:00

0.85.0
------

* [konstasa](http://staff/konstasa)

 * BI-3920 Use same crypto keys config for file-uploader and worker in fu tests                                [ https://a.yandex-team.ru/arc/commit/10331113 ]
 * BI-3922 Handle aiogoogle errors with unknown structure                                                      [ https://a.yandex-team.ru/arc/commit/10330963 ]
 * BI-3980 Cleanup old files from S3 on data update                                                            [ https://a.yandex-team.ru/arc/commit/10330955 ]
 * Improve request handling in file-uploader-api: ignore unknown fields in requests, handle validation errors  [ https://a.yandex-team.ru/arc/commit/10330921 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-14 11:57:59+00:00

0.84.0
------

* [dmifedorov](http://staff/dmifedorov)

 * BI-4016: AppType.NEBIUS for file uploader  [ https://a.yandex-team.ru/arc/commit/10317798 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-10 16:28:29+03:00

0.83.0
------

* [konstasa](http://staff/konstasa)

 * Token refreshing for gsheets  [ https://a.yandex-team.ru/arc/commit/10293280 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-07 13:36:53+03:00

0.82.0
------

* [konstasa](http://staff/konstasa)

 * BI-4012 Make authorize field required in request schema  [ https://a.yandex-team.ru/arc/commit/10273114 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-02 20:06:49+03:00

0.81.0
------

* [konstasa](http://staff/konstasa)

 * BI-4012 New authorization flow in GSheets  [ https://a.yandex-team.ru/arc/commit/10265843 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-01 14:15:50+03:00

0.80.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-25 15:59:36+03:00

0.79.0
------

* [konstasa](http://staff/konstasa)

 * BI-3628 Add missing gsheets related fields into file-uploader source info response  [ https://a.yandex-team.ru/arc/commit/10220597 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-24 13:24:52+03:00

0.78.0
------

* [konstasa](http://staff/konstasa)

 * BI-3901 Add missing fields to request schema in file uploader client for /update_connection_data && partially remove tenant_id  [ https://a.yandex-team.ru/arc/commit/10220134 ]
 * BI-3922 Update status codes in links api (not found and permission denied are now 400)                                          [ https://a.yandex-team.ru/arc/commit/10219964 ]
 * BI-3901 GSheets autoupdate: use UTC time everywhere                                                                             [ https://a.yandex-team.ru/arc/commit/10213268 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-24 12:47:21+03:00

0.77.0
------

* [asnytin](http://staff/asnytin)

 * BI-3986: genericdatetime in file-connector  [ https://a.yandex-team.ru/arc/commit/10211553 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-21 15:33:22+03:00

0.76.0
------

* [konstasa](http://staff/konstasa)

 * BI-3921 GSheets one-time update; no save by default  [ https://a.yandex-team.ru/arc/commit/10205816 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-20 18:34:02+03:00

0.75.0
------

* [konstasa](http://staff/konstasa)

 * Fix internal params retieval when there is no raw_schema     [ https://a.yandex-team.ru/arc/commit/10186280 ]
 * Fix raw schema calculation for gsheets                       [ https://a.yandex-team.ru/arc/commit/10184931 ]
 * BI-3922 Improve error handling for gsheets in file uploader  [ https://a.yandex-team.ru/arc/commit/10184877 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-18 10:53:29+03:00

0.74.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-17 15:43:12+03:00

0.73.0
------

* [konstasa](http://staff/konstasa)

 * Fix data_settings serialization in file uploader source info response  [ https://a.yandex-team.ru/arc/commit/10180982 ]

* [mcpn](http://staff/mcpn)

 * BI-3885: file uploader metric view  [ https://a.yandex-team.ru/arc/commit/10170603 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-17 15:37:41+03:00

0.72.0
------

* [konstasa](http://staff/konstasa)

 * BI-3921 GSheets one-time update  [ https://a.yandex-team.ru/arc/commit/10169420 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-14 13:19:38+03:00

0.71.0
------

* [konstasa](http://staff/konstasa)

 * BI-3920 Encryption in file-uploader(-worker) + use token for GSheets  [ https://a.yandex-team.ru/arc/commit/10157386 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-12 17:15:45+03:00

0.70.0
------

* [konstasa](http://staff/konstasa)

 * BI-3901 GSheets autoupdate; step 2: tasks & notification  [ https://a.yandex-team.ru/arc/commit/10134513 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-07 18:09:35+03:00

0.69.0
------

* [konstasa](http://staff/konstasa)

 * Fix data_settings in source info response  [ https://a.yandex-team.ru/arc/commit/10124148 ]
 * BI-3901 GSheets autoupdate; step 1: API    [ https://a.yandex-team.ru/arc/commit/10105838 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-06 10:43:21+03:00

0.68.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-28 16:30:03+03:00

0.67.0
------

* [konstasa](http://staff/konstasa)

 * BI-3784 Add source errors to file-uploader responses  [ https://a.yandex-team.ru/arc/commit/10052712 ]
 * BI-3784 Remove column types overrides for gsheets     [ https://a.yandex-team.ru/arc/commit/10051038 ]
 * Move GSheetsClient into bi_file_uploader              [ https://a.yandex-team.ru/arc/commit/10047947 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-28 16:22:00+03:00

0.66.0
------

* [konstasa](http://staff/konstasa)

 * BI-3784 Remove re-guessing logic from file parser for gsheets  [ https://a.yandex-team.ru/arc/commit/10043919 ]
 * BI-3628 Add title into file-uploader upload response           [ https://a.yandex-team.ru/arc/commit/10028719 ]
 * BI-3784 Switch to aiogoogle in GSheetsClient                   [ https://a.yandex-team.ru/arc/commit/10014820 ]
 * BI-3867 Use local task processor in tests                      [ https://a.yandex-team.ru/arc/commit/9993902 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-20 13:22:12+03:00

0.65.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-09 15:06:34+03:00

0.64.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-07 19:11:11+03:00

0.63.0
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

[robot-statinfra](http://staff/robot-statinfra) 2022-09-06 15:08:17+03:00

0.62.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-01 17:06:50+03:00

0.61.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-08-30 12:11:30+03:00

0.60.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-08-26 15:51:19+03:00

0.59.0
------

* [konstasa](http://staff/konstasa)

 * BI-3784 GSheets processing in file-uploader-worker: download and parse              [ https://a.yandex-team.ru/arc/commit/9919711 ]
 * BI-3628 Add file type diversity into DataFile; add links endpoint to file-uploader  [ https://a.yandex-team.ru/arc/commit/9879129 ]

* [thenno](http://staff/thenno)

 * BI-3404: add request_id to TaskProcessor  [ https://a.yandex-team.ru/arc/commit/9875189 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-26 12:41:14+03:00

0.58.0
------

* [konstasa](http://staff/konstasa)

 * BI-3691 Cleanup file previews on tenant deletion  [ https://a.yandex-team.ru/arc/commit/9844484 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-10 12:56:56+03:00

0.57.0
------

* [konstasa](http://staff/konstasa)

 * BI-3519 Explicitly set tenant in RCI for the request to file-uploader and skip auth on cleanup endpoint  [ https://a.yandex-team.ru/arc/commit/9702585 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-18 11:49:13+03:00

0.56.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-07-06 21:14:43+03:00

0.55.0
------

* [konstasa](http://staff/konstasa)

 * BI-3519 Cleanup files in s3 on org/folder deletion  [ https://a.yandex-team.ru/arc/commit/9632918 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-27 16:40:16+03:00

0.54.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-22 10:58:39+03:00

0.53.0
------

* [konstasa](http://staff/konstasa)

 * BI-3502 Remove conditions related to preview_id backward compatibility after migration  [ https://a.yandex-team.ru/arc/commit/9616934 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-21 19:36:31+03:00

0.52.0
------

* [konstasa](http://staff/konstasa)

 * BI-3502 Update SaveSourceTask to handle replaced sources properly  [ https://a.yandex-team.ru/arc/commit/9608414 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-20 13:16:30+03:00

0.51.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-15 22:37:01+03:00

0.50.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-10 20:47:59+03:00

0.49.0
------

* [asnytin](http://staff/asnytin)

 * BI-3412: fetch iam token for SA in yc-auth  [ https://a.yandex-team.ru/arc/commit/9575551 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-10 14:23:18+03:00

0.48.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-10 11:11:32+03:00

0.47.0
------

* [asnytin](http://staff/asnytin)

 * BI-3412: static sa token for file-uploader  [ https://a.yandex-team.ru/arc/commit/9551660 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-06 14:59:16+03:00

0.46.0
------

* [asnytin](http://staff/asnytin)

 * BI-3412: redis ssl settings  [ https://a.yandex-team.ru/arc/commit/9547428 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-04 10:06:35+03:00

0.45.0
------

* [konstasa](http://staff/konstasa)

 * BI-3502 Fix internal params api schemas  [ https://a.yandex-team.ru/arc/commit/9546818 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-03 23:13:02+03:00

0.44.0
------

* [konstasa](http://staff/konstasa)

 * BI-3502: File conn: Use preview_id instead of source_id to manage previews; Source replacement  [ https://a.yandex-team.ru/arc/commit/9544700 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-03 16:16:42+03:00

0.43.0
------

* [asnytin](http://staff/asnytin)

 * BI-3412: use session-service in async yc_auth middleware (for cookies resolving)  [ https://a.yandex-team.ru/arc/commit/9518631 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-31 00:07:29+03:00

0.42.0
------

* [asnytin](http://staff/asnytin)

 * BI-3412: file-uploader cloud auth settings  [ https://a.yandex-team.ru/arc/commit/9509385 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-27 10:24:07+03:00

0.41.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-27 10:03:45+03:00

0.40.0
------

* [asnytin](http://staff/asnytin)

 * BI-3395: skip_csrf view flag  [ https://a.yandex-team.ru/arc/commit/9507580 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-26 19:04:44+03:00

0.39.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-26 14:06:21+03:00

0.38.0
------

* [asnytin](http://staff/asnytin)

 * BI-3412: file-uploader: fixed redis db settings fetching (repeat)  [ https://a.yandex-team.ru/arc/commit/9500788 ]
 * BI-3412: file-uploader settings for doublecloud                    [ https://a.yandex-team.ru/arc/commit/9500663 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-25 17:34:14+03:00

0.37.0
------

* [asnytin](http://staff/asnytin)

 * BI-3412: file-uploader: fixed redis db settings fetching  [ https://a.yandex-team.ru/arc/commit/9496796 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-24 23:36:56+03:00

0.36.0
------

* [asnytin](http://staff/asnytin)

 * BI-3499: connectors_data refactoring  [ https://a.yandex-team.ru/arc/commit/9492671 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-24 17:57:36+03:00

0.35.0
------

* [asnytin](http://staff/asnytin)

 * BI-3395: file_uploader: fixed source info request schema  [ https://a.yandex-team.ru/arc/commit/9492260 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-24 11:58:11+03:00

0.34.0
------

* [asnytin](http://staff/asnytin)

 * BI-3395: fill file conn raw_schema before save tasks scheduling  [ https://a.yandex-team.ru/arc/commit/9487322 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-23 22:05:27+03:00

0.33.0
------

* [konstasa](http://staff/konstasa)

 * BI-3396 S3 file deletion task  [ https://a.yandex-team.ru/arc/commit/9383282 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-04 19:42:57+03:00

0.32.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: file-uploader allow utf-16 encoding  [ https://a.yandex-team.ru/arc/commit/9371549 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-19 21:26:24+03:00

0.31.0
------

* [asnytin](http://staff/asnytin)

 * file uploader fixes  [ https://a.yandex-team.ru/arc/commit/9367093 ]

* [konstasa](http://staff/konstasa)

 * BI-3360: File uploader file size limit  [ https://a.yandex-team.ru/arc/commit/9346929 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-19 10:36:50+03:00

0.30.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: file uploader api cors 500 fix attempt  [ https://a.yandex-team.ru/arc/commit/9324769 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-07 21:06:14+03:00

0.29.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150: file uploader s3 bucket settings  [ https://a.yandex-team.ru/arc/commit/9323655 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-07 17:33:11+03:00

0.28.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: file source status fix  [ https://a.yandex-team.ru/arc/commit/9321963 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-07 13:42:50+03:00

0.27.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: file uploader redis model authorization by user_id  [ https://a.yandex-team.ru/arc/commit/9321116 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-07 11:50:42+03:00

0.26.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-06 20:53:29+03:00

0.25.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: file uploader api sentry  [ https://a.yandex-team.ru/arc/commit/9319441 ]

* [konstasa](http://staff/konstasa)

 * BI-3099: Fixed distincts with file connection  [ https://a.yandex-team.ru/arc/commit/9316211 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-06 20:06:56+03:00

0.24.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: file_uploader preview API handler  [ https://a.yandex-team.ru/arc/commit/9311726 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-05 13:45:46+03:00

0.23.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: file_uploader fixes                        [ https://a.yandex-team.ru/arc/commit/9311199 ]
 * BI-3099: file_uploader apply_settings api and task  [ https://a.yandex-team.ru/arc/commit/9310687 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-05 10:21:41+03:00

0.22.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 22:24:24+03:00

0.21.0
------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 21:26:07+03:00

0.20.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150: file_uploder: upload to s3 with DataSink  [ https://a.yandex-team.ru/arc/commit/9309333 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 19:10:38+03:00

0.19.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: call bi_core.register_all_connectors in file_uploader app  [ https://a.yandex-team.ru/arc/commit/9307532 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 14:23:46+03:00

0.18.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150 s3 config fix  [ https://a.yandex-team.ru/arc/commit/9306920 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 12:38:08+03:00

0.17.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150: task processor task scheduling from bi_api  [ https://a.yandex-team.ru/arc/commit/9306288 ]
 * BI-3150: file_uploader save source task - part 2     [ https://a.yandex-team.ru/arc/commit/9304809 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 10:57:36+03:00

0.16.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150: file uploader source save task - part 1  [ https://a.yandex-team.ru/arc/commit/9301170 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-01 17:00:18+03:00

0.15.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: file uploader file/source status API handler  [ https://a.yandex-team.ru/arc/commit/9289220 ]

* [konstasa](http://staff/konstasa)

 * BI-3151 ConnExecutor and DBAdapter for CHS3  [ https://a.yandex-team.ru/arc/commit/9286381 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-30 11:06:05+03:00

0.14.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: file uploader: preview generation and source info api  [ https://a.yandex-team.ru/arc/commit/9284335 ]
 * bi_file_uploader init-db for s3                                 [ https://a.yandex-team.ru/arc/commit/9280307 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-29 11:02:44+03:00

0.13.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: arq redis service              [ https://a.yandex-team.ru/arc/commit/9278766 ]
 * BI-3099: file_uploader parse_file task  [ https://a.yandex-team.ru/arc/commit/9274613 ]
 * BI-3099: redis_model storage schemas    [ https://a.yandex-team.ru/arc/commit/9253112 ]

* [konstasa](http://staff/konstasa)

 * Correct deploy units IDs for file uploader and worker  [ https://a.yandex-team.ru/arc/commit/9269473 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-28 13:07:23+03:00

0.12.0
------

* [asnytin](http://staff/asnytin)

 * BI-3099: fixed file_uploader settings  [ https://a.yandex-team.ru/arc/commit/9250913 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-18 21:05:39+03:00

0.11.0
------

* [asnytin](http://staff/asnytin)

 * BI-3256: csrf conf fix                    [ https://a.yandex-team.ru/arc/commit/9245976 ]
 * BI-3150: bi_file_uploader_worker setup    [ https://a.yandex-team.ru/arc/commit/9242320 ]
 * BI-3149: file uploader bi_core connector  [ https://a.yandex-team.ru/arc/commit/9239438 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-17 19:25:36+03:00

0.10.0
------

* [asnytin](http://staff/asnytin)

 * BI-3150: file_uploader s3 upload                                            [ https://a.yandex-team.ru/arc/commit/9230542 ]
 * BI-3148: added bi_file_uploader_worker and bi_file_uploader_task_interface  [ https://a.yandex-team.ru/arc/commit/9227262 ]

* [konstasa](http://staff/konstasa)

 * [BI-3256] Aiohttp CSRF middleware                                                                 [ https://a.yandex-team.ru/arc/commit/9224986 ]
 * [BI-3245] Combine req id and error handling middlewares in the RequestBootstrap and log ERR_CODE  [ https://a.yandex-team.ru/arc/commit/9224180 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-15 23:18:05+03:00

0.9.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3099: file_uploader - one more CORS attempt  [ https://a.yandex-team.ru/arc/commit/9198903 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-03 11:51:14+03:00

0.8.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3099: CORS debug  [ https://a.yandex-team.ru/arc/commit/9195432 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-02 13:45:38+03:00

0.7.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3099: CORS for bi_file_uploader  [ https://a.yandex-team.ru/arc/commit/9192088 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-01 15:56:17+03:00

0.6.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3148: commonize redis settings  [ https://a.yandex-team.ru/arc/commit/9162639 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-18 20:09:55+03:00

0.5.0
-----

[robot-statinfra](http://staff/robot-statinfra) 2022-02-15 20:56:20+03:00

0.4.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3148: bi_file_uploader app settings         [ https://a.yandex-team.ru/arc/commit/9145830 ]
 * BI-3148: bi_file_uploader one more api schema  [ https://a.yandex-team.ru/arc/commit/9143476 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-15 16:07:45+03:00

0.3.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3148: bi_file_uploader app settings     [ https://a.yandex-team.ru/arc/commit/9134695 ]
 * fixed .release.hjson for bi-file-uploader  [ https://a.yandex-team.ru/arc/commit/9128636 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-11 23:26:26+03:00

0.2.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3148: bi_file_uploader app.py fix  [ https://a.yandex-team.ru/arc/commit/9128111 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-10 10:40:01+03:00

0.1.0
-----

* [asnytin](http://staff/asnytin)

 * BI-3148: bi_file_uploader api schemas       [ https://a.yandex-team.ru/arc/commit/9121523 ]
 * BI-3148: bi_file_uploader - init changelog  [ https://a.yandex-team.ru/arc/commit/9120783 ]
 * BI-3148: bi_file_uploader app               [ https://a.yandex-team.ru/arc/commit/9101127 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-08 18:54:49+03:00

