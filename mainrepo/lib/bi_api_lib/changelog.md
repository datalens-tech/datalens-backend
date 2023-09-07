0.1993.0
--------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * BI-4852: fix CONNECTOR_WHITELIST (#397)     [ https://github.com/datalens-tech/datalens-backend-private/commit/773eea3c ]
 * BI-4779: remove chydb tests (#398)          [ https://github.com/datalens-tech/datalens-backend-private/commit/9eb7b69e ]
 * remove (oracle|mssql)_libs (#389)           [ https://github.com/datalens-tech/datalens-backend-private/commit/eb601242 ]
 * BI-4779: remove the CHYDB connector (#297)  [ https://github.com/datalens-tech/datalens-backend-private/commit/3a0da5cc ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * Fix double tool.mypy sections in .tomls (#396)         [ https://github.com/datalens-tech/datalens-backend-private/commit/ea4562c3 ]
 * Fix typo in pyproject tomls, mypy section name (#393)  [ https://github.com/datalens-tech/datalens-backend-private/commit/8a739fc7 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-07 15:07:06+00:00

0.1992.0
--------

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * fix-detect-affected-packages-to-handle-tests-deps (#390)  [ https://github.com/datalens-tech/datalens-backend-private/commit/3ec751ff ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-07 13:31:21+00:00

0.1991.0
--------

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version mainrepo/lib/bi_file_uploader_worker_lib 0.144.0 (#388)  [ https://github.com/datalens-tech/datalens-backend-private/commit/4fce00f1 ]
 * releasing version mainrepo/lib/bi_file_uploader_worker_lib 0.143.0 (#373)  [ https://github.com/datalens-tech/datalens-backend-private/commit/c3821980 ]

* [Sergei Borodin](http://staff/seray@yandex-team.ru)

 * BI-4428 use custom ca_cert (#384)        [ https://github.com/datalens-tech/datalens-backend-private/commit/630e5498 ]
 * BI-4428 reinstall certifi-yandex (#372)  [ https://github.com/datalens-tech/datalens-backend-private/commit/8aeb1501 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Minor updates to the local_dev README (#377)  [ https://github.com/datalens-tech/datalens-backend-private/commit/8739bbbb ]

* [Ovsyannikov Dmitrii](http://staff/tamatuk@ya.ru)

 * BI-4907: remove yatest (#335)  [ https://github.com/datalens-tech/datalens-backend-private/commit/c1f0f17a ]

* [Max Zuev](http://staff/mail@thenno.me)

 * BI-4359: delete black fabric magic for connectors (#374)  [ https://github.com/datalens-tech/datalens-backend-private/commit/0fcce078 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-07 13:24:57+00:00

0.1990.0
--------

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * BI-4896 Connectorize notifications; some mypy fixes (#358)                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/bfacbbea ]
 * Add user-id cookie customization to CSRF middlewares to avoid hardcoded yandexuid (#347)     [ https://github.com/datalens-tech/datalens-backend-private/commit/78543050 ]
 * Preload entrypoint connectors before loading settings to force constants declaration (#360)  [ https://github.com/datalens-tech/datalens-backend-private/commit/5e7ea9be ]
 * BI-4905 Split bi_api_commons into public and private parts (#311)                            [ https://github.com/datalens-tech/datalens-backend-private/commit/d5fc8a87 ]

* [Konstantin Chupin](http://staff/91148200+ya-kc@users.noreply.github.com)

 * [BI-4902] Resync OS metapkg after dependencies cleanup (#369)                      [ https://github.com/datalens-tech/datalens-backend-private/commit/eb7ffeb2 ]
 * [BI-4902] Actualize dependencies in main-repo (#365)                               [ https://github.com/datalens-tech/datalens-backend-private/commit/671b951c ]
 * [BI-4902] Move `bi-connector-solomon` from bi-api-lib to bi-api (path fix) (#350)  [ https://github.com/datalens-tech/datalens-backend-private/commit/0c6b5efe ]
 * [BI-4902] Move `bi-connector-solomon` from bi-api-lib to bi-api (#349)             [ https://github.com/datalens-tech/datalens-backend-private/commit/8602265a ]
 * [BI-4902] Tool to compare resolved depdendencies for apps (#345)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/196cedfa ]

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version mainrepo/lib/bi_file_uploader_worker_lib 0.142.0 (#367)  [ https://github.com/datalens-tech/datalens-backend-private/commit/1d265ea4 ]
 * releasing version app/bi_file_secure_reader 0.14.0 (#363)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/246f33b6 ]
 * releasing version mainrepo/lib/bi_file_uploader_worker_lib 0.141.0 (#361)  [ https://github.com/datalens-tech/datalens-backend-private/commit/f29eb9a4 ]

* [Sergei Borodin](http://staff/seray@yandex-team.ru)

 * BI-4428 fix file-worker deps (#366)                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/db51dd62 ]
 * BI-4428 adding an ability to connect to security-reader via TCP (#346)  [ https://github.com/datalens-tech/datalens-backend-private/commit/1be37cd6 ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * some more mypy stubs and fixes for mypy sync tool (#362)  [ https://github.com/datalens-tech/datalens-backend-private/commit/c98cd2ac ]
 * move ACCESS_SERVICE_PERMISSIONS_CHECK_DELAY (#357)        [ https://github.com/datalens-tech/datalens-backend-private/commit/def5dfa2 ]
 * mypy fixes vol. 6 (#351)                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/d4aa169e ]
 * move tvm.py from bi_core (#343)                           [ https://github.com/datalens-tech/datalens-backend-private/commit/63170213 ]
 * BI-4852: postgresql mdb-specific bi api connector (#277)  [ https://github.com/datalens-tech/datalens-backend-private/commit/2a6649a4 ]
 * BI-4860: CONNECTORS_DATA -> CONNECTORS (#340)             [ https://github.com/datalens-tech/datalens-backend-private/commit/d6a0ca42 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Taskfile cleanup (#359)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/7ca2daec ]
 * Added bake for doc generation (#353)                         [ https://github.com/datalens-tech/datalens-backend-private/commit/4c5cb1e4 ]
 * Some renamings in dl-repmanager (#355)                       [ https://github.com/datalens-tech/datalens-backend-private/commit/13da9dd3 ]
 * Correct generation of localizations for all packages (#352)  [ https://github.com/datalens-tech/datalens-backend-private/commit/54969d19 ]
 * Removed env-dependent scopes from formula_ref (#338)         [ https://github.com/datalens-tech/datalens-backend-private/commit/726e0b59 ]
 * Created empty package for formula testing tools (#246)       [ https://github.com/datalens-tech/datalens-backend-private/commit/e72b3057 ]

* [Max Zuev](http://staff/mail@thenno.me)

 * BI-4359: move bi_api_lib tests to legacy bundle (#322)  [ https://github.com/datalens-tech/datalens-backend-private/commit/205b6109 ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * Removing workaround  for different docker-compose in arc ci and gh ci. (#341)  [ https://github.com/datalens-tech/datalens-backend-private/commit/490493ed ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-06 16:44:32+00:00

0.1989.0
--------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * cleanup enums (#331)                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/c199b8df ]
 * unskip test_dataset_revision_id (#323)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/70646a0c ]
 * move metrica form rows to the connector package (#321)  [ https://github.com/datalens-tech/datalens-backend-private/commit/d0cbea49 ]

* [vallbull](http://staff/33630435+vallbull@users.noreply.github.com)

 * BI-4803: Fix full date filtering (#312)    [ https://github.com/datalens-tech/datalens-backend-private/commit/7b9a852c ]
 * BI-4758: Get rid of read_only=True (#303)  [ https://github.com/datalens-tech/datalens-backend-private/commit/363e2cd6 ]

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * Fix import in chyt internal connector tests (#334)         [ https://github.com/datalens-tech/datalens-backend-private/commit/96a27451 ]
 * BI-4898 Separate base bi-api app factories (#288)          [ https://github.com/datalens-tech/datalens-backend-private/commit/ca3234ca ]
 * BI-4901 Separate file-uploader-* base app settings (#299)  [ https://github.com/datalens-tech/datalens-backend-private/commit/43eeff3a ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Normalized dependencies for connectors and some other libraries (#290)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e9fdf9c4 ]
 * Moved diff_utils to bi_maintenance (#330)                               [ https://github.com/datalens-tech/datalens-backend-private/commit/b0b35d6f ]
 * Moved maintenance tools and crawlers to bi_maintenance (#293)           [ https://github.com/datalens-tech/datalens-backend-private/commit/1072a661 ]
 * Partailly normalized bi_core dependencies (#291)                        [ https://github.com/datalens-tech/datalens-backend-private/commit/62db91ba ]
 * Added a simple test to dl_repmanager (#308)                             [ https://github.com/datalens-tech/datalens-backend-private/commit/971790c2 ]
 * Removed ArcadiaFileLoader from bi_testing (#294)                        [ https://github.com/datalens-tech/datalens-backend-private/commit/f8373c2a ]

* [Max Zuev](http://staff/mail@thenno.me)

 * Enable tests in legacy bundle (#326)  [ https://github.com/datalens-tech/datalens-backend-private/commit/76e7240d ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * removed bi_core/_aux (#284)  [ https://github.com/datalens-tech/datalens-backend-private/commit/21a15c8f ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * GH_CI_mypy_job_fix (#316)                                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/e468d967 ]
 * pathlib-in-dl-repmanager instead of plain str (#317)                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/607f4eb8 ]
 * fix_mypy_bi_ci (#320)                                                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/1eda379b ]
 * GH_CI up_gh_ci_actions_checkout (#309)                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/3ecafe1d ]
 * GH_CI_router_and_split_on_light_runners (#305)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/59da4c1b ]
 * Modified GH CI test targets discovery logic to also search in mainrepo/terrarium (#307)  [ https://github.com/datalens-tech/datalens-backend-private/commit/adc314a1 ]

* [Konstantin Chupin](http://staff/91148200+ya-kc@users.noreply.github.com)

 * [BI-4902] Move OS data API to main repo (#319)                               [ https://github.com/datalens-tech/datalens-backend-private/commit/3c1d36fe ]
 * [BI-4902] Move OS control API to main repo (#318)                            [ https://github.com/datalens-tech/datalens-backend-private/commit/cb29e57e ]
 * [BI-4902] Cleanup metapkg scope-shrinker & fix transition to pathlib (#310)  [ https://github.com/datalens-tech/datalens-backend-private/commit/19f09dda ]
 * [BI-4902] Move antlr code-gen to main repo (#301)                            [ https://github.com/datalens-tech/datalens-backend-private/commit/b638276a ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-05 10:37:17+00:00

0.1988.0
--------

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * single mypy (#296)  [ https://github.com/datalens-tech/datalens-backend-private/commit/57787881 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-04 12:51:11+00:00

0.1987.0
--------

* [vallbull](http://staff/33630435+vallbull@users.noreply.github.com)

 * BI-4791: Delete port check (#295)  [ https://github.com/datalens-tech/datalens-backend-private/commit/3c616d15 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-04 12:30:03+00:00

0.1986.0
--------

* [vallbull](http://staff/33630435+vallbull@users.noreply.github.com)

 * BI-4776: Fix typo in code (#300)  [ https://github.com/datalens-tech/datalens-backend-private/commit/f7892a5d ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-04 12:15:10+00:00

0.1985.0
--------

* [vallbull](http://staff/33630435+vallbull@users.noreply.github.com)

 * BI-4776: Chyt forms (#256)  [ https://github.com/datalens-tech/datalens-backend-private/commit/01ecf70 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Implemented virtual FS editor and dry run for repmanager (#287)  [ https://github.com/datalens-tech/datalens-backend-private/commit/78f6aa8 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-04 11:46:45+00:00

0.1984.0
--------

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * Add missing dependencies to app/bi_api (#283)  [ https://github.com/datalens-tech/datalens-backend-private/commit/74c7546 ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * added script to distribute common mypy settings across repo (#286)  [ https://github.com/datalens-tech/datalens-backend-private/commit/4b34547 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Switching to fully managed FS access in repmanager (#278)  [ https://github.com/datalens-tech/datalens-backend-private/commit/f326b39 ]
 * Fixes for connectorization of bi_formula_ref (#267)        [ https://github.com/datalens-tech/datalens-backend-private/commit/3f5c290 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-04 08:50:59+00:00

0.1983.0
--------

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * Update old connector availability configs (#280)  [ https://github.com/datalens-tech/datalens-backend-private/commit/2ceaead ]

* [dmi-feo](http://staff/fdi1992@gmail.com)

 * remove unused stuff from statcommons + run tests on file removals (#266)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e9a2985 ]
 * BI-4904: mount internal cert in int-rqe containers (#281)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/70ea817 ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * BI-4834: move the yql bi api connectors to a separate package (#247)     [ https://github.com/datalens-tech/datalens-backend-private/commit/8323adc ]
 * BI-4834: move the solomon bi api connector to a separate package (#243)  [ https://github.com/datalens-tech/datalens-backend-private/commit/12f30a0 ]
 * BI-4834: move the file bi api connectors to a separate package (#242)    [ https://github.com/datalens-tech/datalens-backend-private/commit/8097203 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * delete bi_sqlalchemy_mysql dependency, drop legacy maintenance script, fix deps (#279)  [ https://github.com/datalens-tech/datalens-backend-private/commit/372eb59 ]
 * BI-4385: cleanup (#272)                                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/8f4f9c0 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Normalized bi_api_lib's requirements (but without test deps) (#274)                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/e36bc96 ]
 * Normalized bi_api_commons' requirements (#270)                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/6805eb3 ]
 * Normalized bi_api_connector requirements by moving ConnectionFormTestBase to bi_api_lib_testing (#271)  [ https://github.com/datalens-tech/datalens-backend-private/commit/b3a71a1 ]
 * Added path validation to fs editor (#259)                                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/b11a258 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-01 21:24:34+00:00

0.1982.0
--------

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * replaced plain str with pathlib.Path in dl_repmanager (#261)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e8cc05b ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * BI-4860: allow empty connector settings (#275)  [ https://github.com/datalens-tech/datalens-backend-private/commit/7ec4d37 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Normalized bi_api_client's requirements (#269)                            [ https://github.com/datalens-tech/datalens-backend-private/commit/db39ebc ]
 * Switched to prproject.toml-compatible format in requirement check (#273)  [ https://github.com/datalens-tech/datalens-backend-private/commit/fb1ae2d ]

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * BI-4899 Tags instead of APP_TYPE to control function availability (#264)  [ https://github.com/datalens-tech/datalens-backend-private/commit/9c96e0a ]
 * BI-4901 Separate base app settings for file-uploader-api (#240)           [ https://github.com/datalens-tech/datalens-backend-private/commit/026ab89 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-01 14:06:51+00:00

0.1981.0
--------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * more mypy fixes for bi_api_lib (#255)           [ https://github.com/datalens-tech/datalens-backend-private/commit/ff09d70 ]
 * BI-4852: implement connectors whitelist (#251)  [ https://github.com/datalens-tech/datalens-backend-private/commit/3cb7ff5 ]
 * add grpc stubs to the mypy image (#252)         [ https://github.com/datalens-tech/datalens-backend-private/commit/ac0fffa ]

* [Max Zuev](http://staff/mail@thenno.me)

 * BI-4359: fast fix app configs (it should fix the build) (#262)  [ https://github.com/datalens-tech/datalens-backend-private/commit/78711a8 ]

* [Konstantin Chupin](http://staff/91148200+ya-kc@users.noreply.github.com)

 * [BI-4902] Initial implementation of OS metapkg sync (#265)                        [ https://github.com/datalens-tech/datalens-backend-private/commit/ae74f54 ]
 * [BI-4902] Cleanup BQ crunches in meta pkg (#260)                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/8d74841 ]
 * [BI-4902] Sort dependencies in meta-package by groups according to policy (#258)  [ https://github.com/datalens-tech/datalens-backend-private/commit/22b92ab ]
 * [BI-4830] Remove manual installation of BQ (#221)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/caed181 ]

* [Sergei Borodin](http://staff/seray@yandex-team.ru)

 * BI-4884 rootless yc-dls (#257)  [ https://github.com/datalens-tech/datalens-backend-private/commit/4b419ad ]

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * Remove some private stuff from base connection forms (#263)         [ https://github.com/datalens-tech/datalens-backend-private/commit/b1b3037 ]
 * Remove hardcoded relative env file path in OsEnvParamGetter (#239)  [ https://github.com/datalens-tech/datalens-backend-private/commit/cb820a5 ]
 * BI-4229 Move connector availability into configs (#156)             [ https://github.com/datalens-tech/datalens-backend-private/commit/88a7aec ]
 * BI-4898 Separate base app settings for bi-api (#238)                [ https://github.com/datalens-tech/datalens-backend-private/commit/312acf2 ]
 * Fix .po generation for non-connnector packages (#254)               [ https://github.com/datalens-tech/datalens-backend-private/commit/98dee9a ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * custom runner for mypy, repsects [datalens.meta.mypy] > targets section in the sub project's pyproject.toml (#253)  [ https://github.com/datalens-tech/datalens-backend-private/commit/f51fd03 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Implemented renaming of packages (#244)         [ https://github.com/datalens-tech/datalens-backend-private/commit/532d804 ]
 * Removed custom test dependency sections (#248)  [ https://github.com/datalens-tech/datalens-backend-private/commit/d1aee37 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-01 12:03:33+00:00

0.1980.0
--------

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Switched back to git in repmanager, but without the cp command (#237)                        [ https://github.com/datalens-tech/datalens-backend-private/commit/1cf900f ]
 * Cleaned up some legacy stuff in makefiles (#228)                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/6280b7f ]
 * Removed dist-info from some packages (#236)                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/eba1d5c ]
 * Switched dl-repmanager to default fs editor (#229)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/d833b26 ]
 * Moved testenv-common to mainrepo (#227)                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/a49028b ]
 * BI-4894: Moved libraries to mainrepo (#223)                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/b36037b ]
 * Moved bi_connector_postgresql to mainrepo (#215)                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/7acd0ae ]
 * Fix package moving (#214)                                                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/e7160c9 ]
 * Fixed unregistration of packages (#209)                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/86d147b ]
 * Added ch-package-type to repmanager (#198)                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/8c6ae18 ]
 * Fully connectorized bi_formula_ref and reversed its dependencies (#183)                      [ https://github.com/datalens-tech/datalens-backend-private/commit/eb37775 ]
 * Removed empty modules from bi_testing (#172)                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/73b57b5 ]
 * Moved TestClientConverterAiohttpToFlask from bi_testing_ya to bi_api_lib_testing (#175)      [ https://github.com/datalens-tech/datalens-backend-private/commit/15d0d59 ]
 * BI-4150: Implemented optimization of IF and CASE functions with constant conditions (#148)   [ https://github.com/datalens-tech/datalens-backend-private/commit/bb1bdbf ]
 * BI-4703: Partial implementation of data source migration for connection replacement (#19)    [ https://github.com/datalens-tech/datalens-backend-private/commit/d14823c ]
 * Moved dialect support settings to formula-ref configs (#143)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/0fb32cc ]
 * Removed and refactored some legacy core data source tests (#53)                              [ https://github.com/datalens-tech/datalens-backend-private/commit/7a7df16 ]
 * Connectorized DialectName (#147)                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/3433273 ]
 * Added recursive config discovery to dl-repomanager (#171)                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/6ebc5ed ]
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

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * fix Path wrap (#245)                                                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/90f21e7 ]
 * Added script to sync mypy annotation requirements to 3rd party requirements. (#234)                    [ https://github.com/datalens-tech/datalens-backend-private/commit/add228a ]
 * gh-ci-cancel-in-progress-on-pr-update (#204)                                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/c440263 ]
 * build did image (#176)                                                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/6f04fc3 ]
 * Moving from shell hell to bake new world (#120)                                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/194c8cd ]
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

* [dmi-feo](http://staff/fdi1992@gmail.com)

 * BI-4835: increase cloudlogging retention_period (#241)      [ https://github.com/datalens-tech/datalens-backend-private/commit/5942304 ]
 * BI-4835: common message key (#225)                          [ https://github.com/datalens-tech/datalens-backend-private/commit/08838fb ]
 * BI-4835: formatted cloudlogging logs (#224)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/1d9734d ]
 * sentry: add sec group + up version (#218)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/90c49fe ]
 * update left stands (#217)                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/07b9d50 ]
 * upload_to_ycr: use yc-prod build folder repo (#211)         [ https://github.com/datalens-tech/datalens-backend-private/commit/c54ff85 ]
 * fluentbit: trigger helm upgrade on config change (#212)     [ https://github.com/datalens-tech/datalens-backend-private/commit/4458f29 ]
 * return USE_IAM_SUBJECT_RESOLVER (#216)                      [ https://github.com/datalens-tech/datalens-backend-private/commit/4cfe192 ]
 * BI-4750: monitoring connection for istrael and nemax (#65)  [ https://github.com/datalens-tech/datalens-backend-private/commit/a1bf019 ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * move the core solomon connector to a separate package (#232)                              [ https://github.com/datalens-tech/datalens-backend-private/commit/3579a91 ]
 * add more stubs packages (#235)                                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/5ed9bef ]
 * BI-4860: switch to the new connectors settings loading schema (#201)                      [ https://github.com/datalens-tech/datalens-backend-private/commit/e1ef68a ]
 * BI-4834: move the postgresql and greenplum bi api connectors to separate packages (#219)  [ https://github.com/datalens-tech/datalens-backend-private/commit/624d2bd ]
 * BI-4834: move the clickhouse bi api connector to a separate library (#178)                [ https://github.com/datalens-tech/datalens-backend-private/commit/e24a7c8 ]
 * Move the clickhouse core connector to a separate package (#177)                           [ https://github.com/datalens-tech/datalens-backend-private/commit/d3949db ]
 * move sa_creds.py to bi_cloud_integration (#188)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/c5ddc5b ]
 * Move the greenplum core connector to a separate package (#181)                            [ https://github.com/datalens-tech/datalens-backend-private/commit/feb99c5 ]
 * BI-4852: clean up comments in the postgresql adapter (#185)                               [ https://github.com/datalens-tech/datalens-backend-private/commit/4ff0e29 ]
 * BI-4860: load connectors settings using a dynamically generated class (#150)              [ https://github.com/datalens-tech/datalens-backend-private/commit/d0a942d ]
 * some mypy fixes in connectors (#161)                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/434d93c ]
 * even more mypy fixes (#154)                                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/07e33e4 ]
 * BI-4664: remove dls client from a common SR (#130)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/1b2990a ]
 * remove explain query (#128)                                                               [ https://github.com/datalens-tech/datalens-backend-private/commit/d2c373d ]
 * BI-4860: connector settings fallback registry (#119)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/48f0f80 ]
 * mark failing oracle test (#110)                                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/cc953d2 ]
 * move metrica connector aliases to the connector package (#111)                            [ https://github.com/datalens-tech/datalens-backend-private/commit/d7bf72e ]
 * BI-4860: connector settings registry (#80)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/ae48ac5 ]
 * some more mypy fixes (#93)                                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/96fa442 ]
 * BI-4860: generate ConnectorsSettingsByType by settings registry (#88)                     [ https://github.com/datalens-tech/datalens-backend-private/commit/c531841 ]
 * fix typing (#91)                                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/0d95eb3 ]
 * remove old bitrix conncetor settings (#76)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/197ff7d ]
 * BI-4834: move the gsheets bi api connector to a separate package (#71)                    [ https://github.com/datalens-tech/datalens-backend-private/commit/7c8eb90 ]
 * BI-4834: move chyt bi api connectors to separate packages (#50)                           [ https://github.com/datalens-tech/datalens-backend-private/commit/da59831 ]
 * fix mypy in the monitoring connector (#59)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/d1270b4 ]
 * replace most do_xxx classvars with regulated marks in core tests (#36)                    [ https://github.com/datalens-tech/datalens-backend-private/commit/e9a0487 ]
 * remove unused file (#51)                                                                  [ https://github.com/datalens-tech/datalens-backend-private/commit/9592af6 ]
 * remove method_not_implemented (#48)                                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/312cd5c ]
 * BI-4834: move the bitrix bi api connector to a separate package (#46)                     [ https://github.com/datalens-tech/datalens-backend-private/commit/088d08a ]
 * enable more tests in ci (#41)                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/2598b2b ]
 * remove remaining setup pys (#42)                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/8b23500 ]
 * small fixes for mypy (#37)                                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/9981e20 ]
 * BI-4834: move metrica bi api connectors to a separate package (#27)                       [ https://github.com/datalens-tech/datalens-backend-private/commit/ba21b47 ]
 * BI-4817: move some dashsql tests from bi api to connectors packages (#31)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/8b2af58 ]
 * BI-4834: move the monitoring bi api connector to a separate package (#23)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/a20b872 ]

* [Konstantin Chupin](http://staff/91148200+ya-kc@users.noreply.github.com)

 * [BI-4894] Yet another fix of testenv-common/images (#233)                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/b9ce8dd ]
 * [BI-4894] Sync local dev 3rd party deps (#230)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/0d6eab5 ]
 * [BI-4894] Fix version bump actions (#226)                                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/12e48d7 ]
 * [BI-4830] Fix BQ dependencies (#213)                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/205fb59 ]
 * [BI-4830] Fix BQ listing in all_packages.lst (#210)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/fafe5f3 ]
 * [BI-4830] Moved bi_connector_bigquery to mainrepo (#200)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/99e4793 ]
 * [BI-4830] Remove tier-0 build rudiments (entry points) (#199)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/5fc2705 ]
 * [BI-4830] Remove tier-0 build rudiments (#196)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/cc681a8 ]
 * [BI-4830] Cleanup bi-api image before moving libs to mainrepo (#194)                          [ https://github.com/datalens-tech/datalens-backend-private/commit/059aae5 ]
 * [BI-4632] Externalize tenant resolution in public US workaround middleware & FU tasks (#158)  [ https://github.com/datalens-tech/datalens-backend-private/commit/b799e05 ]
 * [BI-4830] Adopt mainrepo/libs to all Docker builds (#105)                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/44459ff ]
 * [BI-4830] Parametrize base rootless image at bake level (#98)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/6dfdb3c ]
 * [BI-4830] Version bump PR auto-merge (#84)                                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/363f45f ]
 * [BI-4830] Version bump for integration tests fix (#57)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/c4188ee ]
 * [BI-4830] Version bump for integration tests (#56)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/56fcece ]
 * [BI-4830] Fix release PR branch name (#52)                                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/24eeb5d ]
 * [BI-4830] Add dedicated dir for CI artifacts to .gitignore (#43)                              [ https://github.com/datalens-tech/datalens-backend-private/commit/a787a30 ]
 * [BI-4830] Fix make pycharm (#11)                                                              [ https://github.com/datalens-tech/datalens-backend-private/commit/bc7a3ab ]
 * [BI-4830] Remove GH sync tools (#9)                                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/88c518a ]

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * BI-4800 Remove some usages of YENV_TYPE (#114)                                            [ https://github.com/datalens-tech/datalens-backend-private/commit/86ba000 ]
 * Always use env in MDBDomainManager instead of a hardcoded config (#192)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/40faee4 ]
 * Remove preprod ig configs (#170)                                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/534386e ]
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

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * Replace EnumField to native marshmallow Enum field (#195)  [ https://github.com/datalens-tech/datalens-backend-private/commit/6736c94 ]
 * remove ya-team specific apps from github (#197)            [ https://github.com/datalens-tech/datalens-backend-private/commit/e752fa6 ]
 * mypy fixes (#163)                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/d2c6403 ]
 * Removed DLS from github repo (#149)                        [ https://github.com/datalens-tech/datalens-backend-private/commit/91f8be9 ]
 * BI-4385: exception classes cleanup (#144)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/9fc86f9 ]
 * BI-4385: utils cleanup (#137)                              [ https://github.com/datalens-tech/datalens-backend-private/commit/b5034d0 ]
 * BI-4385: drop unnecessary conn_opts mutation (#138)        [ https://github.com/datalens-tech/datalens-backend-private/commit/266d421 ]
 * BI-3546: rewrite mdb domains in YaTeam (#104)              [ https://github.com/datalens-tech/datalens-backend-private/commit/9f3ee91 ]
 * BI-4855: added MDB protos to yc_apis_proto_stubs (#72)     [ https://github.com/datalens-tech/datalens-backend-private/commit/ae18987 ]

* [Sergei Borodin](http://staff/serayborodin@gmail.com)

 * BI-4478 rootless file* apps (#193)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/1a4a959 ]
 * BI-4876 rename entrypoint file-worker (#207)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/31f12ca ]
 * BI-4876 fix factory (#189)                                                   [ https://github.com/datalens-tech/datalens-backend-private/commit/745dbd2 ]
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

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version lib/bi_file_uploader_worker_lib 0.140.0 (#208)  [ https://github.com/datalens-tech/datalens-backend-private/commit/03f15eb ]
 * releasing version lib/bi_file_uploader_worker_lib 0.139.0 (#190)  [ https://github.com/datalens-tech/datalens-backend-private/commit/117cb4a ]
 * releasing version lib/bi_file_uploader_worker_lib 0.138.0 (#186)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e362e09 ]
 * releasing version lib/bi_file_uploader_api_lib 0.115.0 (#184)     [ https://github.com/datalens-tech/datalens-backend-private/commit/c5ee769 ]
 * releasing version lib/bi_file_uploader_worker_lib 0.137.0 (#174)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e01d0a1 ]
 * releasing version lib/bi_file_uploader_api_lib 0.114.0 (#173)     [ https://github.com/datalens-tech/datalens-backend-private/commit/5fd20e9 ]
 * releasing version app/bi_file_secure_reader 0.13.0 (#167)         [ https://github.com/datalens-tech/datalens-backend-private/commit/a744607 ]
 * releasing version lib/bi_file_uploader_worker_lib 0.136.0 (#166)  [ https://github.com/datalens-tech/datalens-backend-private/commit/6a012d7 ]
 * releasing version lib/bi_file_uploader_api_lib 0.113.0 (#165)     [ https://github.com/datalens-tech/datalens-backend-private/commit/db4174a ]
 * releasing version lib/bi_api_lib 0.1979.0 (#132)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/74689b2 ]
 * releasing version lib/bi_api_lib 0.1978.0 (#124)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/f557177 ]
 * releasing version ops/bi_integration_tests 0.20.0 (#118)          [ https://github.com/datalens-tech/datalens-backend-private/commit/b896c79 ]
 * releasing version lib/bi_api_lib 0.1977.0 (#116)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/4a28df1 ]
 * releasing version lib/bi_api_lib 0.1976.0 (#115)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/7dd0c10 ]
 * releasing version ops/bi_integration_tests 0.19.0 (#113)          [ https://github.com/datalens-tech/datalens-backend-private/commit/e6f21fe ]
 * releasing version lib/bi_api_lib 0.1975.0 (#109)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/0e10952 ]
 * releasing version lib/bi_api_lib 0.1974.0 (#107)                  [ https://github.com/datalens-tech/datalens-backend-private/commit/180b8a8 ]
 * releasing version ops/bi_integration_tests 0.18.0 (#101)          [ https://github.com/datalens-tech/datalens-backend-private/commit/9d66cbd ]
 * releasing version ops/bi_integration_tests 0.17.0 (#97)           [ https://github.com/datalens-tech/datalens-backend-private/commit/55a217b ]
 * releasing version lib/bi_api_lib 0.1973.0 (#94)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/bb14b67 ]
 * releasing version lib/bi_api_lib 0.1972.0 (#89)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/643bfaf ]
 * releasing version ops/bi_integration_tests 0.16.0 (#85)           [ https://github.com/datalens-tech/datalens-backend-private/commit/038232f ]
 * releasing version lib/bi_api_lib 0.1971.0 (#81)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/23f04eb ]
 * releasing version ops/bi_integration_tests 0.15.0 (#78)           [ https://github.com/datalens-tech/datalens-backend-private/commit/5a1933e ]
 * releasing version ops/bi_integration_tests 0.14.0 (#73)           [ https://github.com/datalens-tech/datalens-backend-private/commit/986cbc8 ]
 * releasing version ops/bi_integration_tests 0.13.0 (#64)           [ https://github.com/datalens-tech/datalens-backend-private/commit/9fdff6a ]
 * releasing version ops/bi_integration_tests 0.12.0 (#58)           [ https://github.com/datalens-tech/datalens-backend-private/commit/cf0b7ac ]
 * releasing version lib/bi_api_lib 0.1970.0 (#40)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/3c2ed13 ]

* [vallbull](http://staff/33630435+vallbull@users.noreply.github.com)

 * BI-4803: Fix data filtering in selectors (#179)        [ https://github.com/datalens-tech/datalens-backend-private/commit/d4f4a5f ]
 * BI-4624: Use anyascii from pypi (#206)                 [ https://github.com/datalens-tech/datalens-backend-private/commit/3c5403c ]
 * BI-4626: Add transfer, source and new log-group (#70)  [ https://github.com/datalens-tech/datalens-backend-private/commit/aac8c56 ]

* [Max Zuev](http://staff/mail@thenno.me)

 * Remove bi_test_project_task_interface (#205)               [ https://github.com/datalens-tech/datalens-backend-private/commit/78cb3eb ]
 * BI-4359: don't use defaults from libs (#182)               [ https://github.com/datalens-tech/datalens-backend-private/commit/da152cc ]
 * BI-4359: return back nebius defaults to bi-configs (#164)  [ https://github.com/datalens-tech/datalens-backend-private/commit/4714d55 ]
 * BI-4359: delete old tooling and defaults (#155)            [ https://github.com/datalens-tech/datalens-backend-private/commit/6af6ee1 ]
 * BI-4359: move old defaults to new package (#99)            [ https://github.com/datalens-tech/datalens-backend-private/commit/f0b1831 ]
 * Try to fix ci tests (delete terrarium from root level)     [ https://github.com/datalens-tech/datalens-backend-private/commit/7053fc5 ]

* [Dmitry Nadein](http://staff/pr45dima@mail.ru)

 * BI-4749: Update sentry dsn env vars (#38)  [ https://github.com/datalens-tech/datalens-backend-private/commit/f55e014 ]

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * SYNC On branch trunk aabaf57ac056cd9d5ab8541280330f76bedb90d6  [ https://github.com/datalens-tech/datalens-backend-private/commit/f47b646 ]
 * SYNC On branch trunk f9b55cf0bbe5880d1f49b17ad9615c7e010f2b55  [ https://github.com/datalens-tech/datalens-backend-private/commit/6e5572b ]
 * SYNC On branch trunk b62b8da33d58eb7fb22395578156db63e1549606  [ https://github.com/datalens-tech/datalens-backend-private/commit/5274b30 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-31 15:33:37+00:00

0.1979.0
--------

* [Sergei Borodin](http://staff/serayborodin@gmail.com)

 * BI-4873 BI-4874 yc-public-data-api yc-embedded-data-api docker tier1 (#131)  [ https://github.com/datalens-tech/datalens-backend-private/commit/71caa7d ]
 * BI-4872 yc-data-api docker tier1 (#126)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/adb226e ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Switched to short locales in formula_ref and fixed locales in clickhouse connector (#90)     [ https://github.com/datalens-tech/datalens-backend-private/commit/f71f6b7 ]
 * Added ConvertBlocksToFunctionsMutation for converting IF and CASE blocks to functions (#24)  [ https://github.com/datalens-tech/datalens-backend-private/commit/66adc9d ]
 * Added mypy task (#125)                                                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/9f56a55 ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * remove explain query (#128)                           [ https://github.com/datalens-tech/datalens-backend-private/commit/d2c373d ]
 * BI-4860: connector settings fallback registry (#119)  [ https://github.com/datalens-tech/datalens-backend-private/commit/48f0f80 ]

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * Add dev-machine secrets sync to the taskfile (#127)  [ https://github.com/datalens-tech/datalens-backend-private/commit/ece8e9e ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-25 13:41:47+00:00

0.1978.0
--------

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * Sync secrets from yav to dev machine on secrets-update for local ext tests runs (#123)  [ https://github.com/datalens-tech/datalens-backend-private/commit/5236bc0 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Added docker-remote and docker-reinstall commands to taskfile (#106)  [ https://github.com/datalens-tech/datalens-backend-private/commit/83bd071 ]

* [Sergei Borodin](http://staff/serayborodin@gmail.com)

 * Up integration-tests version (#121)  [ https://github.com/datalens-tech/datalens-backend-private/commit/1cabd36 ]

* [Max Zuev](http://staff/mail@thenno.me)

 * BI-4359: move old defaults to new package (#99)  [ https://github.com/datalens-tech/datalens-backend-private/commit/f0b1831 ]

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version ops/bi_integration_tests 0.20.0 (#118)  [ https://github.com/datalens-tech/datalens-backend-private/commit/b896c79 ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * mark failing oracle test (#110)  [ https://github.com/datalens-tech/datalens-backend-private/commit/cc953d2 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-25 10:14:19+00:00

0.1977.0
--------

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-24 14:03:42+00:00

0.1976.0
--------

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * fix_main_flow_git_fetch_trunk (#112)  [ https://github.com/datalens-tech/datalens-backend-private/commit/0c4a370 ]

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version ops/bi_integration_tests 0.19.0 (#113)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e6f21fe ]

* [Sergei Borodin](http://staff/serayborodin@gmail.com)

 * integration-tests tier1 only (#108)  [ https://github.com/datalens-tech/datalens-backend-private/commit/6a42522 ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * move metrica connector aliases to the connector package (#111)  [ https://github.com/datalens-tech/datalens-backend-private/commit/d7bf72e ]
 * BI-4860: connector settings registry (#80)                      [ https://github.com/datalens-tech/datalens-backend-private/commit/ae48ac5 ]

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * Enable bitrix ext tests in CI (#77)  [ https://github.com/datalens-tech/datalens-backend-private/commit/d7b7cf1 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-24 13:53:01+00:00

0.1975.0
--------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * some more mypy fixes (#93)  [ https://github.com/datalens-tech/datalens-backend-private/commit/96fa442 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-24 10:16:14+00:00

0.1974.0
--------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * BI-3546: rewrite mdb domains in YaTeam (#104)  [ https://github.com/datalens-tech/datalens-backend-private/commit/9f3ee91 ]

* [Konstantin Chupin](http://staff/91148200+ya-kc@users.noreply.github.com)

 * [BI-4830] Adopt mainrepo/libs to all Docker builds (#105)      [ https://github.com/datalens-tech/datalens-backend-private/commit/44459ff ]
 * [BI-4830] Parametrize base rootless image at bake level (#98)  [ https://github.com/datalens-tech/datalens-backend-private/commit/6dfdb3c ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Moved the boilerplate package to mainrepo (#102)  [ https://github.com/datalens-tech/datalens-backend-private/commit/4051eba ]
 * Introduced taskfiles (#86)                        [ https://github.com/datalens-tech/datalens-backend-private/commit/d04e821 ]
 * Moved dl_repmanager to mainrepo (#92)             [ https://github.com/datalens-tech/datalens-backend-private/commit/687c59f ]

* [Max Zuev](http://staff/mail@thenno.me)

 * Try to fix ci tests (delete terrarium from root level)  [ https://github.com/datalens-tech/datalens-backend-private/commit/7053fc5 ]

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version ops/bi_integration_tests 0.18.0 (#101)  [ https://github.com/datalens-tech/datalens-backend-private/commit/9d66cbd ]
 * releasing version ops/bi_integration_tests 0.17.0 (#97)   [ https://github.com/datalens-tech/datalens-backend-private/commit/55a217b ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * BI-4860: generate ConnectorsSettingsByType by settings registry (#88)  [ https://github.com/datalens-tech/datalens-backend-private/commit/c531841 ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * moved terrarium/bi_ci into mainrepo/ (#82)  [ https://github.com/datalens-tech/datalens-backend-private/commit/1dc7e25 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-24 07:59:39+00:00

0.1973.0
--------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * fix typing (#91)  [ https://github.com/datalens-tech/datalens-backend-private/commit/0d95eb3 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-23 16:16:36+00:00

0.1972.0
--------

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * BI-4863: Added clickhouse formula_ref plugin (#74)  [ https://github.com/datalens-tech/datalens-backend-private/commit/2ade140 ]
 * Fixed bake targets for i18n (#75)                   [ https://github.com/datalens-tech/datalens-backend-private/commit/d15fa6f ]
 * Added --no-location option to .po generator (#83)   [ https://github.com/datalens-tech/datalens-backend-private/commit/54020f4 ]

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version ops/bi_integration_tests 0.16.0 (#85)  [ https://github.com/datalens-tech/datalens-backend-private/commit/038232f ]

* [Konstantin Chupin](http://staff/91148200+ya-kc@users.noreply.github.com)

 * [BI-4830] Version bump PR auto-merge (#84)  [ https://github.com/datalens-tech/datalens-backend-private/commit/363f45f ]

* [Dmitry Nadein](http://staff/pr45dima@mail.ru)

 * BI-4749: Update sentry dsn env vars (#38)  [ https://github.com/datalens-tech/datalens-backend-private/commit/f55e014 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-23 15:21:55+00:00

0.1971.0
--------

* [dmi-feo](http://staff/fdi1992@gmail.com)

 * BI-4750: monitoring connection for istrael and nemax (#65)  [ https://github.com/datalens-tech/datalens-backend-private/commit/a1bf019 ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * enable_ext_tag_in_workflow (#79)                         [ https://github.com/datalens-tech/datalens-backend-private/commit/8b5e3b5 ]
 * gh ci option to run only mypy tests (#55)                [ https://github.com/datalens-tech/datalens-backend-private/commit/b0d84f3 ]
 * GH CI fixes: Skip publish if not executed pytests (#33)  [ https://github.com/datalens-tech/datalens-backend-private/commit/94232ef ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * BI-4855: added MDB protos to yc_apis_proto_stubs (#72)  [ https://github.com/datalens-tech/datalens-backend-private/commit/ae18987 ]

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * remove old bitrix conncetor settings (#76)                              [ https://github.com/datalens-tech/datalens-backend-private/commit/197ff7d ]
 * BI-4834: move the gsheets bi api connector to a separate package (#71)  [ https://github.com/datalens-tech/datalens-backend-private/commit/7c8eb90 ]
 * BI-4834: move chyt bi api connectors to separate packages (#50)         [ https://github.com/datalens-tech/datalens-backend-private/commit/da59831 ]
 * fix mypy in the monitoring connector (#59)                              [ https://github.com/datalens-tech/datalens-backend-private/commit/d1270b4 ]
 * replace most do_xxx classvars with regulated marks in core tests (#36)  [ https://github.com/datalens-tech/datalens-backend-private/commit/e9a0487 ]
 * remove unused file (#51)                                                [ https://github.com/datalens-tech/datalens-backend-private/commit/9592af6 ]
 * remove method_not_implemented (#48)                                     [ https://github.com/datalens-tech/datalens-backend-private/commit/312cd5c ]
 * BI-4834: move the bitrix bi api connector to a separate package (#46)   [ https://github.com/datalens-tech/datalens-backend-private/commit/088d08a ]
 * enable more tests in ci (#41)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/2598b2b ]
 * remove remaining setup pys (#42)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/8b23500 ]

* [github-actions[bot]](http://staff/41898282+github-actions[bot]@users.noreply.github.com)

 * releasing version ops/bi_integration_tests 0.15.0 (#78)  [ https://github.com/datalens-tech/datalens-backend-private/commit/5a1933e ]
 * releasing version ops/bi_integration_tests 0.14.0 (#73)  [ https://github.com/datalens-tech/datalens-backend-private/commit/986cbc8 ]
 * releasing version ops/bi_integration_tests 0.13.0 (#64)  [ https://github.com/datalens-tech/datalens-backend-private/commit/9fdff6a ]
 * releasing version ops/bi_integration_tests 0.12.0 (#58)  [ https://github.com/datalens-tech/datalens-backend-private/commit/cf0b7ac ]

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * BI-4692 Comment out unconfigured ext tests instead of a whitelist in test splitter (#69)  [ https://github.com/datalens-tech/datalens-backend-private/commit/00af47f ]
 * Remove YC file-uploader[-worker] meta packages (#68)                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/790186a ]
 * Remove OS meta packages (#63)                                                             [ https://github.com/datalens-tech/datalens-backend-private/commit/7766dbd ]
 * Remove YC bi-api meta packages (#60)                                                      [ https://github.com/datalens-tech/datalens-backend-private/commit/d59f021 ]
 * BI-4692 GitHub Actions secrets for ext tests (#44)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/4271639 ]
 * Fix bitrix_gds bi-api-connector dependency (#66)                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/a1f35b9 ]
 * Update Kontur.Market texts; fix translations generation (#47)                             [ https://github.com/datalens-tech/datalens-backend-private/commit/338074a ]

* [vallbull](http://staff/33630435+vallbull@users.noreply.github.com)

 * BI-4626: Add transfer, source and new log-group (#70)  [ https://github.com/datalens-tech/datalens-backend-private/commit/aac8c56 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Refactored update-po and msfmt common makefile targets (#67)                             [ https://github.com/datalens-tech/datalens-backend-private/commit/45db6d0 ]
 * Added make remote-shell command (#49)                                                    [ https://github.com/datalens-tech/datalens-backend-private/commit/321c8af ]
 * Added registrations to formula_ref plugins and switched to new localization logic (#34)  [ https://github.com/datalens-tech/datalens-backend-private/commit/4cfe507 ]

* [Sergei Borodin](http://staff/serayborodin@gmail.com)

 * phusion base image for integration-tests (#62)  [ https://github.com/datalens-tech/datalens-backend-private/commit/5a458a6 ]
 * integration-tests with bake (#54)               [ https://github.com/datalens-tech/datalens-backend-private/commit/fb99f60 ]
 * BI-4809 control-api yc-preprod (#45)            [ https://github.com/datalens-tech/datalens-backend-private/commit/ccc6044 ]

* [Konstantin Chupin](http://staff/91148200+ya-kc@users.noreply.github.com)

 * [BI-4830] Version bump for integration tests fix (#57)            [ https://github.com/datalens-tech/datalens-backend-private/commit/c4188ee ]
 * [BI-4830] Version bump for integration tests (#56)                [ https://github.com/datalens-tech/datalens-backend-private/commit/56fcece ]
 * [BI-4830] Fix release PR branch name (#52)                        [ https://github.com/datalens-tech/datalens-backend-private/commit/24eeb5d ]
 * [BI-4830] Add dedicated dir for CI artifacts to .gitignore (#43)  [ https://github.com/datalens-tech/datalens-backend-private/commit/a787a30 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-23 13:47:38+00:00

0.1970.0
--------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * small fixes for mypy (#37)                                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/9981e20 ]
 * BI-4834: move metrica bi api connectors to a separate package (#27)        [ https://github.com/datalens-tech/datalens-backend-private/commit/ba21b47 ]
 * BI-4817: move some dashsql tests from bi api to connectors packages (#31)  [ https://github.com/datalens-tech/datalens-backend-private/commit/8b2af58 ]
 * BI-4834: move the monitoring bi api connector to a separate package (#23)  [ https://github.com/datalens-tech/datalens-backend-private/commit/a20b872 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Removed empty bi_core/i18n/localizer_base.py (#35)                    [ https://github.com/datalens-tech/datalens-backend-private/commit/6b38614 ]
 * Added the mainrepo dir (#32)                                          [ https://github.com/datalens-tech/datalens-backend-private/commit/08cc8da ]
 * Some mypy fixes for bi_i18n,  bi_core and connectors (#28)            [ https://github.com/datalens-tech/datalens-backend-private/commit/7927363 ]
 * git-related fixes for repmanager (#30)                                [ https://github.com/datalens-tech/datalens-backend-private/commit/7898fcf ]
 * Fixed i18n import in monitoring connector (#29)                       [ https://github.com/datalens-tech/datalens-backend-private/commit/6804cdc ]
 * Removed some unused attributes and methods from USEntry (#14)         [ https://github.com/datalens-tech/datalens-backend-private/commit/82ca37e ]
 * Added the option of using whitelists in connector registration (#18)  [ https://github.com/datalens-tech/datalens-backend-private/commit/cbbcf25 ]
 * Removed __init__ from the root dir (#26)                              [ https://github.com/datalens-tech/datalens-backend-private/commit/ad1f2f7 ]
 * Added formula_ref plugins (#20)                                       [ https://github.com/datalens-tech/datalens-backend-private/commit/c1fa53d ]
 * Moved some modules from bi_testing to bi_testing_ya (#22)             [ https://github.com/datalens-tech/datalens-backend-private/commit/808304f ]
 * Implemented regulated test mechanism (#21)                            [ https://github.com/datalens-tech/datalens-backend-private/commit/a3e1816 ]
 * Partially moved some more constants to connectors (#12)               [ https://github.com/datalens-tech/datalens-backend-private/commit/8457968 ]
 * Added bi_i18n package (#10)                                           [ https://github.com/datalens-tech/datalens-backend-private/commit/21fcbf8 ]
 * Added .mo files to bi_formula_ref package (#15)                       [ https://github.com/datalens-tech/datalens-backend-private/commit/af79cd6 ]
 * Added __pycache__ to .gitignore (#13)                                 [ https://github.com/datalens-tech/datalens-backend-private/commit/864e9cb ]

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * Remove the remaining gen-parser prerequisites from Makefiles  [ https://github.com/datalens-tech/datalens-backend-private/commit/380fbd8 ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * Detect which files was changed between pr branch and base (#25)                                        [ https://github.com/datalens-tech/datalens-backend-private/commit/412a2e0 ]
 * placeholder workflow for simpler creation of new flow and avoid confusion in running flows list (#17)  [ https://github.com/datalens-tech/datalens-backend-private/commit/90d8b6a ]

* [Konstantin Chupin](http://staff/91148200+ya-kc@users.noreply.github.com)

 * [BI-4830] Fix make pycharm (#11)     [ https://github.com/datalens-tech/datalens-backend-private/commit/bc7a3ab ]
 * [BI-4830] Remove GH sync tools (#9)  [ https://github.com/datalens-tech/datalens-backend-private/commit/88c518a ]

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * SYNC On branch trunk aabaf57ac056cd9d5ab8541280330f76bedb90d6  [ https://github.com/datalens-tech/datalens-backend-private/commit/f47b646 ]
 * SYNC On branch trunk f9b55cf0bbe5880d1f49b17ad9615c7e010f2b55  [ https://github.com/datalens-tech/datalens-backend-private/commit/6e5572b ]
 * SYNC On branch trunk b62b8da33d58eb7fb22395578156db63e1549606  [ https://github.com/datalens-tech/datalens-backend-private/commit/5274b30 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-21 18:08:25+00:00

0.1969.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-18 07:00:39+00:00

0.1968.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 16:10:36+00:00

0.1967.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 15:43:51+00:00

0.1966.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 14:37:58+00:00

0.1965.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 13:30:35+00:00

0.1964.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 11:49:33+00:00

0.1963.0
--------

* [thenno](http://staff/thenno)

 * BI-4359: fix extracting redis settings  [ https://a.yandex-team.ru/arc/commit/12196749 ]

* [mcpn](http://staff/mcpn)

 * BI-4851: register mysql bi api connector using entrypoints  [ https://a.yandex-team.ru/arc/commit/12194989 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 11:40:32+00:00

0.1962.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 00:14:39+00:00

0.1961.0
--------

* [asnytin](http://staff/asnytin)

 * BI-4740: removed gen-parser dependency from make files  [ https://a.yandex-team.ru/arc/commit/12189204 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-16 23:22:08+00:00

0.1960.0
--------

* [ovsds](http://staff/ovsds)

 * PR from branch users/ovsds/f/BI-4798-fix-tls-connection-forms

BI-4798: move file field to special ma field

BI-4798: small fixes  [ https://a.yandex-team.ru/arc/commit/12187798 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added conn_executor_factory arg to connection test method  [ https://a.yandex-team.ru/arc/commit/12183545 ]

* [mcpn](http://staff/mcpn)

 * BI-4851: register the greenplum bi api connector using entrypoints     [ https://a.yandex-team.ru/arc/commit/12180527 ]
 * BI-4834: move the ch_geo_filtered api connector to a separate package  [ https://a.yandex-team.ru/arc/commit/12180521 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-16 14:28:34+00:00

0.1959.0
--------

* [mcpn](http://staff/mcpn)

 * BI-4834: move the promQL api connector to a separate package  [ https://a.yandex-team.ru/arc/commit/12177452 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-15 16:47:47+00:00

0.1958.0
--------

* [mcpn](http://staff/mcpn)

 * BI-4851: register the YDB bi api connector using entrypoints              [ https://a.yandex-team.ru/arc/commit/12176485 ]
 * BI-4817: move monitoring tests from bi api to the connector package       [ https://a.yandex-team.ru/arc/commit/12173142 ]
 * BI-4851: register the ch_geo_filtered bi api connector using entrypoints  [ https://a.yandex-team.ru/arc/commit/12171772 ]
 * BI-4851: register the oracle bi api connector using entrypoints           [ https://a.yandex-team.ru/arc/commit/12168776 ]
 * BI-4851: register the metrica bi api connectors using entrypoints         [ https://a.yandex-team.ru/arc/commit/12168772 ]

* [konstasa](http://staff/konstasa)

 * BI-4730 YC apps: sec embeds  [ https://a.yandex-team.ru/arc/commit/12173604 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed execute and execute_iter methods from connections  [ https://a.yandex-team.ru/arc/commit/12170330 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-15 15:33:28+00:00

0.1957.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-14 15:51:12+00:00

0.1956.0
--------

* [thenno](http://staff/thenno)

 * BI-4359: load configs from yaml files  [ https://a.yandex-team.ru/arc/commit/12166705 ]

* [mcpn](http://staff/mcpn)

 * BI-4834: move frozen bi api connectors to a separate package  [ https://a.yandex-team.ru/arc/commit/12166060 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-14 14:38:29+00:00

0.1955.0
--------

* [mcpn](http://staff/mcpn)

 * BI-4834: move partners bi api connectors to a separate package  [ https://a.yandex-team.ru/arc/commit/12163709 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-14 11:43:44+00:00

0.1954.0
--------

* [mcpn](http://staff/mcpn)

 * BI-4817: dashsql test class  [ https://a.yandex-team.ru/arc/commit/12152560 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-11 14:13:23+00:00

0.1953.0
--------

* [mcpn](http://staff/mcpn)

 * BI-4834: move service bi api connectors to separate packages     [ https://a.yandex-team.ru/arc/commit/12150598 ]
 * BI-4817: move promql tests from bi api to the connector package  [ https://a.yandex-team.ru/arc/commit/12145218 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-11 10:35:15+00:00

0.1952.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved some more const declarations out of bi_constants to connecrtors      [ https://a.yandex-team.ru/arc/commit/12144284 ]
 * BI-4593: Moved ya-specific pytest_plugin from bi_testing to bi_testing_ya  [ https://a.yandex-team.ru/arc/commit/12134965 ]

* [mcpn](http://staff/mcpn)

 * BI-4668: remove passport config settings                                      [ https://a.yandex-team.ru/arc/commit/12144095 ]
 * moved bitrix gds core connector to a separate library                         [ https://a.yandex-team.ru/arc/commit/12135689 ]
 * moved gsheets core connector to a separate library                            [ https://a.yandex-team.ru/arc/commit/12135589 ]
 * BI-4817: move some filter and result tests from bi api to connector packages  [ https://a.yandex-team.ru/arc/commit/12131643 ]

* [bulanovavv](http://staff/bulanovavv)

 * BI-4721: Create data_export_forbidden button for connection creation  [ https://a.yandex-team.ru/arc/commit/12143747 ]

* [dmifedorov](http://staff/dmifedorov)

 * chyt in nemax  [ https://a.yandex-team.ru/arc/commit/12142715 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-10 15:18:43+00:00

0.1951.0
--------

* [ovsds](http://staff/ovsds)

 * BI-4798: enable ssl on connection forms

BI-4798: enable ssl on connection forms

Revert "BI-4798: disable ssl on connection forms"
This reverts commit 995fc00be7484a2bf90c40b2bc52d08f0cba4a93.

BI-4798: disable ssl on connection forms  [ https://a.yandex-team.ru/arc/commit/12128113 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-09 10:52:37+00:00

0.1950.0
--------

* [mcpn](http://staff/mcpn)

 * BI-4815: split bi_service_registries by installations                     [ https://a.yandex-team.ru/arc/commit/12119370 ]
 * BI-4817: move test_ch_billing_analytics_connection to connector package   [ https://a.yandex-team.ru/arc/commit/12110114 ]
 * BI-4817: move test_mysql_percent_char to connector package                [ https://a.yandex-team.ru/arc/commit/12110103 ]
 * remove test_geo_features.py                                               [ https://a.yandex-team.ru/arc/commit/12110094 ]
 * BI-4817: move frozen connectors' tests to the connector package           [ https://a.yandex-team.ru/arc/commit/12097135 ]
 * BI-4817: remove bi_api_lib tests with alternatives in connector packages  [ https://a.yandex-team.ru/arc/commit/12097127 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved ch_frozen_base to the bi_connector_bundle_ch_frozen package              [ https://a.yandex-team.ru/arc/commit/12104234 ]
 * Removed integration_tests_dc_project_id fixture from bi_testing.pytest_plugin  [ https://a.yandex-team.ru/arc/commit/12104217 ]
 * Renamed several usages of ConnectionType.postgres to CONNECTION_TYPE_POSTGRES  [ https://a.yandex-team.ru/arc/commit/12099738 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-08 16:50:33+00:00

0.1949.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * hidden chyt connector for israel  [ https://a.yandex-team.ru/arc/commit/12094698 ]

* [mcpn](http://staff/mcpn)

 * BI-4817: move ch geo filtered tests to connector package  [ https://a.yandex-team.ru/arc/commit/12094639 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-04 08:42:43+00:00

0.1948.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 20:18:09+00:00

0.1947.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 18:57:15+00:00

0.1946.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 18:31:37+00:00

0.1945.0
--------

* [ovsds](http://staff/ovsds)

 * BI-4798: disable ssl on connection forms  [ https://a.yandex-team.ru/arc/commit/12089651 ]

* [bulanovavv](http://staff/bulanovavv)

 * BI-4721: Add flag data_export_forbidden to connection  [ https://a.yandex-team.ru/arc/commit/12088012 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 15:32:18+00:00

0.1944.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved some more constant declarations  [ https://a.yandex-team.ru/arc/commit/12086808 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 12:43:07+00:00

0.1943.0
--------

* [ovsds](http://staff/ovsds)

 * BI-4798: add tls to connection form clickhouse  [ https://a.yandex-team.ru/arc/commit/12085093 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3737: Removed unused execution methods from connection  [ https://a.yandex-team.ru/arc/commit/12082943 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 10:13:13+00:00

0.1942.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 19:40:28+00:00

0.1941.0
--------

* [asnytin](http://staff/asnytin)

 * BI-4451: translations fix  [ https://a.yandex-team.ru/arc/commit/12081169 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 18:30:28+00:00

0.1940.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 15:25:41+00:00

0.1939.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 14:29:15+00:00

0.1938.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 09:26:47+00:00

0.1937.0
--------

* [ovsds](http://staff/ovsds)

 * BI-4451: add tls to connection form postgres                                                                                                  [ https://a.yandex-team.ru/arc/commit/12072574 ]
 * PR from branch users/ovsds/f/BI-4588-add-tls-clickhouse-connector

BI-4588: add tls to CH connector

BI-4588: refactor adapter ssl to common  [ https://a.yandex-team.ru/arc/commit/12072559 ]

* [vgol](http://staff/vgol)

 * using new oracle pdb outside of arcadia tests  [ https://a.yandex-team.ru/arc/commit/12068949 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 08:51:24+00:00

0.1936.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Silenced a mypy error in app_settings      [ https://a.yandex-team.ru/arc/commit/12062859 ]
 * Removed the enabled flag from connections  [ https://a.yandex-team.ru/arc/commit/12062832 ]

* [mcpn](http://staff/mcpn)

 * remove unused connection settings in bi api tests  [ https://a.yandex-team.ru/arc/commit/12062439 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-01 11:04:57+00:00

0.1935.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-4775: nemax app configs  [ https://a.yandex-team.ru/arc/commit/12061393 ]

* [vgol](http://staff/vgol)

 * BI-4726-fix-oracle-connector-tests  [ https://a.yandex-team.ru/arc/commit/12057606 ]

* [konstasa](http://staff/konstasa)

 * BI-4782 Changes in configs to make OS apps runnable  [ https://a.yandex-team.ru/arc/commit/12056797 ]

* [asnytin](http://staff/asnytin)

 * BI-4385: removed BACK_SUPERUSER_OAUTH settings  [ https://a.yandex-team.ru/arc/commit/12056765 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-01 09:00:30+00:00

0.1934.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Removed direct usage of get_secret from bi_external_api_tests  [ https://a.yandex-team.ru/arc/commit/12052985 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-31 14:34:45+00:00

0.1933.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4782 OS meta & apps: control-api, data-api  [ https://a.yandex-team.ru/arc/commit/12046241 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-29 16:35:59+00:00

0.1932.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-28 21:57:04+00:00

0.1931.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-28 18:31:47+00:00

0.1930.0
--------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4787 Add bi-core-rqe-async script to bi_api_lib pyproject.toml  [ https://a.yandex-team.ru/arc/commit/12044216 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-28 16:38:56+00:00

0.1929.0
--------

* [asnytin](http://staff/asnytin)

 * BI-4780: disable CHYDB connections creation  [ https://a.yandex-team.ru/arc/commit/12040367 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-28 13:31:31+00:00

0.1928.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-28 08:34:18+00:00

0.1927.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 18:16:47+00:00

0.1926.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 16:59:39+00:00

0.1925.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 16:46:29+00:00

0.1924.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 14:56:59+00:00

0.1923.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 14:33:20+00:00

0.1922.0
--------

* [mcpn](http://staff/mcpn)

 * move metrica core tests to metrica connector package  [ https://a.yandex-team.ru/arc/commit/12029885 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved constant declarations for ch_filtered_ya_cloud connectors to the connector bundle package  [ https://a.yandex-team.ru/arc/commit/12029570 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 13:33:32+00:00

0.1921.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-27 09:41:03+00:00

0.1920.0
--------

* [kchupin](http://staff/kchupin)

 * Partial fix of DC dataset-api tests  [ https://a.yandex-team.ru/arc/commit/12024265 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed direct usage of secrets from datacloud tests in bi_api_lib  [ https://a.yandex-team.ru/arc/commit/12023442 ]

* [nadein](http://staff/nadein)

 * BI-4759: Fix dataset preview with failed logins

Added method to clean-up rls_entry subject from failed prefix
Use this method on rls check for allow_rls_change disallowed  [ https://a.yandex-team.ru/arc/commit/12016939 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-26 16:50:35+00:00

0.1919.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-25 19:09:10+00:00

0.1918.0
--------

* [mcpn](http://staff/mcpn)

 * BI-4716: moved chyt core connector to a separate library  [ https://a.yandex-team.ru/arc/commit/12010052 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed pytest dummy modules from bi_core_testing  [ https://a.yandex-team.ru/arc/commit/12009831 ]

* [yoptar](http://staff/yoptar)

 * BI-4731: add an optimization class for the ISNULL funciton  [ https://a.yandex-team.ru/arc/commit/12006582 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-25 12:29:33+00:00

0.1917.0
--------

* [konstasa](http://staff/konstasa)

 * DLPROJECTS-186 Make CHYT visible in YC prod                 [ https://a.yandex-team.ru/arc/commit/12000993 ]
 * BI-4681 Use error handler in public mode for secure embeds  [ https://a.yandex-team.ru/arc/commit/11992017 ]
 * BI-4727 Make RQE URL scheme configurable via env            [ https://a.yandex-team.ru/arc/commit/11988597 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved all snowflake tests to connector package  [ https://a.yandex-team.ru/arc/commit/11995046 ]
 * Made new LODs default                           [ https://a.yandex-team.ru/arc/commit/11994402 ]

* [vgol](http://staff/vgol)

 * BI-4726: More fixes + modified CI workflow to split tests between multiple jobs  [ https://a.yandex-team.ru/arc/commit/11984828 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-24 08:44:43+00:00

0.1916.0
--------

* [yoptar](http://staff/yoptar)

 * BI-4728: Automatically register translation configs from entry points  [ https://a.yandex-team.ru/arc/commit/11978304 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-20 12:17:06+00:00

0.1915.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4730 Introduce bi_meta_yc_* packages  [ https://a.yandex-team.ru/arc/commit/11978108 ]
 * BI-4681 Secure embeds for YC             [ https://a.yandex-team.ru/arc/commit/11978079 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved testing folders from bi_core to bi_core_testing  [ https://a.yandex-team.ru/arc/commit/11977106 ]

* [shadchin](http://staff/shadchin)

 * Fix datalens/backend/lib/bi_api_lib/tests/unit with pandas 2  [ https://a.yandex-team.ru/arc/commit/11977024 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-20 09:16:21+00:00

0.1914.0
--------

* [konstasa](http://staff/konstasa)

 * Remove GoZora usages in gsheets_v2 connector; disable gsheets_v2 in yandex-team  [ https://a.yandex-team.ru/arc/commit/11972803 ]

* [mcpn](http://staff/mcpn)

 * BI-4716: moved chyt internal core connector to a separate library   [ https://a.yandex-team.ru/arc/commit/11969256 ]
 * moved usage_tracking_ya_team core connector to a separated library  [ https://a.yandex-team.ru/arc/commit/11968411 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved all bigquery tests to connector package                                               [ https://a.yandex-team.ru/arc/commit/11968313 ]
 * Moved POSTGRESQL and CLICKHOUSE dialect definitions to connectors                           [ https://a.yandex-team.ru/arc/commit/11967756 ]
 * Switched to backend type from connection type in connectorized query processing components  [ https://a.yandex-team.ru/arc/commit/11967754 ]

* [bulanovavv](http://staff/bulanovavv)

 * Uint substraction  [ https://a.yandex-team.ru/arc/commit/11959201 ]

* [yoptar](http://staff/yoptar)

 * BI-4640: fix an error when using `CONTAINS` function with string arrays in PostgreSQL                                                 [ https://a.yandex-team.ru/arc/commit/11955779 ]
 * BI-4653: forbid concatenation of strings in metrica dialect

documentation PR: https://github.com/yandex-cloud/docs-source/pull/2834  [ https://a.yandex-team.ru/arc/commit/11955757 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-19 15:59:58+00:00

0.1913.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * hidden monitoring conn for israel  [ https://a.yandex-team.ru/arc/commit/11953083 ]

* [vgol](http://staff/vgol)

 * PR from branch users/vgol/BI-4549_rename_tests_dirs_bi_api_lib

BI-4549: bi_api_lib 2/2

BI-4549: bi_api_lib 1/2  [ https://a.yandex-team.ru/arc/commit/11947917 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-17 16:58:22+00:00

0.1912.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-14 16:24:34+00:00

0.1911.0
--------

* [bulanovavv](http://staff/bulanovavv)

 * BI-4725: Fix uf_token  [ https://a.yandex-team.ru/arc/commit/11933539 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3737: Added conn_executor_factory arg to several connection methods  [ https://a.yandex-team.ru/arc/commit/11932552 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-14 10:28:05+00:00

0.1910.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved SourceBackendType, ConnectionType and CreateDSFrom definitions to connector packages for mssql, mysql and oracle  [ https://a.yandex-team.ru/arc/commit/11926116 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-13 16:31:19+00:00

0.1909.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-13 13:51:07+00:00

0.1908.0
--------

* [seray](http://staff/seray)

 * Copy base set  [ https://a.yandex-team.ru/arc/commit/11925719 ]

* [mcpn](http://staff/mcpn)

 * BI-4715: moved partners connectors core base to a separate library  [ https://a.yandex-team.ru/arc/commit/11923775 ]
 * BI-4715: moved kontur market core connector to a separate library   [ https://a.yandex-team.ru/arc/commit/11922286 ]
 * BI-4715: moved equeo core connector to a separate library           [ https://a.yandex-team.ru/arc/commit/11920710 ]
 * BI-4715: moved moysklad core connector to a separate library        [ https://a.yandex-team.ru/arc/commit/11919330 ]

* [bulanovavv](http://staff/bulanovavv)

 * BI-4711: Add user, crm_company_uf, crm_contact_uf tables  [ https://a.yandex-team.ru/arc/commit/11922180 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-13 13:39:06+00:00

0.1907.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-12 20:01:38+00:00

0.1906.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-12 17:20:36+00:00

0.1905.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-12 16:38:44+00:00

0.1904.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-12 16:03:13+00:00

0.1903.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-4687] First attempt to build tier1 bi-api  [ https://a.yandex-team.ru/arc/commit/11916452 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-12 15:12:38+00:00

0.1902.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-12 14:41:14+00:00

0.1901.0
--------

* [mcpn](http://staff/mcpn)

 * move blackbox_client from common service registry  [ https://a.yandex-team.ru/arc/commit/11912052 ]

* [vgol](http://staff/vgol)

 * BI-4592: updated .release.hjson to use pyproject.toml instead of setup.py      [ https://a.yandex-team.ru/arc/commit/11909029 ]
 * BI-4592: re using old local dev, but supplying deps from pyproject toml files  [ https://a.yandex-team.ru/arc/commit/11908104 ]

* [konstasa](http://staff/konstasa)

 * Make get_sr_factory into an interface for app factories to implement  [ https://a.yandex-team.ru/arc/commit/11907814 ]

* [bulanovavv](http://staff/bulanovavv)

 * BI-4517: Add smart-process-tables to bitrix connector  [ https://a.yandex-team.ru/arc/commit/11904370 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved manual registration of data source schemas out of bi_api_connector   [ https://a.yandex-team.ru/arc/commit/11902056 ]
 * Added query processing classes to bi_api_lib connector                     [ https://a.yandex-team.ru/arc/commit/11902048 ]
 * BI-4601: Some more refactoring of base cases classes for bi_api_lib tests  [ https://a.yandex-team.ru/arc/commit/11897526 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-12 13:47:35+00:00

0.1900.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved postgresql core connector to the connector package  [ https://a.yandex-team.ru/arc/commit/11893584 ]

* [konstasa](http://staff/konstasa)

 * CLOUD-139221 Remove BACK_SUPERUSER_IAM_KEY_DATA  [ https://a.yandex-team.ru/arc/commit/11893579 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-10 11:03:23+00:00

0.1899.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-07 16:11:39+00:00

0.1898.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-07 15:55:41+00:00

0.1897.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-07 15:41:41+00:00

0.1896.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-07 15:33:57+00:00

0.1895.0
--------

* [robot-yc-arcadia](http://staff/robot-yc-arcadia)

 * releasing version 0.1895.0

releasing version 0.1895.0  [ https://a.yandex-team.ru/arc/commit/11884057 ]

* [seray](http://staff/seray)

 * BI-4478 adding connectors dependency  [ https://a.yandex-team.ru/arc/commit/11883643 ]

* [mcpn](http://staff/mcpn)

 * BI-4691: moved SMB heatmaps core connector to a separate library               [ https://a.yandex-team.ru/arc/commit/11883527 ]
 * BI-4691: moved ch_geo_filtered core connector to a separate library            [ https://a.yandex-team.ru/arc/commit/11881997 ]
 * BI-4691: moved schoolbook core connector to a separate library                 [ https://a.yandex-team.ru/arc/commit/11880224 ]
 * BI-4691: moved market couriers core connector to a separate library            [ https://a.yandex-team.ru/arc/commit/11870170 ]
 * BI-4691: moved ch_ya_music_podcast_stats core connector to a separate library  [ https://a.yandex-team.ru/arc/commit/11866977 ]
 * BI-4540: move RequestBootstrap and related middlewares to bi_api_commons       [ https://a.yandex-team.ru/arc/commit/11860977 ]

* [ovsds](http://staff/ovsds)

 * BI-4453: add data api connection ssl support for postgresql  [ https://a.yandex-team.ru/arc/commit/11880349 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved resources and resources_async to the respective app folders                                           [ https://a.yandex-team.ru/arc/commit/11872615 ]
 * Partially moved ch_frozen_ connection types to connectors                                                   [ https://a.yandex-team.ru/arc/commit/11872585 ]
 * Added secret reader abstraction to snowflake tests                                                          [ https://a.yandex-team.ru/arc/commit/11869545 ]
 * BI-4431: Moved compeng out of bi_core                                                                       [ https://a.yandex-team.ru/arc/commit/11865019 ]
 * Moved SnowflakeRefreshTokenSoonToExpire to snowflake connector                                              [ https://a.yandex-team.ru/arc/commit/11857311 ]
 * Moved more folders from bi_api_lib.query to bi_query_processing                                             [ https://a.yandex-team.ru/arc/commit/11850196 ]
 * BI-4431: Data processors as plugins registered via entrypoints                                              [ https://a.yandex-team.ru/arc/commit/11850183 ]
 * BI-4601: Refactored SnowFlakeConnectionTestBase, separated BiApiTestBase, added bi_api_lib_testing package  [ https://a.yandex-team.ru/arc/commit/11849625 ]
 * Moved ConnectionType declarations to connectors from bi_core for bigquery and snowflake                     [ https://a.yandex-team.ru/arc/commit/11847240 ]

* [konstasa](http://staff/konstasa)

 * DLPROJECTS-186 Update CHYT translations                                                       [ https://a.yandex-team.ru/arc/commit/11867007 ]
 * BI-4693 Remove materializer client, materialization enums, dataset-api materializer handlers  [ https://a.yandex-team.ru/arc/commit/11867004 ]
 * BI-4393 Remove access_mode, AccessModeManager, dataset_mode, MaterializationManager           [ https://a.yandex-team.ru/arc/commit/11849226 ]

* [vgol](http://staff/vgol)

 * BI-4680: poetry based local dev

still need to update pycharm related stuff, but already usable  [ https://a.yandex-team.ru/arc/commit/11857188 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-07 15:24:39+00:00

0.1894.0
--------

* [mcpn](http://staff/mcpn)

 * BI-4540: move some more aio middlewares to bi_api_commons     [ https://a.yandex-team.ru/arc/commit/11840996 ]
 * BI-4540: move some more flask middlewares to bi_api_commons   [ https://a.yandex-team.ru/arc/commit/11830637 ]
 * BI-4667: remove outdated connectors                           [ https://a.yandex-team.ru/arc/commit/11823679 ]
 * BI-4540: move yc auth middlewares to bi_api_commons_ya_cloud  [ https://a.yandex-team.ru/arc/commit/11820848 ]
 * Move DLBaseException and bi_core.types to bi_constants        [ https://a.yandex-team.ru/arc/commit/11820826 ]

* [vgol](http://staff/vgol)

 * BI-4674: reworked build process to avoid poetry install, added newer mypy deps, more stubs, updating pkg name ref with a script + bunch of minor fixes  [ https://a.yandex-team.ru/arc/commit/11832613 ]

* [dmifedorov](http://staff/dmifedorov)

 * hosts'n'ports from docker-compose for bi_api_lib  [ https://a.yandex-team.ru/arc/commit/11832481 ]

* [gstatsenko](http://staff/gstatsenko)

 * Replaced replaced direct init calls with classmethod factories for FuncCall and WindowFuncCall nodes  [ https://a.yandex-team.ru/arc/commit/11832184 ]
 * BI-4601: Removed US preparation from common_pytest_configure                                          [ https://a.yandex-team.ru/arc/commit/11830608 ]
 * Moved registration of connector-specific query processing components to a single registry file        [ https://a.yandex-team.ru/arc/commit/11830554 ]
 * BI-4431: Moved compeng implementation files to separate folders                                       [ https://a.yandex-team.ru/arc/commit/11824730 ]
 * BI-4601: Moved env-dependent code to set_up_environment in DataApiAppFactory                          [ https://a.yandex-team.ru/arc/commit/11823158 ]
 * Fixed field creation                                                                                  [ https://a.yandex-team.ru/arc/commit/11811761 ]
 * Removed us container name from all tests and configs                                                  [ https://a.yandex-team.ru/arc/commit/11809819 ]
 * Switched to using .make factories for several more node types                                         [ https://a.yandex-team.ru/arc/commit/11806722 ]
 * Transformed async create_app into DataApiAppFactory                                                   [ https://a.yandex-team.ru/arc/commit/11806461 ]

* [thenno](http://staff/thenno)

 * BI-4685: remove old locales from bi_api_lib  [ https://a.yandex-team.ru/arc/commit/11828185 ]
 * BI-4685: move locales inside python package  [ https://a.yandex-team.ru/arc/commit/11821584 ]

* [konstasa](http://staff/konstasa)

 * BI-4356 Introduce bi_file_uploader_worker_lib  [ https://a.yandex-team.ru/arc/commit/11814784 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-03 11:50:53+00:00

0.1893.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-4652: Fixed measure filters with 0-dimensional measure in select  [ https://a.yandex-team.ru/arc/commit/11805525 ]

* [mcpn](http://staff/mcpn)

 * BI-4540: move blackbox middlewares to bi_api_commons_ya_team  [ https://a.yandex-team.ru/arc/commit/11797588 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-27 14:31:14+00:00

0.1892.0
--------

* [mcpn](http://staff/mcpn)

 * BI-4407: fix pivot measure sorting with totals  [ https://a.yandex-team.ru/arc/commit/11793012 ]
 * BI-4350: remove clickhouse 19.13 support        [ https://a.yandex-team.ru/arc/commit/11792186 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-4643: Fixed handling of BFBs in window functions  [ https://a.yandex-team.ru/arc/commit/11791053 ]

* [dmifedorov](http://staff/dmifedorov)

 * update psycopg2 & switch to binary for local-dev  [ https://a.yandex-team.ru/arc/commit/11784794 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-26 13:33:29+00:00

0.1891.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4630 BI-4249 Connection sections; CHYT group; remove ENABLED_BACKEND_DRIVEN_FORMS app setting  [ https://a.yandex-team.ru/arc/commit/11783814 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-23 16:40:00+00:00

0.1890.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * replace aioredis with fresh redis             [ https://a.yandex-team.ru/arc/commit/11777084 ]
 * hosts and ports from docker-compose in tests  [ https://a.yandex-team.ru/arc/commit/11774830 ]

* [vgol](http://staff/vgol)

 * BI-4592: Added script to generate pyproject.toml from setup.py + ya.make, with reference versions taken from prod container  [ https://a.yandex-team.ru/arc/commit/11774366 ]

* [mcpn](http://staff/mcpn)

 * BI-4540: remove blackbox client from env manager  [ https://a.yandex-team.ru/arc/commit/11774071 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-23 08:20:36+00:00

0.1889.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-22 08:00:19+00:00

0.1888.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-4632] Generalize USCrawlerBase & US client/manager tenant control: generalize crawler scope limit, remove ability to set tenant from entry config in crawler, generalize tenancy related methods in USM/C  [ https://a.yandex-team.ru/arc/commit/11761805 ]

* [vgol](http://staff/vgol)

 * BI-4605: [snowflake-connector] validate account_name; renamed tests dir to bi_connector_snowflake_tests  [ https://a.yandex-team.ru/arc/commit/11761366 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-22 07:30:40+00:00

0.1887.0
--------

* [mcpn](http://staff/mcpn)

 * BI-4541: installation-specific service registries     [ https://a.yandex-team.ru/arc/commit/11754767 ]
 * BI-4540: move ReqCtxInfoMiddleware to bi_api_commons  [ https://a.yandex-team.ru/arc/commit/11754292 ]

* [kchupin](http://staff/kchupin)

 * [BI-4632] Extract DLRequestBase to bi_api_commons  [ https://a.yandex-team.ru/arc/commit/11730197 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-21 10:17:24+00:00

0.1886.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-4632] Extract base part of reporting framework to API commons  [ https://a.yandex-team.ru/arc/commit/11708070 ]
 * [BI-4631] Remove yenv as direct dependency                         [ https://a.yandex-team.ru/arc/commit/11707903 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-15 14:18:07+00:00

0.1885.0
--------

* [konstasa](http://staff/konstasa)

 * CHARTS-7424 Workaround for the required name field in connection schemas  [ https://a.yandex-team.ru/arc/commit/11703726 ]

* [mcpn](http://staff/mcpn)

 * BI-4591: fix email batching for ListMembersRequest in RLS  [ https://a.yandex-team.ru/arc/commit/11700597 ]

* [kchupin](http://staff/kchupin)

 * [BI-4631] Remove `yenv` usage in file uploader worker. Was used to determine if lauched in tests. Replaced with flag in settings.  [ https://a.yandex-team.ru/arc/commit/11700552 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-14 11:28:39+00:00

0.1884.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-4632] Remove unused security mechanism to restrict access for network proxies (due to lack of those)  [ https://a.yandex-team.ru/arc/commit/11691940 ]
 * [BI-4550] Remove /<connection_id>/command/<command>                                                       [ https://a.yandex-team.ru/arc/commit/11691247 ]

* [bulanovavv](http://staff/bulanovavv)

 * BI-4589: Catching RowTooLarge exception from JSONCompactChunksParser  [ https://a.yandex-team.ru/arc/commit/11691155 ]

* [konstasa](http://staff/konstasa)

 * BI-4610 Do not delete data and throw errors on exec for errors not caused by the user on gsheets autoupdate  [ https://a.yandex-team.ru/arc/commit/11685256 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved SF, BQ, MYSQL and YDB/YQL dialect definitions out of bi_formula                   [ https://a.yandex-team.ru/arc/commit/11682844 ]
 * Moved more modules from bi_api_lib to bi_query_processing                               [ https://a.yandex-team.ru/arc/commit/11682778 ]
 * logical_children -> autonomous_children                                                 [ https://a.yandex-team.ru/arc/commit/11678978 ]
 * Switched to using .make factories for literal and logical block formula nodes           [ https://a.yandex-team.ru/arc/commit/11678037 ]
 * BI-4615: Tried to move snowflake and bigquery ConnectionType definitions to connectors  [ https://a.yandex-team.ru/arc/commit/11678035 ]
 * BI-4601: Introduced ControlApiAppFactory                                                [ https://a.yandex-team.ru/arc/commit/11678022 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-13 07:04:35+00:00

0.1883.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved come code to lib_bi_api.query.legacy_pipeline  [ https://a.yandex-team.ru/arc/commit/11641931 ]

* [vitaliy3000](http://staff/vitaliy3000)

 * BI-4102: change oracle lib  [ https://a.yandex-team.ru/arc/commit/11623668 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-08 14:09:24+00:00

0.1882.0
--------

* [mcpn](http://staff/mcpn)

 * Moved usage tracking and billing connectors to bi_connector_bundle_ch_filtered  [ https://a.yandex-team.ru/arc/commit/11620163 ]
 * Moved billing analytics core connector to a separate library                    [ https://a.yandex-team.ru/arc/commit/11612029 ]
 * BI-4541: move cloud manager to bi_api_commons                                   [ https://a.yandex-team.ru/arc/commit/11611049 ]
 * Moved YQ and YQL core connectors to a separate library                          [ https://a.yandex-team.ru/arc/commit/11604426 ]
 * Moved monitoring core connector to a separate library                           [ https://a.yandex-team.ru/arc/commit/11600579 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved the remap_formula_obj_fields function to another module for future removal of the legacy query pipeline  [ https://a.yandex-team.ru/arc/commit/11620089 ]
 * Moved some more files to bi_query_processing                                                                   [ https://a.yandex-team.ru/arc/commit/11620086 ]
 * Switched ConnectionType to DynamicEnum base                                                                    [ https://a.yandex-team.ru/arc/commit/11619914 ]
 * BI-4601: Reorganized US params in bi_api_lib tests, added base class for bi-api tests                          [ https://a.yandex-team.ru/arc/commit/11613606 ]
 * BI-4593: Implemented env param getters and added their usage in bigquery tests                                 [ https://a.yandex-team.ru/arc/commit/11605318 ]
 * Implemented native window function mode for CLICKHOUSE, MYSQL and POSTGRESQL                                   [ https://a.yandex-team.ru/arc/commit/11593135 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-06-02 10:40:50+00:00

0.1881.0
--------

* [nadein](http://staff/nadein)

 * BI-4563: Handle non-existing logins in RLS

Added new subject type for rls
New logic for handling non-existing logins

Update DLS resolver after merge
Added test case and test data  [ https://a.yandex-team.ru/arc/commit/11590332 ]

* [mcpn](http://staff/mcpn)

 * BI-3627: Removed connector-specific ConnectOptions checks from EnvManagerFactory  [ https://a.yandex-team.ru/arc/commit/11581324 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-30 11:35:22+00:00

0.1880.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4585 Common oauth application alias for service connection forms  [ https://a.yandex-team.ru/arc/commit/11565995 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-26 09:33:42+00:00

0.1879.0
--------

* [konstasa](http://staff/konstasa)

 * CHARTS-7424 BI-4224 Remove name, dirPath & workbookId from form configs; remove USE_WORKBOOKS_IN_FORMS app setting  [ https://a.yandex-team.ru/arc/commit/11551962 ]

* [gstatsenko](http://staff/gstatsenko)

 * Updated BigQuery tests for new cloud and service account                                               [ https://a.yandex-team.ru/arc/commit/11551749 ]
 * Removed direct usage of sa dialects from bi_formula; moved remaining metrics definitions to connector  [ https://a.yandex-team.ru/arc/commit/11550105 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-24 16:03:12+00:00

0.1878.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed column name conflicts in compeng sub-queries  [ https://a.yandex-team.ru/arc/commit/11543446 ]

* [mcpn](http://staff/mcpn)

 * BI-4541: move DLSSubjectResolver to bi_dls_client  [ https://a.yandex-team.ru/arc/commit/11541160 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-23 17:23:10+00:00

0.1877.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added additional splitting of sub-queries so that everything below window functions goes to source DB  [ https://a.yandex-team.ru/arc/commit/11537810 ]

* [konstasa](http://staff/konstasa)

 * Bring back CHYT connection alias (temporarily)  [ https://a.yandex-team.ru/arc/commit/11530714 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-23 09:50:15+00:00

0.1876.0
--------

* [vitaliy3000](http://staff/vitaliy3000)

 * BI-4565: fix binary decoding ch response

BI-4565: fix binary decoding ch response  [ https://a.yandex-team.ru/arc/commit/11529420 ]

* [gstatsenko](http://staff/gstatsenko)

 * Reorganized multi-query mutator files  [ https://a.yandex-team.ru/arc/commit/11528951 ]

* [nadein](http://staff/nadein)

 * BI-4442: Enable BigQuery alias group by  [ https://a.yandex-team.ru/arc/commit/11520254 ]

* [mcpn](http://staff/mcpn)

 * Refactor pivot headers  [ https://a.yandex-team.ru/arc/commit/11519348 ]

* [konstasa](http://staff/konstasa)

 * BI-4501 CHYT ext connection form: add  secure field into check api schema  [ https://a.yandex-team.ru/arc/commit/11516192 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-22 12:21:13+00:00

0.1875.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved CHS3 core connectors to a separate package  [ https://a.yandex-team.ru/arc/commit/11509091 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-19 10:11:27+00:00

0.1874.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Optimized COMPENG usage (sending as many sub-queries to source db as possible)        [ https://a.yandex-team.ru/arc/commit/11504138 ]
 * Moved metrica, mssql and oracle core connectors to the respective connector packages  [ https://a.yandex-team.ru/arc/commit/11492240 ]

* [konstasa](http://staff/konstasa)

 * BI-4501 CHYT external connector: connection form                           [ https://a.yandex-team.ru/arc/commit/11504115 ]
 * BI-2631 freeform_sources i18n                                              [ https://a.yandex-team.ru/arc/commit/11504112 ]
 * BI-4448 Ref to non-ref sources in file-based datasets - migration crawler  [ https://a.yandex-team.ru/arc/commit/11494937 ]
 * BI-4499 CHYT external connector: bi_api_lib part                           [ https://a.yandex-team.ru/arc/commit/11489773 ]

* [mcpn](http://staff/mcpn)

 * BI-4541: remove flask_utils from bi_api_lib  [ https://a.yandex-team.ru/arc/commit/11502518 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-18 09:56:20+00:00

0.1873.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved several CreateDSFrom declarations to connectors  [ https://a.yandex-team.ru/arc/commit/11481741 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-16 08:16:14+00:00

0.1872.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4526 Partner Connector: Kontur.Market  [ https://a.yandex-team.ru/arc/commit/11476318 ]

* [gstatsenko](http://staff/gstatsenko)

 * Replaced missing and default with load_default and dump_default in marshmallow fields  [ https://a.yandex-team.ru/arc/commit/11465506 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-15 10:58:17+00:00

0.1871.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-11 14:41:25+00:00

0.1870.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-11 14:30:32+00:00

0.1869.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Improved the functions for telling the type of QueryFork node (agg vs lookup vs window)  [ https://a.yandex-team.ru/arc/commit/11456473 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-11 13:37:09+00:00

0.1868.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-4534: Fixed inconsistent aggregation status error       [ https://a.yandex-team.ru/arc/commit/11455404 ]
 * Added AGO to auto-generated tests                          [ https://a.yandex-team.ru/arc/commit/11454593 ]
 * Fixed recursion error in LODs with measures as dimensions  [ https://a.yandex-team.ru/arc/commit/11454586 ]

* [konstasa](http://staff/konstasa)

 * BI-4499 CHYT external connector: bi_core part  [ https://a.yandex-team.ru/arc/commit/11453526 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-11 12:39:59+00:00

0.1867.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4447 Make origin_source_id optional in parameters schema  [ https://a.yandex-team.ru/arc/commit/11444642 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-10 10:23:22+00:00

0.1866.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4447 Hide non-ref sources behind a flag  [ https://a.yandex-team.ru/arc/commit/11443410 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved mysql core connector from bi_core to bi_connector_mysql  [ https://a.yandex-team.ru/arc/commit/11429463 ]
 * Added more info to assertion error in query_fork               [ https://a.yandex-team.ru/arc/commit/11429362 ]

* [vgol](http://staff/vgol)

 * BI-4201 fixed snowflake notifications about expiring refresh token; fixed  snowflake tests  [ https://a.yandex-team.ru/arc/commit/11429119 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-10 08:25:41+00:00

0.1865.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-05 11:43:00+00:00

0.1864.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4447 Non-ref sources in file based connections  [ https://a.yandex-team.ru/arc/commit/11427853 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved some more files to bi_query_processing                                                      [ https://a.yandex-team.ru/arc/commit/11426307 ]
 * CreateDSFrom as DynamicEnum; Implemented DynamicEnumField                                         [ https://a.yandex-team.ru/arc/commit/11425957 ]
 * Added 'make' method to some FormulaItem subclasses and switched to it instead of the constructor  [ https://a.yandex-team.ru/arc/commit/11425756 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-05 11:23:09+00:00

0.1863.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-05 08:38:23+00:00

0.1862.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Removed the cotrib version of EnumField and EnumNameField                                    [ https://a.yandex-team.ru/arc/commit/11425491 ]
 * Separated bi_core.testing.executors into connector-specific files                            [ https://a.yandex-team.ru/arc/commit/11414502 ]
 * BI-4520: Fixed ambiguous column error                                                        [ https://a.yandex-team.ru/arc/commit/11414499 ]
 * Removed generic dialect combinations from bi_formula                                         [ https://a.yandex-team.ru/arc/commit/11410748 ]
 * Initialized bi_query_processing library                                                      [ https://a.yandex-team.ru/arc/commit/11405995 ]
 * Connectorization of enums. Part 1                                                            [ https://a.yandex-team.ru/arc/commit/11380163 ]
 * BI-4492: Fixed incorrect handling of filters in the query splitting process                  [ https://a.yandex-team.ru/arc/commit/11364178 ]
 * BI-4477: Fixed bug with ignored original dimension and new one in LOD; added more LOD tests  [ https://a.yandex-team.ru/arc/commit/11300156 ]
 * BI-4484: Fixed pre-loading of connections in dataset PUT                                     [ https://a.yandex-team.ru/arc/commit/11297846 ]
 * BI-4431: First iteration of data processor connectorization                                  [ https://a.yandex-team.ru/arc/commit/11289490 ]
 * BI-4193: Added an optimization for splitting window function sub-queries                     [ https://a.yandex-team.ru/arc/commit/11231616 ]
 * BI-4386: Removed raw data streamers                                                          [ https://a.yandex-team.ru/arc/commit/11182108 ]
 * BI-4193: Implemented handling of extended aggregations and lookup functions v2               [ https://a.yandex-team.ru/arc/commit/11177600 ]
 * BI-4387: Removed source features from code                                                   [ https://a.yandex-team.ru/arc/commit/11172269 ]
 * Moved ch_frozen_ core connectors to a separate package                                       [ https://a.yandex-team.ru/arc/commit/11172104 ]
 * Normalized the structure of the promql connector                                             [ https://a.yandex-team.ru/arc/commit/11161719 ]
 * Added dataset dependency loading to publicity checker                                        [ https://a.yandex-team.ru/arc/commit/11160753 ]
 * Fixed the loading of dataset dependencies in a couple of cases                               [ https://a.yandex-team.ru/arc/commit/11116131 ]
 * BI-4193: Added list of columns to FromObject                                                 [ https://a.yandex-team.ru/arc/commit/11100345 ]
 * Replaced some usages of us_manager with us_entry_buffer                                      [ https://a.yandex-team.ru/arc/commit/11093787 ]
 * BI-4193: Outlined the new query processing pipeline                                          [ https://a.yandex-team.ru/arc/commit/11025609 ]
 * Removed the update_access_mode update action from bi-api; removed a couple of tests          [ https://a.yandex-team.ru/arc/commit/11020178 ]
 * Re-generated library dist-infos                                                              [ https://a.yandex-team.ru/arc/commit/11020174 ]
 * BI-4193: Switched to new implementation of TranslatedMultiQuery                              [ https://a.yandex-team.ru/arc/commit/10979836 ]
 * BI-4193: Simplified sql source preparation in bi_core                                        [ https://a.yandex-team.ru/arc/commit/10972580 ]
 * Moved the remaining data source editing methods to DatasetComponentEditor                    [ https://a.yandex-team.ru/arc/commit/10904533 ]

* [seray](http://staff/seray)

 * BI-4539 changelog in bi_api_lib  [ https://a.yandex-team.ru/arc/commit/11425289 ]
 * BI-4195 moving tests             [ https://a.yandex-team.ru/arc/commit/11398408 ]
 * BI-4353 auth for embedding       [ https://a.yandex-team.ru/arc/commit/11145609 ]

* [konstasa](http://staff/konstasa)

 * BI-4323 Add missing template_name into bitrix24 connection form                                                                                                                         [ https://a.yandex-team.ru/arc/commit/11384226 ]
 * BI-4389 Remove ConnectionInternalCH, leaving InternalMaterializationConnectionRef                                                                                                       [ https://a.yandex-team.ru/arc/commit/11377220 ]
 * BI-4502 Add alias for CHYT and change title to CHYT instead of CH over YT                                                                                                               [ https://a.yandex-team.ru/arc/commit/11364838 ]
 * BI-4500 Configure public, forbidden and default CHYT cliques via env                                                                                                                    [ https://a.yandex-team.ru/arc/commit/11364242 ]
 * BI-4381 Rename to ConnectionInfoProvider; fix typo in chyt translation                                                                                                                  [ https://a.yandex-team.ru/arc/commit/11336124 ]
 * BI-4323 Swap token and portal row in bitrix connection form                                                                                                                             [ https://a.yandex-team.ru/arc/commit/11329592 ]
 * Remove statface connector info provider                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/11325635 ]
 * BI-4381 Introduce ConnectorInfoProvider that serves connector alias and title                                                                                                           [ https://a.yandex-team.ru/arc/commit/11324718 ]
 * BI-4388 Remove obsolete crawlers                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/11313739 ]
 * BI-4461 Use different connection type in oauth token row for SMB Heatmaps form for now                                                                                                  [ https://a.yandex-team.ru/arc/commit/11290741 ]
 * BI-4461 SMB Heatmaps service connector                                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/11289604 ]
 * BI-4391 Remove MaterializerSchedulerActionConfig                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/11286859 ]
 * BI-4413 Show create QL chart button for ch_frozen_demo                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/11260092 ]
 * BI-4333 BigQuery connection form                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/11235438 ]
 * BI-4413 Allow user defined subselect sources & dashsql for ch_frozen_demo, pass query to user                                                                                           [ https://a.yandex-team.ru/arc/commit/11233569 ]
 * BI-4325 BI-4326 Add missing hostname row to manual host selection in Postgres and Greenplum forms with mdb enabled                                                                      [ https://a.yandex-team.ru/arc/commit/11233547 ]
 * BI-4265 Add proper names to tenant pickers in mdb forms; fix mdb forms in organizations                                                                                                 [ https://a.yandex-team.ru/arc/commit/11233368 ]
 * BI-4324 CHYDB connection form                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/11233109 ]
 * BI-4334 Snowflake connection form                                                                                                                                                       [ https://a.yandex-team.ru/arc/commit/11231667 ]
 * BI-4331 YDB connection form                                                                                                                                                             [ https://a.yandex-team.ru/arc/commit/11210999 ]
 * BI-4328 MSSQL connection form                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/11192203 ]
 * BI-4314 Service connection forms                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/11190026 ]
 * BI-4329 MySQL connection form                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/11187942 ]
 * BI-4321 Monitoring & Solomon connection forms                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/11187934 ]
 * BI-4325 Greenplum connection form                                                                                                                                                       [ https://a.yandex-team.ru/arc/commit/11187929 ]
 * BI-4319 Fix appmetrica counter row                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/11187633 ]
 * BI-4326 Postgres connection form                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/11182021 ]
 * BI-4400 Add translations to existing forms in bi_api_lib                                                                                                                                [ https://a.yandex-team.ru/arc/commit/11177988 ]
 * BI-4320 PromQL connection form                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/11177878 ]
 * BI-4323 BitrixGDS connection form                                                                                                                                                       [ https://a.yandex-team.ru/arc/commit/11177607 ]
 * BI-4327 GSheets connection form                                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/11177449 ]
 * Fix counter field name in metrica forms                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/11170131 ]
 * BI-4265 Fix duplicate username field in CH form with MDB                                                                                                                                [ https://a.yandex-team.ru/arc/commit/11168159 ]
 * BI-4319 Metrica & AppMetrica connection forms                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/11165661 ]
 * BI-2629 Add Horeca frozen connector                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/11135633 ]
 * BI-4332 YQ connection form                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/11100823 ]
 * BI-4330 Oracle connection form                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/11060974 ]
 * BI-4052 Skip special source migration for ch_frozen subselect to allow changing to it from ch_subselect                                                                                 [ https://a.yandex-team.ru/arc/commit/11060958 ]
 * BI-4298 DLPROJECTS-138 Frozen & geolayer connectors' forms                                                                                                                              [ https://a.yandex-team.ru/arc/commit/11060950 ]
 * BI-4299 BI-4248 Partners connections forms                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/11036174 ]
 * BI-4224 Env var to enable backend driven forms one by one                                                                                                                               [ https://a.yandex-team.ru/arc/commit/11035462 ]
 * BI-4265 Sync CH form with the current UI config                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/11024357 ]
 * BI-4224 BI-4265 Two more common row shortcuts; make FormFieldName extendable for connector specific fields; prepare form registry for not connectorized connections to avoid conflicts  [ https://a.yandex-team.ru/arc/commit/11003571 ]
 * BI-4224 Add top-level form fields; add nullable form field api schema; update CH form accordingly                                                                                       [ https://a.yandex-team.ru/arc/commit/10999743 ]
 * BI-4268 BI-4278 Remove allow_subselect from connections API + add crawler                                                                                                               [ https://a.yandex-team.ru/arc/commit/10959727 ]
 * BI-4265 ClickHouse form config; introduce inner UI fields; form config tests                                                                                                            [ https://a.yandex-team.ru/arc/commit/10954780 ]

* [mcpn](http://staff/mcpn)

 * BI-4479: fix pivot header role spec for subtotals      [ https://a.yandex-team.ru/arc/commit/11380223 ]
 * BI-4404: pass is_dashsql_query flag in RQE             [ https://a.yandex-team.ru/arc/commit/11187069 ]
 * BI-3520: fix pivot sorting by measure with pagination  [ https://a.yandex-team.ru/arc/commit/11121063 ]
 * BI-3520: fix pivot measure sorting with measure names  [ https://a.yandex-team.ru/arc/commit/11111994 ]
 * BI-3520: pivot sorting by measure                      [ https://a.yandex-team.ru/arc/commit/11075931 ]
 * BI-4283: add metainfo to pivot headers                 [ https://a.yandex-team.ru/arc/commit/11001883 ]
 * BI-3520: add sorting setting for measure in pivots     [ https://a.yandex-team.ru/arc/commit/10994976 ]

* [asnytin](http://staff/asnytin)

 * BI-4361: delete statface connector  [ https://a.yandex-team.ru/arc/commit/11313243 ]

* [kchupin](http://staff/kchupin)

 * [BI-4353] Fix logging app name for secure embeds data API                              [ https://a.yandex-team.ru/arc/commit/11177709 ]
 * Fix loading attempt to load mat conn on us_manager.load_dependencies() in dataset-api  [ https://a.yandex-team.ru/arc/commit/11160857 ]
 * [BI-MAINTENANCE] Fix setup.py for bi_api_lib                                           [ https://a.yandex-team.ru/arc/commit/11065162 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4398 Move TenantDef and AuthData implementations to bi_api_commons.   [ https://a.yandex-team.ru/arc/commit/11155774 ]
 * BI-4260 Use special rls config syntax for service account rls subjects.  [ https://a.yandex-team.ru/arc/commit/11000199 ]
 * BI-4281 feature bi_api_commons package                                   [ https://a.yandex-team.ru/arc/commit/10951763 ]

* [thenno](http://staff/thenno)

 * BI-4271: add localization (infra + forms)                                         [ https://a.yandex-team.ru/arc/commit/11149394 ]
 * Don't init subject_resolver for every field                                       [ https://a.yandex-team.ru/arc/commit/11030936 ]
 * BI-3978: delete forgotten feature-related things                                  [ https://a.yandex-team.ru/arc/commit/10999861 ]
 * Fix enforce collate attr in PostgresConnectionSchema                              [ https://a.yandex-team.ru/arc/commit/10995453 ]
 * BI-4284: Don't return 500 status code for csv-based datasets in dataset-data-api  [ https://a.yandex-team.ru/arc/commit/10975312 ]
 * Don't return 500 status code for csv-based datasets                               [ https://a.yandex-team.ru/arc/commit/10973818 ]
 * BI-4284: delete old csv connector                                                 [ https://a.yandex-team.ru/arc/commit/10919082 ]

* [dmifedorov](http://staff/dmifedorov)

 * quick fix for materializationless envs                      [ https://a.yandex-team.ru/arc/commit/10991789 ]
 * do not try to delete materialized data on ds/conn deleting  [ https://a.yandex-team.ru/arc/commit/10962729 ]

* [ovsds](http://staff/ovsds)

 * BI-4219: show empty connection sources on read permission absence

BI-4219: show empty connection sources on read permission absence  [ https://a.yandex-team.ru/arc/commit/10942728 ]

* [vgol](http://staff/vgol)

 * BI-4254: Disable streaming in dashsql; return 400 if error occured during data fetch.  [ https://a.yandex-team.ru/arc/commit/10927375 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-05 08:31:25+00:00

0.1861.0
--------

* [seray](http://staff/seray)

 * BI-4195 moving tests  [ https://a.yandex-team.ru/arc/commit/11398408 ]

* [mcpn](http://staff/mcpn)

 * BI-4479: fix pivot header role spec for subtotals  [ https://a.yandex-team.ru/arc/commit/11380223 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-05-04 10:27:55+00:00

0.1860.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-27 12:39:03+00:00

0.1859.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-4515: Fixed integer division in COMPENG  [ https://a.yandex-team.ru/arc/commit/11377431 ]

* [konstasa](http://staff/konstasa)

 * BI-4389 Remove ConnectionInternalCH, leaving InternalMaterializationConnectionRef  [ https://a.yandex-team.ru/arc/commit/11377220 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-27 11:58:12+00:00

0.1858.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4500 Configure public, forbidden and default CHYT cliques via env  [ https://a.yandex-team.ru/arc/commit/11364242 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-27 11:14:13+00:00

0.1857.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-4492: Fixed incorrect handling of filters in the query splitting process  [ https://a.yandex-team.ru/arc/commit/11364178 ]
 * BI-4491: Switched to safe division in COMPENG                                [ https://a.yandex-team.ru/arc/commit/11337621 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-26 07:43:36+00:00

0.1856.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-20 12:31:26+00:00

0.1855.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4388 Remove obsolete crawlers  [ https://a.yandex-team.ru/arc/commit/11313739 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-20 11:21:51+00:00

0.1854.0
--------

* [asnytin](http://staff/asnytin)

 * BI-4361: delete statface connector  [ https://a.yandex-team.ru/arc/commit/11313243 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-19 08:19:38+00:00

0.1853.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4461 Add configs for YC prod  [ https://a.yandex-team.ru/arc/commit/11313019 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-19 07:47:25+00:00

0.1852.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-4474: Added a workaround for the ROUND error in compeng PG                                [ https://a.yandex-team.ru/arc/commit/11300834 ]
 * BI-4477: Fixed bug with ignored original dimension and new one in LOD; added more LOD tests  [ https://a.yandex-team.ru/arc/commit/11300156 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-17 13:15:25+00:00

0.1851.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4461 SMB Heatmaps service connector  [ https://a.yandex-team.ru/arc/commit/11289604 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-4431: First iteration of data processor connectorization  [ https://a.yandex-team.ru/arc/commit/11289490 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-17 10:18:57+00:00

0.1850.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed LOD joins for dates  [ https://a.yandex-team.ru/arc/commit/11273788 ]

* [mcpn](http://staff/mcpn)

 * BI-4399: register ClickHouse connector using entry points  [ https://a.yandex-team.ru/arc/commit/11264512 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-12 16:48:18+00:00

0.1849.0
--------

* [ovsds](http://staff/ovsds)

 * BI-4454: decrease dataset api uwsgi max-requests  [ https://a.yandex-team.ru/arc/commit/11259110 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-11 11:32:20+00:00

0.1848.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4413 Allow user defined subselect sources & dashsql for ch_frozen_demo, pass query to user  [ https://a.yandex-team.ru/arc/commit/11233569 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-06 20:38:22+00:00

0.1847.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-4193: Added an optimization for splitting window function sub-queries  [ https://a.yandex-team.ru/arc/commit/11231616 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-06 13:32:30+00:00

0.1846.0
--------

* [vgol](http://staff/vgol)

 * Updated snowflake creds, added sql snippet to setup dl test account/data in snowflake  [ https://a.yandex-team.ru/arc/commit/11199577 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-04 15:47:31+00:00

0.1845.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Re-oorganized complex query tests (window funcs, lookup funcs, ext aggregations)  [ https://a.yandex-team.ru/arc/commit/11189872 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-04-03 08:48:55+00:00

0.1844.0
--------

* [thenno](http://staff/thenno)

 * BI-4272: don't validate locales  [ https://a.yandex-team.ru/arc/commit/11187455 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-31 16:52:38+00:00

0.1843.0
--------

* [mcpn](http://staff/mcpn)

 * BI-4404: pass is_dashsql_query flag in RQE  [ https://a.yandex-team.ru/arc/commit/11187069 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-31 15:20:14+00:00

0.1842.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-31 14:19:52+00:00

0.1841.0
--------

* [seray](http://staff/seray)

 * BI-4406 disable debug info  [ https://a.yandex-team.ru/arc/commit/11185698 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-31 13:45:28+00:00

0.1840.0
--------

* [kchupin](http://staff/kchupin)

 * [ORION-2921] Update DC preprod SA/project ID & add it for DC prod  [ https://a.yandex-team.ru/arc/commit/11179327 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-31 12:31:28+00:00

0.1839.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-30 20:28:32+00:00

0.1838.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-30 17:37:20+00:00

0.1837.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-4193: Implemented handling of extended aggregations and lookup functions v2           [ https://a.yandex-team.ru/arc/commit/11177600 ]
 * BI-4340: Implemented equality operator for sub-query joins that circumvents null values  [ https://a.yandex-team.ru/arc/commit/11173955 ]
 * Updated AGO tests                                                                        [ https://a.yandex-team.ru/arc/commit/11172113 ]
 * Moved ch_frozen_ core connectors to a separate package                                   [ https://a.yandex-team.ru/arc/commit/11172104 ]

* [thenno](http://staff/thenno)

 * BI-4390: delete mat app from tests                            [ https://a.yandex-team.ru/arc/commit/11173590 ]
 * Delete api_lib_connectors fixture <- we don't use it anymore  [ https://a.yandex-team.ru/arc/commit/11170063 ]
 * Don't register connectors for every session                   [ https://a.yandex-team.ru/arc/commit/11169770 ]

* [konstasa](http://staff/konstasa)

 * BI-4268 Remove remaining allow_subselect mentions  [ https://a.yandex-team.ru/arc/commit/11170129 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-30 16:56:41+00:00

0.1836.0
--------

* [seray](http://staff/seray)

 * BI-4406 enable debug info  [ https://a.yandex-team.ru/arc/commit/11168183 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-29 17:19:41+00:00

0.1835.0
--------

* [seray](http://staff/seray)

 * BI-4406 Adding tool for profiling  [ https://a.yandex-team.ru/arc/commit/11166668 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-29 15:30:24+00:00

0.1834.0
--------

* [vgol](http://staff/vgol)

 * BI-4269: Subselect data source for snowflake  [ https://a.yandex-team.ru/arc/commit/11164117 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-29 13:03:51+00:00

0.1833.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-29 10:01:01+00:00

0.1832.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-29 00:08:47+00:00

0.1831.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-28 22:42:16+00:00

0.1830.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-28 21:14:58+00:00

0.1829.0
--------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4398 Move TenantDef and AuthData implementations to bi_api_commons.  [ https://a.yandex-team.ru/arc/commit/11155774 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-4193: Added auto-generated LOD tests  [ https://a.yandex-team.ru/arc/commit/11152298 ]

* [seray](http://staff/seray)

 * BI-4353 auth for embedding  [ https://a.yandex-team.ru/arc/commit/11145609 ]

* [konstasa](http://staff/konstasa)

 * BI-2629 Add Horeca frozen connector  [ https://a.yandex-team.ru/arc/commit/11135633 ]

* [mcpn](http://staff/mcpn)

 * BI-4377: Moved PostgreSQL formula and db testing connectors to a separate library  [ https://a.yandex-team.ru/arc/commit/11133645 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-28 19:48:10+00:00

0.1828.0
--------

* [thenno](http://staff/thenno)

 * Skip broken test_dataset_revision_id  [ https://a.yandex-team.ru/arc/commit/11126177 ]

* [mcpn](http://staff/mcpn)

 * BI-4377: Moved Clickhouse formula and db testing connectors to a separate library  [ https://a.yandex-team.ru/arc/commit/11122003 ]
 * BI-3520: fix pivot sorting by measure with pagination                              [ https://a.yandex-team.ru/arc/commit/11121063 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-24 11:16:27+00:00

0.1827.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3520: fix pivot measure sorting with measure names  [ https://a.yandex-team.ru/arc/commit/11111994 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-22 16:32:00+00:00

0.1826.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-22 14:05:38+00:00

0.1825.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-4193: Added list of columns to FromObject             [ https://a.yandex-team.ru/arc/commit/11100345 ]
 * Replaced some usages of us_manager with us_entry_buffer  [ https://a.yandex-team.ru/arc/commit/11093787 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-21 15:35:32+00:00

0.1824.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3520: pivot sorting by measure  [ https://a.yandex-team.ru/arc/commit/11075931 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-20 19:13:25+00:00

0.1823.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-17 12:03:43+00:00

0.1822.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-16 20:03:28+00:00

0.1821.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4052 Skip special source migration for ch_frozen subselect to allow changing to it from ch_subselect  [ https://a.yandex-team.ru/arc/commit/11060958 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-16 09:14:11+00:00

0.1820.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-13 13:22:27+00:00

0.1819.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4224 Env var to enable backend driven forms one by one  [ https://a.yandex-team.ru/arc/commit/11035462 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-13 13:00:56+00:00

0.1818.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added db_testing connector abstraction                                               [ https://a.yandex-team.ru/arc/commit/11025618 ]
 * Removed the update_access_mode update action from bi-api; removed a couple of tests  [ https://a.yandex-team.ru/arc/commit/11020178 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-13 09:20:34+00:00

0.1817.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-08 15:41:42+00:00

0.1816.0
--------

* [thenno](http://staff/thenno)

 * BI-3978: delete forgotten feature-related things  [ https://a.yandex-team.ru/arc/commit/10999861 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-07 12:04:32+00:00

0.1815.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3520: add sorting setting for measure in pivots  [ https://a.yandex-team.ru/arc/commit/10994976 ]

* [thenno](http://staff/thenno)

 * Use arq from contrib (and drop some mat-tests)  [ https://a.yandex-team.ru/arc/commit/10991557 ]
 * Delete broken mat tests                         [ https://a.yandex-team.ru/arc/commit/10977638 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-4193: Switched to new implementation of TranslatedMultiQuery  [ https://a.yandex-team.ru/arc/commit/10979836 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-07 10:33:09+00:00

0.1814.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-03 09:27:40+00:00

0.1813.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-03 08:12:24+00:00

0.1812.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-03 01:38:10+00:00

0.1811.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-03 00:16:36+00:00

0.1810.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-02 18:26:45+00:00

0.1809.0
--------

* [konstasa](http://staff/konstasa)

 * Remove connector settings set to None in tests  [ https://a.yandex-team.ru/arc/commit/10968577 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-02 11:21:54+00:00

0.1808.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-02 09:24:09+00:00

0.1807.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-01 16:37:45+00:00

0.1806.0
--------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4056 Restore Feature Date32 for clickhouse connections. Convert user date type to Date32.  [ https://a.yandex-team.ru/arc/commit/10960390 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-01 12:19:05+00:00

0.1805.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4268 BI-4278 Remove allow_subselect from connections API + add crawler  [ https://a.yandex-team.ru/arc/commit/10959727 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-01 10:58:01+00:00

0.1804.0
--------

* [alex-ushakov](http://staff/alex-ushakov)

 * Revert "BI-4056 Feature Date32 for clickhouse connections. Convert user date type to Date32."

This reverts commit b65d02f2548784d3b12f72b4602774362dc96749, reversing
changes made to 4a4ed7ae678b93240b64a3ca00389df61033e4ba.  [ https://a.yandex-team.ru/arc/commit/10957507 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-03-01 07:48:09+00:00

0.1803.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-28 16:27:50+00:00

0.1802.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * decrease rqe timeouts  [ https://a.yandex-team.ru/arc/commit/10953398 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4281 feature bi_api_commons package  [ https://a.yandex-team.ru/arc/commit/10951763 ]

* [thenno](http://staff/thenno)

 * BI-4279: drop converter image from tests  [ https://a.yandex-team.ru/arc/commit/10947278 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-28 15:22:57+00:00

0.1801.0
--------

* [konstasa](http://staff/konstasa)

 * BI-2125 Metrica & AppMetrica connections data schematics -> attrs  [ https://a.yandex-team.ru/arc/commit/10943358 ]

* [ovsds](http://staff/ovsds)

 * BI-4219: show empty connection sources on read permission absence

BI-4219: show empty connection sources on read permission absence  [ https://a.yandex-team.ru/arc/commit/10942728 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-27 11:21:21+00:00

0.1800.0
--------

* [vgol](http://staff/vgol)

 * BI-4254: Disable streaming in dashsql; return 400 if error occured during data fetch.  [ https://a.yandex-team.ru/arc/commit/10927375 ]

* [thenno](http://staff/thenno)

 * BI-4284: delete old csv connector  [ https://a.yandex-team.ru/arc/commit/10919082 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-27 09:18:37+00:00

0.1799.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved the remaining data source editing methods to DatasetComponentEditor  [ https://a.yandex-team.ru/arc/commit/10904533 ]
 * BI-4193: Refactored creation of FromObjects                                [ https://a.yandex-team.ru/arc/commit/10904472 ]
 * BI-4193: Refactored multi-query translator. Part 1                         [ https://a.yandex-team.ru/arc/commit/10884842 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-21 10:32:40+00:00

0.1798.0
--------

* [konstasa](http://staff/konstasa)

 * BI-2125 Migrate ClickHouse based connections data models from schematics to attrs  [ https://a.yandex-team.ru/arc/commit/10884271 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-16 15:07:31+00:00

0.1797.0
--------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4056 Feature Date32 for clickhouse connections. Convert user date type to Date32.  [ https://a.yandex-team.ru/arc/commit/10880983 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-16 11:32:02+00:00

0.1796.0
--------

* [mcpn](http://staff/mcpn)

 * BI-4173: visibility enum for a field  [ https://a.yandex-team.ru/arc/commit/10876718 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-16 09:23:12+00:00

0.1795.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-15 14:50:27+00:00

0.1794.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-4174: Fixed handling of US errors  [ https://a.yandex-team.ru/arc/commit/10871246 ]

* [konstasa](http://staff/konstasa)

 * BI-4244 Equeo partner connector  [ https://a.yandex-team.ru/arc/commit/10871053 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-15 13:05:32+00:00

0.1793.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4230 Remove old api schemas completely in favor of the GenericConnectionSchema; remove more rkeeper mentions  [ https://a.yandex-team.ru/arc/commit/10870092 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-15 08:20:23+00:00

0.1792.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-14 15:04:50+00:00

0.1791.0
--------

* [konstasa](http://staff/konstasa)

 * Remove bi/app_settings.py and bring back accidentally removed USE_BACKEND_DRIVEN_FORMS app setting  [ https://a.yandex-team.ru/arc/commit/10857534 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-4195: bi_api_lib pt 2  [ https://a.yandex-team.ru/arc/commit/10855847 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added a test for DLHELP-7119  [ https://a.yandex-team.ru/arc/commit/10852607 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-14 12:09:00+00:00

0.1790.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-4193: Refactored translated_multi_query interfaces  [ https://a.yandex-team.ru/arc/commit/10844929 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-10 16:42:51+00:00

0.1789.0
--------

* [vgol](http://staff/vgol)

 * fix build script to retry apt operations  [ https://a.yandex-team.ru/arc/commit/10843840 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-10 12:24:09+00:00

0.1788.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-10 10:09:43+00:00

0.1787.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-09 20:15:04+00:00

0.1786.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-09 18:24:55+00:00

0.1785.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-09 16:46:38+00:00

0.1784.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-09 16:13:35+00:00

0.1783.0
--------

* [vgol](http://staff/vgol)

 * BI-4211 - snowflake fix refresh_token_expire_time was not stored  [ https://a.yandex-team.ru/arc/commit/10836631 ]

* [konstasa](http://staff/konstasa)

 * BI-4224 Backend driven forms constructor  [ https://a.yandex-team.ru/arc/commit/10825767 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-09 15:53:37+00:00

0.1782.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4233 Introduce connector alias and add it to /info/connectors response  [ https://a.yandex-team.ru/arc/commit/10816667 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-08 11:17:09+00:00

0.1781.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4230 Remove useless old schemas configs (where generic is present); ch_billing_analytics as an entrypoint + use generic api schema; remove swagger in connections api; add TODOs  [ https://a.yandex-team.ru/arc/commit/10793864 ]

* [gstatsenko](http://staff/gstatsenko)

 * Cleaned up some legacy logic in data sources  [ https://a.yandex-team.ru/arc/commit/10789689 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-4195: introducing bi_api_lib  [ https://a.yandex-team.ru/arc/commit/10785732 ]

* [mcpn](http://staff/mcpn)

 * Moved MSSQL formula connector to a separate library  [ https://a.yandex-team.ru/arc/commit/10785635 ]

* [vgol](http://staff/vgol)

 * BI-4200: Handle oauth token related errors  [ https://a.yandex-team.ru/arc/commit/10782754 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-07 11:25:14+00:00

0.1780.0
--------

* [konstasa](http://staff/konstasa)

 * Remove env_var_converter from boolean settings  [ https://a.yandex-team.ru/arc/commit/10776160 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-02-01 08:44:29+00:00

0.1779.0
--------

* [asnytin](http://staff/asnytin)

 * BI-4225: remove bi-billing usages from bi-api  [ https://a.yandex-team.ru/arc/commit/10773596 ]

* [kchupin](http://staff/kchupin)

 * [BI-4112] Update & fix tier-1 dependencies versions according A to make local installation possible  [ https://a.yandex-team.ru/arc/commit/10772550 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-31 18:22:06+00:00

0.1778.0
--------

* [konstasa](http://staff/konstasa)

 * BI-2629 Frozen connectors via TF; enable hidden frozen demo in Israel  [ https://a.yandex-team.ru/arc/commit/10768055 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-31 08:58:28+00:00

0.1777.0
--------

* [asnytin](http://staff/asnytin)

 * BI-4225: do not require billing checker in data api views  [ https://a.yandex-team.ru/arc/commit/10761077 ]

* [mcpn](http://staff/mcpn)

 * Moved MySQL formula connector to a separate library  [ https://a.yandex-team.ru/arc/commit/10753908 ]
 * Moved YQL formula connector to a separate library    [ https://a.yandex-team.ru/arc/commit/10753332 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-4193: Removed required_avatar_ids from query meta  [ https://a.yandex-team.ru/arc/commit/10749047 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-30 14:00:21+00:00

0.1776.0
--------

* [konstasa](http://staff/konstasa)

 * BI-2629 Add remaining ch_frozen connectors  [ https://a.yandex-team.ru/arc/commit/10747579 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-4193: Replaced used_avatar_ids with JoinedFromObject  [ https://a.yandex-team.ru/arc/commit/10734617 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-27 09:09:21+00:00

0.1775.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-24 10:53:05+00:00

0.1774.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed measure role support in result and preview                 [ https://a.yandex-team.ru/arc/commit/10720584 ]
 * BI-4193: Removed meta objects from multi-level queries           [ https://a.yandex-team.ru/arc/commit/10715726 ]
 * Moved oracle formula connector to a separate library             [ https://a.yandex-team.ru/arc/commit/10712307 ]
 * BI-4193: Separated optimizations from LOD logic in QueryMutator  [ https://a.yandex-team.ru/arc/commit/10701505 ]

* [vgol](http://staff/vgol)

 * BI-4003: Support user_role in the snowflake connection parameters  [ https://a.yandex-team.ru/arc/commit/10720307 ]

* [mcpn](http://staff/mcpn)

 * Moved metrica formula connector to a separate library    [ https://a.yandex-team.ru/arc/commit/10716915 ]
 * BI-3888: remove % duplication for dashsql mysql queries  [ https://a.yandex-team.ru/arc/commit/10702836 ]
 * fix ch_frozen_samples entrypoint                         [ https://a.yandex-team.ru/arc/commit/10686363 ]

* [thenno](http://staff/thenno)

 * BI-4186: notify users about mat google sheets connections being deprecated  [ https://a.yandex-team.ru/arc/commit/10682569 ]

[robot-statinfra](http://staff/robot-statinfra) 2023-01-24 12:34:27+03:00

0.1773.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-17 16:43:36+00:00

0.1772.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-16 13:00:28+00:00

0.1771.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4013 BI-4116 Store original errors instead if INVALID_SOURCE on data update failure; store the whole error in DataFile, not just code; refactor connection source failing  [ https://a.yandex-team.ru/arc/commit/10667127 ]

* [vgol](http://staff/vgol)

 * BI-4039: More detailed info in SourceDoesNotExist  [ https://a.yandex-team.ru/arc/commit/10667062 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-16 10:50:32+00:00

0.1770.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3737: Switched to explicit usage of conn_executor in data sources  [ https://a.yandex-team.ru/arc/commit/10651133 ]

[robot-statinfra](http://staff/robot-statinfra) 2023-01-13 14:19:13+03:00

0.1769.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * decrease uwsgi max-requests  [ https://a.yandex-team.ru/arc/commit/10647802 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed legacy pivot API  [ https://a.yandex-team.ru/arc/commit/10647597 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-12 10:30:55+00:00

0.1768.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-4181: Added support for dimension annotations in pivot tables  [ https://a.yandex-team.ru/arc/commit/10643095 ]
 * Moved test_lod to the lod folder and renamed it                   [ https://a.yandex-team.ru/arc/commit/10642437 ]
 * Reorganized dsmake shortcut modules                               [ https://a.yandex-team.ru/arc/commit/10635459 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4176 Extract bi_dls_client module.  [ https://a.yandex-team.ru/arc/commit/10642418 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-01-12 09:26:28+00:00

0.1767.0
--------

* [vgol](http://staff/vgol)

 * BI-4188: fix snowflake bi api  [ https://a.yandex-team.ru/arc/commit/10635090 ]

[robot-statinfra](http://staff/robot-statinfra) 2023-01-10 17:43:29+03:00

0.1766.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-4180: Added pivot item IDs to pivot data output  [ https://a.yandex-team.ru/arc/commit/10629296 ]

* [shadchin](http://staff/shadchin)

 * IGNIETFERRO-1154 Rename dateutil to python-dateutil  [ https://a.yandex-team.ru/arc/commit/10611000 ]
 * IGNIETFERRO-1154 Rename uwsgi to uWSGI               [ https://a.yandex-team.ru/arc/commit/10610666 ]

[robot-statinfra](http://staff/robot-statinfra) 2023-01-09 23:44:25+03:00

0.1765.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4083 Unbind missed [connection class -> connection type] map in connection schemas  [ https://a.yandex-team.ru/arc/commit/10602698 ]

* [vgol](http://staff/vgol)

 * BI-4003: fix include for snowflake tests  [ https://a.yandex-team.ru/arc/commit/10601447 ]

* [thenno](http://staff/thenno)

 * BI-3973: remove USE_RQE_PUBLIC usage  [ https://a.yandex-team.ru/arc/commit/10594796 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-30 13:50:04+00:00

0.1764.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3656: Updated pivot pagination API  [ https://a.yandex-team.ru/arc/commit/10592429 ]

* [konstasa](http://staff/konstasa)

 * BI-4143 Revive CHYT ext tests; move test data and cliques to hahn; use CHYT Controller to start cliques  [ https://a.yandex-team.ru/arc/commit/10591767 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-29 09:45:14+00:00

0.1763.0
--------

* [vgol](http://staff/vgol)

 * PR from branch users/vgol/BI-4003-connector-bi-api-part

BI-4003: bi-api part, initial implementation  [ https://a.yandex-team.ru/arc/commit/10591206 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-4150: Extended const comparison optimization  [ https://a.yandex-team.ru/arc/commit/10590430 ]
 * BI-3694: Added a test for pivot drm normalizer   [ https://a.yandex-team.ru/arc/commit/10586387 ]
 * BI-4150: Minor optimization for query mutator    [ https://a.yandex-team.ru/arc/commit/10585953 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-12-28 19:00:27+03:00

0.1762.0
--------

* [konstasa](http://staff/konstasa)

 * BI-2629 Api connector for CH frozen                       [ https://a.yandex-team.ru/arc/commit/10585242 ]
 * BI-4100 Remove VALIDATE_RESULT_SCHEMA_LENGTH flag usages  [ https://a.yandex-team.ru/arc/commit/10585090 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-27 14:16:26+00:00

0.1761.0
--------

* [konstasa](http://staff/konstasa)

 * BI-2629 New ch frozen base core connector & ch frozen sample  [ https://a.yandex-team.ru/arc/commit/10579540 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-26 13:12:44+00:00

0.1760.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4013 Do not remove raw_schema in failed sources during data update to let them load in dataset; move connection error raise back into datasource  [ https://a.yandex-team.ru/arc/commit/10572555 ]
 * Fix mypy after marshmallow update to 3.19.0                                                                                                          [ https://a.yandex-team.ru/arc/commit/10571470 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-23 15:25:48+00:00

0.1759.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-23 11:05:18+00:00

0.1758.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4013 Fix the issue with unknown field errors occuring before connection component errors; allow filling file source params with None values to fail them properly during data update  [ https://a.yandex-team.ru/arc/commit/10568355 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-22 20:45:55+00:00

0.1757.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4154 Respond with 400 instead of 500 when can not find source by id in file based connection                        [ https://a.yandex-team.ru/arc/commit/10562180 ]
 * BI-4013 Store errors that occur during data update in file-based connections error registry and throw them on execute  [ https://a.yandex-team.ru/arc/commit/10559993 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-22 10:21:14+00:00

0.1756.0
--------

* [thenno](http://staff/thenno)

 * BI-4091: rls - add login->user_id resolving to bi api  [ https://a.yandex-team.ru/arc/commit/10559660 ]

* [konstasa](http://staff/konstasa)

 * BI-4057 Make sure filename/table_name cannot be spoofed in file based connections  [ https://a.yandex-team.ru/arc/commit/10556691 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-21 19:22:44+00:00

0.1755.0
--------

* [vgol](http://staff/vgol)

 * BI-2527: Enabling conn sec manager for double cloud  [ https://a.yandex-team.ru/arc/commit/10544112 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-12-20 01:22:01+03:00

0.1754.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4083 Unbind strict 1-to-1 connection between Connection class and ConnectionType

Убрана строгая связь между классом коннекшена и ConnectionType, теперь эта связь существует на уровне объекта и мапин регистрации коннекторов
Осталась жесткая связь класса датасорса и класса подключения через conn_type, потому что она прорастает глубоко в транслятор, валидатор, и датасет в целом, нужно попытаться тоже вынести эту связь на уровень регистрации и объекта
Таким образом сейчас связи:
`1 DataSource class -> 1 ConnectionType -> 1 Connection class -> 1 ConnectionSchema`
`1 ConnectionSchema  -> 1 Connection class -> N ConnectionType`  [ https://a.yandex-team.ru/arc/commit/10521534 ]
 * Update file uploader client mock in tests                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/10513929 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-4044 Fix issue with % sign in queries  [ https://a.yandex-team.ru/arc/commit/10514276 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-15 09:47:25+00:00

0.1753.0
--------

* [thenno](http://staff/thenno)

 * BI-3974: drop geoinfo from dataset in crawler  [ https://a.yandex-team.ru/arc/commit/10501030 ]

* [vgol](http://staff/vgol)

 * BI-4003: Initial implementation of bi_connector_snowflake core/formula parts

BI-4003: Added initial implementation of bi_connector_snowflake
BI-4003: less SF tests to run untill cacert.pem vendoring issues are solved  [ https://a.yandex-team.ru/arc/commit/10500826 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-13 12:30:58+00:00

0.1752.0
--------

* [thenno](http://staff/thenno)

 * DoubleRevert "BI-3978: drop geofunctions and features"  [ https://a.yandex-team.ru/arc/commit/10499462 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-12 16:55:55+00:00

0.1751.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-12 16:16:53+00:00

0.1750.0
--------

* [thenno](http://staff/thenno)

 * Revert "BI-3978: drop geofunctions and features"  [ https://a.yandex-team.ru/arc/commit/10496425 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-12 13:02:18+00:00

0.1749.0
--------

* [thenno](http://staff/thenno)

 * BI-3974: add personal tenant id to usm in crawler  [ https://a.yandex-team.ru/arc/commit/10493318 ]
 * BI-3978: drop geofunctions and features            [ https://a.yandex-team.ru/arc/commit/10489283 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-4107: Fixed sorting of totals in pivot tables  [ https://a.yandex-team.ru/arc/commit/10489156 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-12 11:50:45+00:00

0.1748.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-09 08:40:00+00:00

0.1747.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-12-08 22:44:35+03:00

0.1746.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-12-08 22:18:34+03:00

0.1745.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Data api test abstraction layer  [ https://a.yandex-team.ru/arc/commit/10475501 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-12-08 16:21:24+03:00

0.1744.0
--------

* [thenno](http://staff/thenno)

 * BI-3974: prepare make_dataset_direct crawler for ext prod/preprod  [ https://a.yandex-team.ru/arc/commit/10471547 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-07 17:45:51+00:00

0.1743.0
--------

* [vgol](http://staff/vgol)

 * BI-2527: Fix run scripts for RQE to run in DC  [ https://a.yandex-team.ru/arc/commit/10468700 ]

* [konstasa](http://staff/konstasa)

 * Add another endpoint for update_connection_data with auth skip  [ https://a.yandex-team.ru/arc/commit/10463625 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-12-07 16:49:15+03:00

0.1742.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-12-02 16:29:38+00:00

0.1741.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4068 Move task processor task schedulling from dataset-api views to lifecycle manager; autouse use_local_task_processor  [ https://a.yandex-team.ru/arc/commit/10429680 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-30 17:24:40+00:00

0.1740.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-30 11:37:11+00:00

0.1739.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3731: add proper pickling for lambdas in clickhouse-sqlalchemy  [ https://a.yandex-team.ru/arc/commit/10386254 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-23 15:12:08+03:00

0.1738.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-23 11:20:17+00:00

0.1737.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3869 Add ANTLR files generation to other bi_formula dependent packages  [ https://a.yandex-team.ru/arc/commit/10356818 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-18 16:40:08+00:00

0.1736.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-17 15:30:51+00:00

0.1735.0
--------

* [konstasa](http://staff/konstasa)

 * Allow file source polling by returning file_id from connection and syncing DataFile on replace  [ https://a.yandex-team.ru/arc/commit/10355287 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-17 15:09:12+00:00

0.1734.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3901 Add 'authorized' field to update_connection_data method of fu-client in bi-api  [ https://a.yandex-team.ru/arc/commit/10340995 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-15 14:53:53+00:00

0.1733.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-15 10:41:14+00:00

0.1732.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-15 08:31:25+00:00

0.1731.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-11-14 16:05:56+03:00

0.1730.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3892 Make it possible to replace CSV conn with file conn in dataset  [ https://a.yandex-team.ru/arc/commit/10330910 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added a pagination optimization for multi-query requests  [ https://a.yandex-team.ru/arc/commit/10325619 ]
 * BI-4006: Fix for feature functions without args           [ https://a.yandex-team.ru/arc/commit/10323523 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-14 11:18:35+00:00

0.1729.0
--------

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-3976 Notify users about CSV connections being deprecated.  [ https://a.yandex-team.ru/arc/commit/10321620 ]

* [konstasa](http://staff/konstasa)

 * Token refreshing for gsheets  [ https://a.yandex-team.ru/arc/commit/10293280 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2022-11-11 10:40:46+00:00

0.1728.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-11-03 15:58:50+03:00

0.1727.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3749: remove DONT_REFRESH_DATETIME  [ https://a.yandex-team.ru/arc/commit/10282419 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-03 14:41:25+03:00

0.1726.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3825: Added nullifying of totals for extended aggregations  [ https://a.yandex-team.ru/arc/commit/10274259 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-02 20:05:26+03:00

0.1725.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4012 New authorization flow in GSheets  [ https://a.yandex-team.ru/arc/commit/10265843 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-11-01 14:15:44+03:00

0.1724.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-31 16:59:52+03:00

0.1723.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3982: fix a swap of sources with the same table name  [ https://a.yandex-team.ru/arc/commit/10260364 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-3970 Refactoring. Move bi-connector-promql files to bi-connector-promql package.  [ https://a.yandex-team.ru/arc/commit/10258821 ]

* [konstasa](http://staff/konstasa)

 * BI-3901 GSheets autoupdate: consider refresh_enabled when scheduling auto update  [ https://a.yandex-team.ru/arc/commit/10234142 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-31 16:07:10+03:00

0.1722.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-25 17:29:45+03:00

0.1721.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-25 14:31:25+03:00

0.1720.0
--------

* [seray](http://staff/seray)

 * BI-3780 bitrix user fields  [ https://a.yandex-team.ru/arc/commit/10223652 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-25 10:13:35+03:00

0.1719.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * yacloud functions for nebius  [ https://a.yandex-team.ru/arc/commit/10222646 ]

* [alex-ushakov](http://staff/alex-ushakov)

 * BI-2470 Feature an error transformer for 'Name or service not known' pg sync and async drivers error.  [ https://a.yandex-team.ru/arc/commit/10222597 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-24 17:11:10+03:00

0.1718.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3901 Add missing fields to request schema in file uploader client for /update_connection_data && partially remove tenant_id  [ https://a.yandex-team.ru/arc/commit/10220134 ]
 * BI-3790 Add missing gsheets related fields into gsheets_v2 connection source schema                                             [ https://a.yandex-team.ru/arc/commit/10219957 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-24 12:47:42+03:00

0.1717.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-23 09:55:51+03:00

0.1716.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3901 GSheets autoupdate: use UTC time everywhere  [ https://a.yandex-team.ru/arc/commit/10213268 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-21 18:02:03+03:00

0.1715.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3921 Force file save when updating connection and file_id is passed  [ https://a.yandex-team.ru/arc/commit/10212723 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-21 17:06:26+03:00

0.1714.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-21 15:33:12+03:00

0.1713.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-20 18:34:20+03:00

0.1712.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-20 15:20:22+03:00

0.1711.0
--------

* [vgol](http://staff/vgol)

 * BI-1147: Handle Null values in the IN expression.  [ https://a.yandex-team.ru/arc/commit/10190283 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-19 17:33:47+03:00

0.1710.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-18 16:52:56+03:00

0.1709.0
--------

* [konstasa](http://staff/konstasa)

 * BI-2654 data-api ping logs and timeouts  [ https://a.yandex-team.ru/arc/commit/10173685 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-18 10:53:29+03:00

0.1708.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-14 17:09:48+03:00

0.1707.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3885: file uploader metric view  [ https://a.yandex-team.ru/arc/commit/10170603 ]

* [konstasa](http://staff/konstasa)

 * BI-3921 GSheets one-time update  [ https://a.yandex-team.ru/arc/commit/10169420 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-14 15:57:38+03:00

0.1706.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-13 17:37:54+03:00

0.1705.0
--------

* [konstasa](http://staff/konstasa)

 * BI-2654 Check RQE and PG on data-api ping  [ https://a.yandex-team.ru/arc/commit/10162005 ]

* [gstatsenko](http://staff/gstatsenko)

 * Genericized DbDispenser and moved it to bi_db_testing  [ https://a.yandex-team.ru/arc/commit/10161556 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-13 13:53:18+03:00

0.1704.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3920 Encryption in file-uploader(-worker) + use token for GSheets  [ https://a.yandex-team.ru/arc/commit/10157386 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-12 18:16:42+03:00

0.1703.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3931: ignore zulu timezone for genericdatetime in filters  [ https://a.yandex-team.ru/arc/commit/10148143 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-11 12:31:45+03:00

0.1702.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3901 GSheets autoupdate; step 2: tasks & notification  [ https://a.yandex-team.ru/arc/commit/10134513 ]

* [kchupin](http://staff/kchupin)

 * [ORION-1570] New DC preprod services requisites  [ https://a.yandex-team.ru/arc/commit/10134410 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-07 18:09:38+03:00

0.1701.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Inherited the bi_formula Db implementation from the bi_db_testing one  [ https://a.yandex-team.ru/arc/commit/10134001 ]
 * DLHELP-6253: Added required=True to DimensionValueSpecSchema           [ https://a.yandex-team.ru/arc/commit/10132733 ]

* [vgol](http://staff/vgol)

 * BI-3903: Optimized FormulaItem.replace_nodes to avoid redundant copy calls

BI-3903: Optimized FormulaItem.replace_nodes to avoid redundant copy calls  [ https://a.yandex-team.ru/arc/commit/10129571 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-07 17:06:47+03:00

0.1700.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-05 19:21:34+03:00

0.1699.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-10-05 17:28:05+03:00

0.1698.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * [WIP] BI-3796: Completely switch from DATETIME to GENERICDATETIME  [ https://a.yandex-team.ru/arc/commit/10119489 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-05 14:27:08+03:00

0.1697.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3911: trim timezone suffix in mysql  [ https://a.yandex-team.ru/arc/commit/10118810 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-05 13:11:15+03:00

0.1696.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3624: auth for async-api in israel  [ https://a.yandex-team.ru/arc/commit/10113204 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-04 15:54:01+03:00

0.1695.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3901 GSheets autoupdate; step 1: API  [ https://a.yandex-team.ru/arc/commit/10105838 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-10-03 15:34:14+03:00

0.1694.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3773: Added first data api tests for BigQuery and made several fixes  [ https://a.yandex-team.ru/arc/commit/10099792 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-30 20:28:24+03:00

0.1693.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3773: Added first bi_api tests for BigQuery and made several fixes  [ https://a.yandex-team.ru/arc/commit/10098857 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-30 19:12:50+03:00

0.1692.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-30 16:04:01+03:00

0.1691.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-30 15:23:39+03:00

0.1690.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-30 15:15:18+03:00

0.1689.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3773: Various BigQuery connector fixes + basic formula connector  [ https://a.yandex-team.ru/arc/commit/10093775 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-30 12:10:01+03:00

0.1688.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-29 16:12:44+03:00

0.1687.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3773: Added bi_api connector for BigQuery  [ https://a.yandex-team.ru/arc/commit/10084729 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-28 19:19:11+03:00

0.1686.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * default value for RUN_IPTABLES_SETUP  [ https://a.yandex-team.ru/arc/commit/10084456 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-28 17:16:09+03:00

0.1685.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-28 16:32:50+03:00

0.1684.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Switched to using instances of NodeTranslation instead of subclasses  [ https://a.yandex-team.ru/arc/commit/10082845 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-28 15:35:47+03:00

0.1683.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * run iptables_setup for all cloud stands  [ https://a.yandex-team.ru/arc/commit/10081476 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3627: Separated bi_api_connector package from bi_api  [ https://a.yandex-team.ru/arc/commit/10081139 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-28 11:09:52+03:00

0.1682.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * bi_api connectors via entrypoints  [ https://a.yandex-team.ru/arc/commit/10077795 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-27 17:18:13+03:00

0.1681.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added core connectors for PostgreSQL, Greenplum and MySQL     [ https://a.yandex-team.ru/arc/commit/10076890 ]
 * Moved base schema objects to separate package bi_model_tools  [ https://a.yandex-team.ru/arc/commit/10076875 ]
 * Started separation of bi_api connector from bi_api            [ https://a.yandex-team.ru/arc/commit/10073369 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-27 14:08:02+03:00

0.1680.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-26 18:36:58+03:00

0.1679.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-26 16:28:55+03:00

0.1678.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-22 22:03:57+03:00

0.1677.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-22 16:50:26+03:00

0.1676.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-22 10:21:13+03:00

0.1675.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-21 16:31:44+03:00

0.1674.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3890: ya team usage tracking connector  [ https://a.yandex-team.ru/arc/commit/10051650 ]

* [konstasa](http://staff/konstasa)

 * BI-3784 Remove column types overrides for gsheets  [ https://a.yandex-team.ru/arc/commit/10051038 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-21 14:34:22+03:00

0.1673.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3887: Remove hack for CH join conditions  [ https://a.yandex-team.ru/arc/commit/10046893 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-20 16:26:25+03:00

0.1672.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3773: Removed direct dialect dependencies from bi_formula  [ https://a.yandex-team.ru/arc/commit/10043334 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-20 13:22:49+03:00

0.1671.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3819: usage tracking connector  [ https://a.yandex-team.ru/arc/commit/10042294 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-19 23:46:46+03:00

0.1670.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-19 20:53:21+03:00

0.1669.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3883: Keep UTC timezone for generic datetime literal values  [ https://a.yandex-team.ru/arc/commit/10039033 ]
 * CONTRIB-1748: Fix mypy after marshmallow version update         [ https://a.yandex-team.ru/arc/commit/10032098 ]
 * BI-3843: Reorganize fixtures for bi_api YDB ext tests           [ https://a.yandex-team.ru/arc/commit/10024011 ]

* [mcpn](http://staff/mcpn)

 * BI-3769: usage tracking connector  [ https://a.yandex-team.ru/arc/commit/10036382 ]

* [vgol](http://staff/vgol)

 * BI-3623: BI API: Cleanup component errors in dataset validation endpoint  [ https://a.yandex-team.ru/arc/commit/10027442 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-19 15:26:36+03:00

0.1668.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-15 12:35:54+03:00

0.1667.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3286: Handle MS SQL unable to connect to data source error  [ https://a.yandex-team.ru/arc/commit/10018289 ]
 * BI-3843: Use local YDB for bi_api ext tests when possible      [ https://a.yandex-team.ru/arc/commit/10018278 ]

* [zhukoff-pavel](http://staff/zhukoff-pavel)

 * DEVTOOLS-9690 Enable E306 rule check

Enable E306 rule check  [ https://a.yandex-team.ru/arc/commit/10015096 ]

* [konstasa](http://staff/konstasa)

 * BI-3784 Switch to aiogoogle in GSheetsClient  [ https://a.yandex-team.ru/arc/commit/10014820 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-14 23:10:26+03:00

0.1666.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3875: Do not show deprecated datetime notification for service connectors  [ https://a.yandex-team.ru/arc/commit/10014553 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-14 15:17:01+03:00

0.1665.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-13 23:52:39+03:00

0.1664.0
--------

* [thenno](http://staff/thenno)

 * BI-3837: filter functions by app type  [ https://a.yandex-team.ru/arc/commit/10009811 ]

* [konstasa](http://staff/konstasa)

 * BI-3872 Fix notification about totals in pivots with measure filter  [ https://a.yandex-team.ru/arc/commit/10009319 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-13 21:27:23+03:00

0.1663.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3845: mdb domain manager  [ https://a.yandex-team.ru/arc/commit/10001287 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-12 17:00:46+03:00

0.1662.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3867 Use local task processor in tests  [ https://a.yandex-team.ru/arc/commit/9993902 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-12 12:31:20+03:00

0.1661.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-09 19:50:55+03:00

0.1660.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3773: Separated ConnectionSQL into two classes  [ https://a.yandex-team.ru/arc/commit/9991668 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-09 17:22:45+03:00

0.1659.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-09 15:05:47+03:00

0.1658.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-08 20:26:43+03:00

0.1657.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-07 19:17:27+03:00

0.1656.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3790 GSheets CHS3 connector  [ https://a.yandex-team.ru/arc/commit/9980495 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-07 19:11:39+03:00

0.1655.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-06 15:09:36+03:00

0.1654.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-05 19:18:44+03:00

0.1653.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-05 16:25:49+03:00

0.1652.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3771: Move request timeout handling inside RequestBootstrap middleware  [ https://a.yandex-team.ru/arc/commit/9965635 ]
 * BI-3849: Exclude managed by feature fields from old datetime notification  [ https://a.yandex-team.ru/arc/commit/9965589 ]
 * BI-3842: Do not show notification in public app                            [ https://a.yandex-team.ru/arc/commit/9965585 ]
 * BI-3847: Change link in datetime notifications                             [ https://a.yandex-team.ru/arc/commit/9965572 ]

* [dmifedorov](http://staff/dmifedorov)

 * Revert "managed net settings from env"

This reverts commit 5752df0d669ff9fa1d85bb10e7c413cb96f90b42, reversing
changes made to 02c63be6a66fe290ce3a0461ae89f8bb8297dc94.  [ https://a.yandex-team.ru/arc/commit/9965427 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-05 16:18:45+03:00

0.1651.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-02 15:34:56+03:00

0.1650.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * managed net settings from env  [ https://a.yandex-team.ru/arc/commit/9956680 ]

* [gstatsenko](http://staff/gstatsenko)

 * DLHELP-4288: Unskipped test for LOD in ORDER BY                                                      [ https://a.yandex-team.ru/arc/commit/9956228 ]
 * BI-3627: Moved connection factories from global testing to the respective connector testing folders  [ https://a.yandex-team.ru/arc/commit/9950709 ]

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

[robot-statinfra](http://staff/robot-statinfra) 2022-09-02 15:11:14+03:00

0.1649.0
--------

* [thenno](http://staff/thenno)

 * BI-3829: make mypy large  [ https://a.yandex-team.ru/arc/commit/9950090 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-09-01 14:44:35+03:00

0.1648.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-09-01 11:03:08+03:00

0.1647.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-08-31 16:34:55+03:00

0.1646.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3821: Remove OLDDATETIME functions                                    [ https://a.yandex-team.ru/arc/commit/9944142 ]
 * BI-3809: Allow different datetime notifications for different app types  [ https://a.yandex-team.ru/arc/commit/9944130 ]

* [thenno](http://staff/thenno)

 * Remove trash strings

Похоже, что строки остались от кривого rebase в предыдущем пул-реквесте  [ https://a.yandex-team.ru/arc/commit/9944112 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-31 16:02:35+03:00

0.1645.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3145: add semaphore to compeng executor  [ https://a.yandex-team.ru/arc/commit/9943008 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-31 15:40:56+03:00

0.1644.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3816: use rci tenant_id as s3 file prefix in file-connector  [ https://a.yandex-team.ru/arc/commit/9935411 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-30 12:11:28+03:00

0.1643.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3756: log getting slicer in debug mode only  [ https://a.yandex-team.ru/arc/commit/9928987 ]

* [thenno](http://staff/thenno)

 * BI-3802: make task processor's redis pool per request  [ https://a.yandex-team.ru/arc/commit/9922173 ]

* [konstasa](http://staff/konstasa)

 * BI-3784 GSheets processing in file-uploader-worker: download and parse  [ https://a.yandex-team.ru/arc/commit/9919711 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-30 10:50:46+03:00

0.1642.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3397: fix cast of str/int/float/date to old datetime  [ https://a.yandex-team.ru/arc/commit/9917547 ]

* [shashkin](http://staff/shashkin)

 * BI-3751: Fix YQ tests  [ https://a.yandex-team.ru/arc/commit/9917359 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-25 16:20:16+03:00

0.1641.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3761: add link to atushka post  [ https://a.yandex-team.ru/arc/commit/9913043 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-24 19:42:33+03:00

0.1640.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3397: Remove DATETIMETZ from casts options  [ https://a.yandex-team.ru/arc/commit/9905068 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-23 15:55:25+03:00

0.1639.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-08-22 17:14:23+03:00

0.1638.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3756: a bit less logs in query slicing  [ https://a.yandex-team.ru/arc/commit/9896868 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-22 13:00:53+03:00

0.1637.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3761: Add notification about deprecated datetime  [ https://a.yandex-team.ru/arc/commit/9896420 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-22 10:44:22+03:00

0.1636.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3756: add even more logs on query slicing in debug mode, fix slicers cache  [ https://a.yandex-team.ru/arc/commit/9890049 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-19 14:24:35+03:00

0.1635.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-08-19 12:08:45+03:00

0.1634.0
--------

* [seray](http://staff/seray)

 * BI-3108 eliminating column name collisions  [ https://a.yandex-team.ru/arc/commit/9881258 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-18 10:52:07+03:00

0.1633.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3397: Allow GENERICDATETIME in casts and parameters  [ https://a.yandex-team.ru/arc/commit/9880362 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-17 18:36:05+03:00

0.1632.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3756: more logs on query slicing  [ https://a.yandex-team.ru/arc/commit/9879307 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-17 17:00:04+03:00

0.1631.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3759: some stuff for iam integration  [ https://a.yandex-team.ru/arc/commit/9879181 ]

* [konstasa](http://staff/konstasa)

 * BI-3628 Add file type diversity into DataFile; add links endpoint to file-uploader  [ https://a.yandex-team.ru/arc/commit/9879129 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-17 16:32:00+03:00

0.1630.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3397: Replace DATETIME with GENERICDATETIME in allowed type casts  [ https://a.yandex-team.ru/arc/commit/9879042 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-17 16:20:54+03:00

0.1629.0
--------

* [seray](http://staff/seray)

 * BI-3637 get raw_schema for source  [ https://a.yandex-team.ru/arc/commit/9877825 ]

* [thenno](http://staff/thenno)

 * BI-3404: add request_id to TaskProcessor  [ https://a.yandex-team.ru/arc/commit/9875189 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-17 14:14:31+03:00

0.1628.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3697: fix AppType + hide billing stuff from app + some fixes  [ https://a.yandex-team.ru/arc/commit/9871244 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-16 12:26:21+03:00

0.1627.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-08-15 16:53:22+03:00

0.1626.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3713: fix genericdatetime -> datetime downgrade  [ https://a.yandex-team.ru/arc/commit/9867177 ]
 * BI-3145: make execute_all_queries concurrent        [ https://a.yandex-team.ru/arc/commit/9866783 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-15 16:43:20+03:00

0.1625.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-08-15 13:00:12+03:00

0.1624.0
--------

* [seray](http://staff/seray)

 * BI-3564 support date cast for bitrix  [ https://a.yandex-team.ru/arc/commit/9862342 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-13 21:07:31+03:00

0.1623.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3397: Switch from DATETIME to GENERICDATETIME  [ https://a.yandex-team.ru/arc/commit/9858948 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-12 16:45:36+03:00

0.1622.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-08-12 16:06:40+03:00

0.1621.0
--------

* [thenno](http://staff/thenno)

 * BI-3741: add prlimit for rqe processes and rename QE_ prefix to RQE_  [ https://a.yandex-team.ru/arc/commit/9855066 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-12 14:25:40+03:00

0.1620.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3702: Updated row limit for pivot tables  [ https://a.yandex-team.ru/arc/commit/9852587 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-11 21:17:35+03:00

0.1619.0
--------

* [seray](http://staff/seray)

 * BI-3564 data_type fixes                         [ https://a.yandex-team.ru/arc/commit/9852216 ]
 * BI-3593 adding wrong query parameter exception  [ https://a.yandex-team.ru/arc/commit/9849501 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3701: Added maintenance tooling for pivot tables  [ https://a.yandex-team.ru/arc/commit/9847365 ]

* [konstasa](http://staff/konstasa)

 * BI-3691 Cleanup file previews on tenant deletion  [ https://a.yandex-team.ru/arc/commit/9844484 ]

* [mcpn](http://staff/mcpn)

 * BI-3713: don't update datetime to genericdatetime on refresh  [ https://a.yandex-team.ru/arc/commit/9843639 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-11 16:12:59+03:00

0.1618.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * add fields to data-v2-api  [ https://a.yandex-team.ru/arc/commit/9843586 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-10 10:35:44+03:00

0.1617.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-08-08 21:54:29+03:00

0.1616.0
--------

* [thenno](http://staff/thenno)

 * BI-3602: unify use rqe public env name  [ https://a.yandex-team.ru/arc/commit/9834060 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-08 16:49:19+03:00

0.1615.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3340: fix integer casts for postgresql  [ https://a.yandex-team.ru/arc/commit/9832074 ]

* [seray](http://staff/seray)

 * BI-3564 datetime filtration on bitrix source level  [ https://a.yandex-team.ru/arc/commit/9831465 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-08 13:03:32+03:00

0.1614.0
--------

* [thenno](http://staff/thenno)

 * BI-3602: setup iptables on public  [ https://a.yandex-team.ru/arc/commit/9827068 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3701: Moved pivot serialization to a separate class                    [ https://a.yandex-team.ru/arc/commit/9826684 ]
 * BI-3710: Added simplified totals in result to API schema                  [ https://a.yandex-team.ru/arc/commit/9826673 ]
 * Removed test splitting for mypy in bi_api                                 [ https://a.yandex-team.ru/arc/commit/9825612 ]
 * Monkeypatched field limit tests with smaller limits for faster execution  [ https://a.yandex-team.ru/arc/commit/9822875 ]

* [shashkin](http://staff/shashkin)

 * BI-3704: Remove lark parser               [ https://a.yandex-team.ru/arc/commit/9816474 ]
 * BI-3705: Fix test_create_file_conn tests  [ https://a.yandex-team.ru/arc/commit/9816471 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-05 20:00:18+03:00

0.1613.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3603: return revision_id from /fields, check it in dataset handles  [ https://a.yandex-team.ru/arc/commit/9814839 ]

* [seray](http://staff/seray)

 * BI-3682 new bitrix sources  [ https://a.yandex-team.ru/arc/commit/9814335 ]

* [shashkin](http://staff/shashkin)

 * BI-3705: Update materializer image for tests  [ https://a.yandex-team.ru/arc/commit/9812957 ]

* [gstatsenko](http://staff/gstatsenko)

 * Separated base bi_core-independent db logic from bi_core.testing.database  [ https://a.yandex-team.ru/arc/commit/9812656 ]
 * Split bi_api/tests/db into two groups                                      [ https://a.yandex-team.ru/arc/commit/9809360 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-03 19:13:05+03:00

0.1612.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-08-02 12:28:46+03:00

0.1611.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Turned off annotations for totals in pivot tables  [ https://a.yandex-team.ru/arc/commit/9801416 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-01 17:49:48+03:00

0.1610.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3249: add ERR.DS_API prefix to errors from the error_registry  [ https://a.yandex-team.ru/arc/commit/9798595 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-08-01 12:10:15+03:00

0.1609.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Switched from single target_legend_item_id to list in annotation specs  [ https://a.yandex-team.ru/arc/commit/9793589 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-29 18:48:05+03:00

0.1608.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3668: support new CH Bool type  [ https://a.yandex-team.ru/arc/commit/9793317 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-29 18:08:57+03:00

0.1607.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-07-29 12:36:58+03:00

0.1606.0
--------

* [seray](http://staff/seray)

 * BI-3534 RQE with caches  [ https://a.yandex-team.ru/arc/commit/9787402 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-28 19:01:35+03:00

0.1605.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-07-28 17:40:28+03:00

0.1604.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3676: Validate required id/title in field refs  [ https://a.yandex-team.ru/arc/commit/9783965 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-28 12:20:08+03:00

0.1603.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Minor fixes for totals and annotations in pivot tables  [ https://a.yandex-team.ru/arc/commit/9783656 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-28 12:08:16+03:00

0.1602.0
--------

* [seray](http://staff/seray)

 * BI-3534 redis caches for RQE  [ https://a.yandex-team.ru/arc/commit/9783337 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-28 11:09:59+03:00

0.1601.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3636: Change jaeger service names on testing             [ https://a.yandex-team.ru/arc/commit/9782112 ]
 * BI-3679: Update mat connection config preloading condition  [ https://a.yandex-team.ru/arc/commit/9782076 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-27 22:05:45+03:00

0.1600.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-07-27 20:30:50+03:00

0.1599.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-07-27 19:02:44+03:00

0.1598.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-07-27 18:01:39+03:00

0.1597.0
--------

* [mcpn](http://staff/mcpn)

 * BI-3555: add an exception for disable_group_by with measure fields usage  [ https://a.yandex-team.ru/arc/commit/9778194 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved dsmaker from bi_testing to bi_api_client                                                           [ https://a.yandex-team.ru/arc/commit/9769536 ]
 * Moved link collection logic to lc manager and dsrc collection specs                                      [ https://a.yandex-team.ru/arc/commit/9769232 ]
 * Added FORK_TESTS to bi_api medium tests                                                                  [ https://a.yandex-team.ru/arc/commit/9768921 ]
 * Separated pivot logic into base and custom implementations                                               [ https://a.yandex-team.ru/arc/commit/9768537 ]
 * BI-3341: Added SR as a parameter to the DataSource.get_filter method                                     [ https://a.yandex-team.ru/arc/commit/9768072 ]
 * BI-3627: Moved the rest of connections and most of the data sources to the respective connector folders  [ https://a.yandex-team.ru/arc/commit/9766611 ]

* [shashkin](http://staff/shashkin)

 * BI-3397: More stuff for generic datetimes  [ https://a.yandex-team.ru/arc/commit/9769515 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-27 13:53:51+03:00

0.1596.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3341: Removed some usages of USEntry.us_manager  [ https://a.yandex-team.ru/arc/commit/9758050 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-22 17:26:51+03:00

0.1595.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3595 Add new fields (dash id, chart kind) to reporting  [ https://a.yandex-team.ru/arc/commit/9757889 ]
 * BI-2372 Increase dataset field limit to 1200/1250          [ https://a.yandex-team.ru/arc/commit/9757884 ]

* [gstatsenko](http://staff/gstatsenko)

 * Refactored drm normalization a bit  [ https://a.yandex-team.ru/arc/commit/9755731 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-22 13:13:21+03:00

0.1594.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * DLHELP-5539: Fixed simple totals in pivot tables with Measure Names  [ https://a.yandex-team.ru/arc/commit/9754567 ]
 * BI-3653: Made SR required in USM                                     [ https://a.yandex-team.ru/arc/commit/9753654 ]
 * BI-3658: Fixed LODs with constant dimensions                         [ https://a.yandex-team.ru/arc/commit/9753514 ]

* [shashkin](http://staff/shashkin)

 * BI-3601: Change status code and default timeout for timeout middleware  [ https://a.yandex-team.ru/arc/commit/9751296 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-21 18:07:20+03:00

0.1593.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-07-20 19:19:19+03:00

0.1592.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-07-20 18:31:43+03:00

0.1591.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3341: Removed some data source editing methods from Dataset  [ https://a.yandex-team.ru/arc/commit/9746830 ]
 * BI-3627: Moved more files and classes to connector folders      [ https://a.yandex-team.ru/arc/commit/9746803 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-20 15:47:25+03:00

0.1590.0
--------

* [konstasa](http://staff/konstasa)

 * Skip supported_functions in options comparison in test  [ https://a.yandex-team.ru/arc/commit/9741937 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-20 11:47:18+03:00

0.1589.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3615: Fixed simple totals in pivot tables  [ https://a.yandex-team.ru/arc/commit/9741579 ]

* [konstasa](http://staff/konstasa)

 * BI-3599 Fix public rqe test fixture parametrization  [ https://a.yandex-team.ru/arc/commit/9736376 ]

* [shashkin](http://staff/shashkin)

 * BI-3448: Add chart notifications for statface and public chyt clique            [ https://a.yandex-team.ru/arc/commit/9736012 ]
 * BI-3397: Rename DATETIMENAIVE to GENERICDATETIME and add tz changing functions  [ https://a.yandex-team.ru/arc/commit/9735405 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-19 16:48:28+03:00

0.1588.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-07-18 10:05:00+03:00

0.1587.0
--------

* [konstasa](http://staff/konstasa)

 * Added more update caches tests; deparametrized data-api fixture cache-wise a bit  [ https://a.yandex-team.ru/arc/commit/9731928 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-17 22:56:10+03:00

0.1586.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-07-15 20:06:58+03:00

0.1585.0
--------

* [konstasa](http://staff/konstasa)

 * BI-2372 No fatal error when removing fields  [ https://a.yandex-team.ru/arc/commit/9727690 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-15 17:08:51+03:00

0.1584.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3615: Implemented data request normalization for pivot totals               [ https://a.yandex-team.ru/arc/commit/9727653 ]
 * Added labels for RQE params in bi_api test fixtures                            [ https://a.yandex-team.ru/arc/commit/9726061 ]
 * BI-3341: Removed lifecycle (save, delete, publish, etc.) methods from USEntry  [ https://a.yandex-team.ru/arc/commit/9711221 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-15 16:23:17+03:00

0.1583.0
--------

* [seray](http://staff/seray)

 * BI-3565 cache lock for bitrix  [ https://a.yandex-team.ru/arc/commit/9706539 ]

* [thenno](http://staff/thenno)

 * BI-3599: enable rqe in public  [ https://a.yandex-team.ru/arc/commit/9706191 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-12 13:36:09+03:00

0.1582.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3598 Use DateTime64 in the FileTypeTransformer  [ https://a.yandex-team.ru/arc/commit/9702462 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-11 18:49:00+03:00

0.1581.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3311: Load mat connection config on dataset prepare stage only for materialized datasets  [ https://a.yandex-team.ru/arc/commit/9693666 ]

* [gstatsenko](http://staff/gstatsenko)

 * CHARTS-6038: Added title customization to pivot API  [ https://a.yandex-team.ru/arc/commit/9692914 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-08 18:29:56+03:00

0.1580.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved some tree testing logic to dsmaker.tree_utils  [ https://a.yandex-team.ru/arc/commit/9681980 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-06 20:07:03+03:00

0.1579.0
--------

* [thenno](http://staff/thenno)

 * BI-3599: don't repeat requests for datasets in crawler  [ https://a.yandex-team.ru/arc/commit/9676882 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-05 16:36:15+03:00

0.1578.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-07-04 17:28:51+03:00

0.1577.0
--------

* [thenno](http://staff/thenno)

 * BI-3599: set timeout for mypy tests                [ https://a.yandex-team.ru/arc/commit/9671064 ]
 * BI-3599: set direct mode to materialized datasets  [ https://a.yandex-team.ru/arc/commit/9669932 ]

* [shashkin](http://staff/shashkin)

 * BI-3448: Add chart notifications for removed totals  [ https://a.yandex-team.ru/arc/commit/9668931 ]
 * BI-3397: Introduce naive datetime type               [ https://a.yandex-team.ru/arc/commit/9668851 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-04 17:22:52+03:00

0.1576.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-07-01 17:06:11+03:00

0.1575.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Reorganization of some connector files  [ https://a.yandex-team.ru/arc/commit/9661060 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-01 14:19:26+03:00

0.1574.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Switched remaining legacy tests to new pivot API                     [ https://a.yandex-team.ru/arc/commit/9660538 ]
 * BI-3573: Fixed pivot table when totals and measure filters are used  [ https://a.yandex-team.ru/arc/commit/9660478 ]

* [asnytin](http://staff/asnytin)

 * BI-3588: file-conn prod settings  [ https://a.yandex-team.ru/arc/commit/9660506 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-07-01 13:06:15+03:00

0.1573.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3544: Add permissions to dataset data  [ https://a.yandex-team.ru/arc/commit/9649327 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-30 11:02:06+03:00

0.1572.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3573: A couple of v2/pivot API fixes                                 [ https://a.yandex-team.ru/arc/commit/9646350 ]
 * BI-2879: Added deduplication of pivot dimensions in multi-block tables  [ https://a.yandex-team.ru/arc/commit/9646339 ]
 * BI-3387: Added headers for row dimensions                               [ https://a.yandex-team.ru/arc/commit/9643405 ]

* [shashkin](http://staff/shashkin)

 * BI-3459: Log formula handling errors  [ https://a.yandex-team.ru/arc/commit/9645613 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-28 19:45:43+03:00

0.1571.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-27 16:49:25+03:00

0.1570.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-27 16:39:20+03:00

0.1569.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3539 Validate file sources before modifying connection  [ https://a.yandex-team.ru/arc/commit/9632926 ]
 * BI-3519 Cleanup files in s3 on org/folder deletion         [ https://a.yandex-team.ru/arc/commit/9632918 ]

* [gstatsenko](http://staff/gstatsenko)

 * Add optimization of filters that are always true  [ https://a.yandex-team.ru/arc/commit/9629825 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-27 12:07:06+03:00

0.1568.0
--------

* [seray](http://staff/seray)

 * BI-3563 new bitrix sources  [ https://a.yandex-team.ru/arc/commit/9627722 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-2879: Minor fix for pivot API scehmas                 [ https://a.yandex-team.ru/arc/commit/9627271 ]
 * BI-2879: First iteration of (sub)totals in pivot tables  [ https://a.yandex-team.ru/arc/commit/9625773 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-24 10:26:38+03:00

0.1567.0
--------

* [konstasa](http://staff/konstasa)

 * BI-2372 Result schema validation env flag; skip validation in data-api  [ https://a.yandex-team.ru/arc/commit/9625500 ]

* [asnytin](http://staff/asnytin)

 * BI-3412: DataCloudEnvManagerFactory  [ https://a.yandex-team.ru/arc/commit/9624080 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-23 13:22:51+03:00

0.1566.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3558 Market Couriers connector  [ https://a.yandex-team.ru/arc/commit/9620826 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-22 15:49:34+03:00

0.1565.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-22 11:03:28+03:00

0.1564.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3502 Remove conditions related to preview_id backward compatibility after migration  [ https://a.yandex-team.ru/arc/commit/9616934 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-21 19:36:09+03:00

0.1563.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3489: Third iteration of new pivot API  [ https://a.yandex-team.ru/arc/commit/9610917 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-21 13:02:25+03:00

0.1562.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3502 Update SaveSourceTask to handle replaced sources properly  [ https://a.yandex-team.ru/arc/commit/9608414 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3341: Removed most dataset.us_manager usages  [ https://a.yandex-team.ru/arc/commit/9602591 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-20 13:16:25+03:00

0.1561.0
--------

* [konstasa](http://staff/konstasa)

 * BI-2372: Dataset field limit  [ https://a.yandex-team.ru/arc/commit/9596180 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-16 15:59:16+03:00

0.1560.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-15 19:08:41+03:00

0.1559.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3554: Better handling of invalid roles                                                                    [ https://a.yandex-team.ru/arc/commit/9591517 ]
 * BI-3425: Added a failing test that reproduces the issue from BI-3425; migrated all LOD tests to data api v2  [ https://a.yandex-team.ru/arc/commit/9591505 ]
 * BI-3489: Second iteration of new pivot API                                                                   [ https://a.yandex-team.ru/arc/commit/9588428 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-15 18:09:04+03:00

0.1558.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-14 15:02:19+03:00

0.1557.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-10 20:52:04+03:00

0.1556.0
--------

* [thenno](http://staff/thenno)

 * BI-3446: fix postprocessing for geocache  [ https://a.yandex-team.ru/arc/commit/9575652 ]

* [asnytin](http://staff/asnytin)

 * BI-3412: fetch iam token for SA in yc-auth  [ https://a.yandex-team.ru/arc/commit/9575551 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-10 13:46:26+03:00

0.1555.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3341: Removed data source interfaces from connections                                                    [ https://a.yandex-team.ru/arc/commit/9572243 ]
 * BI-3489: First iteration of new pivot API                                                                   [ https://a.yandex-team.ru/arc/commit/9553062 ]
 * BI-3341: Removed some more source-related methods from Dataset; removed all us_manager usages from Dataset  [ https://a.yandex-team.ru/arc/commit/9552976 ]

* [konstasa](http://staff/konstasa)

 * BI-3502 Clearer api schema  [ https://a.yandex-team.ru/arc/commit/9562454 ]

* [thenno](http://staff/thenno)

 * BI-3446: disable logs in geo postprocessing  [ https://a.yandex-team.ru/arc/commit/9550913 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-10 02:08:21+03:00

0.1554.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3412: redis ssl settings  [ https://a.yandex-team.ru/arc/commit/9547428 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-04 14:15:42+03:00

0.1553.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-03 23:13:11+03:00

0.1552.0
--------

* [thenno](http://staff/thenno)

 * BI-3446: add logs to geo postprocessing  [ https://a.yandex-team.ru/arc/commit/9544992 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-03 16:44:14+03:00

0.1551.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-03 16:19:08+03:00

0.1550.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3502: File conn: Use preview_id instead of source_id to manage previews; Source replacement  [ https://a.yandex-team.ru/arc/commit/9544700 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-03 16:16:31+03:00

0.1549.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3395: reverted csrf forwarding hacks  [ https://a.yandex-team.ru/arc/commit/9542594 ]

* [konstasa](http://staff/konstasa)

 * BI-3090 Make mutation cache ttl configurable from env  [ https://a.yandex-team.ru/arc/commit/9540895 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-03 13:04:31+03:00

0.1548.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-02 22:21:29+03:00

0.1547.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-06-02 17:33:25+03:00

0.1546.0
--------

* [seray](http://staff/seray)

 * BI-3218 editable portal param for bitrix  [ https://a.yandex-team.ru/arc/commit/9537872 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-02 14:53:37+03:00

0.1545.0
--------

* [seray](http://staff/seray)

 * BI-3474 caches for bitrix connection  [ https://a.yandex-team.ru/arc/commit/9534760 ]

* [thenno](http://staff/thenno)

 * BI-3446: geocode and geoinfo should always return null  [ https://a.yandex-team.ru/arc/commit/9534262 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-06-02 11:01:31+03:00

0.1544.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * fixes for default-sa-from-env  [ https://a.yandex-team.ru/arc/commit/9527722 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-31 19:18:57+03:00

0.1543.0
--------

* [seray](http://staff/seray)

 * BI-3218 rename bitrix_gds to bitrix24  [ https://a.yandex-team.ru/arc/commit/9523990 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-31 14:20:18+03:00

0.1542.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3412: use session-service in async yc_auth middleware (for cookies resolving)  [ https://a.yandex-team.ru/arc/commit/9518631 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-30 13:44:22+03:00

0.1541.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3335: deault sa token from serv registry  [ https://a.yandex-team.ru/arc/commit/9518123 ]
 * introducing unknown connections              [ https://a.yandex-team.ru/arc/commit/9518107 ]

* [shadchin](http://staff/shadchin)

 * Put flask-marshmallow under yamaker pypi  [ https://a.yandex-team.ru/arc/commit/9517716 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3489: Separated pivot legend from the main legend   [ https://a.yandex-team.ru/arc/commit/9513316 ]
 * Added restarting of MSSQL in bi_core and bi_api tests  [ https://a.yandex-team.ru/arc/commit/9511032 ]

* [asnytin](http://staff/asnytin)

 * BI-3412: file-uploader cloud auth settings  [ https://a.yandex-team.ru/arc/commit/9509385 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-30 12:27:09+03:00

0.1540.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-27 01:43:19+03:00

0.1539.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-26 17:13:29+03:00

0.1538.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-26 15:29:02+03:00

0.1537.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3488 BI-3090: Fix mutation caches date[time] issue + enable proper env trigger  [ https://a.yandex-team.ru/arc/commit/9500055 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-25 16:44:58+03:00

0.1536.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-25 14:51:07+03:00

0.1535.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3395: file-uploader: forward csrf header to fu-api  [ https://a.yandex-team.ru/arc/commit/9495230 ]
 * BI-3499: connectors_data refactoring                   [ https://a.yandex-team.ru/arc/commit/9492671 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3341: Removed some more source-related methods from Dataset  [ https://a.yandex-team.ru/arc/commit/9494997 ]
 * BI-3503: Fixed data source merging                              [ https://a.yandex-team.ru/arc/commit/9487834 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-24 17:58:13+03:00

0.1534.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-23 13:51:06+03:00

0.1533.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3500: Fix compiling MySQL query for execution  [ https://a.yandex-team.ru/arc/commit/9487387 ]

* [asnytin](http://staff/asnytin)

 * BI-3395: fill file conn raw_schema before save tasks scheduling  [ https://a.yandex-team.ru/arc/commit/9487322 ]

* [gstatsenko](http://staff/gstatsenko)

 * DLHELP-5035: Added missing value for limit in DistinctDataRequestV2Schema                           [ https://a.yandex-team.ru/arc/commit/9478416 ]
 * BI-3423: Removed workarounbd for global ORDER BY in v2 requests with totals                         [ https://a.yandex-team.ru/arc/commit/9476055 ]
 * Removed support of the tree role for arrays                                                         [ https://a.yandex-team.ru/arc/commit/9476049 ]
 * BI-3489: Moved pivot legend validation to formalizer                                                [ https://a.yandex-team.ru/arc/commit/9475984 ]
 * BI-3489: refactored pivot table sorting, i.e. added SortValueStrategy and SortValueNormalizer APIs  [ https://a.yandex-team.ru/arc/commit/9475976 ]
 * BI-3341: Removed get_data_source_list from Dataset; renamed delete_obligatory_filter                [ https://a.yandex-team.ru/arc/commit/9475970 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-23 13:37:51+03:00

0.1532.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-19 13:08:00+03:00

0.1531.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-18 13:21:25+03:00

0.1530.0
--------

* [seray](http://staff/seray)

 * BI-3482 all tables in one connection  [ https://a.yandex-team.ru/arc/commit/9468628 ]

* [kchupin](http://staff/kchupin)

 * [BI-3342] Fix tier-1 DoubleCloud tests in bi-api  [ https://a.yandex-team.ru/arc/commit/9465006 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-18 12:51:01+03:00

0.1529.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3481: Sub-total support                                                                 [ https://a.yandex-team.ru/arc/commit/9459341 ]
 * BI-3341: More strict source-fetching methods in data source collections                    [ https://a.yandex-team.ru/arc/commit/9458747 ]
 * BI-3341: Removed get_db_info method from dataset                                           [ https://a.yandex-team.ru/arc/commit/9450382 ]
 * BI-3341: Moved relation and obligatory filter methods from dataset to accessor and editor  [ https://a.yandex-team.ru/arc/commit/9442640 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-17 16:11:54+03:00

0.1528.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3179 BI-3154: delete logs-api and uploads  [ https://a.yandex-team.ru/arc/commit/9434418 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-11 12:59:45+03:00

0.1527.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-06 18:36:29+03:00

0.1526.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-05-06 17:44:35+03:00

0.1525.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2585: Ignoring totals if measure filters are present  [ https://a.yandex-team.ru/arc/commit/9432276 ]
 * BI-3454: Added tests for pagination                      [ https://a.yandex-team.ru/arc/commit/9432271 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-06 13:17:40+03:00

0.1524.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3345: SMB AutoHints connector  [ https://a.yandex-team.ru/arc/commit/9432082 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-06 13:00:13+03:00

0.1523.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Several typing fixes in validator                               [ https://a.yandex-team.ru/arc/commit/9422929 ]
 * BI-3191: Supported CONTAINS and NOTCONTAINS filters for arrays  [ https://a.yandex-team.ru/arc/commit/9415357 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-05-05 15:42:53+03:00

0.1522.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * fix mutations caching enabled by default  [ https://a.yandex-team.ru/arc/commit/9406805 ]

* [gstatsenko](http://staff/gstatsenko)

 * Seperated legend item obj spec as a nested member  [ https://a.yandex-team.ru/arc/commit/9406297 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-27 20:57:59+03:00

0.1521.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3393: use x-dl-tenantid header in backend components interaction  [ https://a.yandex-team.ru/arc/commit/9404983 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3341: Removed all data source interfaces from DatasetComponentAccessor  [ https://a.yandex-team.ru/arc/commit/9402958 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-27 16:08:06+03:00

0.1520.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3454: Fixed single block pagination optimization  [ https://a.yandex-team.ru/arc/commit/9400972 ]

* [thenno](http://staff/thenno)

 * BI-3428: move install-python parts to local dev  [ https://a.yandex-team.ru/arc/commit/9398853 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-26 23:48:56+03:00

0.1519.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-22 18:46:30+03:00

0.1518.0
--------

* [quantum-0](http://staff/quantum-0)

 * BI-3417: Add exception: already exists  [ https://a.yandex-team.ru/arc/commit/9387887 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-22 18:28:23+03:00

0.1517.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-22 13:05:19+03:00

0.1516.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3410: Prioritize formula over guid_formula in updates  [ https://a.yandex-team.ru/arc/commit/9383680 ]

* [konstasa](http://staff/konstasa)

 * BI-3396 S3 file deletion task  [ https://a.yandex-team.ru/arc/commit/9383282 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-22 12:13:11+03:00

0.1515.0
--------

* [quantum-0](http://staff/quantum-0)

 * BI-3398: Add exception CHYT Auth Error  [ https://a.yandex-team.ru/arc/commit/9383024 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-21 22:35:58+03:00

0.1514.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3423: Added a workaround that ignores ORDER BY in (sub)total blocks           [ https://a.yandex-team.ru/arc/commit/9378814 ]
 * Removed handling of legacy overflow error in pivot, skipped test_pivot_overflow  [ https://a.yandex-team.ru/arc/commit/9369587 ]

* [seray](http://staff/seray)

 * BI-3218 using COMPENG dialect for Bitrix filter operations  [ https://a.yandex-team.ru/arc/commit/9378192 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-21 12:10:34+03:00

0.1513.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3410: Allow both formula and guid_formula in updates  [ https://a.yandex-team.ru/arc/commit/9367968 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-19 12:45:23+03:00

0.1512.0
--------

* [quantum-0](http://staff/quantum-0)

 * BI-3390: Add exeption clique is not running  [ https://a.yandex-team.ru/arc/commit/9367847 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-19 12:32:06+03:00

0.1511.0
--------

* [seray](http://staff/seray)

 * BI-3218 select columns right way  [ https://a.yandex-team.ru/arc/commit/9364083 ]

* [thenno](http://staff/thenno)

 * DEVTOOLSSUPPORT-18100: increase memlimit in bi_api/tests/unit  [ https://a.yandex-team.ru/arc/commit/9363413 ]

* [gstatsenko](http://staff/gstatsenko)

 * Simplify generation of totals  [ https://a.yandex-team.ru/arc/commit/9362175 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-18 16:26:18+03:00

0.1510.0
--------

* [quantum-0](http://staff/quantum-0)

 * HOTFIX: Remove "no nulls" check  [ https://a.yandex-team.ru/arc/commit/9358709 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-15 20:54:41+03:00

0.1509.0
--------

* [quantum-0](http://staff/quantum-0)

 * BI-3090: Refactor mutation cache tests  [ https://a.yandex-team.ru/arc/commit/9358315 ]
 * BI-3385: Refactoring updates schemas    [ https://a.yandex-team.ru/arc/commit/9356427 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-15 19:32:02+03:00

0.1508.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-14 17:45:01+03:00

0.1507.0
--------

* [seray](http://staff/seray)

 * BI-3310 default allow_subselect, raw_sql_level for promql and monitoring  [ https://a.yandex-team.ru/arc/commit/9349297 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3251: Implemented template formatting for sub-totals                       [ https://a.yandex-team.ru/arc/commit/9346274 ]
 * Implement pagination as two separate stages of the query processing pipeline  [ https://a.yandex-team.ru/arc/commit/9346267 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-14 11:16:42+03:00

0.1506.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3099: Hide db query from user  [ https://a.yandex-team.ru/arc/commit/9344574 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3341: Removed most usages of data source fetching from DatasetComponentAccessor  [ https://a.yandex-team.ru/arc/commit/9341487 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-13 15:35:04+03:00

0.1505.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3342: check datalens.objects.read in doublecloud  [ https://a.yandex-team.ru/arc/commit/9340389 ]

* [shashkin](http://staff/shashkin)

 * BI-3207: Add guid_formula  [ https://a.yandex-team.ru/arc/commit/9335165 ]

* [quantum-0](http://staff/quantum-0)

 * Revert checking edit in password or token in test connection  [ https://a.yandex-team.ru/arc/commit/9332920 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-12 14:48:31+03:00

0.1504.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-08 17:38:16+03:00

0.1503.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3343: Turned on LOD grouping with smarter compatibility critera  [ https://a.yandex-team.ru/arc/commit/9328251 ]
 * BI-3355: Implemnted wrapping of tree fields during substitution     [ https://a.yandex-team.ru/arc/commit/9328244 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-08 16:25:42+03:00

0.1502.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-08 15:18:11+03:00

0.1501.0
--------

* [quantum-0](http://staff/quantum-0)

 * BI-3289: Step 4: Schematization updates  [ https://a.yandex-team.ru/arc/commit/9327582 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-08 15:02:46+03:00

0.1500.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3099: close session in file uploader client  [ https://a.yandex-team.ru/arc/commit/9326121 ]

* [gstatsenko](http://staff/gstatsenko)

 * Fixed limit in v2 for explicitly specified blocks  [ https://a.yandex-team.ru/arc/commit/9325815 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-08 11:09:55+03:00

0.1499.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3099: Bad Request when file not ready v2  [ https://a.yandex-team.ru/arc/commit/9325180 ]

* [quantum-0](http://staff/quantum-0)

 * BI-3317: Don't check edit permission in test connection  [ https://a.yandex-team.ru/arc/commit/9324650 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-08 00:46:27+03:00

0.1498.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Refactored block formalizer a little  [ https://a.yandex-team.ru/arc/commit/9322483 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-07 18:15:16+03:00

0.1497.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3099: Bad Request when file is not ready  [ https://a.yandex-team.ru/arc/commit/9321926 ]

* [thenno](http://staff/thenno)

 * BI-3346: move profiling and ylog from bi_core to bi_app_tools  [ https://a.yandex-team.ru/arc/commit/9321275 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-07 13:52:51+03:00

0.1496.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed limit and offset for multi-block queries  [ https://a.yandex-team.ru/arc/commit/9321229 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-07 12:08:02+03:00

0.1495.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Switch to level-1-oriented tree roots  [ https://a.yandex-team.ru/arc/commit/9319761 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-06 21:53:29+03:00

0.1494.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3099: file uploader preview fetch auth fix  [ https://a.yandex-team.ru/arc/commit/9318932 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-06 18:34:06+03:00

0.1493.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3099: file conn preview fetch fix  [ https://a.yandex-team.ru/arc/commit/9318125 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-06 16:52:40+03:00

0.1492.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3099: file connection preview in bi_api  [ https://a.yandex-team.ru/arc/commit/9317225 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-06 14:34:08+03:00

0.1491.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3099: Fixed distincts with file connection                          [ https://a.yandex-team.ru/arc/commit/9316211 ]
 * BI-3099: Revert validator change and fix dataset source addition test  [ https://a.yandex-team.ru/arc/commit/9314430 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3191: Added array filters to options; implemented LENNE array filter  [ https://a.yandex-team.ru/arc/commit/9314747 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-06 12:30:05+03:00

0.1490.0
--------

* [konstasa](http://staff/konstasa)

 * BI-3099 File connection dataset creation  [ https://a.yandex-team.ru/arc/commit/9311327 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-05 10:58:57+03:00

0.1489.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-05 10:21:53+03:00

0.1488.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3173: Added error for measure filters in distincts and ranges  [ https://a.yandex-team.ru/arc/commit/9308925 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 21:26:14+03:00

0.1487.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3150: task processor task scheduling from bi_api  [ https://a.yandex-team.ru/arc/commit/9306288 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-04 10:57:27+03:00

0.1486.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-04-02 10:46:11+03:00

0.1485.0
--------

* [seray](http://staff/seray)

 * BI-3218 bitrix connector with alchemy  [ https://a.yandex-team.ru/arc/commit/9302672 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-01 23:48:22+03:00

0.1484.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3150: file uploader source save task - part 1  [ https://a.yandex-team.ru/arc/commit/9301170 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-04-01 17:00:13+03:00

0.1483.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-03-31 23:34:45+03:00

0.1482.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-03-31 22:51:44+03:00

0.1481.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3315: Fixed empty query handling for measureless totals  [ https://a.yandex-team.ru/arc/commit/9291694 ]
 * BI-2962: Minor fix for tree spec  schema                    [ https://a.yandex-team.ru/arc/commit/9291212 ]
 * BI-2962: Added basic validation of tree role specs          [ https://a.yandex-team.ru/arc/commit/9291201 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-30 17:17:05+03:00

0.1480.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3099: bi_api file connection update fix             [ https://a.yandex-team.ru/arc/commit/9291080 ]
 * BI-3099: file uploader file/source status API handler  [ https://a.yandex-team.ru/arc/commit/9289220 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-30 15:40:55+03:00

0.1479.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2962: Added full support for order_by in tree queries              [ https://a.yandex-team.ru/arc/commit/9289070 ]
 * BI-3315: Fixed block placement for the case of template-only queries  [ https://a.yandex-team.ru/arc/commit/9288810 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-30 10:37:09+03:00

0.1478.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added block debug info to data api v2 responses  [ https://a.yandex-team.ru/arc/commit/9286671 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-29 18:27:31+03:00

0.1477.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Renamed several modules  [ https://a.yandex-team.ru/arc/commit/9285206 ]

* [shashkin](http://staff/shashkin)

 * BI-3312: Exclude parameter fields from dataset preview  [ https://a.yandex-team.ru/arc/commit/9285065 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-3230: alb rewrites + update internal configs for new fqdns  [ https://a.yandex-team.ru/arc/commit/9283224 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-29 14:12:42+03:00

0.1476.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3304: add calc_mode to /fields  [ https://a.yandex-team.ru/arc/commit/9282302 ]

* [quantum-0](http://staff/quantum-0)

 * BI-3090: Fixes mutation cache: Enabling/Disabling, disable cache in preview and fix tests

BI-3090: turning off mutations cache in app start

BI-3090: Disable mutations for preview

BI-3090: Optional enabling mutations for prepare_dataset_for_request  [ https://a.yandex-team.ru/arc/commit/9282299 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-28 18:29:30+03:00

0.1475.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2962: Many fixes and first test for trees  [ https://a.yandex-team.ru/arc/commit/9281587 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-28 16:46:55+03:00

0.1474.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3099: file connector in bi_api  [ https://a.yandex-team.ru/arc/commit/9280314 ]

* [quantum-0](http://staff/quantum-0)

 * BI-3292: Fix mutation cache: saving dataset after loading from cache  [ https://a.yandex-team.ru/arc/commit/9275980 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-2962: Added handling of tree role in select wrapper compilation  [ https://a.yandex-team.ru/arc/commit/9274760 ]
 * BI-2962: Added auto-generation of filters for trees                 [ https://a.yandex-team.ru/arc/commit/9274151 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-28 13:22:43+03:00

0.1473.0
--------

* [quantum-0](http://staff/quantum-0)

 * BI-3090 Hotfix: dont log CacheInitializationError as an error  [ https://a.yandex-team.ru/arc/commit/9269044 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-24 14:43:06+03:00

0.1472.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2962: Transformed select_type into select wrapper spec  [ https://a.yandex-team.ru/arc/commit/9267362 ]
 * BI-2962: Added TREE function                               [ https://a.yandex-team.ru/arc/commit/9267360 ]
 * BI-3191: Added support for STARTSWITH array filter         [ https://a.yandex-team.ru/arc/commit/9266110 ]
 * BI-2962, BI-3251: Fixed legend item response schema        [ https://a.yandex-team.ru/arc/commit/9265515 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-24 12:58:49+03:00

0.1471.0
--------

* [quantum-0](http://staff/quantum-0)

 * BI-3090 HOTFIX disabling caches  [ https://a.yandex-team.ru/arc/commit/9264958 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-23 17:25:32+03:00

0.1470.0
--------

* [shashkin](http://staff/shashkin)

 * Fix missing parameter value constraint

https://sentry.stat.yandex-team.ru/share/issue/4790ef09f0b047348c7fced47a407bc3/  [ https://a.yandex-team.ru/arc/commit/9262890 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-2962: Added Tree role specs                [ https://a.yandex-team.ru/arc/commit/9261037 ]
 * BI-2961: Implemented AfterBinaryStreamMerger  [ https://a.yandex-team.ru/arc/commit/9260930 ]

* [quantum-0](http://staff/quantum-0)

 * BI-3090: Step 3: MutationCacheFactory + Redis Integration  [ https://a.yandex-team.ru/arc/commit/9258642 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-23 10:58:20+03:00

0.1469.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2961: Implemented resolution of template legend items in query postprocessor  [ https://a.yandex-team.ru/arc/commit/9257452 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-21 19:58:46+03:00

0.1468.0
--------

* [shashkin](http://staff/shashkin)

 * Allow missing calc mode parameters  [ https://a.yandex-team.ru/arc/commit/9257268 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-21 19:22:36+03:00

0.1467.0
--------

* [quantum-0](http://staff/quantum-0)

 * BI-2258: Fix folder_id -> tenant_id in bi_api ext tests  [ https://a.yandex-team.ru/arc/commit/9256665 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3251: Implemented automatic generation of block placement for blocks containing totals            [ https://a.yandex-team.ru/arc/commit/9256507 ]
 * BI-3191: Implemented basic array filters                                                             [ https://a.yandex-team.ru/arc/commit/9255181 ]
 * BI-2961: Implemented the BinaryStreamMerger interface for merging data from multiple blocks/queries  [ https://a.yandex-team.ru/arc/commit/9254475 ]
 * BI-3251: Partial implementation of totals in data API v2                                             [ https://a.yandex-team.ru/arc/commit/9254472 ]

* [thenno](http://staff/thenno)

 * BI-3282: validate formula field for calc_mode=formula  [ https://a.yandex-team.ru/arc/commit/9251930 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-21 17:45:15+03:00

0.1466.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added empty string defaults for formula and source attributes of fields in serialization for backwards compatibility  [ https://a.yandex-team.ru/arc/commit/9242376 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-17 12:12:49+03:00

0.1465.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-03-14 12:46:19+03:00

0.1464.0
--------

* [seray](http://staff/seray)

 * [BI-3062] using Monitoring host from config  [ https://a.yandex-team.ru/arc/commit/9224577 ]

* [konstasa](http://staff/konstasa)

 * [BI-3245] Combine req id and error handling middlewares in the RequestBootstrap and log ERR_CODE  [ https://a.yandex-team.ru/arc/commit/9224180 ]

* [shashkin](http://staff/shashkin)

 * BI-3259: Add CalculationSpec into BIField

WIP

Implemented CalculationSpec inside BIField  [ https://a.yandex-team.ru/arc/commit/9223724 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-2961: Refactored QuerySpecFormalizer to use block_spec as input  [ https://a.yandex-team.ru/arc/commit/9220315 ]
 * BI-2961: More metadata in block specs for data api v2               [ https://a.yandex-team.ru/arc/commit/9214410 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-11 18:47:49+03:00

0.1463.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-03-05 19:20:27+03:00

0.1462.0
--------

* [seray](http://staff/seray)

 * BI-3062 bi_api cloud Monitoring connector  [ https://a.yandex-team.ru/arc/commit/9207924 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-05 16:06:22+03:00

0.1461.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * tls for prod yq  [ https://a.yandex-team.ru/arc/commit/9203171 ]

* [shashkin](http://staff/shashkin)

 * BI-3216: Allow comparison for MARKUP        [ https://a.yandex-team.ru/arc/commit/9199978 ]
 * BI-3255: Restrict possible parameter types  [ https://a.yandex-team.ru/arc/commit/9196377 ]

* [quantum-0](http://staff/quantum-0)

 * BI-3090: Step 2: InMemory Cache Engine & MutationCache

BI-3090: ServiceRegistry, factory and cache  [ https://a.yandex-team.ru/arc/commit/9199016 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-04 21:20:40+03:00

0.1460.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2961: Added data stream merger; fixed per-line legends in data api v2  [ https://a.yandex-team.ru/arc/commit/9192737 ]
 * Added more wrapping of FormulaError into FormulaHandlingError             [ https://a.yandex-team.ru/arc/commit/9191376 ]
 * Added initial implementation of DatasetComponentEditor                    [ https://a.yandex-team.ru/arc/commit/9182439 ]
 * Added legend item ids to block meta                                       [ https://a.yandex-team.ru/arc/commit/9182058 ]
 * BI-2961: Implemented data api v1.5 and v2 endpoints                       [ https://a.yandex-team.ru/arc/commit/9178279 ]

* [thenno](http://staff/thenno)

 * BI-2840: remove async postgres flag  [ https://a.yandex-team.ru/arc/commit/9187686 ]

* [shashkin](http://staff/shashkin)

 * BI-2993: Always use async MySQL adapter  [ https://a.yandex-team.ru/arc/commit/9179320 ]

* [quantum-0](http://staff/quantum-0)

 * BI-3090: Mutations serialization of dataset updates  [ https://a.yandex-team.ru/arc/commit/9177986 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-03-01 18:06:47+03:00

0.1459.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2961: Added endpoint stubs for data API v1.5 and v2  [ https://a.yandex-team.ru/arc/commit/9167479 ]

* [asnytin](http://staff/asnytin)

 * BI-3148: commonize redis settings  [ https://a.yandex-team.ru/arc/commit/9162639 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-22 16:26:04+03:00

0.1458.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-3226] Initial adding of charts & dashboards to workbook context  [ https://a.yandex-team.ru/arc/commit/9161373 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-18 16:59:39+03:00

0.1457.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2961: pre-v2 refactoring of the range endpoint    [ https://a.yandex-team.ru/arc/commit/9159653 ]
 * Moved get_root_avatar to DatasetComponentAccessor    [ https://a.yandex-team.ru/arc/commit/9159530 ]
 * Fixed restarting of oracle in tests                  [ https://a.yandex-team.ru/arc/commit/9159518 ]
 * BI-2962: Added OB, filters and parameters to legend  [ https://a.yandex-team.ru/arc/commit/9156091 ]

* [shashkin](http://staff/shashkin)

 * BI-3239: Reply with 400 code instead of 500 in DashSQL  [ https://a.yandex-team.ru/arc/commit/9159547 ]
 * BI-3236: Change YDB endpoint for ext tests              [ https://a.yandex-team.ru/arc/commit/9159541 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-18 12:58:19+03:00

0.1456.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * CHARTS-5386: Added automatic avatar selection for update_field of direct fields without avatar  [ https://a.yandex-team.ru/arc/commit/9155192 ]
 * BI-2961: Implemented the final version of the v2 format                                         [ https://a.yandex-team.ru/arc/commit/9154773 ]
 * Automatic oracle restart                                                                        [ https://a.yandex-team.ru/arc/commit/9153362 ]

* [shashkin](http://staff/shashkin)

 * Fix most typing in bi_api/bi/schemas  [ https://a.yandex-team.ru/arc/commit/9153408 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-17 12:27:27+03:00

0.1455.0
--------

* [shashkin](http://staff/shashkin)

 * Fix most typing in bi_api/bi/utils  [ https://a.yandex-team.ru/arc/commit/9152327 ]
 * Pass app version to sentry          [ https://a.yandex-team.ru/arc/commit/9150066 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added DbDispenser                                          [ https://a.yandex-team.ru/arc/commit/9149763 ]
 * Implemented the first version of DatasetComponentAccessor  [ https://a.yandex-team.ru/arc/commit/9149732 ]

* [asnytin](http://staff/asnytin)

 * BI-3148: bi_file_uploader app settings  [ https://a.yandex-team.ru/arc/commit/9145830 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-16 18:51:37+03:00

0.1454.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3221: Fix loading non-parameter fields on result schema  [ https://a.yandex-team.ru/arc/commit/9144776 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-15 10:40:09+03:00

0.1453.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added descriptions for BIField attributes, removed its direct instantiation, replaced new with make  [ https://a.yandex-team.ru/arc/commit/9134844 ]
 * Removed unused query union primitives                                                                [ https://a.yandex-team.ru/arc/commit/9133288 ]

* [asnytin](http://staff/asnytin)

 * BI-3148: bi_file_uploader app settings  [ https://a.yandex-team.ru/arc/commit/9134695 ]

* [seray](http://staff/seray)

 * [BI-3197] validate new field_id on update

[RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/9134456 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-14 13:05:40+03:00

0.1452.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2962: Added tree str data type  [ https://a.yandex-team.ru/arc/commit/9123352 ]
 * Added more multi-api tests         [ https://a.yandex-team.ru/arc/commit/9123324 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-10 15:20:54+03:00

0.1451.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3179: disable logs api  [ https://a.yandex-team.ru/arc/commit/9121913 ]

* [gstatsenko](http://staff/gstatsenko)

 * DLHELP-4288: Added (skipped) test for LOD in ORDER BY; updated dashsql test queries for compatibility with CH 22.1  [ https://a.yandex-team.ru/arc/commit/9121446 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-08 20:22:24+03:00

0.1450.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed totals for data api v1.5  [ https://a.yandex-team.ru/arc/commit/9119058 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-08 12:56:05+03:00

0.1449.0
--------

* [seray](http://staff/seray)

 * [BI-3202] generating single schema for solomon response

[RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/9117007 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-2961: Switched to arbitrary formula expressions in filter definitions  [ https://a.yandex-team.ru/arc/commit/9115541 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-08 10:08:35+03:00

0.1448.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-02-07 15:00:05+03:00

0.1447.0
--------

* [seray](http://staff/seray)

 * [BI-3200] alias field in timeseries

[RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/9113662 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-07 11:04:32+03:00

0.1446.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Implemented collapsing of double aggregations  [ https://a.yandex-team.ru/arc/commit/9107660 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-04 22:25:22+03:00

0.1445.0
--------

* [seray](http://staff/seray)

 * [BI-3192] setting field id generator by env variable  [ https://a.yandex-team.ru/arc/commit/9106916 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-04 10:47:39+03:00

0.1444.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3094: Add test and some fixes for parameter fields in data api v2  [ https://a.yandex-team.ru/arc/commit/9100313 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-03 13:54:22+03:00

0.1443.0
--------

* [seray](http://staff/seray)

 * [BI-3036] testing update field_id usages

[RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/9098800 ]

* [shashkin](http://staff/shashkin)

 * BI-3094: Pass parameter values to data api v2  [ https://a.yandex-team.ru/arc/commit/9093481 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-02-02 17:05:38+03:00

0.1442.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2961: Implemented data API v2 response and some tests  [ https://a.yandex-team.ru/arc/commit/9091206 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-31 23:00:49+03:00

0.1441.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-01-31 18:38:42+03:00

0.1440.0
--------

* [thenno](http://staff/thenno)

 * BI-3169: fix for date-like fields in pg  [ https://a.yandex-team.ru/arc/commit/9088888 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-31 13:11:27+03:00

0.1439.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added add_fields_data to data api v1.5  [ https://a.yandex-team.ru/arc/commit/9088071 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-31 12:22:20+03:00

0.1438.0
--------

* [seray](http://staff/seray)

 * [BI-3153] support datetime param type

[RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/9082242 ]

* [thenno](http://staff/thenno)

 * BI-3163: don't annotate strings for asyncpg  [ https://a.yandex-team.ru/arc/commit/9082050 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-28 14:20:30+03:00

0.1437.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3152: Some typing fixes for exceptions                     [ https://a.yandex-team.ru/arc/commit/9078660 ]
 * BI-3092: Pass is_bleeding_edge_user flag to DatasetValidator  [ https://a.yandex-team.ru/arc/commit/9078222 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-28 10:40:15+03:00

0.1436.0
--------

* [thenno](http://staff/thenno)

 * STYLE: fix quotes                                                       [ https://a.yandex-team.ru/arc/commit/9076427 ]
 * BI-2840: add test for distinct and run tests for async adapters in api  [ https://a.yandex-team.ru/arc/commit/9075626 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-2961: Implemented multi-block query execution  [ https://a.yandex-team.ru/arc/commit/9073135 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-27 11:56:16+03:00

0.1435.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3092: Add parameter field type  [ https://a.yandex-team.ru/arc/commit/9071754 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-26 12:44:49+03:00

0.1434.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added wrapping of TranslationError into FormulaHandlingError (renamed FormulaCompilerError)  [ https://a.yandex-team.ru/arc/commit/9071091 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-25 23:45:25+03:00

0.1433.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3142: update reqs + fix yq for new sqla  [ https://a.yandex-team.ru/arc/commit/9070557 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3132: Some more data api v2 preparation  [ https://a.yandex-team.ru/arc/commit/9069629 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-25 22:01:03+03:00

0.1432.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Minor refactoring for improving the handling of formula errors               [ https://a.yandex-team.ru/arc/commit/9068589 ]
 * Stability fix for alerts tests and docstring for enum modules                [ https://a.yandex-team.ru/arc/commit/9067844 ]
 * BI-3131: Added handling of pivot overflow and more strict limits for /pivot  [ https://a.yandex-team.ru/arc/commit/9067840 ]
 * Made OrderableNullValueBase hashable and immutable                           [ https://a.yandex-team.ru/arc/commit/9065832 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-25 14:46:36+03:00

0.1431.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3129: drop sessions and tariffs  [ https://a.yandex-team.ru/arc/commit/9063303 ]

* [gstatsenko](http://staff/gstatsenko)

 * Separated data api v1.5 and v2                                            [ https://a.yandex-team.ru/arc/commit/9063014 ]
 * BI-3130: Added support for sorting null dimension values in pivot tables  [ https://a.yandex-team.ru/arc/commit/9057722 ]

* [quantum-0](http://staff/quantum-0)

 * BI-3103: Replace endpoints from YQ Executor into connection data  [ https://a.yandex-team.ru/arc/commit/9060933 ]

* [thenno](http://staff/thenno)

 * BI-2840: add DL_USE_ASYNC_POSTGRES_ADAPTER flag to settings  [ https://a.yandex-team.ru/arc/commit/9059572 ]

* [shashkin](http://staff/shashkin)

 * BI-2993: Add setting to enable async MySQL adapter  [ https://a.yandex-team.ru/arc/commit/9058010 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-24 12:41:23+03:00

0.1430.0
--------

* [seray](http://staff/seray)

 * [BI-3036] removing update_id_field action in favor of update_field

[RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/9055856 ]

* [kchupin](http://staff/kchupin)

 * [BI-2945] High-level workbook designer prototype  [ https://a.yandex-team.ru/arc/commit/9049955 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-21 13:44:35+03:00

0.1429.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2945] Remove read-only result schema fields from UpdateFieldSchema  [ https://a.yandex-team.ru/arc/commit/9047625 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed inheritance of bi-api connectors from bi-core connectors  [ https://a.yandex-team.ru/arc/commit/9046965 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-19 15:55:56+03:00

0.1428.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-01-18 19:20:49+03:00

0.1427.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2963: Added support for formulas defined as NULL  [ https://a.yandex-team.ru/arc/commit/9042360 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-3116: fix some org-specific things  [ https://a.yandex-team.ru/arc/commit/9041027 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-18 12:48:42+03:00

0.1426.0
--------

* [thenno](http://staff/thenno)

 * BI-3107: use is_bleeding_user in cache key  [ https://a.yandex-team.ru/arc/commit/9039138 ]

* [seray](http://staff/seray)

 * [BI-2935] yandex schoolbook connector                                               [ https://a.yandex-team.ru/arc/commit/9037904 ]
 * [BI-3101] rename connector_specific_params start-end -> from-to

[RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/9027440 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added methods for working with blocks in RawQuerySpec   [ https://a.yandex-team.ru/arc/commit/9037522 ]
 * Separated make_response from post in DatasetResultView  [ https://a.yandex-team.ru/arc/commit/9037521 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-17 16:50:08+03:00

0.1425.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2878: Implemented pagination of pivot corner cases               [ https://a.yandex-team.ru/arc/commit/9023410 ]
 * BI-3089: Moved pivot execution to TPE and added profiling to pivot  [ https://a.yandex-team.ru/arc/commit/9023283 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-13 12:13:23+03:00

0.1424.0
--------

* [asnytin](http://staff/asnytin)

 * BI-3102: hide yt token from connection api  [ https://a.yandex-team.ru/arc/commit/9020960 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-11 22:16:42+03:00

0.1423.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-3087: dummy middleware for timeout handling  [ https://a.yandex-team.ru/arc/commit/9018573 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-11 13:43:13+03:00

0.1422.0
--------

* [seray](http://staff/seray)

 * [BI-3036] rename_guid_field action

[RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/9016391 ]

* [gstatsenko](http://staff/gstatsenko)

 * Switched to nested field ref objects and added nested ref support in API v2  [ https://a.yandex-team.ru/arc/commit/9016377 ]

[robot-statinfra](http://staff/robot-statinfra) 2022-01-10 20:31:55+03:00

0.1421.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2022-01-10 14:42:58+03:00

0.1420.0
--------

* [shashkin](http://staff/shashkin)

 * BI-3051: Get folder_id from either dataset or connection in public api middleware  [ https://a.yandex-team.ru/arc/commit/9000363 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-30 13:32:56+03:00

0.1419.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-12-29 18:47:06+03:00

0.1418.0
--------

* [quantum-0](http://staff/quantum-0)

 * BI-3025: Add exception no access to clique  [ https://a.yandex-team.ru/arc/commit/8999414 ]

* [seray](http://staff/seray)

 * [BI-3075] connector_specific_params for DashSQL result

[RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/8999355 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-3070: Error for duplicate dimension values  [ https://a.yandex-team.ru/arc/commit/8999263 ]
 * A better error for unknown avatar in field     [ https://a.yandex-team.ru/arc/commit/8999101 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-29 16:57:20+03:00

0.1417.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-3070: Added support for duplicate measures in pivot tables                           [ https://a.yandex-team.ru/arc/commit/8996255 ]
 * BI-2961: Switched to using DatasetDataBaseView.execute_query in all data api endpoints  [ https://a.yandex-team.ru/arc/commit/8992648 ]

* [shashkin](http://staff/shashkin)

 * BI-3077: Fix middleware typing  [ https://a.yandex-team.ru/arc/commit/8994667 ]

* [thenno](http://staff/thenno)

 * BI-3002: don't send user db errors to sentry  [ https://a.yandex-team.ru/arc/commit/8982670 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-28 19:26:15+03:00

0.1416.0
--------

* [shashkin](http://staff/shashkin)

 * Fix bleeding_edge_users absence

https://sentry.stat.yandex-team.ru/sentry/ds-api_ext_testing/issues/1171577/  [ https://a.yandex-team.ru/arc/commit/8981460 ]

* [kchupin](http://staff/kchupin)

 * [BI-3059] Add sentry outbound cleanup in flask/logging integration via legacy raven client  [ https://a.yandex-team.ru/arc/commit/8981187 ]

* [seray](http://staff/seray)

 * [CHARTS-4903] always enable is_dashsql_allowed for Solomon and Prometheus

[RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/8978119 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-23 17:19:05+03:00

0.1415.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2961: A bit more changes in the query execution pipeline  [ https://a.yandex-team.ru/arc/commit/8976881 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-3041: do not fail in case of `managed_by` lack  [ https://a.yandex-team.ru/arc/commit/8976512 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-22 18:42:24+03:00

0.1414.0
--------

* [quantum-0](http://staff/quantum-0)

 * BI-2948: Add mdb_folder_id into mdb connectors  [ https://a.yandex-team.ru/arc/commit/8972538 ]

* [dim-gonch](http://staff/dim-gonch)

 * MyPy | Remove fixtures call

В новой версии Pytest вызов фикстур явно - запрещен https://docs.pytest.org/en/latest/deprecations.html#calling-fixtures-directly
CROWDFUNDING-15  [ https://a.yandex-team.ru/arc/commit/8969717 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-2961: More formalizations for the query execution pipeline  [ https://a.yandex-team.ru/arc/commit/8968395 ]

* [konstasa](http://staff/konstasa)

 * Reorganize profiling; add fields to profiling

Arch:
\- ReportingRegistry, ServicesRegistry and ReportingProfiler are now a part of DLRequest

\*This may be irrelevant I don't have time right now*
MWs:
\- request_id:
  - create reporting registry
  - create reporting profiler
  - bind them to DLRequest
  - pass the request on
  - save response info to the reporting registry
  - close the reporting profiler
\- services_registry
  - create services registry
  - bind services registry to the reporting profiler of the DLrequest
  - pass the request on
  - close the services registry  [ https://a.yandex-team.ru/arc/commit/8968163 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-21 17:15:53+03:00

0.1413.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-12-20 12:55:16+03:00

0.1412.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-12-17 14:10:49+03:00

0.1411.0
--------

* [seray](http://staff/seray)

 * [BI-3046] organization auth

[RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/8958001 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-17 10:53:30+03:00

0.1410.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-12-16 00:37:41+03:00

0.1409.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2982: bitrix connector  [ https://a.yandex-team.ru/arc/commit/8952729 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-15 14:28:19+03:00

0.1408.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-12-14 19:51:57+03:00

0.1407.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2945] Ability to add source avatar without updating direct fields in result schema  [ https://a.yandex-team.ru/arc/commit/8945300 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-13 21:25:19+03:00

0.1406.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added logging of pivot table size  [ https://a.yandex-team.ru/arc/commit/8943168 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-13 17:37:42+03:00

0.1405.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Refactored crawler to support custom task runners                                               [ https://a.yandex-team.ru/arc/commit/8924789 ]
 * BI-3020: Fixed row duplication in AGO by adding a second condition in case of month-basedunits  [ https://a.yandex-team.ru/arc/commit/8923800 ]
 * BI-3030: Added an implementation for empty pivot tables                                         [ https://a.yandex-team.ru/arc/commit/8922360 ]

* [kchupin](http://staff/kchupin)

 * [BI-2923] Add missing entity usage checker for public data app                              [ https://a.yandex-team.ru/arc/commit/8924184 ]
 * [BI-2945] Introducing workbook context for external api & internal API client improvements  [ https://a.yandex-team.ru/arc/commit/8922897 ]
 * [BI-2945] Fetching all connections from pseudo-workbook                                     [ https://a.yandex-team.ru/arc/commit/8922263 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-10 15:58:29+03:00

0.1404.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2961: Added some primitives and methods for working with query unions  [ https://a.yandex-team.ru/arc/commit/8912398 ]

* [seray](http://staff/seray)

 * moving statface tests from beta to prod  [ https://a.yandex-team.ru/arc/commit/8911112 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-07 18:37:17+03:00

0.1403.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-12-06 16:40:41+03:00

0.1402.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Some minor refactoring of formula errors  [ https://a.yandex-team.ru/arc/commit/8906741 ]

* [shashkin](http://staff/shashkin)

 * BI-2992: Use GetQueryStatus instead of DescribeQuery for YQ query status  [ https://a.yandex-team.ru/arc/commit/8902907 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-12-06 13:44:08+03:00

0.1401.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-12-02 16:39:13+03:00

0.1400.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-12-01 20:58:04+03:00

0.1399.0
--------

* [seray](http://staff/seray)

 * [BI-2864] solomon dashsql adapter  [ https://a.yandex-team.ru/arc/commit/8882406 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved query type-specific logic to formalizer from compiler  [ https://a.yandex-team.ru/arc/commit/8876729 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-29 14:19:54+03:00

0.1398.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2945] Move auth-trust AIOHTTP middleware to bi-core  [ https://a.yandex-team.ru/arc/commit/8871804 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-25 15:55:01+03:00

0.1397.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2983: Added info role to fields in legend and autofill_legend option to data api v2  [ https://a.yandex-team.ru/arc/commit/8867580 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-24 16:20:35+03:00

0.1396.0
--------

* [seray](http://staff/seray)

 * [BI-2864] using BB in test mode  [ https://a.yandex-team.ru/arc/commit/8867169 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-24 14:19:33+03:00

0.1395.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2961: Added new attributes to raw specs for unions and trees  [ https://a.yandex-team.ru/arc/commit/8863686 ]
 * Removed unused option ignore_group_by from LOD logic             [ https://a.yandex-team.ru/arc/commit/8863685 ]
 * Formalized client in dsmaker                                     [ https://a.yandex-team.ru/arc/commit/8862686 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-24 11:08:21+03:00

0.1394.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2966: Removed global group_by for case when extended aggregations are used  [ https://a.yandex-team.ru/arc/commit/8861073 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-23 11:51:06+03:00

0.1393.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added test_gsheets_conn_test_with_gozora  [ https://a.yandex-team.ru/arc/commit/8859629 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-22 20:39:03+03:00

0.1392.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Converted crawlers to async                       [ https://a.yandex-team.ru/arc/commit/8858157 ]
 * Renamed several modules, mostly pivot/v2-related  [ https://a.yandex-team.ru/arc/commit/8848261 ]

* [asnytin](http://staff/asnytin)

 * BI-2895: kinopoinsk interest index connector  [ https://a.yandex-team.ru/arc/commit/8857943 ]

* [seray](http://staff/seray)

 * [BI-2864] blackbox user_ticket support  [ https://a.yandex-team.ru/arc/commit/8848041 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-22 15:11:22+03:00

0.1391.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-11-19 13:38:17+03:00

0.1390.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2611: Switched to using correct source types in geo sources

Разделение основных и материализационных источников в геокодировании и геословарях             [ https://a.yandex-team.ru/arc/commit/8845852 ]
 * Minor refactoring of legend (pivot) formalizer

Подготовка к тому, что легенда будет использоваться не только в pivot, но и во всех остальных ручках data-api  [ https://a.yandex-team.ru/arc/commit/8844042 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-19 12:32:41+03:00

0.1389.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * CHARTS-4784: Fixed sorting of numbers in pivot tables                                                                [ https://a.yandex-team.ru/arc/commit/8842753 ]
 * Fixed pivot merge error in tests                                                                                     [ https://a.yandex-team.ru/arc/commit/8840444 ]
 * Added shortcuts for pivot API tests

Единая и практически всеобъемлющая проверка для тестов сводных таблиц           [ https://a.yandex-team.ru/arc/commit/8840175 ]
 * Fixed resolution of legend items in pivots

Фикс того случая сводных таблиц, когда показатели повторяются в легенде  [ https://a.yandex-team.ru/arc/commit/8840172 ]
 * BI-2950: Added basic data api v2                                                                                     [ https://a.yandex-team.ru/arc/commit/8837976 ]
 * CHARTS-4784: Added casefolding for strings in pivot sorter                                                           [ https://a.yandex-team.ru/arc/commit/8837971 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-18 14:43:33+03:00

0.1388.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-11-16 19:04:41+03:00

0.1387.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2877: Added tests for pivot tables for the case of one-sided dimensions and absent measures  [ https://a.yandex-team.ru/arc/commit/8831173 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-15 21:16:30+03:00

0.1386.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-11-12 22:02:48+03:00

0.1385.0
--------

* [seray](http://staff/seray)

 * [BI-2864] solomon connector api support  [ https://a.yandex-team.ru/arc/commit/8822411 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-12 17:47:07+03:00

0.1384.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-11-11 20:17:58+03:00

0.1383.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Minor fix for LOD compatibility check  [ https://a.yandex-team.ru/arc/commit/8821050 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-11 20:07:57+03:00

0.1382.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-11-11 16:28:55+03:00

0.1381.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-11-11 14:40:25+03:00

0.1380.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-11-11 11:02:51+03:00

0.1379.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2904] Workbook related attributes for connections in legacy serialization schema  [ https://a.yandex-team.ru/arc/commit/8817528 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added logging to pivots  [ https://a.yandex-team.ru/arc/commit/8817434 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-10 21:39:28+03:00

0.1378.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-11-10 19:52:15+03:00

0.1377.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-11-10 19:42:48+03:00

0.1376.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2877: Fixed pivot tables for the case of absent dimensions and multiple measures  [ https://a.yandex-team.ru/arc/commit/8816791 ]

* [kchupin](http://staff/kchupin)

 * [BI-870][BI-2938] Migration to new-style API schema for CHYDB & fix missing unversioned data in USM.clone_entry_instance()  [ https://a.yandex-team.ru/arc/commit/8816574 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-10 19:33:25+03:00

0.1375.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * DLHELP-3819:Added handling of type cast errors                                          [ https://a.yandex-team.ru/arc/commit/8812926 ]
 * BI-2927: Added low-level op registry for inspection                                     [ https://a.yandex-team.ru/arc/commit/8812925 ]
 * DLHELP-3814: Fixed postprocessing of non-ASCII arrays                                   [ https://a.yandex-team.ru/arc/commit/8812924 ]
 * BI-2877: Fixed pivot tables for the case of one-sided dimensions and multiple measures  [ https://a.yandex-team.ru/arc/commit/8811230 ]

* [seray](http://staff/seray)

 * [BI-2925] optional username/pass for promql  [ https://a.yandex-team.ru/arc/commit/8811193 ]
 * [BI-2929] verbose validation error           [ https://a.yandex-team.ru/arc/commit/8808418 ]

* [thenno](http://staff/thenno)

 * BI-2928: change log level for unexisted conns  [ https://a.yandex-team.ru/arc/commit/8808780 ]

* [shadchin](http://staff/shadchin)

 * CONTRIB-1682 Drop contrib/python/aiohttp/gunicorn_worker

Удалил `contrib/python/aiohttp/gunicorn_worker`, если нужна его функциональность, то надо просто добавить PEERDIR на `contrib/python/gunicorn` в дополнение к `contrib/python/aiohttp` и все будет работать  [ https://a.yandex-team.ru/arc/commit/8803206 ]

* [kchupin](http://staff/kchupin)

 * [BI-870] Preliminary removing of unused legacy connection schemas  [ https://a.yandex-team.ru/arc/commit/8800539 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-10 11:47:58+03:00

0.1374.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-11-03 18:48:23+03:00

0.1373.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added a facade wrapper for pivot table modifiers  [ https://a.yandex-team.ru/arc/commit/8798271 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-03 16:42:43+03:00

0.1372.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-11-02 18:31:30+03:00

0.1371.0
--------

* [thenno](http://staff/thenno)

 * Increase SGA_MEM size in oracle  [ https://a.yandex-team.ru/arc/commit/8794465 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved several tests from db/pivot/test_basic.py to new files                   [ https://a.yandex-team.ru/arc/commit/8793400 ]
 * BI-2877: Added automatic usage of Measure Names in some cases of pivot tables  [ https://a.yandex-team.ru/arc/commit/8789973 ]
 * BI-2878: Added pagination to pivot tables                                      [ https://a.yandex-team.ru/arc/commit/8789961 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-11-02 18:18:56+03:00

0.1370.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-11-01 11:37:19+03:00

0.1369.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-10-29 17:31:46+03:00

0.1368.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-10-29 16:51:16+03:00

0.1367.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-10-29 16:14:26+03:00

0.1366.0
--------

* [asnytin](http://staff/asnytin)

 * WIP BI-2810: subselects in podcasters connector  [ https://a.yandex-team.ru/arc/commit/8783062 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-29 14:07:19+03:00

0.1365.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-10-28 13:24:22+03:00

0.1364.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-10-27 18:20:51+03:00

0.1363.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-10-27 17:57:38+03:00

0.1362.0
--------

* [hhell](http://staff/hhell)

 * BI-2636: data-api: ds_validator.apply_batch in TPE  [ https://a.yandex-team.ru/arc/commit/8773676 ]

* [gstatsenko](http://staff/gstatsenko)

 * Fixed error checking in lookup functions                                             [ https://a.yandex-team.ru/arc/commit/8773622 ]
 * BI-2877: Added support for duplicate Measure Names in pivot tables with one measure  [ https://a.yandex-team.ru/arc/commit/8773615 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-27 11:37:07+03:00

0.1361.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Minor import fixes                                                              [ https://a.yandex-team.ru/arc/commit/8767363 ]
 * Renamed DataFrame, separated PivotSorter into abstract and pandas impl classes  [ https://a.yandex-team.ru/arc/commit/8767322 ]

* [kchupin](http://staff/kchupin)

 * [BI-2901] Send X-DL-TenantId to DLS instead of X-YaCloud-FolderID/X-YaCloud-OrgID  [ https://a.yandex-team.ru/arc/commit/8767297 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-26 15:01:51+03:00

0.1360.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2877: Fixed error handling in field resolution from specs  [ https://a.yandex-team.ru/arc/commit/8765588 ]
 * BI-2877: Added error handling for pivot tables                [ https://a.yandex-team.ru/arc/commit/8765582 ]
 * Separated DataFrame from PivotTable                           [ https://a.yandex-team.ru/arc/commit/8765443 ]
 * Removed some unnecessary logging from query processing        [ https://a.yandex-team.ru/arc/commit/8765336 ]
 * Added logging of result data stats                            [ https://a.yandex-team.ru/arc/commit/8765300 ]

* [seray](http://staff/seray)

 * [BI-2689] support secure connection  [ https://a.yandex-team.ru/arc/commit/8765328 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-25 12:48:40+03:00

0.1359.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-10-22 10:41:48+03:00

0.1358.0
--------

* [hhell](http://staff/hhell)

 * BI-2636: dataset-data-api latency-tracking task  [ https://a.yandex-team.ru/arc/commit/8756145 ]
 * BI-2887: add YDB to the docs generation          [ https://a.yandex-team.ru/arc/commit/8756022 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-2877:Added support for pivot tables without measures  [ https://a.yandex-team.ru/arc/commit/8755687 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-21 15:42:36+03:00

0.1357.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Removed FieldRemapping  [ https://a.yandex-team.ru/arc/commit/8751361 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-20 14:32:53+03:00

0.1356.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2871: Fixed markup support in pivot processing  [ https://a.yandex-team.ru/arc/commit/8751072 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-20 13:08:52+03:00

0.1355.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Turned on query complexity logging  [ https://a.yandex-team.ru/arc/commit/8744915 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-19 15:19:24+03:00

0.1354.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2859: Removed MSSQL variant for MEDIAN and QUANTILE       [ https://a.yandex-team.ru/arc/commit/8735831 ]
 * DLHELP-3658: Made title field required for action add_field  [ https://a.yandex-team.ru/arc/commit/8730785 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-18 13:16:39+03:00

0.1353.0
--------

* [hhell](http://staff/hhell)

 * BI-2531 BI-2731: normalize tier1 dependencies    [ https://a.yandex-team.ru/arc/commit/8723490 ]
 * BI-2731: more test fixes and skips               [ https://a.yandex-team.ru/arc/commit/8715636 ]
 * BI-2731: fix chyt bi_core tests and other tests  [ https://a.yandex-team.ru/arc/commit/8715289 ]
 * BI-2731: typing fix                              [ https://a.yandex-team.ru/arc/commit/8714301 ]

* [thenno](http://staff/thenno)

 * BI-2857: fix oracle in docker, improve local dev README  [ https://a.yandex-team.ru/arc/commit/8721513 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-2855: Turned on ignoring of all nested UI aggregations  [ https://a.yandex-team.ru/arc/commit/8714552 ]
 * Implement better recognition for extended aggregations     [ https://a.yandex-team.ru/arc/commit/8714549 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-11 18:39:08+03:00

0.1352.0
--------

* [seray](http://staff/seray)

 * [BI-2748] public clique migration  [ https://a.yandex-team.ru/arc/commit/8711431 ]

* [hhell](http://staff/hhell)

 * BI-2531: normalize SA dialect packages  [ https://a.yandex-team.ru/arc/commit/8710193 ]

* [gstatsenko](http://staff/gstatsenko)

 * Switched to non-SQLA dialect names in dialect resolution         [ https://a.yandex-team.ru/arc/commit/8707155 ]
 * DLHELP-3578 BI-2363: Added phantom error cleaner to maintenance  [ https://a.yandex-team.ru/arc/commit/8704178 ]
 * Minor fixes for bi_api_lib.maintenance.data_access                       [ https://a.yandex-team.ru/arc/commit/8704037 ]
 * Added complexity attr to NodeExtract                             [ https://a.yandex-team.ru/arc/commit/8704034 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-07 22:54:06+03:00

0.1351.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-10-05 08:51:16+03:00

0.1350.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2846: Fixed AmongToWithinGroupingMutation  [ https://a.yandex-team.ru/arc/commit/8699313 ]

* [hhell](http://staff/hhell)

 * BI-2531: bi_sqlalchemy_common, bi_sqlalchemy_clickhouse  [ https://a.yandex-team.ru/arc/commit/8697078 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-04 18:15:03+03:00

0.1349.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2220: Updated PG median test  [ https://a.yandex-team.ru/arc/commit/8691137 ]

* [seray](http://staff/seray)

 * [BI-2689] PromQL query parametrization  [ https://a.yandex-team.ru/arc/commit/8690684 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-01 17:55:13+03:00

0.1348.0
--------

* [hhell](http://staff/hhell)

 * BI-2531: sequential AvatarAliasMapper  [ https://a.yandex-team.ru/arc/commit/8689581 ]

* [asnytin](http://staff/asnytin)

 * BI-2826: connection preview api fix  [ https://a.yandex-team.ru/arc/commit/8689565 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-10-01 00:42:27+03:00

0.1347.0
--------

* [hhell](http://staff/hhell)

 * BI-2590: Update sqlalchemy to 1.4  [ https://a.yandex-team.ru/arc/commit/8679138 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-29 10:11:52+03:00

0.1346.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added LOD compatibility validation and test  [ https://a.yandex-team.ru/arc/commit/8673418 ]
 * Added validation of toplevel LODs            [ https://a.yandex-team.ru/arc/commit/8673412 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-28 11:39:45+03:00

0.1345.0
--------

* [hhell](http://staff/hhell)

 * bi_formula: Drop support for ClickHouse<19.13                     [ https://a.yandex-team.ru/arc/commit/8672577 ]
 * BI-2827: pass ‘query’ and ‘db_message’ to the user in most cases  [ https://a.yandex-team.ru/arc/commit/8672395 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-27 12:31:32+03:00

0.1344.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Common preliminary LOD changes                [ https://a.yandex-team.ru/arc/commit/8664899 ]
 * BI-2738: Fixed compatibility of AGO and LODs  [ https://a.yandex-team.ru/arc/commit/8664744 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-24 15:15:40+03:00

0.1343.0
--------

* [hhell](http://staff/hhell)

 * BI-2693 BI-2827: schematic_request on connection http resources  [ https://a.yandex-team.ru/arc/commit/8661443 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-23 14:37:28+03:00

0.1342.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2738: Refactored aggregation validator in preparation for future extended LOD support  [ https://a.yandex-team.ru/arc/commit/8653353 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-21 21:03:57+03:00

0.1341.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2819: Anonymized RLS filters                               [ https://a.yandex-team.ru/arc/commit/8652332 ]
 * Fix and test for aggregations with LOD over window functions  [ https://a.yandex-team.ru/arc/commit/8651163 ]
 * Added test for LODs in any db                                 [ https://a.yandex-team.ru/arc/commit/8642230 ]

* [seray](http://staff/seray)

 * [BI-2689] adding docker image for prometheus node-exporter  [ https://a.yandex-team.ru/arc/commit/8651538 ]
 * [BI-2689] prometheus async adapter                          [ https://a.yandex-team.ru/arc/commit/8641693 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-2775: update clickhouse to 21.8 in tests  [ https://a.yandex-team.ru/arc/commit/8641626 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-21 16:48:51+03:00

0.1340.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2737: Yet another micro fix for GROUP BY in complex LODs  [ https://a.yandex-team.ru/arc/commit/8639394 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-17 09:54:11+03:00

0.1339.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2737: Fixed sub-query groupings by adding the GroupByNormalizer hack  [ https://a.yandex-team.ru/arc/commit/8638030 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-16 19:38:19+03:00

0.1338.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-09-16 15:36:32+03:00

0.1337.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2798: Separated dsmaker.api.api_http into several files  [ https://a.yandex-team.ru/arc/commit/8635385 ]
 * BI-2737: Partially fixed triple aggregations                [ https://a.yandex-team.ru/arc/commit/8635240 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-16 13:27:08+03:00

0.1336.0
--------

* [hhell](http://staff/hhell)

 * BI-2807: YQL/YDB/YQ: fix Timestamp type support                          [ https://a.yandex-team.ru/arc/commit/8635125 ]
 * BI-2693: fix yq tests, add ydb svcacc_id test, fix ydb svacc_id support  [ https://a.yandex-team.ru/arc/commit/8634913 ]

* [gstatsenko](http://staff/gstatsenko)

 * Implemented sorting for pivot tables, BI-2769  [ https://a.yandex-team.ru/arc/commit/8634563 ]
 * BI-2802: Disabled constant lookup dimensions   [ https://a.yandex-team.ru/arc/commit/8634561 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-16 12:28:43+03:00

0.1335.0
--------

* [hhell](http://staff/hhell)

 * bi_api enum_field: further synchronize with the upstream  [ https://a.yandex-team.ru/arc/commit/8633181 ]

* [gstatsenko](http://staff/gstatsenko)

 * BI-2737: Switched to sanitizing by level instead of the whole multi-query                  [ https://a.yandex-team.ru/arc/commit/8632568 ]
 * BI-2737: Disabled inconsistent aggregation again                                           [ https://a.yandex-team.ru/arc/commit/8631385 ]
 * BI-2737: Disabled grouping of LODs together                                                [ https://a.yandex-team.ru/arc/commit/8631376 ]
 * Fixed resolution of legend item IDs in pivot API and added a test for targeted annotation  [ https://a.yandex-team.ru/arc/commit/8631363 ]
 * Renamed fork functions to lookup functions                                                 [ https://a.yandex-team.ru/arc/commit/8630957 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-16 00:06:52+03:00

0.1334.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2721: moysklad: cut db_name from dsrc params                [ https://a.yandex-team.ru/arc/commit/8630330 ]
 * call register_all_connectors in MaintenanceEnvironmentManager  [ https://a.yandex-team.ru/arc/commit/8629919 ]

* [hhell](http://staff/hhell)

 * BI-2722: pre-normalize bi_api enum field  [ https://a.yandex-team.ru/arc/commit/8626793 ]

* [seray](http://staff/seray)

 * [BI-2689] using conftest for prometheus  [ https://a.yandex-team.ru/arc/commit/8620780 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-15 13:04:18+03:00

0.1333.0
--------

* [asnytin](http://staff/asnytin)

 * CHARTS-4573: enable ds revision_ds generation again

Выкатывать строго после фронта (https://st.yandex-team.ru/CHARTS-4573)  [ https://a.yandex-team.ru/arc/commit/8615239 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added tracing to get_single_formula_errors                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/8614358 ]
 * Moved and renamed dsmaker api files, BI-2798

Только перемещение и переименование файлов, никаких изменений логики

В дальнейшем буду перекраивать `dsmaker.api.base` и `dsmaker.api.api_http`  [ https://a.yandex-team.ru/arc/commit/8614353 ]
 * Moved some mutations to separate files from bi_formula.mutations.window, renamed subquery to lookup                                                                                             [ https://a.yandex-team.ru/arc/commit/8614347 ]
 * Allowed inconsistent aggregation inside other aggregations, DLHELP-3302                                                                                                                         [ https://a.yandex-team.ru/arc/commit/8614341 ]
 * Added disabled test test_two_same_dim_lods_in_different_subqueries                                                                                                                              [ https://a.yandex-team.ru/arc/commit/8614339 ]
 * Removed unused code block from forker                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/8614335 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-10 15:48:13+03:00

0.1332.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-1408] Remove ContextTransferSession & send_context flag in get_requests_session_*() util due to lack of usage  [ https://a.yandex-team.ru/arc/commit/8612336 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-09 21:05:06+03:00

0.1331.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2721: added a few more moysklad connector tests  [ https://a.yandex-team.ru/arc/commit/8609864 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added sanitizing of filters and group_by in sub-queries, BI-2761  [ https://a.yandex-team.ru/arc/commit/8609769 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-09 14:14:55+03:00

0.1330.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-09-08 16:51:10+03:00

0.1329.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2721: fix moysklad source schemas  [ https://a.yandex-team.ru/arc/commit/8604400 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-2762: handle unknown avatar id  [ https://a.yandex-team.ru/arc/commit/8604099 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-08 11:23:32+03:00

0.1328.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Reverted refreshing of revision_id on dataset save, CHARTS-4573            [ https://a.yandex-team.ru/arc/commit/8602243 ]
 * Partially separated API serialization logic from HTTP requests in dsmaker  [ https://a.yandex-team.ru/arc/commit/8600218 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-2781: handle field KeyError  [ https://a.yandex-team.ru/arc/commit/8600223 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-07 17:17:53+03:00

0.1327.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-09-06 23:33:35+03:00

0.1326.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-09-06 14:53:03+03:00

0.1325.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2721: moysklad fixes  [ https://a.yandex-team.ru/arc/commit/8596718 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-06 14:40:50+03:00

0.1324.0
--------

* [seray](http://staff/seray)

 * [BI-2689] prometheus support  [ https://a.yandex-team.ru/arc/commit/8596166 ]

* [gstatsenko](http://staff/gstatsenko)

 * Partially fixed double aggregations with LODs, BI-2761  [ https://a.yandex-team.ru/arc/commit/8596023 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-06 13:58:51+03:00

0.1323.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-09-03 23:03:02+03:00

0.1322.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2495] Turn on authentication on dataset-data-api for DataCloud  [ https://a.yandex-team.ru/arc/commit/8588940 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-03 19:42:36+03:00

0.1321.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2718] Add workbook ID to dataset  [ https://a.yandex-team.ru/arc/commit/8587184 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-02 20:41:33+03:00

0.1320.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2717][BI-2767] Replace folder_id with TenantDef in RCI  [ https://a.yandex-team.ru/arc/commit/8587057 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-02 20:06:14+03:00

0.1319.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2717] Fix us_auth_ctx_blackbox_middleware  [ https://a.yandex-team.ru/arc/commit/8586401 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-02 17:48:29+03:00

0.1318.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Enabled updating of dataset's revision_id on save, BI-2378  [ https://a.yandex-team.ru/arc/commit/8582890 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-02 16:32:49+03:00

0.1317.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-09-01 17:28:16+03:00

0.1316.0
--------

* [asnytin](http://staff/asnytin)

 * BI-668: stringify arrays in api postprocessing  [ https://a.yandex-team.ru/arc/commit/8581819 ]
 * BI-2721: moysklad connector - bi_api part       [ https://a.yandex-team.ru/arc/commit/8581577 ]

* [gstatsenko](http://staff/gstatsenko)

 * Revert "Removed EnumNameField from bi_api"

This reverts commit de46ec8a69ca8c738c7ac806785e6cab1bdcaeed.  [ https://a.yandex-team.ru/arc/commit/8579479 ]
 * Removed some typing TODOs                                                                                  [ https://a.yandex-team.ru/arc/commit/8575531 ]
 * Removed EnumNameField from bi_api                                                                          [ https://a.yandex-team.ru/arc/commit/8575015 ]
 * Moved more validation methods to formalizer                                                                [ https://a.yandex-team.ru/arc/commit/8574990 ]
 * Moved dsmaker to bi_testing                                                                                [ https://a.yandex-team.ru/arc/commit/8574988 ]
 * Reimplemented (de)serialization of dataset components in dsmaker via marshmallow                           [ https://a.yandex-team.ru/arc/commit/8571329 ]
 * Moved BIType from bi_core.data_types to bi_constants.enums                                                 [ https://a.yandex-team.ru/arc/commit/8566626 ]
 * Removed enum_field from bi_core/contrib                                                                    [ https://a.yandex-team.ru/arc/commit/8565447 ]

* [kchupin](http://staff/kchupin)

 * [BI-2717][BI-1408] New Flask request ID middleware & total migration to RCI (auth_data/folder_id/etc...) in Flask environment       [ https://a.yandex-team.ru/arc/commit/8578665 ]
 * [BI-2717][BI-1680] Migrating DLS client from Flask context to RCI                                                                   [ https://a.yandex-team.ru/arc/commit/8577555 ]
 * [BI-2717] Preliminary refactoring: minimize usage of bi_core.current_folder_id() & ReqCtxInfoMiddleware.get_request_context_info()  [ https://a.yandex-team.ru/arc/commit/8576738 ]
 * [BI-2718][BI-2717] Worbooks+project_id+IAM_auth setup for datacloud dataset-api                                                     [ https://a.yandex-team.ru/arc/commit/8572399 ]
 * [BI-2718] Initial implementation of workbooks support for US entries                                                                [ https://a.yandex-team.ru/arc/commit/8571961 ]
 * [BI-2717][BI-1408] Remove legacy flask app keys `FOLDER_ID` & `FOLDER_ID_ENABLED`                                                   [ https://a.yandex-team.ru/arc/commit/8571932 ]
 * [BI-2495] Workaroud for flask-restplus bug (it resets app error hadling on API registration)                                        [ https://a.yandex-team.ru/arc/commit/8566578 ]

* [dmifedorov](http://staff/dmifedorov)

 * CHARTS-4382: do not require token for ydb conn  [ https://a.yandex-team.ru/arc/commit/8578207 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-09-01 17:16:41+03:00

0.1315.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * fix csv preview error  [ https://a.yandex-team.ru/arc/commit/8562683 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved most of the enums from bi_core.enums to bi_constants.enums  [ https://a.yandex-team.ru/arc/commit/8561234 ]
 * Fixed dimension node type assertion in forker, BI-2749            [ https://a.yandex-team.ru/arc/commit/8561206 ]
 * Removed `__getitem__` from dsmaker's Dataset class                [ https://a.yandex-team.ru/arc/commit/8560814 ]
 * Added assert message                                              [ https://a.yandex-team.ru/arc/commit/8557134 ]

* [kchupin](http://staff/kchupin)

 * [BI-2717] Preliminary refactoring: migrating to RCI in cross-service session factories. Generic authentication data was added to RCI.  [ https://a.yandex-team.ru/arc/commit/8558801 ]
 * [BI-2717] Preliminary refactoring: RCI for RawDataStreamer                                                                             [ https://a.yandex-team.ru/arc/commit/8555473 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-26 19:27:41+03:00

0.1314.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-08-24 15:45:39+03:00

0.1313.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-08-24 15:01:18+03:00

0.1312.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Temporarily disabled 'Field ... is invalid and cannot be selected' error, BI-2714  [ https://a.yandex-team.ru/arc/commit/8552880 ]

* [kchupin](http://staff/kchupin)

 * [BI-2693] Fix test for config loading after adding YC_RM_CP_ENDPOINT & YC_IAM_TS_ENDPOINT to BaseAppSettings  [ https://a.yandex-team.ru/arc/commit/8550488 ]
 * [BI-2717] Yet another preliminary refactoring: remove unused staff groups operations in bi_core               [ https://a.yandex-team.ru/arc/commit/8549551 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-24 14:54:59+03:00

0.1311.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-08-23 15:48:23+03:00

0.1310.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2717] Fix missing of user USM in tests: replace folder ID MW with TrustAuthService in testing flask apps which sets fake user ID in request context  [ https://a.yandex-team.ru/arc/commit/8548933 ]

* [hhell](http://staff/hhell)

 * BI-2693: YC TS/RM endpoints in base app settings  [ https://a.yandex-team.ru/arc/commit/8548817 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-23 15:19:22+03:00

0.1309.0
--------

* [hhell](http://staff/hhell)

 * BI-2722: preliminary style tuning (a missed piece)  [ https://a.yandex-team.ru/arc/commit/8539888 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-20 18:02:47+03:00

0.1308.0
--------

* [hhell](http://staff/hhell)

 * BI-2722: preliminary style tuning  [ https://a.yandex-team.ru/arc/commit/8538452 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-19 15:31:15+03:00

0.1307.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-08-19 14:18:50+03:00

0.1306.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2495] Migrating aiohttp yc_auth middleware to YCAuthController       [ https://a.yandex-team.ru/arc/commit/8537719 ]
 * [BI-2495] Apply FlaskYCAuthService & fix auth issue in materializer API  [ https://a.yandex-team.ru/arc/commit/8536482 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-19 13:18:55+03:00

0.1305.0
--------

* [hhell](http://staff/hhell)

 * BI-2693: ConnectionBase.validate_new_data, YDB/YQ service_account_id  [ https://a.yandex-team.ru/arc/commit/8533971 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-18 14:33:30+03:00

0.1304.0
--------

* [hhell](http://staff/hhell)

 * BI-2710: fix yql groupby aliasing  [ https://a.yandex-team.ru/arc/commit/8533029 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-2697: use paas-base-g4 image  [ https://a.yandex-team.ru/arc/commit/8530053 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-18 12:02:28+03:00

0.1303.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-08-16 19:50:09+03:00

0.1302.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-08-16 17:03:45+03:00

0.1301.0
--------

* [hhell](http://staff/hhell)

 * BI-668: tests: fix dashsql tests for array types                                                                                   [ https://a.yandex-team.ru/arc/commit/8519465 ]
 * BI-2703: dashsql ch type parsing fix                                                                                               [ https://a.yandex-team.ru/arc/commit/8516175 ]
 * tests: avoid random test parameters

fix for flakiness, i.e. http://jing.yandex-team.ru/files/hhell/2021-08-12T11:22:53+03:00.png  [ https://a.yandex-team.ru/arc/commit/8514558 ]

* [tsimafeip](http://staff/tsimafeip)

 * BI-2442 Fix tests.                                        [ https://a.yandex-team.ru/arc/commit/8519447 ]
 * BI-2692 Run integration tests for internal installations  [ https://a.yandex-team.ru/arc/commit/8519437 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-13 15:22:15+03:00

0.1300.0
--------

* [hhell](http://staff/hhell)

 * BI-2700: quick-fix postgresql mdb_cluster_id  [ https://a.yandex-team.ru/arc/commit/8510812 ]

* [asnytin](http://staff/asnytin)

 * fixed appmetrica connection view check  [ https://a.yandex-team.ru/arc/commit/8509622 ]

* [tsimafeip](http://staff/tsimafeip)

 * BI-2442 Add extra read permissions checks for dataset object.  [ https://a.yandex-team.ru/arc/commit/8506141 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-11 14:14:17+03:00

0.1299.0
--------

* [shadchin](http://staff/shadchin)

 * IGNIETFERRO-1154 Rename contrib/python/flask_sqlalchemy to contrib/python/Flask-SQLAlchemy

<!-- DEVEXP BEGIN -->
![review](https://codereview.in.yandex-team.ru/badges/review-in_progress-yellow.svg) [![mamogaaa](https://codereview.in.yandex-team.ru/badges/mamogaaa-...-yellow.svg)](https://staff.yandex-team.ru/mamogaaa)
<!-- DEVEXP END -->  [ https://a.yandex-team.ru/arc/commit/8502907 ]

* [hhell](http://staff/hhell)

 * BI-2691: dashsql: serialize NaN/Inf into strings    [ https://a.yandex-team.ru/arc/commit/8502579 ]
 * increase default sources response  limit to 10_000  [ https://a.yandex-team.ru/arc/commit/8502282 ]

* [asnytin](http://staff/asnytin)

 * BI-668: bi-mat tests image version up  [ https://a.yandex-team.ru/arc/commit/8502375 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-09 15:24:57+03:00

0.1298.0
--------

* [asnytin](http://staff/asnytin)

 * BI-668: array types  [ https://a.yandex-team.ru/arc/commit/8501717 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added several fixes and new tests for pivot API, moved some constants to bi_constants  [ https://a.yandex-team.ru/arc/commit/8499445 ]
 * Minor fix for data requests with invalid fields, BI-2672                               [ https://a.yandex-team.ru/arc/commit/8498313 ]

* [hhell](http://staff/hhell)

 * tooling: fix environment filling for migrators            [ https://a.yandex-team.ru/arc/commit/8494771 ]
 * BI-2381: uuid values serialization (cache, rqe, dashsql)  [ https://a.yandex-team.ru/arc/commit/8492839 ]

* [kchupin](http://staff/kchupin)

 * [BI-2495] Move TestClientConverterAiohttpToFlask to bi_testing  [ https://a.yandex-team.ru/arc/commit/8492001 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-09 11:12:39+03:00

0.1297.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added a comprehensive comment about the `use_empty_source` hack and a minor fix in `LodAggregationToQueryForkMutation`  [ https://a.yandex-team.ru/arc/commit/8490900 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-05 13:17:32+03:00

0.1296.0
--------

* [tsimafeip](http://staff/tsimafeip)

 * BI-2643 Extend integration tests for window functions.  [ https://a.yandex-team.ru/arc/commit/8490396 ]

* [gstatsenko](http://staff/gstatsenko)

 * Yet another fix for zero-dimensional LODs  [ https://a.yandex-team.ru/arc/commit/8490306 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-05 12:04:48+03:00

0.1295.0
--------

* [hhell](http://staff/hhell)

 * BI-2471: dashsql bindparams+cache fix  [ https://a.yandex-team.ru/arc/commit/8486757 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added dataset creation shortcuts to bi_api tests  [ https://a.yandex-team.ru/arc/commit/8486245 ]

* [tsimafeip](http://staff/tsimafeip)

 * BI-2668 Restrict dataset creation from connection with execute rights.  [ https://a.yandex-team.ru/arc/commit/8484675 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-04 14:14:22+03:00

0.1294.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Separated test_totals from test_main in bi_api/tests/db/result  [ https://a.yandex-team.ru/arc/commit/8482955 ]

* [hhell](http://staff/hhell)

 * BI-2652: dashsql bindparams: fix oracle string params  [ https://a.yandex-team.ru/arc/commit/8482835 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-03 16:19:45+03:00

0.1293.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-08-02 17:27:34+03:00

0.1292.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added more attributes to pivot legend items, BI-2542  [ https://a.yandex-team.ru/arc/commit/8477854 ]
 * Supported BEFORE FILTER BY for LODs, BI-2407          [ https://a.yandex-team.ru/arc/commit/8477850 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-02 12:05:14+03:00

0.1291.0
--------

* [hhell](http://staff/hhell)

 * BI-2652: dashsql bindparams: support lists for 'in'  [ https://a.yandex-team.ru/arc/commit/8477781 ]
 * tests: move common SQL queries to bi_testing         [ https://a.yandex-team.ru/arc/commit/8477626 ]
 * BI-2665: YQ connector                                [ https://a.yandex-team.ru/arc/commit/8472432 ]
 * BI-2665: YQ connector base                           [ https://a.yandex-team.ru/arc/commit/8471363 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed legacy debug info              [ https://a.yandex-team.ru/arc/commit/8472936 ]
 * Several minor fixes for new pivot API  [ https://a.yandex-team.ru/arc/commit/8471359 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-08-02 11:53:35+03:00

0.1290.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-07-29 20:56:51+03:00

0.1289.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added pivot data API, BI-2542  [ https://a.yandex-team.ru/arc/commit/8468980 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-29 17:15:38+03:00

0.1288.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-07-29 11:59:38+03:00

0.1287.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added support for zero-dimensional LODs, BI-2407  [ https://a.yandex-team.ru/arc/commit/8465294 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-28 18:26:23+03:00

0.1286.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-07-28 15:06:00+03:00

0.1285.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-07-28 13:48:01+03:00

0.1284.0
--------

* [hhell](http://staff/hhell)

 * BI-2521: fix: return debug logging for async apps (attempt 02)  [ https://a.yandex-team.ru/arc/commit/8462987 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-28 11:30:19+03:00

0.1283.0
--------

* [hhell](http://staff/hhell)

 * BI-2521: fix: return debug logging for async apps  [ https://a.yandex-team.ru/arc/commit/8461852 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-28 00:42:11+03:00

0.1282.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added DOCUMENTED and SUGGESTED function scopes                [ https://a.yandex-team.ru/arc/commit/8459739 ]
 * Moved more logic from handlers and DatasetView to Formalizer  [ https://a.yandex-team.ru/arc/commit/8459724 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-27 13:12:08+03:00

0.1281.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Finalized implementation of basic LOD usage, BI-2407  [ https://a.yandex-team.ru/arc/commit/8459382 ]

* [kchupin](http://staff/kchupin)

 * [BI-2663] Remove sample generation in `DatasetResource.enforce_materialization_and_cleanup()`  [ https://a.yandex-team.ru/arc/commit/8457868 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-27 12:07:54+03:00

0.1280.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Made refresh faster, DLHELP-3023                        [ https://a.yandex-team.ru/arc/commit/8456989 ]
 * Fixed empty query requests and subquery prefixes        [ https://a.yandex-team.ru/arc/commit/8445437 ]
 * Added support for annotations to pivot tables, BI-2542  [ https://a.yandex-team.ru/arc/commit/8439962 ]
 * Merged two LOD mutations into one, BI-2407              [ https://a.yandex-team.ru/arc/commit/8438908 ]

* [kchupin](http://staff/kchupin)

 * [BI-2521] Change req id separator `-` -> `--`                                                        [ https://a.yandex-team.ru/arc/commit/8456318 ]
 * [BI-2663] `CONVERTER_HOST` and `MATERIALIZER_HOST` become non-required in `ControlPlaneAppSettings`  [ https://a.yandex-team.ru/arc/commit/8448947 ]
 * [BI-2663] Change default value for `preview` in CreateDatasetSchema to `false`                       [ https://a.yandex-team.ru/arc/commit/8447291 ]

* [hhell](http://staff/hhell)

 * BI-2521: enable full stdout logs in all applications  [ https://a.yandex-team.ru/arc/commit/8447109 ]
 * BI-2652: dashsql bindparams                           [ https://a.yandex-team.ru/arc/commit/8444640 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-26 17:36:36+03:00

0.1279.0
--------

* [hhell](http://staff/hhell)

 * BI-2501: fix of r8386031 r8386331 r8422825 r8432861  [ https://a.yandex-team.ru/arc/commit/8436548 ]

* [gstatsenko](http://staff/gstatsenko)

 * Fixed default filter value for non-preview values  [ https://a.yandex-team.ru/arc/commit/8435631 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-20 18:48:15+03:00

0.1278.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-2648: fix subselect conn replace  [ https://a.yandex-team.ru/arc/commit/8435510 ]

* [seray](http://staff/seray)

 * [BI-2580] testing downloadable geolayers  [ https://a.yandex-team.ru/arc/commit/8433376 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-20 14:27:28+03:00

0.1277.0
--------

* [hhell](http://staff/hhell)

 * fix ydeploy/ycloud deployment after r8422825  [ https://a.yandex-team.ru/arc/commit/8432861 ]
 * tests: fix bi_api rls tests                   [ https://a.yandex-team.ru/arc/commit/8431497 ]

* [gstatsenko](http://staff/gstatsenko)

 * Separated DatasetDataResource into DataPostprocessor and DataRequestResponseSerializer, BI-2542  [ https://a.yandex-team.ru/arc/commit/8431610 ]
 * Added cleaning up of useless JOINs to sanitizer, BI-2407                                         [ https://a.yandex-team.ru/arc/commit/8430655 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-19 18:53:29+03:00

0.1276.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-07-16 14:09:19+03:00

0.1275.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-07-16 13:39:14+03:00

0.1274.0
--------

* [hhell](http://staff/hhell)

 * BI-2501: more overridable workerscount                              [ https://a.yandex-team.ru/arc/commit/8422825 ]
 * BI-2644: YQL char, top_concat; making the YQL-YDB library required  [ https://a.yandex-team.ru/arc/commit/8420470 ]

* [dmifedorov](http://staff/dmifedorov)

 * DLHELP-2968: refresh_source in maintenance tools  [ https://a.yandex-team.ru/arc/commit/8421496 ]

* [gstatsenko](http://staff/gstatsenko)

 * Several minor post-review fixes for FieldRole and pivot functionality, BI-2542                                            [ https://a.yandex-team.ru/arc/commit/8421481 ]
 * Added PivotRole, prepared pivot table for vector values, combined Legend and PivotTableSpec into a single class, BI-2542  [ https://a.yandex-team.ru/arc/commit/8420736 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-16 13:11:14+03:00

0.1273.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved more logic from data api handlers to formalizer and params from drm to raw query spec, BI-2542  [ https://a.yandex-team.ru/arc/commit/8420263 ]
 * Made field updating optional in refresh_data_source                                                   [ https://a.yandex-team.ru/arc/commit/8420113 ]
 * Separated DatasetApiLoader from DatasetDataResource, BI-2542                                          [ https://a.yandex-team.ru/arc/commit/8414557 ]

* [hhell](http://staff/hhell)

 * BI-2371: dashsql cache                                                                                                                     [ https://a.yandex-team.ru/arc/commit/8417481 ]
 * BI-2471: allow_subselect -> raw_sql_level transition support                                                                               [ https://a.yandex-team.ru/arc/commit/8416198 ]
 * BI-1603 CHARTS-3215: chyt_table_list duplicates check on the backend side, to simplify frontend                                            [ https://a.yandex-team.ru/arc/commit/8415369 ]
 * tests: remove a more-problematic-than-helpful check                                                                                        [ https://a.yandex-team.ru/arc/commit/8413155 ]
 * rename ConnectionTypes -> ConnectionType

Reproduction: `find . -name '*.py' | xargs -d '\n' sed -i 's/ConnectionTypes/ConnectionType/g'`  [ https://a.yandex-team.ru/arc/commit/8411315 ]

* [asnytin](http://staff/asnytin)

 * added greenplum result api test  [ https://a.yandex-team.ru/arc/commit/8411490 ]
 * BI-2629: ch_frozen test          [ https://a.yandex-team.ru/arc/commit/8411479 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-15 11:38:57+03:00

0.1272.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added CHYTSubqueryWeightLimitExceeded                                                     [ https://a.yandex-team.ru/arc/commit/8408832 ]
 * Minor fixes in bi_query (some refactoring in forker and usage of clone methods), BI-2407  [ https://a.yandex-team.ru/arc/commit/8408795 ]

* [dmifedorov](http://staff/dmifedorov)

 * remove obsolete ensure_data_selection_allowed method  [ https://a.yandex-team.ru/arc/commit/8408189 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-13 12:17:59+03:00

0.1271.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-07-12 12:32:34+03:00

0.1270.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-07-09 20:49:02+03:00

0.1269.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed type hint in remap_compiled_formula_fields               [ https://a.yandex-team.ru/arc/commit/8403329 ]
 * Fixed remapping of CompiledJoinOnFormulaInfo objects, BI-2407  [ https://a.yandex-team.ru/arc/commit/8402938 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-09 19:06:36+03:00

0.1268.0
--------

* [hhell](http://staff/hhell)

 * BI-2593: tests: mypy: switch from file-ignore to line-ignore in bi_api  [ https://a.yandex-team.ru/arc/commit/8402092 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-09 15:13:00+03:00

0.1267.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-07-09 12:47:54+03:00

0.1266.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-07-08 20:48:50+03:00

0.1265.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2626: enabled podcasters conn in public  [ https://a.yandex-team.ru/arc/commit/8388934 ]

* [gstatsenko](http://staff/gstatsenko)

 * Several minor fixes and improvements for maintenance tools  [ https://a.yandex-team.ru/arc/commit/8388656 ]
 * Implemented recursive sub-query cloning, BI-2407            [ https://a.yandex-team.ru/arc/commit/8388652 ]
 * Added DataRequestModel class, BI-2542                       [ https://a.yandex-team.ru/arc/commit/8388623 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-08 17:43:37+03:00

0.1264.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-07-08 12:44:48+03:00

0.1263.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Separated feature source types for different roles, BI-2611                   [ https://a.yandex-team.ru/arc/commit/8387255 ]
 * Added function scope mechanism, BI-2610                                       [ https://a.yandex-team.ru/arc/commit/8383915 ]
 * Implemented multi-level patching of CompiledMultiLevelQuery objects, BI-2407  [ https://a.yandex-team.ru/arc/commit/8383382 ]

* [kchupin](http://staff/kchupin)

 * [BI-2497] Support of launching bi-api docker image in IPv4-only environments fix (fails in IPv6 env)                  [ https://a.yandex-team.ru/arc/commit/8386331 ]
 * [BI-2497] Add support of launching bi-api docker image in ipv4-only environments & RQE env var configs normalization  [ https://a.yandex-team.ru/arc/commit/8386031 ]
 * [BI-2497] Crypto keys config JSON value loading support in bi-api BaseAppSettings                                     [ https://a.yandex-team.ru/arc/commit/8385841 ]

* [asnytin](http://staff/asnytin)

 * created_via fix  [ https://a.yandex-team.ru/arc/commit/8382670 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-08 10:59:18+03:00

0.1262.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed materializer URL in settings  [ https://a.yandex-team.ru/arc/commit/8381599 ]

* [kchupin](http://staff/kchupin)

 * [BI-2497] DataCloud variants of dataset-api & dataset-data-api  [ https://a.yandex-team.ru/arc/commit/8381180 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-06 18:20:27+03:00

0.1261.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-07-06 15:44:15+03:00

0.1260.0
--------

* [hhell](http://staff/hhell)

 * BI-2371: preliminary typing refactoring         [ https://a.yandex-team.ru/arc/commit/8380626 ]
 * BI-2381: ydb fixes and actualized tests         [ https://a.yandex-team.ru/arc/commit/8380025 ]
 * BI-2371: dashsql refactor into data_processing  [ https://a.yandex-team.ru/arc/commit/8362083 ]

* [kchupin](http://staff/kchupin)

 * [BI-2497] Kostyl for dataset-api to be able to work w/o materializer client  [ https://a.yandex-team.ru/arc/commit/8380412 ]
 * [BI-2497] ControlPlaneAppSettings: naming fixes & missing tests              [ https://a.yandex-team.ru/arc/commit/8372221 ]
 * [BI-2497] Sync dataset API migration from GS to attr.s config                [ https://a.yandex-team.ru/arc/commit/8362049 ]
 * [BI-2497] Fix billing config fallback for public                             [ https://a.yandex-team.ru/arc/commit/8357144 ]

* [gstatsenko](http://staff/gstatsenko)

 * Renamed query spec primitives, BI-2542                                                   [ https://a.yandex-team.ru/arc/commit/8379438 ]
 * Added registration of connectors to RQE, BI-2608                                         [ https://a.yandex-team.ru/arc/commit/8378767 ]
 * Switched to using RawFieldSpec, BI-2542                                                  [ https://a.yandex-team.ru/arc/commit/8375962 ]
 * Moved compiled expression objects from bi_api_lib.query.base to bi_api_lib.query.compilation.primitives  [ https://a.yandex-team.ru/arc/commit/8375759 ]
 * Moved validation and data request schemas to separate files                              [ https://a.yandex-team.ru/arc/commit/8356814 ]
 * Removed dataset-cli                                                                      [ https://a.yandex-team.ru/arc/commit/8354386 ]
 * Several fixes for pivot tables + one more test                                           [ https://a.yandex-team.ru/arc/commit/8354380 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-07-06 15:35:29+03:00

0.1259.0
--------

* [hhell](http://staff/hhell)

 * CHARTS-4182: gsheets cache_ttl_sec  [ https://a.yandex-team.ru/arc/commit/8354258 ]

* [kchupin](http://staff/kchupin)

 * [BI-2497] Rename app_async_settings.py -> app_settings.py     [ https://a.yandex-team.ru/arc/commit/8351584 ]
 * [BI-2497] Migrate AsyncAppSettings to declarative env loader  [ https://a.yandex-team.ru/arc/commit/8351414 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-30 16:34:19+03:00

0.1258.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * First phase of further formalization of query handling     [ https://a.yandex-team.ru/arc/commit/8349126 ]
 * Added a better object interface for pivot tables, BI-2542  [ https://a.yandex-team.ru/arc/commit/8347241 ]

* [kchupin](http://staff/kchupin)

 * [BI-2497] Eliminating usage of direct reading of SAMPLES_CH_HOST from environment  [ https://a.yandex-team.ru/arc/commit/8347427 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-29 12:56:06+03:00

0.1257.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Yet another step of query postprocessing refactoring  [ https://a.yandex-team.ru/arc/commit/8346416 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-28 14:10:51+03:00

0.1256.0
--------

* [hhell](http://staff/hhell)

 * BI-2578: totals values postprocessing  [ https://a.yandex-team.ru/arc/commit/8345777 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-28 12:18:38+03:00

0.1255.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-2581: add special chyt fields (table_path, table_name, table_index) to dataset  [ https://a.yandex-team.ru/arc/commit/8344914 ]

* [kchupin](http://staff/kchupin)

 * [BI-2497] Move geolayers configs from granular_settings                          [ https://a.yandex-team.ru/arc/commit/8344856 ]
 * [BI-2497] Migrating CryptoKeysConfig & RQEConfig to declarative settings loader  [ https://a.yandex-team.ru/arc/commit/8344752 ]

* [tsimafeip](http://staff/tsimafeip)

 * BI-2579 Materialization status check fixed for connections with data sources  [ https://a.yandex-team.ru/arc/commit/8342527 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-28 10:26:39+03:00

0.1254.0
--------

* [hhell](http://staff/hhell)

 * BI-1603: freeform_sources tab_title  [ https://a.yandex-team.ru/arc/commit/8332890 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added handling of ClickHouse error code 70  [ https://a.yandex-team.ru/arc/commit/8331631 ]
 * Added test_ago_with_different_measures      [ https://a.yandex-team.ru/arc/commit/8330527 ]

* [kchupin](http://staff/kchupin)

 * [BI-2497] Moving all URLs from BI API granular settings to environments.py  [ https://a.yandex-team.ru/arc/commit/8329545 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-25 21:01:26+03:00

0.1253.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Yet another set of fixes for forking  [ https://a.yandex-team.ru/arc/commit/8324829 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-23 15:51:48+03:00

0.1252.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added meta to query primitves, BI-2542               [ https://a.yandex-team.ru/arc/commit/8323360 ]
 * Added get_query_dimension_list to query_tools        [ https://a.yandex-team.ru/arc/commit/8322109 ]
 * Minor refactoring and simplification of QueryForker  [ https://a.yandex-team.ru/arc/commit/8317554 ]

* [tsimafeip](http://staff/tsimafeip)

 * BI-2554 RLS integration test added, code refactoring.  [ https://a.yandex-team.ru/arc/commit/8318793 ]

* [kchupin](http://staff/kchupin)

 * [BI-1496] Remove setting PUBLIC_USE_MASTER_TOKEN_FOR_MAIN_USM  [ https://a.yandex-team.ru/arc/commit/8318566 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-23 12:50:33+03:00

0.1251.0
--------

* [hhell](http://staff/hhell)

 * BI-2381: fix ydb-yql minmax aliases  [ https://a.yandex-team.ru/arc/commit/8279569 ]

* [gstatsenko](http://staff/gstatsenko)

 * Updated maintenance tools  [ https://a.yandex-team.ru/arc/commit/8277605 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-21 11:50:05+03:00

0.1250.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved SRFactory creation to a single function in bi-api, BI-2541  [ https://a.yandex-team.ru/arc/commit/8274155 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-17 18:03:23+03:00

0.1249.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added query_tools module, BI-2407  [ https://a.yandex-team.ru/arc/commit/8272881 ]

* [kchupin](http://staff/kchupin)

 * [BI-2497] Preliminary granular settings cleanup  [ https://a.yandex-team.ru/arc/commit/8270655 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-17 14:22:52+03:00

0.1248.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-06-16 17:35:34+03:00

0.1247.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-06-16 13:39:28+03:00

0.1246.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed feature functions for constants, BI-2544  [ https://a.yandex-team.ru/arc/commit/8267617 ]
 * GSheets as connector                            [ https://a.yandex-team.ru/arc/commit/8256256 ]

* [hhell](http://staff/hhell)

 * CHARTS-3936: fix totals+offset                       [ https://a.yandex-team.ru/arc/commit/8266597 ]
 * BI-2471: dashsql for most of the remaining main DBs  [ https://a.yandex-team.ru/arc/commit/8256238 ]

* [shadchin](http://staff/shadchin)

 * IGNIETFERRO-1835 Fix mypy checks

Правим следующие ошибки
```
bi/connectors/base/connector.py:26: error: Type argument "bi_core.connectors.base.connector.CoreSourceDefinition" of "CoreConnector" must be a subtype of "bi_core.connectors.base.connector.CoreConnectionDefinition"
bi/connectors/base/connector.py:26: error: Type argument "bi_core.connectors.base.connector.CoreConnectionDefinition" of "CoreConnector" must be a subtype of "bi_core.connectors.base.connector.CoreSourceDefinition"
bi/connectors/base/registrator.py:24: error: Argument "source_type" to "register_source_api_schema" has incompatible type "Type[CreateDSFrom]"; expected "CreateDSFrom"
bi/connectors/base/registrator.py:26: error: Argument "source_type" to "register_source_template_api_schema" has incompatible type "Type[CreateDSFrom]"; expected "CreateDSFrom"
bi/connectors/base/registrator.py:32: error: Argument "conn_type" to "register_connection_edit_schema_cls" has incompatible type "Type[ConnectionTypes]"; expected "ConnectionTypes"
bi/connectors/base/registrator.py:33: error: Argument "conn_type" to "register_connection_test_schema_cls" has incompatible type "Type[ConnectionTypes]"; expected "ConnectionTypes"
bi/connectors/base/registrator.py:38: error: Argument "conn_type" to "register_connection_create_schema_cls" has incompatible type "Type[ConnectionTypes]"; expected "ConnectionTypes"
bi/connectors/greenplum/schemas.py:8: error: Incompatible types in assignment (expression has type "Type[GreenplumConnection]", base class "PostgresConnectionSchema" defined the type as "Type[ConnectionPostgreSQL]")
bi/maintenance/crawlers/access_mode_normalizer.py:29: error: Item "None" of "Optional[SyncUSManager]" has no attribute "get_raw_collection"
bi/maintenance/crawlers/access_mode_normalizer.py:43: error: Item "None" of "Optional[SyncUSManager]" has no attribute "get_by_id"
bi/maintenance/crawlers/access_mode_normalizer.py:63: error: Enum index should be a string (actual index type "Optional[str]")
bi/maintenance/crawlers/field_dependency_gen.py:24: error: Item "None" of "Optional[SyncUSManager]" has no attribute "get_raw_collection"
bi/maintenance/crawlers/unbound_avatar_cleaner.py:73: error: Item "None" of "Optional[SyncUSManager]" has no attribute "get_raw_collection"
bi/query/formalization/avatar_tools.py:18: error: Item "None" of "Optional[SourceAvatar]" has no attribute "managed_by"
bi/query/subqueries/forker.py:123: error: Need type annotation for 'forked_query_order_by' (hint: "forked_query_order_by: List[<type>] = ...")
bi/query/subqueries/forker.py:232: error: "FormulaItem" has no attribute "name"
bi/query/subqueries/forker.py:244: error: Incompatible types in assignment (expression has type "Tuple[List[CompiledFormulaInfo], List[CompiledFormulaInfo], List[CompiledOrderByFormulaInfo], List[CompiledFormulaInfo], List[CompiledJoinOnFormulaInfo]]", variable has type "Tuple[List[CompiledFormulaInfo], ...]")
bi/query/subqueries/forker.py:279: error: Argument "node_extract" to "SubqueryInfoKey" has incompatible type "Optional[NodeExtract]"; expected "NodeExtract"
bi/query/subqueries/forker.py:281: error: Generator has incompatible item type "Optional[NodeExtract]"; expected "NodeExtract"
bi/query/subqueries/forker.py:295: error: Incompatible types in assignment (expression has type "Optional[SubqueryUsageInfo]", variable has type "SubqueryUsageInfo")
bi/query/subqueries/forker.py:368: error: Unsupported operand types for + ("List[CompiledFormulaInfo]" and "List[CompiledJoinOnFormulaInfo]")
bi/query/subqueries/forker.py:390: error: Name 'updated_queries' already defined on line 388
bi/query/subqueries/forker.py:403: error: Unsupported operand types for + ("List[CompiledQuery]" and "Sequence[CompiledQuery]")
bi/query/subqueries/sanitizer.py:46: error: Unsupported operand types for + ("List[CompiledFormulaInfo]" and "List[CompiledOrderByFormulaInfo]")
bi/query/subqueries/sanitizer.py:47: error: Unsupported operand types for + ("List[CompiledFormulaInfo]" and "List[CompiledJoinOnFormulaInfo]")
bi/schemas/connection_provider.py:47: error: Incompatible types in assignment (expression has type "Dict[Any, Type[Schema]]", base class "OneOfSchema" defined the type as "List[Any]")
bi/schemas/connection_provider.py:48: error: "Type[Meta]" has no attribute "type_discriminator"
bi/schemas/registry.py:73: error: Variable "edit_schema_cls" is not valid as a type
bi/schemas/registry.py:73: note: See https://mypy.readthedocs.io/en/latest/common_issues.html#variables-vs-type-aliases
bi/schemas/registry.py:73: error: Invalid base class "edit_schema_cls"
```  [ https://a.yandex-team.ru/arc/commit/8262611 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-15 19:36:37+03:00

0.1245.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-2546: do not remove orig ds mat table on copy  [ https://a.yandex-team.ru/arc/commit/8254711 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added data fetching functions to maintenance  [ https://a.yandex-team.ru/arc/commit/8252696 ]
 * node type error in QueryForker                [ https://a.yandex-team.ru/arc/commit/8252637 ]

* [hhell](http://staff/hhell)

 * tests: fix a test const  [ https://a.yandex-team.ru/arc/commit/8249422 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-10 11:43:45+03:00

0.1244.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-06-08 10:50:48+03:00

0.1243.0
--------

* [hhell](http://staff/hhell)

 * list stabilized error codes in a test  [ https://a.yandex-team.ru/arc/commit/8245859 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed legacy errors from validation and added new attributes to the remaining ones, BI-2362  [ https://a.yandex-team.ru/arc/commit/8245841 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-08 10:31:14+03:00

0.1242.0
--------

* [hhell](http://staff/hhell)

 * remove DBInvalidResponseError in favor of SourceProtocolError  [ https://a.yandex-team.ru/arc/commit/8245136 ]
 * BI-2509: further YQL bi_formula support                        [ https://a.yandex-team.ru/arc/commit/8245133 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed aggregated param from all source feature logic  [ https://a.yandex-team.ru/arc/commit/8245041 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-07 15:59:06+03:00

0.1241.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added more registrations for connectors in bi-api  [ https://a.yandex-team.ru/arc/commit/8240103 ]
 * Added join_type to QueryFork nodes, BI-2407        [ https://a.yandex-team.ru/arc/commit/8240098 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-07 12:02:28+03:00

0.1240.0
--------

* [hhell](http://staff/hhell)

 * BI-2381 BI-2509: YQL formula dialect for YDB connector, YDB connector improvements  [ https://a.yandex-team.ru/arc/commit/8236174 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-03 19:20:49+03:00

0.1239.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * caches-int-prod: man -> myt  [ https://a.yandex-team.ru/arc/commit/8235291 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-03 12:36:50+03:00

0.1238.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2404: GP test connection fix  [ https://a.yandex-team.ru/arc/commit/8234913 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-03 11:14:23+03:00

0.1237.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-06-02 18:13:24+03:00

0.1236.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed usage of complex WITHIN expressions, DLHELP-2590  [ https://a.yandex-team.ru/arc/commit/8231369 ]
 * Moved import of BIField to if TYPE_CHECKING             [ https://a.yandex-team.ru/arc/commit/8230897 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-02 12:27:06+03:00

0.1235.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added strict option to field actions, DLHELP-2591  [ https://a.yandex-team.ru/arc/commit/8229313 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-01 17:59:54+03:00

0.1234.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed calculation of nesting levels for level tags  [ https://a.yandex-team.ru/arc/commit/8227656 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-06-01 12:36:16+03:00

0.1233.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed sanitizer to include non-SELECT formulas in used_avatar_ids, BI-2498  [ https://a.yandex-team.ru/arc/commit/8223775 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-05-31 14:53:57+03:00

0.1232.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-05-31 11:47:04+03:00

0.1231.0
--------

* [asnytin](http://staff/asnytin)

 * updated bi_materializer version in bi-api tests  [ https://a.yandex-team.ru/arc/commit/8218457 ]

* [dmifedorov](http://staff/dmifedorov)

 * CHARTS-4043: fix test  [ https://a.yandex-team.ru/arc/commit/8217957 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed dataset_errors and fixed handling of None as connection_id, BI-2406  [ https://a.yandex-team.ru/arc/commit/8217366 ]
 * Fixed handling of nested AGOs with BFB                                       [ https://a.yandex-team.ru/arc/commit/8216071 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-05-28 18:16:40+03:00

0.1230.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-05-27 20:45:42+03:00

0.1229.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-05-27 17:20:51+03:00

0.1228.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * CHARTS-4043: user exc for ch parsing error  [ https://a.yandex-team.ru/arc/commit/8203215 ]

* [asnytin](http://staff/asnytin)

 * BI-2483: Obligatory filters check disabled  [ https://a.yandex-team.ru/arc/commit/8201912 ]

* [gstatsenko](http://staff/gstatsenko)

 * Fixed skipped tests                                                         [ https://a.yandex-team.ru/arc/commit/8200962 ]
 * Added tests for QE-serialization of expressions with within_group, BI-2220  [ https://a.yandex-team.ru/arc/commit/8199131 ]
 * Added non-digit prefix for short UUIDs in SQL queries, BI-2448              [ https://a.yandex-team.ru/arc/commit/8199117 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-05-27 13:38:22+03:00

0.1227.0
--------

* [hhell](http://staff/hhell)

 * BI-1478: fix datetimetz between  [ https://a.yandex-team.ru/arc/commit/8192963 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-05-25 12:37:27+03:00

0.1226.0
--------

* [hhell](http://staff/hhell)

 * BI-2481: fix colon for subselect/dashsql  [ https://a.yandex-team.ru/arc/commit/8192387 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-05-24 14:10:37+03:00

0.1225.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * New structure for formula dialects                                   [ https://a.yandex-team.ru/arc/commit/8187689 ]
 * Removed get_data_source_type                                         [ https://a.yandex-team.ru/arc/commit/8184567 ]
 * Added slots=True and frozen=True to bi-api query primitives          [ https://a.yandex-team.ru/arc/commit/8179724 ]
 * Added MultiQuerySanitizer for cleaning up unused SELECT expressions  [ https://a.yandex-team.ru/arc/commit/8175228 ]

* [asnytin](http://staff/asnytin)

 * BI-2404: greenplum connector  [ https://a.yandex-team.ru/arc/commit/8184177 ]

* [hhell](http://staff/hhell)

 * BI-2381: YQL-YDB: more error handling  [ https://a.yandex-team.ru/arc/commit/8183100 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-05-24 12:38:06+03:00

0.1224.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2462] Fix error message for USPermissionRequired  [ https://a.yandex-team.ru/arc/commit/8168052 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-05-17 16:49:40+03:00

0.1223.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved some maintenance stuff from bi-api to bi-core  [ https://a.yandex-team.ru/arc/commit/8167766 ]

* [hhell](http://staff/hhell)

 * BI-2454: with_totals fixes                                 [ https://a.yandex-team.ru/arc/commit/8167675 ]
 * BI-2381: ydb table listing                                 [ https://a.yandex-team.ru/arc/commit/8165518 ]
 * BI-2405: native type storage format legacy-saving cleanup  [ https://a.yandex-team.ru/arc/commit/8164967 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-05-14 13:30:24+03:00

0.1222.0
--------

* [hhell](http://staff/hhell)

 * slightly bigger minor convenience                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/8160757 ]
 * rename enum Aggregations to AggregationFunction

`find . -type f -name '*.py' -print0 | xargs -0 sed -i 's/Aggregations/AggregationFunction/g' && svn revert lib/bi_formula/bi_formula/definitions/functions_window.py`                                                                                                                                                                                                                                                                                                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/8160674 ]
 * mass-rename `_logger` to `LOGGER` (continuation of https://a.yandex-team.ru/review/1727770 )

Reproduction: https://paste.yandex-team.ru/4312305/text                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/8159248 ]
 * rename SomeEnums.somevalue to SomeEnum.somevalue

Reproduction:
`find . -type f -name '*.py' -print0 | xargs -0 sed -i 's/CalcModes/CalcMode/g; s/BinaryJoinOperators/BinaryJoinOperator/g; s/FieldTypes/FieldType/g; s/WhereClauseOperations/WhereClauseOperation/g; s/ConnectionStates/ConnectionState/g; s/AppMetricaLogsApiNamespaces/AppMetricaLogsApiNamespace/g; s/ExtraSourceFeatures/ExtraSourceFeature/g; s/JugglerStatuses/JugglerStatus/g; s/CtxKeys/CtxKey/g; s/BITypes/BIType/g; s/LoadDumpOptions/LoadDumpOption/g; s/JobStates/JobState/g; s/RequiredResources/RequiredResource/g'`

Not included: `s/Aggregations/Aggregation/g`  [ https://a.yandex-team.ru/arc/commit/8158415 ]

* [gstatsenko](http://staff/gstatsenko)

 * Extended BiApiConnectorRegistrator                                                         [ https://a.yandex-team.ru/arc/commit/8160512 ]
 * Fixed usage of materialization flag during formula dialect resolution, CLOUDSUPPORT-75067  [ https://a.yandex-team.ru/arc/commit/8157688 ]
 * Fixed validation response message                                                          [ https://a.yandex-team.ru/arc/commit/8126753 ]
 * Added bi_api_lib.maintenance.helpers                                                               [ https://a.yandex-team.ru/arc/commit/8121345 ]
 * Added a draft for connector classes                                                        [ https://a.yandex-team.ru/arc/commit/8121131 ]
 * Separated raw data streamers from data sources                                             [ https://a.yandex-team.ru/arc/commit/8121067 ]
 * Added UnboundAvatarCleanerCrawler, BI-2434                                                 [ https://a.yandex-team.ru/arc/commit/8115788 ]
 * Separated expressions.py into several files                                                [ https://a.yandex-team.ru/arc/commit/8110340 ]
 * Removed Tagged node, moved LevelTags to node meta                                          [ https://a.yandex-team.ru/arc/commit/8109906 ]

* [kchupin](http://staff/kchupin)

 * [BI-2440] More strict access control for connection handles  [ https://a.yandex-team.ru/arc/commit/8120185 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-05-12 13:13:00+03:00

0.1221.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * profile populate-dataset-from-body  [ https://a.yandex-team.ru/arc/commit/8105896 ]

* [gstatsenko](http://staff/gstatsenko)

 * Renamed BFBTag to LevelTag and moved it to a separate file  [ https://a.yandex-team.ru/arc/commit/8101988 ]
 * Added LODs to fork function internals, BI-2407              [ https://a.yandex-team.ru/arc/commit/8099730 ]
 * DSMaker refactoring begins                                  [ https://a.yandex-team.ru/arc/commit/8098706 ]

* [asnytin](http://staff/asnytin)

 * BI-2247: call issue mat db from bi-uploads  [ https://a.yandex-team.ru/arc/commit/8101847 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-26 13:46:56+03:00

0.1220.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Enabled revision_id check  [ https://a.yandex-team.ru/arc/commit/8097963 ]

* [dmifedorov](http://staff/dmifedorov)

 * EntityUsageNotAllowed as user exception  [ https://a.yandex-team.ru/arc/commit/8097636 ]

* [asnytin](http://staff/asnytin)

 * Wrapped MetrikaApiAccessDeniedException in bi-api  [ https://a.yandex-team.ru/arc/commit/8095822 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-22 14:26:46+03:00

0.1219.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed preview subquery limit  [ https://a.yandex-team.ru/arc/commit/8093997 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-21 11:17:06+03:00

0.1218.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-04-20 20:21:34+03:00

0.1217.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-04-20 19:56:08+03:00

0.1216.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-2187: finally working releaser  [ https://a.yandex-team.ru/arc/commit/8092555 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-20 18:16:49+03:00

0.1215.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-2187: fix releaser               [ https://a.yandex-team.ru/arc/commit/8091123 ]
 * BI-2187: switch releaser to deploy  [ https://a.yandex-team.ru/arc/commit/8090521 ]

* [gstatsenko](http://staff/gstatsenko)

 * Fixed pretty formnatting for CompiledQuery and CompiledFormulaInfo  [ https://a.yandex-team.ru/arc/commit/8091087 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-20 15:48:59+03:00

0.1214.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added logging of YQL schema for data responses, DLHELP-2351  [ https://a.yandex-team.ru/arc/commit/8088574 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-19 18:26:07+03:00

0.1213.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Reverted field deopendency optimization  [ https://a.yandex-team.ru/arc/commit/8088281 ]

* [hhell](http://staff/hhell)

 * postgres:13  [ https://a.yandex-team.ru/arc/commit/8083852 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-19 15:49:40+03:00

0.1212.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added logging of dataset field stats  [ https://a.yandex-team.ru/arc/commit/8082853 ]

* [hhell](http://staff/hhell)

 * BI-2254: source templates backend filtering  [ https://a.yandex-team.ru/arc/commit/8082830 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-16 15:42:49+03:00

0.1211.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added pretty methods to CompiledQuery and CompiledFormulaInfo  [ https://a.yandex-team.ru/arc/commit/8081676 ]

* [hhell](http://staff/hhell)

 * BI-2381: YDB connector  [ https://a.yandex-team.ru/arc/commit/8080837 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-16 10:00:32+03:00

0.1210.0
--------

* [hhell](http://staff/hhell)

 * BI-2254: minimize processing of data source templates in response  [ https://a.yandex-team.ru/arc/commit/8080781 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-15 20:42:57+03:00

0.1209.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added logging of dataset creation  [ https://a.yandex-team.ru/arc/commit/8079376 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-15 19:35:37+03:00

0.1208.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added error for usage of internal CH in origin, BI-2406  [ https://a.yandex-team.ru/arc/commit/8078389 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-15 11:16:30+03:00

0.1207.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-04-14 20:17:59+03:00

0.1206.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-870] More new-style connection schemas  [ https://a.yandex-team.ru/arc/commit/8075389 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-14 13:48:33+03:00

0.1205.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added logging of role resolution info  [ https://a.yandex-team.ru/arc/commit/8074661 ]

* [hhell](http://staff/hhell)

 * BI-1478 BI-2384: datetimetz incoming filters fix                            [ https://a.yandex-team.ru/arc/commit/8074604 ]
 * BI-2261: fix preload_mat_connection_for_ds passthrough in us_manager_async  [ https://a.yandex-team.ru/arc/commit/8074488 ]
 * BI-2385: ClickHouse 21.3 in tests                                           [ https://a.yandex-team.ru/arc/commit/8074203 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-14 10:46:41+03:00

0.1204.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fix ignoring of aggregated expressions in WITHIN  [ https://a.yandex-team.ru/arc/commit/8068211 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-12 13:21:36+03:00

0.1203.0
--------

* [hhell](http://staff/hhell)

 * BI-2388: postgresql: mangle bytea values into something minimally represented  [ https://a.yandex-team.ru/arc/commit/8064459 ]
 * BI-2246: py39 in tox                                                           [ https://a.yandex-team.ru/arc/commit/8064457 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-09 20:14:05+03:00

0.1202.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-04-09 17:00:22+03:00

0.1201.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-04-09 16:50:14+03:00

0.1200.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Switched to non-legacy avatar resolution, BI-2066  [ https://a.yandex-team.ru/arc/commit/8063971 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-09 15:41:16+03:00

0.1199.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added subquery usage for all avatars, BI-2341  [ https://a.yandex-team.ru/arc/commit/8063477 ]

* [hhell](http://staff/hhell)

 * BI-1996: delete CHYT_TABLE_SUBSELECT                                          [ https://a.yandex-team.ru/arc/commit/8063300 ]
 * BI-2254: connections/.../info/metadata_sources API for fast preliminary data  [ https://a.yandex-team.ru/arc/commit/8063211 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-09 13:45:43+03:00

0.1198.0
--------

* [hhell](http://staff/hhell)

 * BI-2261: skip exceptions in the preload_mat_connection_for_ds  [ https://a.yandex-team.ru/arc/commit/8061701 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-09 12:20:49+03:00

0.1197.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added converter to bi-api tests, BI-2289  [ https://a.yandex-team.ru/arc/commit/8061364 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-08 19:20:03+03:00

0.1196.0
--------

* [hhell](http://staff/hhell)

 * fix a minor typo  [ https://a.yandex-team.ru/arc/commit/8061060 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added revision_id to dataset, BI-2378  [ https://a.yandex-team.ru/arc/commit/8061011 ]
 * Fixed RLS tests                        [ https://a.yandex-team.ru/arc/commit/8060561 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-08 17:23:43+03:00

0.1195.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-04-08 15:20:46+03:00

0.1194.0
--------

* [hhell](http://staff/hhell)

 * mass-rename `_log` to `LOGGER`  [ https://a.yandex-team.ru/arc/commit/8058728 ]

* [dmifedorov](http://staff/dmifedorov)

 * handle access denied on conn loading  [ https://a.yandex-team.ru/arc/commit/8058638 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-08 11:59:46+03:00

0.1193.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-04-07 22:31:52+03:00

0.1192.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-04-07 20:52:29+03:00

0.1191.0
--------

* [hhell](http://staff/hhell)

 * BI-2261: attempt to re-enable preload_mat_connection_for_ds heuristic  [ https://a.yandex-team.ru/arc/commit/8058103 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added checks for orphaned component errors, BI-2363  [ https://a.yandex-team.ru/arc/commit/8056624 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-07 19:42:03+03:00

0.1190.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2329: fixed raw conn.data["host"] fetching  [ https://a.yandex-team.ru/arc/commit/8055980 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-07 12:48:31+03:00

0.1189.0
--------

* [hhell](http://staff/hhell)

 * BI-2347: minimize dataset results fields data  [ https://a.yandex-team.ru/arc/commit/8055617 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-07 10:55:29+03:00

0.1188.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-04-06 21:41:34+03:00

0.1187.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added a mutation for removing all parentheses from formulas, BI-2367  [ https://a.yandex-team.ru/arc/commit/8052957 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-06 13:23:03+03:00

0.1186.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Removed effective access_mode in favor of the real one, BI-2320  [ https://a.yandex-team.ru/arc/commit/8052708 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-06 12:41:40+03:00

0.1185.0
--------

* [hhell](http://staff/hhell)

 * BI-1478: filter_compiler DATETIMETZ  [ https://a.yandex-team.ru/arc/commit/8049491 ]
 * make out bi_utils.sanitize           [ https://a.yandex-team.ru/arc/commit/8049486 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed CliqueNotRunning error  [ https://a.yandex-team.ru/arc/commit/8043936 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-05 15:01:21+03:00

0.1184.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-2345: dataset-api fixes for billing connector  [ https://a.yandex-team.ru/arc/commit/8043792 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-02 12:41:39+03:00

0.1183.0
--------

* [hhell](http://staff/hhell)

 * BI-2201: crawlers reorganization  [ https://a.yandex-team.ru/arc/commit/8041004 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed Dataset.get_result_schema method  [ https://a.yandex-team.ru/arc/commit/8039973 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-01 16:02:10+03:00

0.1182.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2132] Final refactoring/TODOs cleanup  [ https://a.yandex-team.ru/arc/commit/8038261 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-04-01 09:39:45+03:00

0.1181.0
--------

* [hhell](http://staff/hhell)

 * BI-2347: add fields to dataset/result by default  [ https://a.yandex-team.ru/arc/commit/8038156 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-31 19:05:21+03:00

0.1180.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added removal of extra dimensions from the WITHIN clause  [ https://a.yandex-team.ru/arc/commit/8037289 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-31 15:54:43+03:00

0.1179.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Removed duplication of dataset in API with only the nested version remaining, BI-1009  [ https://a.yandex-team.ru/arc/commit/8035987 ]

* [kchupin](http://staff/kchupin)

 * [BI-2132][BI-1615] Data source schema fetching refactoring/optimization  [ https://a.yandex-team.ru/arc/commit/8034036 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-31 12:52:18+03:00

0.1178.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-03-30 11:30:31+03:00

0.1177.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2329 connections hardcoded data refactoring - split envs  [ https://a.yandex-team.ru/arc/commit/8030751 ]

* [kchupin](http://staff/kchupin)

 * [BI-2132] Defaulting missing index_info_set in source object in dataset API  [ https://a.yandex-team.ru/arc/commit/8030190 ]

* [hhell](http://staff/hhell)

 * maintenance tooling minor convenience  [ https://a.yandex-team.ru/arc/commit/8029529 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-30 11:18:53+03:00

0.1176.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-03-29 12:27:03+03:00

0.1175.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2132] Env flag for index fetching           [ https://a.yandex-team.ru/arc/commit/7999050 ]
 * [BI-2132] Handling indexes info in dataset-api  [ https://a.yandex-team.ru/arc/commit/7998608 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-26 17:40:23+03:00

0.1174.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added date dimension to bfb by default, BI-2264                                                      [ https://a.yandex-team.ru/arc/commit/7994517 ]
 * Updated NormalizeBeforeFilterByMutation so that it normalizes non-window function BFBs too, BI-2264  [ https://a.yandex-team.ru/arc/commit/7994383 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-25 15:42:16+03:00

0.1173.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-03-25 01:51:01+03:00

0.1172.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added validation of IGNORE DIMENSIONS, BI-2267  [ https://a.yandex-team.ru/arc/commit/7989326 ]

* [hhell](http://staff/hhell)

 * BI-2261: disable the preload_mat_connection_for_ds heuristic for now  [ https://a.yandex-team.ru/arc/commit/7987952 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-24 23:56:53+03:00

0.1171.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-03-23 18:46:54+03:00

0.1170.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-03-23 16:24:53+03:00

0.1169.0
--------

* [kchupin](http://staff/kchupin)

 * [BI] Ability to set service registry for via maintenance USM factory  [ https://a.yandex-team.ru/arc/commit/7985844 ]

* [hhell](http://staff/hhell)

 * BI-2261: determine mat-connection dependency by dataset role  [ https://a.yandex-team.ru/arc/commit/7985030 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-23 15:07:07+03:00

0.1168.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Implemented IGNORE DIMENSIONS for fork functions, BI-2267          [ https://a.yandex-team.ru/arc/commit/7984797 ]
 * Implemented BEFORE FILTER BY handling for fork functions, BI-2264  [ https://a.yandex-team.ru/arc/commit/7984199 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-23 08:33:20+03:00

0.1167.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-03-22 18:31:18+03:00

0.1166.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-03-22 17:20:37+03:00

0.1165.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Made data requests stateless in dsmaker, BI-2290  [ https://a.yandex-team.ru/arc/commit/7981036 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-22 12:58:21+03:00

0.1164.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2297: switch off preview subquery for ClickHouseFiltered datasources (music, geolayers, billing)  [ https://a.yandex-team.ru/arc/commit/7964262 ]

* [gstatsenko](http://staff/gstatsenko)

 * Fixed test_mat_connection_replacement_types after csv hack  [ https://a.yandex-team.ru/arc/commit/7963333 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-19 19:38:13+03:00

0.1163.0
--------

* [hhell](http://staff/hhell)

 * tier1 tests: bi-api formula dep  [ https://a.yandex-team.ru/arc/commit/7958115 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-18 15:27:59+03:00

0.1162.0
--------

* [asnytin](http://staff/asnytin)

 * BI-758: metrica available counters api  [ https://a.yandex-team.ru/arc/commit/7957635 ]

* [gstatsenko](http://staff/gstatsenko)

 * Separated RemapBfbMutation from WindowFunctionLevelTagMutation for use with non-window functions, BI-2264  [ https://a.yandex-team.ru/arc/commit/7956758 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-17 17:57:13+03:00

0.1161.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Allow only CSV in multi-connection datasets, BI-1863                      [ https://a.yandex-team.ru/arc/commit/7955804 ]
 * Switched to ANTLR in FieldDepGenCrawler                                   [ https://a.yandex-team.ru/arc/commit/7955387 ]
 * Added ApiResponse object for use as the result of api calls               [ https://a.yandex-team.ru/arc/commit/7951939 ]
 * Added support for BEFORE FILTER BY clauses in regular funcitons, BI-2264  [ https://a.yandex-team.ru/arc/commit/7948594 ]

* [hhell](http://staff/hhell)

 * BI-2101: use grpc-callback-based async YCAuthService      [ https://a.yandex-team.ru/arc/commit/7954529 ]
 * BI-2101: new YCAuthService aio middleware implementation  [ https://a.yandex-team.ru/arc/commit/7949938 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-17 13:01:53+03:00

0.1160.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * DLHELP-2015: fix metrika counter_id validation  [ https://a.yandex-team.ru/arc/commit/7947293 ]

* [kchupin](http://staff/kchupin)

 * [BI-2132] Migration from raw_schema to SchemaInfo in datasource schema fetching mechanism                                            [ https://a.yandex-team.ru/arc/commit/7943783 ]
 * [BI-2258] YC API exceptions clarification in uploads robot creation process (correct response if there is no permissions to get SA)  [ https://a.yandex-team.ru/arc/commit/7939306 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed a bit of unused code  [ https://a.yandex-team.ru/arc/commit/7943224 ]
 * Removed debug print           [ https://a.yandex-team.ru/arc/commit/7941189 ]

* [alexeykruglov](http://staff/alexeykruglov)

 * BI-1862: refactoring tests  [ https://a.yandex-team.ru/arc/commit/7939712 ]

* [hhell](http://staff/hhell)

 * finish tier1 tests dependencies reorganization  [ https://a.yandex-team.ru/arc/commit/7939155 ]
 * BI-2262: CH 20.8 in the tests                   [ https://a.yandex-team.ru/arc/commit/7938778 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-15 11:37:26+03:00

0.1159.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-03-11 11:25:03+03:00

0.1158.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-03-10 19:39:46+03:00

0.1157.0
--------

* [hhell](http://staff/hhell)

 * minor restyling  [ https://a.yandex-team.ru/arc/commit/7935372 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-10 19:00:52+03:00

0.1156.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2258] YC API exceptions clarification in uploads robot creation process (final error message tuning)  [ https://a.yandex-team.ru/arc/commit/7934619 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-10 15:24:24+03:00

0.1155.0
--------

* [hhell](http://staff/hhell)

 * yet another small tests fix  [ https://a.yandex-team.ru/arc/commit/7933390 ]

* [alexeykruglov](http://staff/alexeykruglov)

 * BI-2190: created_via option for datasets  [ https://a.yandex-team.ru/arc/commit/7932873 ]

* [kchupin](http://staff/kchupin)

 * [BI-2258] YC API exceptions clarification in uploads robot creation process  [ https://a.yandex-team.ru/arc/commit/7932362 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-10 13:14:27+03:00

0.1154.0
--------

* [hhell](http://staff/hhell)

 * BI-1323: gsheets in the internal installation over gozora  [ https://a.yandex-team.ru/arc/commit/7931530 ]
 * crawler supplementer for the internal installation         [ https://a.yandex-team.ru/arc/commit/7930715 ]

* [kchupin](http://staff/kchupin)

 * BI Crawler tooling: get YAV user name from env for SSH auth case  [ https://a.yandex-team.ru/arc/commit/7930887 ]

* [gstatsenko](http://staff/gstatsenko)

 * Some cleanup and minor fixes in validation and schemas  [ https://a.yandex-team.ru/arc/commit/7930592 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-09 18:30:11+03:00

0.1153.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2024: BillingAnalyticsCHConnection  [ https://a.yandex-team.ru/arc/commit/7930134 ]

* [hhell](http://staff/hhell)

 * tests fix                                                        [ https://a.yandex-team.ru/arc/commit/7929921 ]
 * BI-2101: normalize bi_cloud_integration sync/async naming a bit  [ https://a.yandex-team.ru/arc/commit/7923976 ]
 * BI-2246: assorted tier1 python3.9-related fixes                  [ https://a.yandex-team.ru/arc/commit/7920895 ]

* [kchupin](http://staff/kchupin)

 * [BI-2161] Use direct IAM/RM GRPC channels instead of API adapter  [ https://a.yandex-team.ru/arc/commit/7922913 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-09 14:48:37+03:00

0.1152.0
--------

* [hhell](http://staff/hhell)

 * BI-2101: preliminary cleanup and refactoring  [ https://a.yandex-team.ru/arc/commit/7915831 ]
 * BI-1323: gsheets materialization fix          [ https://a.yandex-team.ru/arc/commit/7915414 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-04 12:48:55+03:00

0.1151.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-03-02 18:06:41+03:00

0.1150.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-03-02 15:57:15+03:00

0.1149.0
--------

* [hhell](http://staff/hhell)

 * BI-1323 BI-1393: use compeng for options filters/aggregations for statfacereportsql/gsheets  [ https://a.yandex-team.ru/arc/commit/7911782 ]

* [kchupin](http://staff/kchupin)

 * [BI-2200] Gather all testing folder IDs in bi_testing                               [ https://a.yandex-team.ru/arc/commit/7911739 ]
 * [BI-2200] Migration from yndx-datalens to yndx-datalens-back-1/2 in bi_core/bi_api  [ https://a.yandex-team.ru/arc/commit/7907871 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-03-02 13:33:20+03:00

0.1148.0
--------

* [hhell](http://staff/hhell)

 * BI-1323: gsheets type-casting fix for dates and datetimes  [ https://a.yandex-team.ru/arc/commit/7905247 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-27 19:14:35+03:00

0.1147.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2166: metrica multiple counters  [ https://a.yandex-team.ru/arc/commit/7903766 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-26 18:21:23+03:00

0.1146.0
--------

* [asnytin](http://staff/asnytin)

 * BI-2240: fixed test connection with empty name  [ https://a.yandex-team.ru/arc/commit/7901735 ]

* [hhell](http://staff/hhell)

 * BI-2101: preliminary cleanup: remove staging  [ https://a.yandex-team.ru/arc/commit/7899327 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-26 14:25:57+03:00

0.1145.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-02-25 09:21:17+03:00

0.1144.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-02-24 20:05:37+03:00

0.1143.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added profiling spans to validation                                                [ https://a.yandex-team.ru/arc/commit/7889398 ]
 * Fixed 'NoneType' object has no attribute 'get_feature_usage_specs' error, BI-2096  [ https://a.yandex-team.ru/arc/commit/7886768 ]
 * Fixed messages in access_mode crawler                                              [ https://a.yandex-team.ru/arc/commit/7881285 ]

* [alexeykruglov](http://staff/alexeykruglov)

 * BI-1862 (ch_over_yt_user_auth): auth with Cookie and Authorization headers  [ https://a.yandex-team.ru/arc/commit/7888319 ]

* [hhell](http://staff/hhell)

 * BI-1323: gsheets PoC                              [ https://a.yandex-team.ru/arc/commit/7887833 ]
 * from __future__ import annotations                [ https://a.yandex-team.ru/arc/commit/7886187 ]
 * BI-1323: gsheets work and associated refactoring  [ https://a.yandex-team.ru/arc/commit/7871635 ]

* [kchupin](http://staff/kchupin)

 * [BI-1967] Refactoring to prepare to remove ylog: setting endpoint_code via RequestLoggingContextController & apply RequestLoggingContextControllerMiddleWare to all flask apps & validate flask middleware chains  [ https://a.yandex-team.ru/arc/commit/7873980 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-24 11:52:22+03:00

0.1142.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added AccessModeNormalizerCrawler  [ https://a.yandex-team.ru/arc/commit/7870344 ]

* [hhell](http://staff/hhell)

 * BI-1323: refactoring into us_connection_base  [ https://a.yandex-team.ru/arc/commit/7868569 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-19 10:51:48+03:00

0.1141.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Getting FORMULA_PARSER_TYPE from env  [ https://a.yandex-team.ru/arc/commit/7868236 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-18 10:14:42+03:00

0.1140.0
--------

* [hhell](http://staff/hhell)

 * BI-1323: gsheets connector partial  [ https://a.yandex-team.ru/arc/commit/7865417 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-17 18:18:22+03:00

0.1139.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Moved parser stats logging to parser factory  [ https://a.yandex-team.ru/arc/commit/7864943 ]
 * Added FORMULA_PARSER_TYPE to app settings     [ https://a.yandex-team.ru/arc/commit/7862566 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-17 12:18:35+03:00

0.1138.0
--------

* [hhell](http://staff/hhell)

 * BI-2175: RLS 'userid: userid' support  [ https://a.yandex-team.ru/arc/commit/7857784 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-15 13:31:44+03:00

0.1137.0
--------

* [hhell](http://staff/hhell)

 * BI-2175: minor rls refactoring  [ https://a.yandex-team.ru/arc/commit/7853445 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-12 20:56:17+03:00

0.1136.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-02-11 13:19:43+03:00

0.1135.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added AntlrPyParser, BI-2164  [ https://a.yandex-team.ru/arc/commit/7847845 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-11 10:33:04+03:00

0.1134.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added median parse time by length to parser stats  [ https://a.yandex-team.ru/arc/commit/7847572 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-10 22:20:27+03:00

0.1133.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-1967] Endpoint code for DatasetFieldsView  [ https://a.yandex-team.ru/arc/commit/7846588 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-10 19:19:01+03:00

0.1132.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-02-10 15:29:59+03:00

0.1131.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added nested object for dataset in API, BI-1009  [ https://a.yandex-team.ru/arc/commit/7845045 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-10 12:56:26+03:00

0.1130.0
--------

* [alexeykruglov](http://staff/alexeykruglov)

 * BI-1862: new connection type ch_over_yt_user_auth

В этом PR поведение нового типа соединения не должно отличаться от ch_over_yt  [ https://a.yandex-team.ru/arc/commit/7842352 ]

* [kchupin](http://staff/kchupin)

 * [BI-1967] Open Tracing service name canonization for Cloud  [ https://a.yandex-team.ru/arc/commit/7842250 ]

* [gstatsenko](http://staff/gstatsenko)

 * Fixed import of datetime module  [ https://a.yandex-team.ru/arc/commit/7842149 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-09 17:42:20+03:00

0.1129.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-02-09 11:53:45+03:00

0.1128.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added EmptyQuery, DLHELP-1744  [ https://a.yandex-team.ru/arc/commit/7840310 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-09 10:56:41+03:00

0.1127.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added more parser stats           [ https://a.yandex-team.ru/arc/commit/7838995 ]
 * Renamed several TypeVars to *_TV  [ https://a.yandex-team.ru/arc/commit/7838295 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-08 18:00:47+03:00

0.1126.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added ORDER BY deduplication to fix BI-2138  [ https://a.yandex-team.ru/arc/commit/7837540 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-08 12:20:00+03:00

0.1125.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-1967] use_jaeger_tracer flag reading in sync bi-api  [ https://a.yandex-team.ru/arc/commit/7833745 ]
 * [BI-1967] Open tracing Flask instrumentation             [ https://a.yandex-team.ru/arc/commit/7833593 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added BiApiServiceRegistry  [ https://a.yandex-team.ru/arc/commit/7833403 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-07 14:15:39+03:00

0.1124.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-02-05 15:17:00+03:00

0.1123.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-2093] USM connection cache generification & ability to use public_usm_workaround_middleware without materialization connection  [ https://a.yandex-team.ru/arc/commit/7831543 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-05 13:14:11+03:00

0.1122.0
--------

* [hhell](http://staff/hhell)

 * BI-2135: pass the explain_frac to the connect_options  [ https://a.yandex-team.ru/arc/commit/7830601 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-04 18:25:07+03:00

0.1121.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed usage of MaterializationNotFinished  [ https://a.yandex-team.ru/arc/commit/7828948 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-04 11:56:21+03:00

0.1120.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-02-04 10:14:46+03:00

0.1119.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Separated env-specific factories from SRFactory  [ https://a.yandex-team.ru/arc/commit/7825911 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-03 18:46:12+03:00

0.1118.0
--------

* [hhell](http://staff/hhell)

 * BI-1478: datetimetz cast fix  [ https://a.yandex-team.ru/arc/commit/7824546 ]

* [asnytin](http://staff/asnytin)

 * BI-2163: DEFAULT_MAX_AVATARS 20 -> 32  [ https://a.yandex-team.ru/arc/commit/7823168 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-03 12:20:30+03:00

0.1117.0
--------

* [hhell](http://staff/hhell)

 * BI-2135: explain select to log (postgresql)  [ https://a.yandex-team.ru/arc/commit/7818260 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed undocumented functions from options  [ https://a.yandex-team.ru/arc/commit/7818110 ]

* [kchupin](http://staff/kchupin)

 * [BI-1967] Jaeger tracing in bi_core & bi_api tests  [ https://a.yandex-team.ru/arc/commit/7813522 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-02-02 01:50:10+03:00

0.1116.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-01-29 21:01:06+03:00

0.1115.0
--------

* [hhell](http://staff/hhell)

 * BI-2135 BI-970: slightly refactored CHYT mirroring code, support DL_CHYT_MIRRORING_FRAC  [ https://a.yandex-team.ru/arc/commit/7788988 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-29 19:37:32+03:00

0.1114.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-01-28 18:57:26+03:00

0.1113.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-1967] Base implementation of Jaeger tracing for data-api & sync rqe  [ https://a.yandex-team.ru/arc/commit/7788690 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-28 18:05:39+03:00

0.1112.0
--------

* [hhell](http://staff/hhell)

 * wrap attr.evolve in a 'clone' method, instead of 'replace' or 'update'

```        """ Convenience method so that callers don't need to know about `attr` """```

Renamed: `FilterParams.replace  RequestContextInfo.replace  ClickHouseSyncAdapterConnExecutor.replace  CHConnectOptions.replace  SchemaColumn.update  SchemaColumn.replace  GenericNativeType.replace  ExpressionCtx.replace`  [ https://a.yandex-team.ru/arc/commit/7783633 ]
 * BI-1478: avg(datetimetz)                                                                                                                                                                                                                                                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/7783280 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-28 12:25:02+03:00

0.1111.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-2015: fix for w/o billing installations  [ https://a.yandex-team.ru/arc/commit/7781889 ]

* [hhell](http://staff/hhell)

 * BI-1478: datetimetz filters and casts lists  [ https://a.yandex-team.ru/arc/commit/7781869 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-26 18:39:42+03:00

0.1110.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-1831: conn_availability info: return 'hidden'

some fixes

BI-1831: conn_availability info: return 'hidden'  [ https://a.yandex-team.ru/arc/commit/7780211 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-26 13:43:17+03:00

0.1109.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added AT_DATE function, BI-2089                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/7773411 ]
 * Mass renaming of bi.expression to bi_query and some of the nested modules too

bi.expression -> bi_query
.compilation.query -> .compilation.primitives
.raw -> .formalization  [ https://a.yandex-team.ru/arc/commit/7773375 ]
 * Added logging of parser stats                                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/7771876 ]

* [hhell](http://staff/hhell)

 * bi_utils.aio                     [ https://a.yandex-team.ru/arc/commit/7773410 ]
 * tests: another timeout increase  [ https://a.yandex-team.ru/arc/commit/7772556 ]

* [kchupin](http://staff/kchupin)

 * [BI-1967] Jaeger client to bi_core dependencies + tier1 requirements sync  [ https://a.yandex-team.ru/arc/commit/7772359 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-25 12:35:09+03:00

0.1108.0
--------

* [hhell](http://staff/hhell)

 * fix bi-api rqe  [ https://a.yandex-team.ru/arc/commit/7770026 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-21 18:38:17+03:00

0.1107.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-2105: fix billing_checker usage for int dl  [ https://a.yandex-team.ru/arc/commit/7769066 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-21 14:39:07+03:00

0.1106.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-01-21 00:08:41+03:00

0.1105.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-2105: support free int sessions in bi-api  [ https://a.yandex-team.ru/arc/commit/7767598 ]
 * DLHELP-1633: fix split func in compeng        [ https://a.yandex-team.ru/arc/commit/7767476 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-20 23:54:52+03:00

0.1104.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-01-20 18:42:36+03:00

0.1103.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-01-19 16:13:01+03:00

0.1102.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-870] Refactor get_log_record(caplog, ...)  [ https://a.yandex-team.ru/arc/commit/7761122 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-19 15:14:27+03:00

0.1101.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-870] Mechanism to ignore part of unknown fields in schemas  [ https://a.yandex-team.ru/arc/commit/7758661 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-18 18:31:01+03:00

0.1100.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-01-18 15:12:18+03:00

0.1099.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-01-15 16:49:49+03:00

0.1098.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-01-15 14:56:42+03:00

0.1097.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-01-15 12:47:55+03:00

0.1096.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-870] Moving connection schemas tests in unit instead of db         [ https://a.yandex-team.ru/arc/commit/7752592 ]
 * [BI-870] New schema of connection validation during API create/update  [ https://a.yandex-team.ru/arc/commit/7749371 ]

* [hhell](http://staff/hhell)

 * BI-1896: use YC ts.private-api gRPC instead of identity.private-api for get_yc_service_token  [ https://a.yandex-team.ru/arc/commit/7751602 ]

* [gstatsenko](http://staff/gstatsenko)

 * Fixed debug query in response. BI-2109  [ https://a.yandex-team.ru/arc/commit/7751229 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-15 12:20:05+03:00

0.1095.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-870] Temporary workaround for https://st.yandex-team.ru/CHARTS-3484  [ https://a.yandex-team.ru/arc/commit/7742250 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-1867: new teamcity build job  [ https://a.yandex-team.ru/arc/commit/7741872 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-13 16:01:19+03:00

0.1094.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed usage of avatarless measures in AGO, BI-2092  [ https://a.yandex-team.ru/arc/commit/7740855 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-11 14:28:28+03:00

0.1093.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2021-01-11 13:25:48+03:00

0.1092.0
--------

* [hhell](http://staff/hhell)

 * BI-1849: tier1 fixes                                                                                                                                                                                                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/7730491 ]
 * BI-1849: bicommon -> bi_core

`arc mv lib/bicommon/bicommon lib/bicommon/bi_core && arc mv lib/bicommon lib/bi_core && find . -name '*.py' -or -name '*.ini' -or -name '*.make' -or -name run -or -name '*.conf' -or -name '*.hjson' -or -name '*.lst' | xargs -d '\n' sed -r -i 's|bicommon|bi_core|g; s|yandex-bi_core|yandex-bi-core|g'`  [ https://a.yandex-team.ru/arc/commit/7729715 ]
 * BI-1849: makefile fixes                                                                                                                                                                                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/7729531 ]
 * BI-1849: lib/testenv-common, pycharm structure                                                                                                                                                                                                                                                                                               [ https://a.yandex-team.ru/arc/commit/7728193 ]
 * BI-593: mass-move into app/                                                                                                                                                                                                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/7727584 ]
 * BI-1849: tier1 deps fix, bi_configs dep to bi_cloud_integrations                                                                                                                                                                                                                                                                             [ https://a.yandex-team.ru/arc/commit/7724517 ]
 * BI-1849: reorganize into lib/                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/7724253 ]
 * BI-1511: nativetype parametrization in the API layer                                                                                                                                                                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/7723390 ]
 * BI-2058: YC GRPC client helpers                                                                                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/7723385 ]

* [kchupin](http://staff/kchupin)

 * [BI-870] Framework for unified connection schemas (create/read/update/test) + ClickHouse example  [ https://a.yandex-team.ru/arc/commit/7727087 ]

[robot-statinfra](http://staff/robot-statinfra) 2021-01-11 12:56:17+03:00

0.1091.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * A generalization of AGO for future addition of other lookup functions  [ https://a.yandex-team.ru/arc/commit/7718497 ]
 * Added FieldDepGenCrawler, BI-2066                                      [ https://a.yandex-team.ru/arc/commit/7717699 ]

* [hhell](http://staff/hhell)

 * BI-1746: bi-api mypy  [ https://a.yandex-team.ru/arc/commit/7718161 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-25 19:21:48+03:00

0.1090.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-12-24 13:15:01+03:00

0.1089.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Removed some logging from query handlers  [ https://a.yandex-team.ru/arc/commit/7713125 ]

* [kchupin](http://staff/kchupin)

 * [BI-870] Connection BI API schema: remove usage of name in data/meta + remove possibly useless fields in .as_dict()  [ https://a.yandex-team.ru/arc/commit/7712669 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-23 19:04:32+03:00

0.1088.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added AGO(measure, date_dimension, number) variant, BI-974  [ https://a.yandex-team.ru/arc/commit/7708876 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-23 10:57:54+03:00

0.1087.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed handling of AGO without any dimensions in the query  [ https://a.yandex-team.ru/arc/commit/7708616 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-22 14:03:13+03:00

0.1086.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * NestedQueryForkBoundary now counts nesting levels from bottom instead of top, BI-2085  [ https://a.yandex-team.ru/arc/commit/7706104 ]
 * BI-2079: Added handling of NoCommonRoleError in preview                                [ https://a.yandex-team.ru/arc/commit/7705230 ]

* [hhell](http://staff/hhell)

 * tests: tier1: attempt to pin pip version  [ https://a.yandex-team.ru/arc/commit/7704920 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-21 17:03:18+03:00

0.1085.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-12-21 10:31:07+03:00

0.1084.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed recognition of structurally identical dimensions in window functions and AGO, BI-2081  [ https://a.yandex-team.ru/arc/commit/7703965 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-21 10:00:22+03:00

0.1083.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Removed AGO-related classes and logic that is no longer used  [ https://a.yandex-team.ru/arc/commit/7682221 ]
 * Added support for JOINs and AGO in compeng                    [ https://a.yandex-team.ru/arc/commit/7682217 ]

* [hhell](http://staff/hhell)

 * BI-2070 BI-2074: tests: use images from yandex registry, use common versions  [ https://a.yandex-team.ru/arc/commit/7682129 ]
 * BI-1507: forced https in the message                                          [ https://a.yandex-team.ru/arc/commit/7680266 ]
 * fix of a fix                                                                  [ https://a.yandex-team.ru/arc/commit/7680235 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-18 12:08:20+03:00

0.1082.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed AGO with invalid number of args, added test for various AGO errors  [ https://a.yandex-team.ru/arc/commit/7679828 ]
 * Removed unused import and definition of ABOVE_AGG_SLICER_CONFIG           [ https://a.yandex-team.ru/arc/commit/7678507 ]
 * Refactored Statface planner for nested window function support, BI-1995   [ https://a.yandex-team.ru/arc/commit/7676920 ]

* [hhell](http://staff/hhell)

 * tests: fix bi-api tests rqe, invalid-chyt-clique test  [ https://a.yandex-team.ru/arc/commit/7679392 ]
 * BI-1439: rls+preview wildcard-user case fix            [ https://a.yandex-team.ru/arc/commit/7677478 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-17 16:00:26+03:00

0.1081.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-12-16 16:26:45+03:00

0.1080.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-12-16 14:57:50+03:00

0.1079.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-12-16 14:48:06+03:00

0.1078.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-12-16 13:33:15+03:00

0.1077.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-12-16 13:23:41+03:00

0.1076.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed filters that reference avatars not used by any other expressions, BI-2068  [ https://a.yandex-team.ru/arc/commit/7673886 ]

* [hhell](http://staff/hhell)

 * mypy tuning  [ https://a.yandex-team.ru/arc/commit/7673763 ]

* [shadchin](http://staff/shadchin)

 * Fix flake8 checks  [ https://a.yandex-team.ru/arc/commit/7673168 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-15 17:56:28+03:00

0.1075.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * BI-2065: Fixed JOIN optimization  [ https://a.yandex-team.ru/arc/commit/7672380 ]

* [asnytin](http://staff/asnytin)

 * BI-1696: env variables cleanup and cloud-deploy sync

https://st.yandex-team.ru/BI-1696  [ https://a.yandex-team.ru/arc/commit/7671045 ]

* [hhell](http://staff/hhell)

 * BI-1507: enforce https urls in logs                [ https://a.yandex-team.ru/arc/commit/7670775 ]
 * BI-1507: async routing and view naming correction  [ https://a.yandex-team.ru/arc/commit/7670188 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-15 11:36:25+03:00

0.1074.0
--------

* [hhell](http://staff/hhell)

 * fix  [ https://a.yandex-team.ru/arc/commit/7669227 ]

* [kchupin](http://staff/kchupin)

 * [BI-1841] Unified recursive json sensitive fields masking procedure for logging (client req body & us client)  [ https://a.yandex-team.ru/arc/commit/7669193 ]

* [gstatsenko](http://staff/gstatsenko)

 * Switched to using explicitly listed dimensions in QueryFork  [ https://a.yandex-team.ru/arc/commit/7665700 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-14 14:52:57+03:00

0.1073.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-12-11 19:33:40+03:00

0.1072.0
--------

* [hhell](http://staff/hhell)

 * fix  [ https://a.yandex-team.ru/arc/commit/7663424 ]

* [gstatsenko](http://staff/gstatsenko)

 * Several typing fixes in bi-formula  [ https://a.yandex-team.ru/arc/commit/7663119 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-11 15:26:43+03:00

0.1071.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-12-11 13:35:25+03:00

0.1070.0
--------

* [hhell](http://staff/hhell)

 * BI-2053: tests: common and detailed wait_for  [ https://a.yandex-team.ru/arc/commit/7662559 ]

* [gstatsenko](http://staff/gstatsenko)

 * Fixed ORDER BY fields that depend on fields with window functions, BI-2006  [ https://a.yandex-team.ru/arc/commit/7662518 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-11 12:30:02+03:00

0.1069.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-1428] More granular handling of deleted dependencies in USM  [ https://a.yandex-team.ru/arc/commit/7660122 ]

* [hhell](http://staff/hhell)

 * BI-2047: bi_postgres: support datetimes in literal_binds  [ https://a.yandex-team.ru/arc/commit/7659815 ]
 * bi-api: preliminary mypy changes                          [ https://a.yandex-team.ru/arc/commit/7659813 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-10 16:11:20+03:00

0.1068.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-12-10 14:02:31+03:00

0.1067.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed the usage of the collect_stats flag              [ https://a.yandex-team.ru/arc/commit/7659159 ]
 * Fixed role resolution for preview mode in DatasetView  [ https://a.yandex-team.ru/arc/commit/7659066 ]

* [hhell](http://staff/hhell)

 * BI-1981: finish refactoring                        [ https://a.yandex-team.ru/arc/commit/7658968 ]
 * BI-1968: an additional CHYT error db_message test  [ https://a.yandex-team.ru/arc/commit/7658964 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-10 12:36:54+03:00

0.1066.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed error with validation_mode, fixed quantiles, fixed error handling test  [ https://a.yandex-team.ru/arc/commit/7657341 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-09 18:36:05+03:00

0.1065.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fix for feature functions without user-managed avatars, BI-2045  [ https://a.yandex-team.ru/arc/commit/7656073 ]
 * Added collection of function translation stats, BI-2036          [ https://a.yandex-team.ru/arc/commit/7656065 ]
 * AGO function, BI-974                                             [ https://a.yandex-team.ru/arc/commit/7656052 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-09 14:12:31+03:00

0.1064.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed query logging  [ https://a.yandex-team.ru/arc/commit/7654262 ]

* [hhell](http://staff/hhell)

 * BI-1968: remove a huge rarely involved dependency on yt client  [ https://a.yandex-team.ru/arc/commit/7654247 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-09 00:53:47+03:00

0.1063.0
--------

* [hhell](http://staff/hhell)

 * tests on vanga                                                                                                                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/7652142 ]
 * refactor: split populate_dataset_from_body logic into multiple methods                                                                                                                                                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/7648358 ]
 * sqlalchemy-1.3 in all datalens/backend                                                                                                                                                                                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/7640847 ]
 * BI-1439: RLS handling in preview requests                                                                                                                                                                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/7634096 ]
 * CONTRIB-2042: contrib/python/sqlalchemy -> contrib/python/sqlalchemy/sqlalchemy-1.2

<!-- DEVEXP BEGIN -->
![review](https://codereview.in.yandex-team.ru/badges/review-in_progress-yellow.svg) [![inngonch](https://codereview.in.yandex-team.ru/badges/inngonch-...-yellow.svg)](https://staff.yandex-team.ru/inngonch)
<!-- DEVEXP END -->  [ https://a.yandex-team.ru/arc/commit/7633357 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved db/test_postprocessors.py to unit/query/postprocessing/test_markup.py  [ https://a.yandex-team.ru/arc/commit/7641061 ]
 * Moved relation errors into component_errors, BI-2033                         [ https://a.yandex-team.ru/arc/commit/7641054 ]

* [kchupin](http://staff/kchupin)

 * [BI-1818] Minor improvements in connection tester exception generation  [ https://a.yandex-team.ru/arc/commit/7639420 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-2019: fix public once again    [ https://a.yandex-team.ru/arc/commit/7635688 ]
 * BI-2019: fix sample ch in public  [ https://a.yandex-team.ru/arc/commit/7635291 ]

* [asnytin](http://staff/asnytin)

 * BI-1975: issue mat db on first materialization  [ https://a.yandex-team.ru/arc/commit/7633374 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-12-08 16:19:49+03:00

0.1062.0
--------

* [hhell](http://staff/hhell)

 * BI-1485: couple more wrapped exceptions  [ https://a.yandex-team.ru/arc/commit/7624338 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-30 20:05:54+03:00

0.1061.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-11-26 11:57:03+03:00

0.1060.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-11-25 17:42:21+03:00

0.1059.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-11-25 13:49:33+03:00

0.1058.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed usage of target connection from exec_info  [ https://a.yandex-team.ru/arc/commit/7617420 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-24 21:21:15+03:00

0.1057.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added SourceDbOperationProcessor, BI-974  [ https://a.yandex-team.ru/arc/commit/7616882 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-24 18:33:05+03:00

0.1056.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added RecursiveSlicer, BI-974  [ https://a.yandex-team.ru/arc/commit/7613662 ]

* [hhell](http://staff/hhell)

 * BI-1927: dashsql postgresql, chyt, disable_value_processing  [ https://a.yandex-team.ru/arc/commit/7613661 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-24 15:19:55+03:00

0.1055.0
--------

* [hhell](http://staff/hhell)

 * auxiliary move                                                                          [ https://a.yandex-team.ru/arc/commit/7612718 ]
 * BI-1992: subselect source template 'disabled' flag; chyt subselect fixes and additions  [ https://a.yandex-team.ru/arc/commit/7601294 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-23 17:36:06+03:00

0.1054.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-11-20 12:06:08+03:00

0.1053.0
--------

* [asnytin](http://staff/asnytin)

 * BI-1911: fixed CloudSRFactory blackbox client init  [ https://a.yandex-team.ru/arc/commit/7599364 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-20 11:09:17+03:00

0.1052.0
--------

* [asnytin](http://staff/asnytin)

 * BI-1911: changed blackbox settings (for podcasters connector)  [ https://a.yandex-team.ru/arc/commit/7598728 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-20 01:28:28+03:00

0.1051.0
--------

* [hhell](http://staff/hhell)

 * BI-1927: DashSQL API                                        [ https://a.yandex-team.ru/arc/commit/7598524 ]
 * minor tests warning fix                                     [ https://a.yandex-team.ru/arc/commit/7598513 ]
 * BI-1485: CH async body-read timeout wrap [run large tests]  [ https://a.yandex-team.ru/arc/commit/7597786 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-1326: do not show internal service info in connection test  [ https://a.yandex-team.ru/arc/commit/7598472 ]

* [asnytin](http://staff/asnytin)

 * BI-1924: evolve_ch_conn_to_* tools  [ https://a.yandex-team.ru/arc/commit/7598175 ]

* [gstatsenko](http://staff/gstatsenko)

 * United CompiledQuery and CompiledFlatQuery into a single class, BI-974  [ https://a.yandex-team.ru/arc/commit/7597918 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-19 22:58:31+03:00

0.1050.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed usage of filters in statface planner                                        [ https://a.yandex-team.ru/arc/commit/7595556 ]
 * Added support for cast and aggregation attributes in clone_field action, BI-1985  [ https://a.yandex-team.ru/arc/commit/7594051 ]
 * Moved the WHERE/HAVING differentiation from planner to translator, BI-974         [ https://a.yandex-team.ru/arc/commit/7594044 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-19 01:23:38+03:00

0.1049.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-11-18 12:52:33+03:00

0.1048.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed ordering of SELECT and ORDER BY expressions in top-level queries, BI-1983  [ https://a.yandex-team.ru/arc/commit/7593049 ]
 * Added DataFetcher as a wrapper for data fetching in tests                        [ https://a.yandex-team.ru/arc/commit/7583685 ]

* [hans](http://staff/hans)

 * [BI-1944] Add MediaNation connector  [ https://a.yandex-team.ru/arc/commit/7593048 ]

* [hhell](http://staff/hhell)

 * Refactor bi.utils into a package  [ https://a.yandex-team.ru/arc/commit/7591356 ]

* [dmifedorov](http://staff/dmifedorov)

 * fix typo                  [ https://a.yandex-team.ru/arc/commit/7587806 ]
 * update monitoring config  [ https://a.yandex-team.ru/arc/commit/7587008 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-18 12:15:55+03:00

0.1047.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-11-13 21:14:04+03:00

0.1046.0
--------

* [asnytin](http://staff/asnytin)

 * BI-1911: set up ConnectorAvailability for ch_ya_music_podcast_stats  [ https://a.yandex-team.ru/arc/commit/7582011 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-1493: explicitly check user permissions on dataset-delete / mat-stop  [ https://a.yandex-team.ru/arc/commit/7581002 ]

* [hhell](http://staff/hhell)

 * update bi-materializer and bi-converter image-dependencies in the tests [run large tests]  [ https://a.yandex-team.ru/arc/commit/7579779 ]

* [gstatsenko](http://staff/gstatsenko)

 * Replaced BIQuery with QueryAndResultInfo in get_data_stream, BI-974  [ https://a.yandex-team.ru/arc/commit/7578272 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-13 16:10:35+03:00

0.1045.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * fix app init  [ https://a.yandex-team.ru/arc/commit/7578121 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-12 14:16:08+03:00

0.1044.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-11-12 14:11:36+03:00

0.1043.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-11-12 12:07:29+03:00

0.1042.0
--------

* [asnytin](http://staff/asnytin)

 * BI-1911: ConnectionClickhouseYaMusicPodcastStats, ConnectionClickhouseFrozen

BI-1911: ConnectionClickhouseUserIdFiltered
BI-1924: ConnectionClickhouseFrozen  [ https://a.yandex-team.ru/arc/commit/7576035 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-12 00:45:58+03:00

0.1041.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed context flags for WHERE and HAVING in MSSQL and ORACLE in flat query translator  [ https://a.yandex-team.ru/arc/commit/7575189 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-11 15:06:13+03:00

0.1040.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-11-11 14:23:53+03:00

0.1039.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-11-11 12:24:33+03:00

0.1038.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Removed logging from formalizer                                                   [ https://a.yandex-team.ru/arc/commit/7570997 ]
 * Removed ExecutionPlanner.plan_single_formula method because it is no longer used  [ https://a.yandex-team.ru/arc/commit/7570995 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-10 11:26:56+03:00

0.1037.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Separated ExpressionBuilder into several query processing steps, BI-974  [ https://a.yandex-team.ru/arc/commit/7569084 ]
 * Using prepared sources and relations in source builder, BI-974           [ https://a.yandex-team.ru/arc/commit/7550177 ]

* [hhell](http://staff/hhell)

 * BI-1603: stop mixing freeform_sources (data_source_template_templates) into sources (data_source_templates) for all purposes  [ https://a.yandex-team.ru/arc/commit/7567771 ]
 * BI-1951: unsupported-type fields: disallow dropdown-choice of aggregation                                                     [ https://a.yandex-team.ru/arc/commit/7551366 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-09 18:35:10+03:00

0.1036.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-1753] US entry crawler optimizations  [ https://a.yandex-team.ru/arc/commit/7547834 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-05 15:42:40+03:00

0.1035.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-1753] Alternative migration framework + implementation of crypto keys rotation procedure  [ https://a.yandex-team.ru/arc/commit/7546340 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-11-05 02:13:49+03:00

0.1034.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * OrderByExpressionCtx as subclass instead of wrapper, added attrs_evolve_to_subclass  [ https://a.yandex-team.ru/arc/commit/7534268 ]
 * A couple of fixes for BEFORE FILTER BY                                               [ https://a.yandex-team.ru/arc/commit/7533558 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-30 17:11:35+03:00

0.1033.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added handling of more CH and CHYT errors  [ https://a.yandex-team.ru/arc/commit/7526846 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-29 16:07:15+03:00

0.1032.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-10-27 19:47:10+03:00

0.1031.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-10-27 15:02:29+03:00

0.1030.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-1212] Support for formatted UUID in request ID in IAM req ID normalization  [ https://a.yandex-team.ru/arc/commit/7521180 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-27 14:54:14+03:00

0.1029.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-10-27 13:18:02+03:00

0.1028.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed slicing of queries having both BFB and non-BFB window functions, BI-1825  [ https://a.yandex-team.ru/arc/commit/7488999 ]

* [kchupin](http://staff/kchupin)

 * [BI-1753] Migration to crypto keys config instead of single fernet key [run large tests]  [ https://a.yandex-team.ru/arc/commit/7488221 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-23 11:44:20+03:00

0.1027.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed mutations for BEFORE FILTER BY and nested window functions, BI-1825  [ https://a.yandex-team.ru/arc/commit/7484213 ]
 * Added string filters for integer data type in options                      [ https://a.yandex-team.ru/arc/commit/7483210 ]
 * Added more logging to BEFORE FILTER BY functionality                       [ https://a.yandex-team.ru/arc/commit/7482997 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-1760: cloud balancers  [ https://a.yandex-team.ru/arc/commit/7483937 ]

* [kchupin](http://staff/kchupin)

 * [BI-1753] Support for unversioned data field in US  [ https://a.yandex-team.ru/arc/commit/7482714 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-21 19:04:37+03:00

0.1026.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added more logging to planner  [ https://a.yandex-team.ru/arc/commit/7482563 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-21 12:13:30+03:00

0.1025.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Nested window functions and BEFORE FILTER BY, BI-1825  [ https://a.yandex-team.ru/arc/commit/7482087 ]
 * Several fixes of feature validation                    [ https://a.yandex-team.ru/arc/commit/7480919 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-21 09:33:40+03:00

0.1024.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed filtering by window expressions if no window expression is present in main select clause, BI-1894  [ https://a.yandex-team.ru/arc/commit/7480357 ]
 * Moved some build_source arguments to the SqlSourceBuilder class as attributes, BI-974                    [ https://a.yandex-team.ru/arc/commit/7477620 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-1760: use cloud redis for caches in ext-prod  [ https://a.yandex-team.ru/arc/commit/7477895 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-20 16:29:37+03:00

0.1023.0
--------

* [dmifedorov](http://staff/dmifedorov)

 * BI-1883: fix ya.make          [ https://a.yandex-team.ru/arc/commit/7473140 ]
 * BI-1883: public with compeng  [ https://a.yandex-team.ru/arc/commit/7473107 ]
 * START_COMPENG option          [ https://a.yandex-team.ru/arc/commit/7473083 ]

* [gstatsenko](http://staff/gstatsenko)

 * Smoe minor refactoring of SqlSourceBuilder, BI-974  [ https://a.yandex-team.ru/arc/commit/7472291 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-17 01:03:43+03:00

0.1022.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed planning of source_db filters in combination with secondary compeng filters  [ https://a.yandex-team.ru/arc/commit/7470548 ]

* [hhell](http://staff/hhell)

 * minor refactoring [run large tests]                       [ https://a.yandex-team.ru/arc/commit/7469439 ]
 * BI-1735: minor configuration fix of the unsupported type  [ https://a.yandex-team.ru/arc/commit/7469120 ]
 * minor cleanup of old schemas                              [ https://a.yandex-team.ru/arc/commit/7468428 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-16 11:36:41+03:00

0.1021.0
--------

* [hhell](http://staff/hhell)

 * BI-1735: support unsupported types [run large tests]  [ https://a.yandex-team.ru/arc/commit/7467933 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added support for any number of execution levels, BI-1825  [ https://a.yandex-team.ru/arc/commit/7467048 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-15 12:24:06+03:00

0.1020.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-10-14 17:32:18+03:00

0.1019.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Removed hack with USE_SHORT_UUIDS_IN_ALIASES  [ https://a.yandex-team.ru/arc/commit/7462838 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-14 14:47:54+03:00

0.1018.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added saving of field dependencies, BI-974                                            [ https://a.yandex-team.ru/arc/commit/7459565 ]
 * Replaced Aliased node with Tagged, added new mutations for BEFORE FILTER BY, BI-1825  [ https://a.yandex-team.ru/arc/commit/7459162 ]

* [kchupin](http://staff/kchupin)

 * [BI-1753] Fix test for saved password in bi-api  [ https://a.yandex-team.ru/arc/commit/7458973 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-13 19:55:14+03:00

0.1017.0
--------

* [hhell](http://staff/hhell)

 * BI-1630: pass subselect query and db_message into err "details" [run large tests]  [ https://a.yandex-team.ru/arc/commit/7457954 ]
 * BI-1630: subselect api-tests [run large tests]                                     [ https://a.yandex-team.ru/arc/commit/7457679 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed some unused FormulaItem methods, moved enumerate_fields to inspect and updated BeforeFilterBy  [ https://a.yandex-team.ru/arc/commit/7452571 ]
 * Separated Formula Translator from slicers                                                              [ https://a.yandex-team.ru/arc/commit/7452568 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-13 12:13:26+03:00

0.1016.0
--------

* [asnytin](http://staff/asnytin)

 * BI-1837: allow_public_usage connections property  [ https://a.yandex-team.ru/arc/commit/7443298 ]
 * test_get_field_types_info datetimetz fix          [ https://a.yandex-team.ru/arc/commit/7443278 ]

* [gstatsenko](http://staff/gstatsenko)

 * Reorganized modules in bi.expressions  [ https://a.yandex-team.ru/arc/commit/7442439 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-08 17:28:13+03:00

0.1015.0
--------

[robot-statinfra](http://staff/robot-statinfra) 2020-10-06 19:01:10+03:00

0.1014.0
--------

* [hhell](http://staff/hhell)

 * BI-1824: postgresql enforce_collate full passthrough [run large tests]  [ https://a.yandex-team.ru/arc/commit/7440613 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-06 18:47:00+03:00

0.1013.0
--------

* [hhell](http://staff/hhell)

 * Exclude datetimetz from field_types for now                                                       [ https://a.yandex-team.ru/arc/commit/7440450 ]
 * BI-1630 BI-1603: subselect data sources, preliminary version; freeform_sources [run large tests]  [ https://a.yandex-team.ru/arc/commit/7439948 ]

* [gstatsenko](http://staff/gstatsenko)

 * Refactored ExecutionPlanner to work with CompiledQuery, BI-1825  [ https://a.yandex-team.ru/arc/commit/7440269 ]
 * Switched to using query compiles in DatasetView, BI-1825         [ https://a.yandex-team.ru/arc/commit/7438814 ]
 * Changed fcompiler to formula_compiler everywhere                 [ https://a.yandex-team.ru/arc/commit/7437604 ]

* [asnytin](http://staff/asnytin)

 * BI-1799: billing_invalid_metrics juggler alert config  [ https://a.yandex-team.ru/arc/commit/7423185 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-06 14:20:09+03:00

0.1012.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed access mode and compatibility resolution in options  [ https://a.yandex-team.ru/arc/commit/7422168 ]
 * Implemented QueryCompiler classes                          [ https://a.yandex-team.ru/arc/commit/7421655 ]
 * Added deduplication of connection IDs in options           [ https://a.yandex-team.ru/arc/commit/7418830 ]
 * Fixed enforcement of row_count_hard_limit                  [ https://a.yandex-team.ru/arc/commit/7418743 ]

* [hhell](http://staff/hhell)

 * BI-1824: sqlalchemy postgresql dialect subclass  [ https://a.yandex-team.ru/arc/commit/7420132 ]
 * style-tuning                                     [ https://a.yandex-team.ru/arc/commit/7418801 ]

* [kchupin](http://staff/kchupin)

 * [BI-1629] Fix field type for cache TTL in CHYT conn API schema  [ https://a.yandex-team.ru/arc/commit/7420004 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-02 15:18:05+03:00

0.1011.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Switching to using QuerySpec in DatasetView, BI-1825  [ https://a.yandex-team.ru/arc/commit/7417724 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-10-01 12:25:04+03:00

0.1010.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added processing of SourceColumnFilterSpec         [ https://a.yandex-team.ru/arc/commit/7415443 ]
 * Added more filtering and validation to formalizer  [ https://a.yandex-team.ru/arc/commit/7415270 ]

* [kchupin](http://staff/kchupin)

 * [BI-1125] More logs  [ https://a.yandex-team.ru/arc/commit/7414072 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-30 17:02:20+03:00

0.1009.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Separated FilterCompiler from FormulaCompiler              [ https://a.yandex-team.ru/arc/commit/7411224 ]
 * Fixed handling of formulas consisting of a single comment  [ https://a.yandex-team.ru/arc/commit/7410960 ]
 * Added spec formalizers                                     [ https://a.yandex-team.ru/arc/commit/7368968 ]
 * Added support for non-string containment filters, BI-1324  [ https://a.yandex-team.ru/arc/commit/7365050 ]

* [kchupin](http://staff/kchupin)

 * [BI-1461] Datalens test secrets deduplication [run large tests]  [ https://a.yandex-team.ru/arc/commit/7366141 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-29 16:53:35+03:00

0.1008.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added component dependency managers  [ https://a.yandex-team.ru/arc/commit/7361863 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-24 12:16:05+03:00

0.1007.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Added support for ASC/DESC in ORDER BY, BI-1802  [ https://a.yandex-team.ru/arc/commit/7361693 ]

* [kchupin](http://staff/kchupin)

 * [BI-1182] Check exact type of connection in publicity checker                               [ https://a.yandex-team.ru/arc/commit/7361257 ]
 * [BI-1496] Flag PUBLIC_USE_MASTER_TOKEN_FOR_MAIN_USM (default - do not use US master token)  [ https://a.yandex-team.ru/arc/commit/7360912 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-23 16:49:42+03:00

0.1006.0
--------

* [asnytin](http://staff/asnytin)

 * BI-1694: geointellect products filters  [ https://a.yandex-team.ru/arc/commit/7358435 ]

* [kchupin](http://staff/kchupin)

 * [BI-1182] Public entity usage checker logging fixes  [ https://a.yandex-team.ru/arc/commit/7358243 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-22 18:28:20+03:00

0.1005.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-1182] Public entity usage checker fix and logging improvements  [ https://a.yandex-team.ru/arc/commit/7358147 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-22 17:34:41+03:00

0.1004.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-1182] Workaround to turn off managed network usage for preprod 2  [ https://a.yandex-team.ru/arc/commit/7357256 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-22 15:18:23+03:00

0.1003.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-1182] Workaround to turn off managed network usage for preprod                                                            [ https://a.yandex-team.ru/arc/commit/7355420 ]
 * [BI-1182] Fix exc type in public connection usage checker                                                                     [ https://a.yandex-team.ru/arc/commit/7354835 ]
 * [BI-1182] Entity usage checker introduction. PublicEnvEntityUsageChecker implementation to allow public datasets over MDB CH  [ https://a.yandex-team.ru/arc/commit/7354706 ]
 * [BI-1461] Multi-env integration tests [run large tests]                                                                       [ https://a.yandex-team.ru/arc/commit/7353182 ]

* [asnytin](http://staff/asnytin)

 * BI-1694: free yandex geolayers configs  [ https://a.yandex-team.ru/arc/commit/7354830 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added schema support to Oracle  [ https://a.yandex-team.ru/arc/commit/7354621 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-22 00:26:36+03:00

0.1002.0
--------

* [asnytin](http://staff/asnytin)

 * PR from branch users/asnytin/geo_products_prod_config

BI-1694: bi-api geo products prod config

BI-1694: bi-billing: marketplace products prod config.  [ https://a.yandex-team.ru/arc/commit/7352319 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-21 13:08:11+03:00

0.1001.0
--------

* [gstatsenko](http://staff/gstatsenko)

 * Feature function usage extractors  [ https://a.yandex-team.ru/arc/commit/7349661 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-21 10:49:49+03:00

0.1000.0
--------

* [kchupin](http://staff/kchupin)

 * [BI-1212] Add associated service account ID field to provider connection schema  [ https://a.yandex-team.ru/arc/commit/7346963 ]

* [hhell](http://staff/hhell)

 * use 'replace' instead of 'updated'  [ https://a.yandex-team.ru/arc/commit/7346098 ]
 * minor fix for datetimetz/uuid cast  [ https://a.yandex-team.ru/arc/commit/7345852 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-18 18:31:03+03:00

0.999.0
-------

[robot-statinfra](http://staff/robot-statinfra) 2020-09-17 15:55:43+03:00

0.998.0
-------

* [hhell](http://staff/hhell)

 * BI-1758: fix tests                                                           [ https://a.yandex-team.ru/arc/commit/7335581 ]
 * BI-1758: extensive datetime-to-filter-a-date-column logic [run large tests]  [ https://a.yandex-team.ru/arc/commit/7335197 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-16 12:43:36+03:00

0.997.0
-------

* [gstatsenko](http://staff/gstatsenko)

 * Moved test_expressions from db to unit  [ https://a.yandex-team.ru/arc/commit/7333968 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-15 21:00:19+03:00

0.996.0
-------

[robot-statinfra](http://staff/robot-statinfra) 2020-09-15 14:37:24+03:00

0.995.0
-------

* [asnytin](http://staff/asnytin)

 * BI-1694: Audience geolayers configuration  [ https://a.yandex-team.ru/arc/commit/7331782 ]

* [hhell](http://staff/hhell)

 * BI-1121: fix for unused-import checks [run large tests]  [ https://a.yandex-team.ru/arc/commit/7329850 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-15 12:12:41+03:00

0.994.0
-------

[robot-statinfra](http://staff/robot-statinfra) 2020-09-14 17:51:29+03:00

0.993.0
-------

* [kchupin](http://staff/kchupin)

 * [BI-1212] Fix request ID normalization pattern for IAM  [ https://a.yandex-team.ru/arc/commit/7323001 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-11 19:06:32+03:00

0.992.0
-------

* [hhell](http://staff/hhell)

 * BI-1478: _datetimetz(...) support in a very unsightly way [run large tests]  [ https://a.yandex-team.ru/arc/commit/7322518 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-11 14:35:38+03:00

0.991.0
-------

* [hhell](http://staff/hhell)

 * BI-1758: filter by midnight-datetimes as dates [run large tests]  [ https://a.yandex-team.ru/arc/commit/7318895 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-10 14:42:47+03:00

0.990.0
-------

[robot-statinfra](http://staff/robot-statinfra) 2020-09-10 13:55:10+03:00

0.989.0
-------

* [kchupin](http://staff/kchupin)

 * [BI-1212] Import fix after late merge                                       [ https://a.yandex-team.ru/arc/commit/7316080 ]
 * [BI-1212] BI API handle implementation & integration tests for ext-testing  [ https://a.yandex-team.ru/arc/commit/7315397 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-1766: remove info/store  [ https://a.yandex-team.ru/arc/commit/7314532 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-10 12:35:52+03:00

0.988.0
-------

* [hhell](http://staff/hhell)

 * BI-1752: async app extended keepalive, and an env-flag to disable it completely  [ https://a.yandex-team.ru/arc/commit/7311278 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-09 12:40:56+03:00

0.987.0
-------

* [kchupin](http://staff/kchupin)

 * [BI-1461] Cloud integrations to dedicated package [run large tests]  [ https://a.yandex-team.ru/arc/commit/7286632 ]

* [hhell](http://staff/hhell)

 * BI-1511: NativeType transition step 2 [run large tests]  [ https://a.yandex-team.ru/arc/commit/7284516 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-04 11:00:52+03:00

0.986.0
-------

[robot-statinfra](http://staff/robot-statinfra) 2020-09-03 11:53:52+03:00

0.985.0
-------

[robot-statinfra](http://staff/robot-statinfra) 2020-09-03 11:24:39+03:00

0.984.0
-------

* [kchupin](http://staff/kchupin)

 * [BI-1461] Dedicated package for common testing utils  [ https://a.yandex-team.ru/arc/commit/7278640 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-02 23:52:21+03:00

0.983.0
-------

* [gstatsenko](http://staff/gstatsenko)

 * Added a field remapping mechanism, BI-974                         [ https://a.yandex-team.ru/arc/commit/7265161 ]
 * Added clone_field upcate action, BI-1691                          [ https://a.yandex-team.ru/arc/commit/7248514 ]
 * Fixed the updating of has_auto_aggregation property, CHARTS-3051  [ https://a.yandex-team.ru/arc/commit/7248120 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-31 13:27:23+03:00

0.982.0
-------

* [gstatsenko](http://staff/gstatsenko)

 * Refactored feature API                                                                                                                             [ https://a.yandex-team.ru/arc/commit/7238949 ]
 * PR from branch users/gstatsenko/feature-base-classes

WIP

Moved source features to a separate folder

Moved source features to a separate folder  [ https://a.yandex-team.ru/arc/commit/7236806 ]
 * Added DivisionByZero exception                                                                                                                     [ https://a.yandex-team.ru/arc/commit/7232567 ]
 * Implemented update_access_mode action, BI-1501                                                                                                     [ https://a.yandex-team.ru/arc/commit/7232566 ]
 * Removed formula suggestions and formatting BI-1686                                                                                                 [ https://a.yandex-team.ru/arc/commit/7227138 ]

* [hhell](http://staff/hhell)

 * BI-1703: fix for constants in compeng queries [run large tests]                                                                                                    [ https://a.yandex-team.ru/arc/commit/7238850 ]
 * BI-1511: NativeType dict-valued primitive [run large test]                                                                                                         [ https://a.yandex-team.ru/arc/commit/7236846 ]
 * fix bi-api-sync swagger                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/7232571 ]
 * tier1 tests: bi-common common test image [run large tests]                                                                                                         [ https://a.yandex-team.ru/arc/commit/7227514 ]
 * minor dataset.add_data_source refactoring [run large tests]                                                                                                        [ https://a.yandex-team.ru/arc/commit/7225042 ]
 * DEVTOOLSSUPPORT-2799: PGCTLTIMEOUT everywhere because apparently sandbox environment is sloooooooooow and the default 60 seconds is not enough to stop a postgres  [ https://a.yandex-team.ru/arc/commit/7221482 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-1718: metrics for aiohttp and flask apps  [ https://a.yandex-team.ru/arc/commit/7233256 ]
 * BI-1717: metrics for dataset-api             [ https://a.yandex-team.ru/arc/commit/7231403 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-20 11:28:58+03:00

0.981.0
-------

* [dmifedorov](http://staff/dmifedorov)

 * BI-1608: ext-testing: use cloud us, dls and uploads  [ https://a.yandex-team.ru/arc/commit/7220085 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-13 23:53:20+03:00

0.980.0
-------

* [hhell](http://staff/hhell)

 * BI-1511: native type classes in bi-api and bi-uploads [run large tests]  [ https://a.yandex-team.ru/arc/commit/7219091 ]
 * clean up tox markers to reduce the disparity with tier0 tests            [ https://a.yandex-team.ru/arc/commit/7214206 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-1608: switch ext-testing redis to cloud  [ https://a.yandex-team.ru/arc/commit/7215700 ]

* [gstatsenko](http://staff/gstatsenko)

 * Moved source features to a separate folder  [ https://a.yandex-team.ru/arc/commit/7214979 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-13 19:14:32+03:00

0.979.0
-------

* [hhell](http://staff/hhell)

 * BI-1121: bi-billing tier0 tests [run large tests]  [ https://a.yandex-team.ru/arc/commit/7210643 ]

* [kchupin](http://staff/kchupin)

 * [BI-1212] Load available connectors in runtime instead of import-time  [ https://a.yandex-team.ru/arc/commit/7208196 ]
 * [BI-1212] commonize flask test client                                  [ https://a.yandex-team.ru/arc/commit/7207749 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-11 17:04:14+03:00

0.978.0
-------

* [asnytin](http://staff/asnytin)

 * BI-1553: geo layers + billing checker small refactoring  [ https://a.yandex-team.ru/arc/commit/7207131 ]

* [hhell](http://staff/hhell)

 * makefiles cleanup and commonize  [ https://a.yandex-team.ru/arc/commit/7206735 ]

* [kchupin](http://staff/kchupin)

 * [BI-1212] SA controller idempotency & instantiation  [ https://a.yandex-team.ru/arc/commit/7206708 ]

* [gstatsenko](http://staff/gstatsenko)

 * Change mode_manager.py (BI-1623)  [ https://a.yandex-team.ru/arc/commit/7185908 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-10 15:52:49+03:00

0.977.0
-------

* [dmifedorov](http://staff/dmifedorov)

 * statbox_relooser: cloud image building triggering  [ https://a.yandex-team.ru/arc/commit/7182930 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-06 18:09:48+03:00

0.976.0
-------

* [gstatsenko](http://staff/gstatsenko)

 * PR from branch users/gstatsenko/implicit-access-mode-switch

WIP

Implemented implicit access mode switch, BI-1501

Implemented mat manager's method that checks if materialization of a given type is required, BI-1501  [ https://a.yandex-team.ru/arc/commit/7182312 ]
 * Added DatasetCapabilities.supports_materialization method; added supports_manual_materialization, supports_scheduled_materialization option flags, BI-1501                                                                [ https://a.yandex-team.ru/arc/commit/7182300 ]

* [kchupin](http://staff/kchupin)

 * [BI-1212] Service account creation  [ https://a.yandex-team.ru/arc/commit/7181747 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-06 16:37:31+03:00

0.975.0
-------

[robot-statinfra](http://staff/robot-statinfra) 2020-08-06 11:46:36+03:00

0.974.0
-------

* [hhell](http://staff/hhell)

 * minor fix: cp -a impies -r        [ https://a.yandex-team.ru/arc/commit/7178563 ]
 * switching to py38 in tier1 tests  [ https://a.yandex-team.ru/arc/commit/7178546 ]

* [gstatsenko](http://staff/gstatsenko)

 * Implemented mat manager's method that checks if materialization of a given type is required, BI-1501  [ https://a.yandex-team.ru/arc/commit/7177362 ]
 * Added CH code 159 for SourceTimeout error, DLHELP-943                                                 [ https://a.yandex-team.ru/arc/commit/7177015 ]

* [asnytin](http://staff/asnytin)

 * lighten metrika tests to avoid flaps  [ https://a.yandex-team.ru/arc/commit/7177057 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-06 03:11:10+03:00

0.973.0
-------

* [dmifedorov](http://staff/dmifedorov)

 * BI-1608: use ext cloud preprod backends for requests between components  [ https://a.yandex-team.ru/arc/commit/7174632 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-04 19:43:21+03:00

0.972.0
-------

* [asnytin](http://staff/asnytin)

 * Геослои + флаг запрета скачивания данных

BI-1666: bi-api tests for data_export_forbidden flag and geolayers

BI-1666: data_export_forbidden flag

BI-1553: geolayers connection allowed tables

BI-1553: geolayers datasource materialization properties

BI-1553: geolayers: tests and fixes

BI-1553: geolayers: get purchased layers with BillingChecker

BI-1553: geolayers connection, filters draft  [ https://a.yandex-team.ru/arc/commit/7170873 ]

* [gstatsenko](http://staff/gstatsenko)

 * Switched to cached data processor                                     [ https://a.yandex-team.ru/arc/commit/7170543 ]
 * Separated materialization management from the Dataset class, BI-1501  [ https://a.yandex-team.ru/arc/commit/7170541 ]

* [hans](http://staff/hans)

 * BI-1616 BI-1638 Fix tests and DS response for Albato and Loginom  [ https://a.yandex-team.ru/arc/commit/7170446 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-03 20:31:11+03:00

0.971.0
-------

* [asnytin](http://staff/asnytin)

 * BI-1661: require billing checker  [ https://a.yandex-team.ru/arc/commit/7168769 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-03 12:13:00+03:00

0.970.0
-------

* [hhell](http://staff/hhell)

 * tier0 unistat fix  [ https://a.yandex-team.ru/arc/commit/7164194 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-31 16:25:41+03:00

0.969.0
-------

* [kchupin](http://staff/kchupin)

 * [BI-1125] Fix mat/preview requirements determination  [ https://a.yandex-team.ru/arc/commit/7159553 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-30 16:18:29+03:00

0.968.0
-------

* [hhell](http://staff/hhell)

 * Further fix  [ https://a.yandex-team.ru/arc/commit/7157763 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-29 20:49:49+03:00

0.967.0
-------

* [hhell](http://staff/hhell)

 * un-overridden rqe async service in bi-api  [ https://a.yandex-team.ru/arc/commit/7157722 ]
 * test async app env loading                 [ https://a.yandex-team.ru/arc/commit/7157438 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-29 20:33:43+03:00

0.966.0
-------

[robot-statinfra](http://staff/robot-statinfra) 2020-07-29 18:44:20+03:00

0.965.0
-------

* [hhell](http://staff/hhell)

 * async app granular settings usage fix  [ https://a.yandex-team.ru/arc/commit/7156589 ]

* [kchupin](http://staff/kchupin)

 * [BI-1121] Manual for running tests from pycharm                 [ https://a.yandex-team.ru/arc/commit/7155819 ]
 * [BI-1121] Common requirements in remote debug docker-container  [ https://a.yandex-team.ru/arc/commit/7153842 ]

* [gstatsenko](http://staff/gstatsenko)

 * Now using AccessModeMager in materialization schedukler, BI-1501  [ https://a.yandex-team.ru/arc/commit/7155557 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-29 18:38:46+03:00

0.964.0
-------

* [hhell](http://staff/hhell)

 * BI-1121: tier1 requirements sync in all projects  [ https://a.yandex-team.ru/arc/commit/7153214 ]
 * tier1 requirements contstraint                    [ https://a.yandex-team.ru/arc/commit/7152656 ]
 * BI-1121: simplify settings-resource loading       [ https://a.yandex-team.ru/arc/commit/7148779 ]
 * Minor cleanups                                    [ https://a.yandex-team.ru/arc/commit/7141980 ]

* [hans](http://staff/hans)

 * BI-1616 BI-1638 Add Albato and Loginom connectors  [ https://a.yandex-team.ru/arc/commit/7152480 ]

* [asnytin](http://staff/asnytin)

 * BI-1661: BillingChecker service in bi-api  [ https://a.yandex-team.ru/arc/commit/7152093 ]

* [dmifedorov](http://staff/dmifedorov)

 * BI-1607: fix some preview cases  [ https://a.yandex-team.ru/arc/commit/7144139 ]

* [kchupin](http://staff/kchupin)

 * [BI-1484] Fix caches configuration for missing CACHES_TTL_SETTINGS case  [ https://a.yandex-team.ru/arc/commit/7142453 ]
 * [BI-1629] Cache TTL override field in connection data                    [ https://a.yandex-team.ru/arc/commit/7140265 ]
 * [BI-1484] Implicit cache TTL configuration/calculation                   [ https://a.yandex-team.ru/arc/commit/7139176 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-28 16:10:33+03:00

0.963.0
-------

* [hhell](http://staff/hhell)

 * BI-1121: tier0 under the unsuffixed version  [ https://a.yandex-team.ru/arc/commit/7134742 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-21 13:34:46+03:00

0.962.0
-------

* [dmifedorov](http://staff/dmifedorov)

 * BI-1607: turn off sample generation for csv + do not use tricks for preview source  [ https://a.yandex-team.ru/arc/commit/7129782 ]

* [hhell](http://staff/hhell)

 * tier0-deploy-experiment preliminary cleanup   [ https://a.yandex-team.ru/arc/commit/7129662 ]
 * BI-1121: bi-api tests reorganization          [ https://a.yandex-team.ru/arc/commit/7129573 ]
 * Tests: return the requirements_actual hack    [ https://a.yandex-team.ru/arc/commit/7111474 ]
 * BI-1121: bi-api docker dependencies hash pin  [ https://a.yandex-team.ru/arc/commit/7111338 ]
 * BI-1121: tests: stylefixes                    [ https://a.yandex-team.ru/arc/commit/7111195 ]

* [seray](http://staff/seray)

 * [BI-1384] adding BillingAdditionalInfoReportingRecord  [ https://a.yandex-team.ru/arc/commit/7112804 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-20 12:27:20+03:00

0.961.0
-------

* [kchupin](http://staff/kchupin)

 * [BI-1125] Materialized dataset copy                                                  [ https://a.yandex-team.ru/arc/commit/7107764 ]
 * [BI-1125] Testing US was updated. Usage was generalized in DC in bi-common & bi-api  [ https://a.yandex-team.ru/arc/commit/7098717 ]

* [hhell](http://staff/hhell)

 * BI-1121: reorganized tests in preparation for tier0  [ https://a.yandex-team.ru/arc/commit/7106976 ]
 * Minor fixes and experimental attempts                [ https://a.yandex-team.ru/arc/commit/7099186 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-14 01:55:45+03:00

0.960.0
-------

* [asnytin](http://staff/asnytin)

 * BI-1070: multihost connection executors

BI-1070: multihost connection executors

BI-1070: bi-api connections schema: comma-separated multiple hosts  [ https://a.yandex-team.ru/arc/commit/7096632 ]

* [kchupin](http://staff/kchupin)

 * PyCharm integration for monorepo format  [ https://a.yandex-team.ru/arc/commit/7095427 ]

* [gstatsenko](http://staff/gstatsenko)

 * Removed data_context from DatasetView

Removed data_context from DatasetView

Added TODO to SR factory

Switched to using data processor factories, BI-1477

Fixed ya.make

Added data processor factory, BI-1477  [ https://a.yandex-team.ru/arc/commit/7090275 ]
 * Switched to using data processor factories, BI-1477                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/7087105 ]

* [hhell](http://staff/hhell)

 * BI-1121 / BI-1540: sync requirements to match arcadia versions  [ https://a.yandex-team.ru/arc/commit/7088891 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-09 19:28:02+03:00

0.959.0
-------

* [hhell](http://staff/hhell)

 * BI-1121: bi-api docker over ya package  [ https://a.yandex-team.ru/arc/commit/7083712 ]
 * makefile: minor rename                  [ https://a.yandex-team.ru/arc/commit/7082680 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-07 12:36:55+03:00

0.958.0
-------

* [hhell](http://staff/hhell)

 * Do not require rsync in build  [ https://a.yandex-team.ru/arc/commit/7082228 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-06 14:49:46+03:00

0.957.0
-------

[robot-statinfra](http://staff/robot-statinfra) 2020-07-06 14:34:46+03:00

0.956.0
-------

* [asnytin](http://staff/asnytin)

 * BI-1634: added dataset get_locked_entry_cm wait_timeout  [ https://a.yandex-team.ru/arc/commit/7081673 ]

* [hhell](http://staff/hhell)

 * avoid putting libraries into the container twice  [ https://a.yandex-team.ru/arc/commit/7081319 ]
 * Same-revision bi-common/bi-formula in tests       [ https://a.yandex-team.ru/arc/commit/7075619 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-06 14:27:22+03:00

0.955.0
-------

* [hhell](http://staff/hhell)

 * BI-1121: convenienced entry points in tier0 docker  [ https://a.yandex-team.ru/arc/commit/7072212 ]

* [gstatsenko](http://staff/gstatsenko)

 * updated bi-formula to 7-43.0  [ https://a.yandex-team.ru/arc/commit/7071956 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-02 12:38:43+03:00

0.954.0
-------

* [gstatsenko](http://staff/gstatsenko)

 * Changed direction of some window functions, uipdated docs and examples, BI-1631  [ https://a.yandex-team.ru/arc/commit/7069282 ]

* [comradeandrew](http://staff/comradeandrew)

 * Contrib: separate tornado from gunicorn GEOINFRA-2117

В gunicorn использование tornado веркера является опциональным. Прямо сейчас по пирдиру на gunicorn всегда приходит tornado, причем версии 4, что делает пирдир на gunicorn не совместимым с contrib/python/motor под PY3, потому что motor пирдирит себе tornado-6.
Так как в gunicorn использовать tornado - опционально, вынес пирдир на tornado всем кто пирдирит gunicorn. Такое изменение никак не меняет поведение и никаких функциональных изменений здесь не ожидается. Все кому нужен tornado вместе с gunicorn могут делать пирдир на него отдельно.

UPD: Вынес пирдир на торнадо явно  [ https://a.yandex-team.ru/arc/commit/7068479 ]

* [hhell](http://staff/hhell)

 * auxiliary: tier0 deploy_experiment script  [ https://a.yandex-team.ru/arc/commit/7067722 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-30 21:09:31+03:00

0.953.0
-------

* [hhell](http://staff/hhell)

 * convenience tier0 entry-points  [ https://a.yandex-team.ru/arc/commit/7051452 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-30 13:53:57+03:00

0.952.0
-------

* [dmifedorov](http://staff/dmifedorov)

 * yandex-bi-common[db]==12.91.0 (fix subselect, tier0 stuff)  [ https://a.yandex-team.ru/arc/commit/7050717 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-29 17:44:42+03:00

0.951.0
-------

* [gstatsenko](http://staff/gstatsenko)

 * PR from branch users/gstatsenko/suppress-double-aggregation

Implemented suppression of double aggregations, BI-633

Implemented suppression of double aggregations, BI-633  [ https://a.yandex-team.ru/arc/commit/7049808 ]
 * Added access_mode to dataset config responses                                                                                                                                [ https://a.yandex-team.ru/arc/commit/7049336 ]
 * Separated AccessModeManager from DatasetResource, BI-1623                                                                                                                    [ https://a.yandex-team.ru/arc/commit/7049326 ]
 * Removed ace_url and syntax_highlighting_url and generation of ACE files, BI-1626                                                                                             [ https://a.yandex-team.ru/arc/commit/7036735 ]

* [hhell](http://staff/hhell)

 * bi-api app in docker

Effectively depends on https://a.yandex-team.ru/review/1320816/details  [ https://a.yandex-team.ru/arc/commit/7036546 ]
 * DLHELP-758: do not raise on cast to markup                                                    [ https://a.yandex-team.ru/arc/commit/7036524 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-29 15:15:35+03:00

0.950.0
-------

* [hhell](http://staff/hhell)

 * sentry-sdk==0.15.1                                                                                  [ https://a.yandex-team.ru/arc/commit/7035133 ]
 * bi-api app                                                                                          [ https://a.yandex-team.ru/arc/commit/7032359 ]
 * BI-1121: bi-api as library

Effectively depends on https://a.yandex-team.ru/review/1317499/details  [ https://a.yandex-team.ru/arc/commit/7030949 ]
 * version marker                                                                                      [ https://a.yandex-team.ru/arc/commit/7029844 ]

* [kchupin](http://staff/kchupin)

 * [BI-1621] use cleanup_common_secret_data in sentry_sdk.init.before_send for data-api  [ https://a.yandex-team.ru/arc/commit/7031512 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added per-field aggregations and casts to options, BI-1593                           [ https://a.yandex-team.ru/arc/commit/7030569 ]
 * Moved BI_TYPE_AGGREGATIONS to bi-api and added count aggregation for all data types  [ https://a.yandex-team.ru/arc/commit/7009745 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-25 12:35:23+03:00

0.949.0
-------

* [dmifedorov](http://staff/dmifedorov)

 * CLOUD-47530: check conn availability on conn creating  [ https://a.yandex-team.ru/arc/commit/7006731 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-23 11:01:40+03:00

0.948.0
-------

* [nslus](http://staff/nslus)

 * ARCADIA-2273 [migration] bb/STATBOX/bi-api  [ https://a.yandex-team.ru/arc/commit/7001275 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-21 17:56:39+03:00

0.947.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * CLOUD-47236: require password on conn check for read perm users  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f69aab17 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-06-19 13:04:50+03:00

0.946.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==12.86.0: [BI-1593] BIField.autoaggregated refactor, [BI-1614] RQE + table-func-over-sqlalchemy-text  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e5333163 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Refactored usage of the has_auto_aggregation_attrib, enabled usage of explicit aggregations for formula fields, BI-1593  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a63d97d7 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-1513] removed distinct_no_rls api handler  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3992d22b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-06-19 12:25:04+03:00

0.945.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==12.85.0: chydb async, cache refactors, chyt/chydb error-passthrough, ...  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f26004d4 ]
 * tests: markup None result                                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/52db7ec1 ]
 * markup: skip None                                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/784609a1 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed usage of allow_subquery  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/14fc6ac6 ]
 * Attempted to fix BI-1571       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d32d2fff ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-06-18 12:02:10+03:00

0.944.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * styling and a minor fix                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3c97b41f ]
 * [BI-1478] CH datetime + int behavior tests: refactored and extended  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0977755d ]
 * [BI-1478] CH datetime + int behavior tests                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/117bb189 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added supported_functions to options, BI-1604  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/975e14e4 ]
 * Added test_source_replacement.py, BI-1571      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7a84b204 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * Revert "[BI-1607] SAMPLE_GENERATION_ENABLED option"  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6835ced8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-06-17 13:14:40+03:00

0.943.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-1070] yandex-bi-common[db]==12.84.0 (FORCE_RQE_MODE flag)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3d514196 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-06-16 12:03:54+03:00

0.942.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1558] Sentry DSNs to https + useless part of secret was removed  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/18dc0f5f ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-1607] SAMPLE_GENERATION_ENABLED option  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5abd0504 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-1070] rqe_int_sync start config  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/31618f41 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-06-15 21:30:22+03:00

0.941.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d58bd61c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-06-09 15:38:09+03:00

0.940.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Public API: anonymous user in RCI  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3d85149b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-06-09 15:21:29+03:00

0.939.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * fixed features sorting  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/16e740dc ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-06-09 13:21:40+03:00

0.938.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==12.82.0: mysql: disable ssl verification, [BI-1538] fixed Connection.update_data_source  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/18e0c17e ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-06-04 19:07:09+03:00

0.937.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==12.80.0: yc auth header fix, minor cleanups                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d91cadd9 ]
 * werkzeug fix                                                                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5441ae3f ]
 * [BI-1540] remove external-libs as it comes from the base image now                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/21a20934 ]
 * [BI-1550] mass-update dependencies                                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2f14c0d0 ]
 * [BI-1584] RLS: strictly require RCI.user_id                                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e9b33d06 ]
 * [BI-1550] yandex-bi-common[db]==12.78.0: yc_auth to yc_as_client                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/38af19c9 ]
 * tests fix                                                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c552522c ]
 * [BI-1550] [BI-1581] yandex-bi-common[db]==12.77.0: marshmallow update, mysql-connector to mysqlclient  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1ea2db69 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-06-03 14:11:03+03:00

0.936.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * fixed USPermissionKind import  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/278b96ce ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-06-01 00:34:31+03:00

0.935.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * DatasetView RLS: warn about empty RCI user_id but do not require it  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3cdfaa67 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-1513] added parameter "disable_rls" for distincts api handler               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/01a07e1d ]
 * [BI-1490] check admin/edit permission before connections deletion/modification  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3a6c9a63 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-05-30 00:30:37+03:00

0.934.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1516] rls tests fix and improvements  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5bccf971 ]
 * [BI-1516] rls fix                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/02fca5f4 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-05-28 13:22:43+03:00

0.933.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-979] Configurable CHYT_MIRRORING_REQ_TIMEOUT_SEC  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b0bacc57 ]
 * Fix cache timings bug                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6873ab2e ]
 * [BI-979] Ability to configure cluster-default mirror  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/140d60b2 ]
 * [BI-979] Per-clique CHYT mirroring                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f35e9a9f ]

* [robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru)

 * releasing version 0.931.1  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/78f0088a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-05-28 09:52:52+03:00

0.932.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-979] Ability to configure cluster-default mirror                                                                                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cf805c50 ]
 * [BI-979] Per-clique CHYT mirroring                                                                                                                                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d03ccdfd ]
 * Fix cache timings bug                                                                                                                                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2f65c853 ]
 * [BI-1508] Direct instantiation of DataSelectors was replaced with factory calls. Cache_usage attribute name was clarified.                                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e52412d9 ]
 * [BI-1508] yandex-bi-common[db]==12.72.0: MyPy fixes, Flask ContextVarMiddleware now create new context instead of copy of parent, cache attributes refactoring in data selectors  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ec325f98 ]
 * [BI-1508] Use default implementations of CE/services registry factories in testing environment                                                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3d931acb ]
 * [BI-1508] yandex-bi-common[db]==12.70.0: yc_auth fixes, service registry with reports registry and selectors factory, connection replacement compatibility checks                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8346f703 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Tests: forgotten files                                                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dc3782af ]
 * Auxiliary: diff_with_prod: minor improvements                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/725b6850 ]
 * [BI-1561] yandex-bi-common[db]==12.76.0: CHYDB extra-parameters                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/85f20d34 ]
 * [BI-1561] CHYDB: ydb_cluster/ydb_database parameters in API                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/11f09b70 ]
 * Minor styling fix                                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/41c047a1 ]
 * yandex-bi-common[db]==12.75.0: depencency cleanup, fixes, etc.                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3b8ba2cc ]
 * [BI-1516] rls pattern_type                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/30cd0dac ]
 * [BI-1516] rls wildcards support                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c75676d9 ]
 * [BI-1574] compeng str concat fix; bi-formula: pg datetime(value, tzname), texts update  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5c3cd8e2 ]
 * minor fix                                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/84867aa8 ]
 * More deps cleanup                                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7f644931 ]
 * [BI-1548] [BI-1551] dependencies update and cleanup                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4586d1d2 ]
 * Tests: a more advanced DLS mockup for RLS                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c6c099d1 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added replacement_types to connection options, BI-1525  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/232622e3 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-05-27 19:52:53+03:00

0.931.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1491] yandex-bi-common[db]==12.67.0: dataset selectors cache fix, yc_session fixes, CE factory caching  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9cf2185c ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Tests: tests container dns timeout tuning for faster (and less prone to aiohttp timeout) tests  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ef70dddb ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Switched to using DatasetCapabilities, BI-1525  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c5742295 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-05-20 12:43:44+03:00

0.930.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1491] yandex-bi-common[db]==12.64.0: query caches refactoring, query execution reports, iam token by cookie fetching, separate DatasetCapabilities & Dataset, tz-aware datetime fix for asyncpg  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ac9a57a2 ]

* [Alexander Shprot](http://staff/shprot@yandex-team.ru)

 * [BI-1469] staging-public  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4a49ded1 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * minor comment                                                                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e2b4c1ab ]
 * [BI-1478] More datetime/timezone behavior tests, suggested 'mostly_aware' behavior for comparison                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e2c3ca42 ]
 * Minor comments                                                                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/136e3efe ]
 * Minor improvements                                                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8704bcc8 ]
 * [BI-1478] Tests: Data API datetimes behavior                                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/98f3d1ac ]
 * [BI-1532] disable StatfaceFormulaCompiler on ConnectionTypes.statface_report_sql as it does more harm than help  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5997ec11 ]
 * Clean up BI_EXPERIMENT_ASYNCPGCOMPENG                                                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1008e3c3 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Removed result_schema patching from data request handlers  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e648c0a7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-05-19 17:06:25+03:00

0.929.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Tests: Use a separate CHYT clique to make the tests less stochastic          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b7621f26 ]
 * yandex-bi-common[db]==12.61.0 (CreateDSFrom.CH_SUBSELECT, partner-callback)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/35f4432e ]
 * BI_TESTS_MARKER -> BI_ALLOW_CH_SUBQUERY                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7ac8d6be ]
 * fixme note                                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d4a8699c ]
 * CreateDSFrom.CH_SUBSELECT for tests                                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8a3213ef ]

* [Anton Vasilev](http://staff/hhell@yandex-team.ru)

 * diff_with_prod: pre-check releaser  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f80357f7 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-formula                                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/671bed88 ]
 * Unified error handling for all formula processing stages, BI-1509  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b5eb5362 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-05-15 11:51:58+03:00

0.928.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1491] Sync data fetching methods of DatasetView was removed & imports of CachedDatasetDataSelector was removed (dataset_cli is broken)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/da854117 ]
 * [BI-1491] Tests was modified to use only async data api                                                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7b67900c ]
 * [BI-1491] Sync web-views for data fetching was removed                                                                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4792fd6e ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Do not repr a bytestring for the user message  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0c1a7f1f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-05-12 12:55:37+03:00

0.927.0
-------

* [Alexander Shprot](http://staff/shprot@yandex-team.ru)

 * [BI-1469] yandex-bi-common[db]==12.60.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f4e7ee6a ]
 * [BI-1469] staging-public                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ef797f08 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-05-08 14:53:33+03:00

0.926.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1514] Fix billing checks in async api  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/51950f65 ]

* [robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru)

 * releasing version 0.901.24  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1335cfbb ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Renamed resolve_source_role     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3ed76c98 ]
 * Fixed tests                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/04402bfa ]
 * Fixed role resolution, BI-1519  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d3e6ef30 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common[db]==12.38.18  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/874d31ae ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix monitoring_config  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6a4d5569 ]
 * fix monitoring_config  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/384efa77 ]
 * up monitoring_config   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8db3f13b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-05-08 12:43:46+03:00

0.925.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1502] Add /api/v1/datasets/{ds_id}/versions/draft/values/distinct_no_rls to async API  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5826b1a1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-30 17:47:43+03:00

0.924.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1505] yandex-bi-common[db]==12.56.0: endpoint code logging in RequestID, fix unicode symbols in CH creds, CHYT Q mirroring DBA closing decoupling, minor error fixes  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/20ad0ae2 ]
 * [BI-1505] Fix missing endpoint codes for async data api                                                                                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a71c7fe7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-30 16:33:12+03:00

0.923.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-979] Fix env var name prefix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0a877497 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-29 13:21:08+03:00

0.922.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-979] Ability to set mirroring_clique_alias per cluster  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/67dfeb2f ]
 * [BI-979] Fix settings                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1c49dbca ]
 * [BI-979] CHYT query mirroring                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8e80cffa ]

* [robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru)

 * releasing version 0.921.2  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f2f4e059 ]
 * releasing version 0.921.1  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/42e9e226 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1474] Fix compeng service getter  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0cf22d56 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-29 11:58:15+03:00

0.921.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * corresponding yandex-bi-common update                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f6e09423 ]
 * Use some class renaming                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/46785d3e ]
 * [BI-1474] BI_EXPERIMENT_ASYNCPGCOMPENG                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/92ee3417 ]
 * Remove BI_EXPERIMENT_STATFACECOMPENG flag (make it always on)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/30abb060 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Moved schemas to a separate folder  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4333b37c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-28 20:30:26+03:00

0.920.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed field updating, BI-1486  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fa78c440 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-28 13:46:20+03:00

0.919.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * freshen tvm2                                                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2cda9519 ]
 * yandex-bi-common[db]==12.50.0: [BI-1478] [BI-1430] ch_types.DateTimeWithTZ  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3717ac24 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-28 12:27:25+03:00

0.918.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common[db]==12.49.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0d936f49 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-27 19:31:15+03:00

0.917.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-987] TODO clarification                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c21863bc ]
 * [BI-987] Fix: cache TTL settings was not applied in data API  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1712e62d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-27 18:46:20+03:00

0.916.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-1475] fix rls config parsing (no space after comma)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/45941683 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed get_avatar_relation_errors                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ee591e9e ]
 * Fixed disabling of features on field deletion, BI-1473  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/19492c3c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-27 14:11:29+03:00

0.915.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * PR comment fix                                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c791e4fe ]
 * Fixed field re-validation while performing action replace_connection  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/693f1eef ]
 * Switched to using component_errors for fields                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/26252792 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-24 15:11:27+03:00

0.914.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * postgres: /tmpfs datadir when available  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/87b2d8ee ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-24 10:59:24+03:00

0.913.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * statinfra-clickhouse-sqlalchemy==0.1.3.10: include DatetimeWithTZ, improve the error-parsing, remove an extraneous print  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/38dd3237 ]
 * [BI-1393] statfacecompeng: add istartswith to the tests                                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b8a9ada8 ]
 * [BI-1393] StatfaceCompengExecutionPlanner: copy prefilters to compeng for consistency                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8b4a9754 ]
 * [BI-1393] StatfaceCompengExecutionPlanner: fielddate between as prefilter                                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c624d840 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==12.46.0 (Fixed resolution of required avatars for features, BI-1413)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d1d716e6 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-23 18:55:08+03:00

0.912.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1454] yandex-bi-common[db]==12.38.17: fix: request-id in response headers, us client fixes  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/54fe118c ]

* [robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru)

 * releasing version 0.901.23  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8192200e ]
 * releasing version 0.901.22  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/498b0b54 ]
 * releasing version 0.901.21  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/69ad1e38 ]
 * releasing version 0.901.20  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/984f6f15 ]
 * releasing version 0.901.19  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c8c802db ]
 * releasing version 0.901.18  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/af653674 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed field revalidation for frefresh_source action, BI-1366  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ce70d636 ]
 * Updated formula                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/21ad9b30 ]
 * Updated formula                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/995fddff ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix public err schema                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/940668aa ]
 * add MaterializationNotFinished handling  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2f0f8db2 ]
 * yandex-bi-formula==7.30.0                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/27bb49d9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-23 13:29:28+03:00

0.911.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.36.0: disable _postcheck_data_grouping for empty groupby  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a1a4605a ]
 * Force disable groupby for distinct queries                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/46c87d40 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-22 19:08:12+03:00

0.910.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.35.0: another fix for distincts  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c14e7088 ]
 * More fixed tests                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ed60b2ac ]
 * yandex-bi-common[db]==12.44.0                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1cf75c9a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-22 18:35:57+03:00

0.909.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.34.0: fix distincts-catch  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a8768e04 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-22 18:04:57+03:00

0.908.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.33.0: Do not use dl_sql for trivial selects  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f16c2299 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Switched management of source avatar errors to component error registry  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/03eb2663 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-22 15:29:29+03:00

0.907.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Minor comment update                                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/65c52540 ]
 * Minor refactoring: EXECUTION_LEVEL_SHORT_ALIASES                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/adfbccf5 ]
 * Minor rename                                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/46ca4da2 ]
 * ExecutionPlanner: rename fields to selecteds in an attempt to reduce confusion  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/831b3d4f ]
 * Minor improvements                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3f57fbd8 ]
 * Cleanup                                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9bbfcbdf ]
 * Shortened aliases                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b448b321 ]
 * [BI-1393] ExecutionPlanner, BI_EXPERIMENT_STATFACECOMPENG                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c9c4550f ]
 * tests: update db-clickhouse to LTS                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/06a94c49 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed password-checking test                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/97718d2c ]
 * Fixed validation of fields after schema udate  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4eea1c32 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * add MaterializationNotFinished handling  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c421b9c1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-22 14:30:51+03:00

0.906.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/49896c8 ]
 * Removed short_uuid alias_mapper for fields  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bcdc9b9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-20 16:56:38+03:00

0.905.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * fix conn_id/conn_type log ctx             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2720b2d ]
 * Minor statfacereportsql test advancement  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9cff1a1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-20 16:32:24+03:00

0.904.0
-------

* [robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru)

 * releasing version 0.901.17  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9bbd8d2 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1393] preliminary updates and fixes               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f196105 ]
 * [CHARTS-1588] not_aggregated after-merge fix          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/02f263a ]
 * Minor comments and fixes                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7a5355e ]
 * [CHARTS-1588] "not_aggregated" dataset result marker  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1e9d5a8 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added a trailing comma                                                                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/78504e2 ]
 * Removed unused import                                                                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c8731d7 ]
 * More minor compeng fixes                                                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e425a01 ]
 * Added StageProcType typing shortcut                                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3e44049 ]
 * Split FormulaCompiler main logic into processing stages, enabled usage of ordering by window functions, BI-1425  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/86c07e2 ]
 * Updated common                                                                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2e1a19f ]
 * Using fixed aliases at all selection levels                                                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b4be8c6 ]
 * Using fixed aliases at all selection levels, BI-1437, BI-1376                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e95c0cb ]
 * Updated common                                                                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/296bdce ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * support SKIP_AUTH in public api  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/392887b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-20 14:08:26+03:00

0.903.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Fix worker_control_cm  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fbad91a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-16 18:45:25+03:00

0.902.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Put all window function tests in one file                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/74c74d3 ]
 * Added support for DESC ordering with window functions, BI-1404  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ae794d6 ]
 * Added support for window function filters, BI-1394              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c4d79ca ]
 * Review fixes                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e4172a0 ]
 * Review fixes                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c4bb622 ]
 * Added more flexible control over expression execution level     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5846fc7 ]
 * Separated column registration logic from FormulaCompiler        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/93bc619 ]
 * Separated FormulaCompiler from ExpressionBuilder                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7442e59 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-formula==7.32.0 sqlalchemy-metrika-api==1.22.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/71c2756 ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * add SourceConnectError to error_handling  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/321d4a1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-16 17:48:17+03:00

0.901.16
--------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1435] yandex-bi-common[db]==12.38.15: fix: async USM not takes in account transitive US dependencies of ProviderConnection  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b679e60 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-16 04:13:24+03:00

0.901.15
--------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1429] Tiny typing fix                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5acaba5 ]
 * [BI-1429] Query execution timeout for CH in public installation  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7810734 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-15 20:35:01+03:00

0.901.14
--------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] yandex-bi-common[db]==12.38.13: Fix metrica api conn sec manager check in cloud app type  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d1f7abe ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-1239] sqlalchemy-metrika-api==1.21.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e154fac ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-15 14:00:00+03:00

0.901.13
--------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] Revert yandex-bi-formula to 7.29.0. To prevent potential behaviour changes in public dashs  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5a39422 ]
 * [BI-1354] yandex-bi-common[db]==12.38.12: fix caches and fix cast for output in async ce              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9d26855 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-14 21:01:18+03:00

0.901.12
--------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] Fix condition to launch async external RQE  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4f9ca5e ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula to 7.30.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ff5462a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-14 18:53:46+03:00

0.901.11
--------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] Workarounds for missing public flags on connections for public dataset  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/681f486 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-14 16:21:17+03:00

0.901.10
--------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * mat client: retry get requests  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d9d1955 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-13 20:56:59+03:00

0.901.9
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] yandex-bi-common[db]==12.38.9: fixes in async CH DBA  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/951ee33 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-13 20:12:00+03:00

0.901.8
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] Actualization of app_name in data API                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d7f4286 ]
 * [BI-1354] yandex-bi-common[db]==12.38.8: folder_id & billing_folder_id in logging fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ae6cf27 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-13 16:12:37+03:00

0.901.7
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] yandex-bi-common[db]==12.38.7: folder_id_log_ctx_key override in yc_auth middleware + folder_id_log_ctx_key='auth_folder_id'  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c99ee50 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-12 21:32:32+03:00

0.901.6
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] yandex-bi-common[db]==12.38.6: fix skip_auth in aiohttp yc_auth middleware  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/34a9efb ]
 * [BI-1354] Fix auth and setup-iptables data API in cloud env                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7b34641 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-12 19:34:30+03:00

0.901.5
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] Configuration of async app in public mode was fixed  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/be74ac0 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-11 00:09:09+03:00

0.901.4
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] yandex-bi-common[db]==12.38.5: fix inf in async CH-like DBAs  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/943dbfd ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * ch_public -> ch_datalens  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e8d9e80 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-10 21:11:52+03:00

0.901.3
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] Executor cls to request context  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3d34d79 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed value range for MetricaAPI, BI-1424  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0fb24ba ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-10 15:01:13+03:00

0.901.2
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] yandex-bi-common[db]==12.38.3  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/61c3436 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-10 01:34:07+03:00

0.901.1
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1354] Pure async CH access  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d5c587b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-10 00:49:07+03:00

0.901.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] yandex-bi-common[db]==12.38.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f864d1a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-09 22:37:14+03:00

0.900.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added shortuuid alias mapper to query compiler, BI-1412  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8f6685d ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * oblig_filters_populate_dataset_fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e1d4132 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-08 20:22:43+03:00

0.899.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix reqs  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/35eefe8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-08 16:10:50+03:00

0.898.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-08 15:58:35+03:00

0.897.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1410] statreport dataset range tests  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2971c3a ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==12.34.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/47d863b ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Renamed sub_node var into new_node                                                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bdc84e0 ]
 * Removed translation from feature replacements, refactored feature replacements as a formula mutation, BI-1394  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f228152 ]
 * Added test_result_with_updates                                                                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/31a1db4 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-08 15:57:24+03:00

0.896.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * fixed oblig filter update body dump  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/695cc27 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-07 22:52:40+03:00

0.895.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] yandex-bi-common[db]==12.31.0: new error handling for async-api + bulk on fixes  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5744ce0 ]
 * [HOTFIX] Fix worker_control_cm 2                                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/be23a1f ]

* [robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru)

 * releasing version 0.877.3  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/54436c0 ]
 * releasing version 0.877.2  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e81fa96 ]
 * releasing version 0.877.1  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e269772 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1393] ExpressionBuilderStatfaceSQL: _filter_is_prefilter implementation for most of the cases  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/12e67c5 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed FieldNotFound in DatasetView              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/04756c3 ]
 * Replaced filter dicts with AmbiguousFilterSpec  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a7dbe7f ]
 * Backported datetime postprocessor               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/809df38 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common[db]==12.2.1  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/be0571c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-07 18:56:18+03:00

0.894.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * obligatory_filter schema fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/23d974d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-07 12:25:01+03:00

0.893.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-formula==7.27.0 happens to be required for a recent merge  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/724af5c ]
 * yandex-bi-common[db]==12.28.0: [BI-1403] blackbox over HTTP POST     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ad2eb8d ]
 * move slicer_always_compeng                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e03fbf2 ]
 * [BI-1393] ExpressionBuilderStatfaceSQL                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2ee0445 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added ignore_nonexistent_filters to async distincts  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1a3eec8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-06 18:25:29+03:00

0.892.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Minor future-use plug  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/efb4879 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common[db]==12.27.0             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5bd2e42 ]
 * fixed typo in tests                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/13c3772 ]
 * [BI-1295] geo points dict source feature  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/31570a2 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-06 15:44:06+03:00

0.891.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [FIXES] Fix postprocess_datetime for None value  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ff61ce4 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * outer_error_handling_middleware_base from bicommon                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9e7918f ]
 * split out bi_api_on_error_common                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f9a918a ]
 * outer_error_handling_middleware: split into biapi and common parts  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b298cf7 ]
 * Move compeng_service to DSAPIRequest from DLRequestBase             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fd662e1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-06 13:09:46+03:00

0.890.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] AIOHTTP: New resources management + final JSON loading middleware  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3396ede ]
 * [BI-1156] AIOHTTP: New resources management + final JSON loading middleware  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cb1550c ]
 * [BI-1156] Prototype of json body logger for aiohttp                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5ebc7ff ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Actually log the body  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b418d23 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * get rid of ports in proxy_to_folder  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/be44502 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * log apply field exception in ds validator  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7dbb1ed ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added all fields to select and all dimensions to group by in validation  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/21e59cd ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-03 15:51:06+03:00

0.889.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fix for datetime postprocessor               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1bff106 ]
 * Added a separate postprocessor for datetime  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2ca2bd2 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-1142] load ds with deleted conn  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4d62e54 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-02 17:20:23+03:00

0.888.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1392] Fix extra name for response body                                                                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d3b69d9 ]
 * [BI-1392] Turn of sentry event deduplication + sentry log event level=WARNING + (event_source, http_response_code) Sentry tags  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a2a1fd5 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * postgres: run only for data api              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d9c57d5 ]
 * Auxiliary: yasm_config update with data-api  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/90ef81c ]
 * ExpressionBuilder query_section              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9aa8a67 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated packages, fixed test_expressions  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/06b7ae2 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-02 15:36:28+03:00

0.887.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] fix logging level for query_execution_context()  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b1b9e4a ]
 * yandex-bi-common[db]==12.20.0                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/22d5d58 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-01 19:38:16+03:00

0.886.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] fix requirements.txt 2                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c237c18 ]
 * [BI-1156] fir requirements.txt                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/aa86313 ]
 * [BI-1156] parent_request_id to allowed sentry tags        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/04d2ef4 ]
 * [BI-1156] yandex-bi-common[db]==12.18.0                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/acddc58 ]
 * [BI-1156] Useless logs was removed in async dataset view  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3baefcf ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Minor fix 02  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/49847df ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==12.19.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9b7da7b ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed typo in DatasetView                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5cec5ac ]
 * Renamed compeng_engine to compeng_pg_engine  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6b2a591 ]
 * Added compeng usage                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a49abba ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-01 18:22:43+03:00

0.885.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * uwsgi stats tuning  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/93224ee ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-01 15:25:43+03:00

0.884.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added a bit of logging for feature management  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c7484d1 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common[db]==12.17.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bd96e11 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-01 15:22:24+03:00

0.883.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1386] Unistat for async  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2e4441a ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1386] async unistat with rqe_int_sync stats  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ccc284b ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-1295] geo polygons dict source feature  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6e28b0f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-01 15:03:56+03:00

0.882.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] Useless logs was removed in async dataset view                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/77bfb75 ]
 * [BI-1156] Connection ID and connection type to RequestLoggingContextController       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/57a1069 ]
 * [BI-1156] Sentry user info correct setting in SentryRequestLoggingContextController  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e2699a2 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * sqlalchemy-metrika-api==1.19.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2c46eb1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-04-01 01:12:23+03:00

0.881.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] yandex-bi-common[db]==12.11.0                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5c9da65 ]
 * [BI-1156] Sentry improvements for async app                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f591062 ]
 * [BI-1156] remove default_query_execution_cm_stack() for totals       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2e33178 ]
 * [BI-1156] Fix async dataset view default_query_execution_cm_stack()  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4781729 ]
 * [BI-1156] Workers control for all async views                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8af7fa2 ]
 * [BI-1156] Async worker CM                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/56b4354 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Minor PR fixes           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/470b4a9 ]
 * Enabled formula slicing  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bd8c8f7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-31 22:17:56+03:00

0.880.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] Tests cleanup                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6a71458 ]
 * [BI-1156] us_auth_ctx_blackbox_middleware for aiohttp   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6ec054e ]
 * [BI-1156] Fix async app/settings building for intranet  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d5f1690 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-31 01:21:51+03:00

0.879.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] Add dataset-data-api to .release.hjson. Redundant release.hjson was removed.                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8feb700 ]
 * [HOTFIX] Fix worker_control_cm                                                                                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fc6bdf9 ]
 * [BI-1156] yandex-bi-common[db]==12.9.0                                                                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bd70e51 ]
 * [BI-1156] Missing env from tox.ini to docker-compose.yml                                                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/eb12721 ]
 * [BI-1156] Fix binds for uwsgi in sync RQE                                                                                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a2530c7 ]
 * [BI-1156] Fix port issue in sync RQE launchers                                                                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1484acb ]
 * [BI-1156] Service for sync internal RQE                                                                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/17421a5 ]
 * [BI-1156] Prepare for RQE int sync service descriptor copy-pasting                                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/08a3ee9 ]
 * [BI-1156] RQE services was renamed                                                                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1aa9106 ]
 * [BI-1156] Blackbox auht for async app                                                                                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f5c5c20 ]
 * [BI-1156] Change scope for fixtures () to 'function' for *_connection_id to meet async tests requirements (event loop reset on each test)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c896a4b ]
 * [BI-1156] Totals & no-group-by support for async result handle                                                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2c79366 ]
 * [BI-1156] column_filtration() adaptation for immutable data structures                                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dca49e9 ]
 * [BI-1156] Non-public option for error handling in async API                                                                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2e8b12f ]
 * [BI-1156] yandex-bi-common[db]==12.7.0                                                                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/edfd6e2 ]
 * [BI-1156] Attempt to make async preview                                                                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5177a23 ]
 * [BI-1156] All preview calls in tests wrapped in HttpV1DataApi                                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/217077a ]
 * [BI-1156] All tests was migrated to HttpV1DataApi from raw http call                                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0fded36 ]
 * [BI-1156] Split HttpV1Api in data and control                                                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/25e0839 ]
 * [BI-1156] WIP tests adaptation for async api                                                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/18aae43 ]
 * [BI-1156] data_client test reference                                                                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/063c54a ]
 * [BI-1156] postprocess_data() adaptation for rows as tuples (async CE version)                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8b0aae4 ]
 * [BI-1156] Flask-like interface for aiohttp test client                                                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1f0e0ab ]
 * [BI-1156] Async views adaptation to DSView with RCI                                                                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/91be526 ]
 * [BI-1156] Async app misconfig fix                                                                                                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/30e17c6 ]
 * [BI-1156] Async app initial implementation + basic test                                                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6b915f2 ]
 * [BI-1156] Settings commonization                                                                                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/43787be ]
 * [BI-1156] Support all app types for async data API (WIP)                                                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a6f3e13 ]
 * [BI-1156] RQE config reading was adapted to internal RQE and was moved to common settings module                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/83043ee ]
 * [BI-1156] Internal RQE adaptation for sync app                                                                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5eb2752 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * fix                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/354b0e2 ]
 * Fix worker_control_cm            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ca2cfcf ]
 * Another fix                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/19c9584 ]
 * future envvar rename             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/77e8bf1 ]
 * BI_COMP_PG_URL                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0678dd3 ]
 * More fixes                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5f178ef ]
 * some +x                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f4fd7d0 ]
 * [BI-1356] postgres on localhost  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0062023 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Removed aunnecessary resolve_source_role calls                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6d04895 ]
 * role inside QueryExecutionInfo                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/609bff3 ]
 * Combined building of bi_query and joint_dsrc_info                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b231d37 ]
 * Moved value_range logic to DatasetView                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/23e63f9 ]
 * Removed all extra bi_query-builder methods                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7d7da45 ]
 * Wrapped translation results into TranslatedExpressionInfo objects  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/24697cf ]
 * Added window check for feature functions                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cd41d3e ]
 * Split dataset_view contents into three files                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e31fc04 ]
 * Switched to the new get_expression_value_range                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c9bc3ba ]
 * Fixed test                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b2802d7 ]
 * Aded comment                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c708384 ]
 * Added method DatasetView.get_bi_query                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e361300 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-30 22:14:28+03:00

0.878.0
-------

* [Alexander Shprot](http://staff/shprot@yandex-team.ru)

 * [BI-1313] proper auth config  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d55eb58 ]
 * [BI-1313] auth staging env    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/28ca72a ]

* [Sergei Borodin](http://staff/seray@yandex-team.ru)

 * [BI-1305] order by field which is not in select  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/96e6823 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-25 19:26:12+03:00

0.877.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [WIP] Context -> RequestContextInfo  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4a47e46 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * One less not-actually-context  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/65c14fd ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed tests                                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/119297a ]
 * Added window function validation                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c376adf ]
 * Updated lark-parser, fixed usage of get_data_stream  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/de6ba90 ]
 * Removed invalid import                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/13df3b0 ]
 * Updated common, switched to new data interfaces      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7f18595 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-25 14:13:29+03:00

0.876.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * simple api for mat task stopping  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4041f97 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-23 18:08:17+03:00

0.875.0
-------

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * docker-compose: bi-materializer:0.417.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/30ed122 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common, switched to using OrderByExpressionCtx in order_by_expressions  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bfa36bc ]
 * Separated AmbiguousOrderBySpec from OrderBySpec                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2e42bca ]
 * Added order_by to DatasetView, added support for RSUM window function           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/369ed9f ]

* [Sergei Borodin](http://staff/seray@yandex-team.ru)

 * [BI-1297] remove has_measure_fields method         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3330e29 ]
 * [BI-1297] fix disable_group_by for measure fields  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7b6a982 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-23 14:58:05+03:00

0.874.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * bypass error_msg from materializer  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9020ffa ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-20 20:07:55+03:00

0.873.0
-------

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * AvatarRelation: missing for 'managed_by'  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b20081d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-20 12:23:09+03:00

0.872.0
-------

* [Alexander Degtyarev](http://staff/shprot@yandex-team.ru)

 * [BI-1313] staging sentry old-style setting  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c4179cf ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-20 00:37:53+03:00

0.871.0
-------

* [Alexander Shprot](http://staff/shprot@yandex-team.ru)

 * [BI-1313] sentry project; billing off  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cd91ad5 ]
 * [BI-1313] staging env                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ba8f11c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-19 21:28:54+03:00

0.870.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1360] RLS: fixes and tests-fixes                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ee8b9b6 ]
 * [BI-1360] RLS: fetch the logins info per-field rather than per-row  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8843024 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-common, switched to using SqlSourceBuilder  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dbd49a7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-19 13:44:26+03:00

0.869.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==11.5.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cd06bdc ]
 * conn info: hide hidden        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c628b8a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-18 21:10:43+03:00

0.868.0
-------

* [Alexander Degtyarev](http://staff/shprot@yandex-team.ru)

 * [BI-1313] SAMPLES_CH_HOST  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a8dd4a5 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix adding fields with deleted avatars  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/18331fc ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-18 18:25:47+03:00

0.867.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-17 23:09:15+03:00

0.866.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Minor fix: avoid failing the logging while doing str() of an SA object  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/09c2246 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed log error message when avatars in a relation have no columns  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1602cd2 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-17 20:52:38+03:00

0.865.0
-------

* [Alexander Shprot](http://staff/shprot@yandex-team.ru)

 * [BI-1313] datalens-assessors deploy  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a1c2de8 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==11.3.0                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e9ab301 ]
 * query_executer_async under ext_query_user                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a6f6449 ]
 * markup: correct argcount mismatch handling to account for NULLs  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/028296a ]
 * Tests: test_in_devenv: more env-vars copy                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9f6d1a9 ]
 * Tests: EXT_QUERY_EXECUTER_SECRET_KEY fix                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5e7cb57 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula and fixed error on unknown autoaggregated column  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fdf4925 ]
 * Updated bi-common, updated usage of select_data                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0df137f ]
 * Using column ids in formulas of direct fields                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a727145 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-17 17:35:13+03:00

0.864.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * sqlalchemy-metrika-api==1.18.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1c08b6f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-13 23:33:42+03:00

0.863.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] yandex-bi-common[db]==10.13.0: RQE sqla query dump fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4429c98 ]

* [robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru)

 * releasing version 0.847.7  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1e3c16b ]
 * releasing version 0.847.6  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/249e47d ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula                                                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/41f6ece ]
 * Switched to using NativeType.normalize_name_and_create instead of the constructor directly  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f9805c2 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * bicommon==9.17.3                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c5c8d99 ]
 * fix ProviderConnection replacing: dsrc titles  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0d91dfa ]
 * fix ProviderConnection replacing: dsrc titles  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d5e5926 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-13 18:26:32+03:00

0.862.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] yandex-bi-common[db]==10.12.0: fix exceptions ol logging end of request  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/da4927c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-13 16:31:03+03:00

0.861.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] yandex-bi-common[db]==10.11.0: Sync RQE + ProviderConnection.has_data_sources() fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fae8cb2 ]

* [robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru)

 * releasing version 0.847.5  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2dec544 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Merged formula with cast and UTC fixes  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/22bf33c ]
 * Updated formula                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a23e45d ]
 * Merged formula with cast and UTC fixes  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/08cd1e2 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * metrica_datetime_cast_fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bc8f672 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-13 14:31:18+03:00

0.860.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==10.9.0: Hack: on-import temporary yenv of the type the blackbox module requires  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/de32729 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-12 18:07:14+03:00

0.859.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-12 17:58:46+03:00

0.858.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Live environment fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d7bd979 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-12 16:36:54+03:00

0.857.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-1241] fix metrica autoaggr field renaming  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1dac4e4 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-12 16:11:09+03:00

0.856.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed FieldError                                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e5405b7 ]
 * Fixed autoaggregated and aggregation_locked attributes of result_schema fields, BI-1349  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3236fe3 ]
 * First iteration of splitting ExpressionBuilder into separate classes                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f6cc79b ]
 * Fixes for window functions                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/98042a6 ]
 * Added USE_DATE_TO_DATETIME_CONV flag                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7f3575d ]
 * Fixed cast to dattitme in filters                                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b503f84 ]
 * Fixed test_generate_filter_expression                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e27781f ]
 * Comparing date as datetime                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e9f4882 ]

* [robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru)

 * releasing version 0.847.4  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/11a5f60 ]
 * releasing version 0.847.3  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2e0b620 ]
 * releasing version 0.847.2  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bfdecda ]
 * releasing version 0.847.1  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4116cdc ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==10.8.0                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9e46612 ]
 * One less one-log-line context CM, linting fixes    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b732443 ]
 * [BI-1343] some logging fields renames              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cd02e73 ]
 * yandex-bi-common[db]==10.7.0: import refactorings  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/af6dc3a ]
 * [BI-1340] semaphore for each chyt clique (hotfix)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b457b8d ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * monitoring_config.yaml fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/04c9d9d ]
 * monitoring_config.yaml up   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/18dd8cf ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==9.17.1  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4b2dccf ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * metrica_datetime_cast_fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/08329c4 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-12 16:01:20+03:00

0.855.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-1335] add mdb_cluster_id for ch/pg/my  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f1216fc ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-06 17:49:32+03:00

0.854.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-06 17:17:24+03:00

0.853.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Enabled window functions for PostgreSQL  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b75f61d ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common[db]==10.5.0                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/acb516f ]
 * fixed geo-featured fields validation. Geo-tests.  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d0d4e29 ]

* [Sergei Borodin](http://staff/seray@yandex-team.ru)

 * [BI-1297] add disable_group_by param  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/286e6c9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-06 14:22:53+03:00

0.852.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] yandex-bi-common[db]==10.4.0: IOCaster, profiling fix, dedicated own dialect for CH (+bi)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fc5c4d4 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-05 21:57:19+03:00

0.851.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] yandex-bi-common[db]==10.3.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1ede3cc ]
 * [BI-1156] URL for EQE fix               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d3f7cf5 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-05 17:09:32+03:00

0.850.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] yandex-bi-common[db]==10.2.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c94ec17 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-05 15:29:35+03:00

0.849.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1340] semaphore for each chyt clique  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3f1f8ad ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added details for field errors, updated error codes for some exceptions  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7b1f02f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-05 13:54:34+03:00

0.848.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] yandex-bi-common[db]==10.1.0 + local dataset-api running                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/31cac92 ]
 * [BI-1156] yandex-bi-common[db]==10.0.0                                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/567fb0e ]
 * [BI-1156] New QE integration                                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5820d81 ]
 * [BI-1156] Do not close event loop and do not apply logging config on tests instance of sync app  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/806b07c ]
 * [BI-1156] Adaptation to CE                                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/80c10cd ]
 * [BI-1156] Tests for legacy connection info handles was removed                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/45e2893 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added USE_DATE_TO_DATETIME_CONV flag   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9d6babc ]
 * Updated bi-formula to 7.13             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/97bfd8c ]
 * Fixed cast to dattitme in filters      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/92ab2b0 ]
 * Fixed test_generate_filter_expression  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e31ad88 ]
 * Comparing date as datetime             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8cad068 ]
 * Fixed source deletion                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/59e76de ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-05 12:13:23+03:00

0.847.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Disable the non-working premature test  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d6e7aa5 ]
 * Fixed and improved geocoding tests      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/51b1562 ]
 * [BI-1201] fix CHYDB CONN_TEST           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e894b00 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-03 14:23:45+03:00

0.846.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1156] Fix default volume mount for external bicommon              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4d698bc ]
 * [BI-1156] Ability to add local bicommon in remote-python interpreter  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ef49108 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==9.17.0: [BI-1333] request_id to record.tags (for sentry)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ff38bb5 ]
 * Auxiliary: logbroker_config: correction for-now                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0f11923 ]
 * tests: bi-materializer:0.411.0 (fixed version)                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ce01e2b ]
 * WIP: fix materializer in tests                                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/159d051 ]
 * flask app: make logging configuration optional                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f5ff87f ]
 * updated bi-common with updated logging-configure                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3a9ce7a ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * fixed geofunction formula parsing  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8959dc8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-03-02 15:17:32+03:00

0.845.0
-------

* [Sergei Borodin](http://staff/seray@yandex-team.ru)

 * yandex-bi-common[db]==9.15.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8a5c8f3 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-27 14:46:00+03:00

0.844.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * More localized handling for bool filter errors, BI-1315  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c98e8cf ]
 * Added handling for bool filter errors, BI-1315           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/de9efc0 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bi-materializer version for tests  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b6a19af ]
 * [BI-1286] geoinfo function processing      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fa12d8f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-26 19:39:11+03:00

0.843.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.32.0: Fix the support for datetime objects  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5d30ddf ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-26 16:43:45+03:00

0.842.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Fixed markup support                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d592e3d ]
 * WIP: test_markup                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/56e50e4 ]
 * [BI-1099] BITypes.markup × DataType.MARKUP  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/538dcd5 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-26 16:34:27+03:00

0.841.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==9.14.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0b09259 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixes for aggregation checks  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1bf1235 ]
 * Updated formula to v.7        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e8289ea ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-26 12:57:22+03:00

0.840.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * yandex-bi-common[db]==9.13.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/55d3253 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * tests fix                                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/966fd1f ]
 * More of the automatic secrets-update                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/076a9f1 ]
 * make `make test` work with almost no repo-specific setup  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ddc04e5 ]
 * more base-image parity cleanup                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b6f3d52 ]
 * minor fixes                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3c8ce92 ]
 * [BI-1314] CHYT replace_connection fix                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8e52a1c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-21 17:50:15+03:00

0.839.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * packaging cleanup (of files that are in the base-image)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/80c4e38 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Swicthed casts to from type transofrmer formula functions  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/456800e ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-20 15:57:10+03:00

0.838.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * don't require all BITypes description in CASTS_BY_TYPE, FILTERS_BY_TYPE  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a436e55 ]
 * yandex-bi-common[db]==9.12.0                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fb8763c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-20 14:55:33+03:00

0.837.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix tests  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0a6d5bc ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common[db]==9.11.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/91a81cd ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-18 00:43:03+03:00

0.836.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==9.10.0                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a682882 ]
 * minor clenaup                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/045aaba ]
 * [BI-886] test_chyt_invalid_subsql            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e2c21ca ]
 * [BI-419] totals: fix the zero-measures case  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f21bf3c ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix test                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8f4edaf ]
 * fix _aux/monitoring_config.yaml  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6b75146 ]
 * up monitoring_config.yaml        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e7d1d48 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-14 18:09:22+03:00

0.835.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==9.9.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a5e815d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-13 19:00:48+03:00

0.834.0
-------

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==9.6.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/59ddd98 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-13 18:34:29+03:00

0.833.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * info: connectors  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/66502c1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-13 18:03:42+03:00

0.832.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1099] postprocessors refactoring, markup postprocessing util module  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/16e7af3 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common[db]==9.5.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c50cb9c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-12 12:55:28+03:00

0.831.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy 1.3  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9088b9b ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * handle no-avatar for direct field adding  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9d6ff5b ]
 * yandex-bi-common[db]==9.4.0               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b280a9d ]
 * handle ConnectionNotEnabled               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f0a0277 ]
 * [BI-1290] 400 on failed connection test   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/045324c ]
 * do not add avatar_id to formula fields    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c0b695e ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-558] added "id" and "valid" fields for obligatory filters  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b919e74 ]
 * [BI-558] obligatory filters                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/475db58 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed date filters                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/22b2b73 ]
 * Fix for PR comments                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1356bce ]
 * Removed test parameters from settings  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/657f99b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-11 17:07:46+03:00

0.830.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * chyt: range under semaphores  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8ccedaa ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-07 17:49:17+03:00

0.829.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * statinfra-clickhouse-sqlalchemy==0.1.2.7           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b4289e2 ]
 * Revert "statinfra-clickhouse-sqlalchemy==0.1.2.7"  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ab2dfc2 ]
 * statinfra-clickhouse-sqlalchemy==0.1.2.7           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f760993 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-06 19:38:43+03:00

0.828.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Auxiliary: diff_with_prod: a fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3afa670 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-1291] options: fix aggr list  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a574af3 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-06 17:32:46+03:00

0.827.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.31.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1247f18 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-common version to 9, removed several usages of local EnumField  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4ac4c81 ]
 * Simplified result_schema usage                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ed22041 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-06 14:50:17+03:00

0.826.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.30.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7aecee8 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * oooops  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8c2fdb5 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-04 19:57:08+03:00

0.825.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-formula==5.5.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a9a91ef ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-04 18:38:36+03:00

0.824.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated mysql-connector-python  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/88486f7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-04 16:52:01+03:00

0.823.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Remove the supports_offset hack for distincts as it makes the large statface report distincts unusable  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f033a49 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5df88fd ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-04 13:47:03+03:00

0.822.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula to 5.x with new Literal* interface   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/01359db ]
 * Fixed incorrect handling of dates with milliseconds  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ab32554 ]
 * Fixed management of source title conflict errors     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7786319 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-02-03 17:20:53+03:00

0.821.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==8.19.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/77a902d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-29 13:31:37+03:00

0.820.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * diff_with_prod: more conveniences  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e5814d2 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added generation of missing data source errors                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/14e2dbc ]
 * Implemented saving of data source errors in registry, BI-1163  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e25e349 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-29 13:25:05+03:00

0.819.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==8.18.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4945983 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-28 21:52:38+03:00

0.818.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-419] dataset result: totals implementation over an additional query                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/42f35ba ]
 * [CHARTS-1953] statface dialect: attempt to passthrough as-is the string filter values for numeric dimensions  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e2a2832 ]
 * Auxiliary: logbroker_config: all 3 logs                                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6fd9de9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-28 20:41:45+03:00

0.817.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed validation crash for avatars without fields       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a6f5790 ]
 * Separated HTTP API from dsmaker into a separate module  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a355720 ]
 * Removed field_resolution module                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/450f26d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-23 15:02:27+03:00

0.816.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common, updated materializer in tests to fix rogue field error  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8606410 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-23 11:48:16+03:00

0.815.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1196] Fix DataSourceTemplateResponseSchema for WZ1C data source  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5f67280 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-22 14:22:18+03:00

0.814.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1262] chyt semaphore: only for ch_datalens  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e635b62 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-22 14:10:03+03:00

0.813.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1203] BI_WORKERS_YT_SEMAPHORE_SIZE  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8208b81 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-22 13:10:08+03:00

0.812.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * A very minor fix                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a4d2d1b ]
 * [BI-886] CHYT_TABLE_SUBSELECT; [BI-1201] CHYDB  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/adc5170 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-22 12:37:36+03:00

0.811.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1196] Test for WZ1C conn             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/16909c7 ]
 * [BI-1196] API schema for WZ1CConnection  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fadeb57 ]
 * [BI-1196] yandex-bi-common[db]==8.6.0    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/099bfce ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1246] statreport data source: check the nullable too            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bedf30b ]
 * [BI-1246] statreport data source: test has_auto_aggregation values  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5d85875 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-21 19:50:02+03:00

0.810.0
-------

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * uwsgi-bi-api: set listen = workers  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/92ecd31 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-21 17:52:20+03:00

0.809.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added guessing of avatar_id to support old saved charts  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/176a2c5 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-21 16:34:30+03:00

0.808.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cb8178d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-20 15:12:10+03:00

0.807.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==8.3.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6314f19 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common to 8.x  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b6eca34 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-20 12:56:03+03:00

0.806.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed 'no info about column' error  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4466acf ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-17 16:25:32+03:00

0.805.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-1247] ignore_nonexistent_filters in distincts  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/244482c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-17 13:13:58+03:00

0.804.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * test_create_and_update_rls without rls  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/558bd3c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-15 17:56:04+03:00

0.803.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Reworked validation, BI-1219  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b68c49f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-15 13:39:32+03:00

0.802.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==7.6.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/52fa0af ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-14 13:40:48+03:00

0.801.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * fixed test_get_field_types_info  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/add6f86 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1049] use the common logging config  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b921a5f ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==7.5.0                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/40bd0d7 ]
 * [BI-1228] monitoring config: mat ch disk space  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8722387 ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * monitoring_config up  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9dcd211 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added lock_timeout to set_access_mode               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8734a1f ]
 * Updated common to version 7                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a2a6949 ]
 * Added NotAvailableError to EXCEPTION_CODES mapping  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/174ded1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2020-01-14 12:49:37+03:00

0.800.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * use short uuids  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/18ec1cc ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-25 18:16:32+03:00

0.799.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix error handling in mat  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5b94734 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-24 15:36:16+03:00

0.798.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1203] CHYT per-cluster semaphore over flock  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dcf78a7 ]
 * [BI-1203] Preliminary refactoring                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/05c5249 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix                                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c418a1a ]
 * do not allow to materialize dataset with non-mat-able sources  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/290a602 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-24 15:12:07+03:00

0.797.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix reqs  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bd4b7cf ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * monitoring_config up  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/177c1c8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-24 15:02:36+03:00

0.796.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Logs: url next to the body       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/792c388 ]
 * Auxiliary: logbroker_config fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4a5788a ]
 * reorganized chyt tests           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8fd5aaf ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==6.29.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c0547a3 ]
 * update monitoring_config      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/65be103 ]
 * add monitoring config         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d393467 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-23 20:08:32+03:00

0.795.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==6.27.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/350f8b1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-19 13:21:26+03:00

0.794.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * fix in bi-common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/36a99e6 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-19 13:10:05+03:00

0.793.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==6.25.0            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/27d7bb9 ]
 * More chyt and chyt tablefunction tests  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/42d5661 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-19 12:04:20+03:00

0.792.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==6.24.0: profiling logging fix   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/685599d ]
 * [BI-1193] statinfra-clickhouse-sqlalchemy==0.0.10.13  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2bb7da0 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-19 11:44:22+03:00

0.791.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Some logging improvements  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/08a850f ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==6.23.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5ddebf9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-18 17:50:21+03:00

0.790.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==6.22.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/deb3227 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-17 16:30:19+03:00

0.789.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Auxiliary: _qloud_edit_all_environments  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8ba81e9 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==6.21.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d1ad41e ]
 * dls: send context             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4a7ca50 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-522] hide uuid from old types info api handler  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/29346d7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-17 13:03:33+03:00

0.788.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1154] yandex-bi-common[db]==6.20.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1c28f58 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix yasm config  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8403f5c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-13 18:43:39+03:00

0.787.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Fix the logrotate           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bf1e758 ]
 * _qloud_juggler_enconfigure  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/762273f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-13 18:09:43+03:00

0.786.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-1049] logrotate juggler action, push-client juggler checks  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7405866 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-13 11:59:14+03:00

0.785.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-522] added UUID type  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/105f5bc ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-12 19:51:24+03:00

0.784.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1145] yandex-bi-common[db]==6.19.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/51e077f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-12 18:31:34+03:00

0.783.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * updated deps                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/54155bd ]
 * [BI-1049] push-client corrections               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9b149d0 ]
 * [BI-1049] json-syslog-file-pushclient-... logs  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/10ebc9e ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * handle TableNameNotConfiguredError  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9e26f43 ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * workers_free_total_frac  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fc67a0f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-12 18:01:21+03:00

0.782.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common[db]==6.17.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2fdcf99 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-11 18:09:26+03:00

0.781.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed usage of limit                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6a3c538 ]
 * Fixed list of data type-dependent filters  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8399236 ]
 * Updated common and formula                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a2c3693 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * sqlalchemy-metrika-api==1.17.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6bfcab5 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-1173] fix aggr formula from aggr direct  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b55f655 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-10 14:15:43+03:00

0.780.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-09 19:42:24+03:00

0.779.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-1042] fix filter_operations response  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/06b9d2b ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added dataset options to API, BI-1042  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6be9cb9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-09 15:39:18+03:00

0.778.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * do not use offset in statface distinct  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6f9ba2e ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * sqlalchemy-metrika-api==1.16.0, yandex-bi-formula==3.102.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e49be32 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed Metrica tests                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9ce3804 ]
 * Fixed action body logging                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3107927 ]
 * Removed external env restriction for Oracle  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cbbdf22 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-06 15:44:02+03:00

0.777.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1145] yandex-bi-common[db]==6.12.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1d36850 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added support for the MATERIALIZATION formula dialect  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b00cc73 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-04 14:13:57+03:00

0.776.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1145] Migrating to created_from=CHYT_TABLE  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8dde082 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * fixes for the local-mssql tests              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/db823d7 ]
 * fix                                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2d1527d ]
 * split the mssql test into external/internal  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/581f186 ]
 * Skip a few broken tests                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b96b4f3 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-03 17:44:47+03:00

0.775.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==6.9.0: Fixed _execute in stat  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7e701dd ]
 * Minor test clarity fix                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9d93fa8 ]
 * A minor note                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b2fd5d9 ]
 * Commonized tests and initdb base images              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c5ce8ed ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix limit-offset for distincts  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/77d14e4 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updtaed tests, updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8f25c4e ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-12-03 16:54:45+03:00

0.774.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * bicommon 6.8.0                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5264333 ]
 * [BI-418] CHYT table-function datasources support and tests  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e59fd45 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-28 12:58:06+03:00

0.773.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Correction of the common test helper name          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c2e07e0 ]
 * A tests misnaming fix                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/45b533f ]
 * style stuff                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0713da8 ]
 * [BI-418] CHYT table functions preliminary support  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9fb4af0 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added some data dumps to log                                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bafc38d ]
 * Fixed updating of result_schema fields when ref source is updated, BI-1137  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c980b14 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-28 11:10:45+03:00

0.772.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Fix: p2  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/36eceb0 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-26 16:09:28+03:00

0.771.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-26 15:02:06+03:00

0.770.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * A fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/aa625b8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-26 14:52:57+03:00

0.769.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-formula==3.100.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5cf3557 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-26 14:45:42+03:00

0.768.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Minor improvement: exclude tests subpackages             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f2d985b ]
 * Further improvements in the extras_require organisation  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/422231e ]
 * Reorganised py dependencies                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6e000b8 ]
 * pip-compile-here comments                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ca43aa3 ]
 * More common main base image                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/316ed21 ]
 * minor dockerfile shuffle                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d06b064 ]
 * ubuntu+runit base                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/60c49ac ]
 * ubuntu+runit base: preliminary version                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fc40bda ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-26 14:35:33+03:00

0.767.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * fixed profile_db_request in connection preview handler  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/88b5891 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-25 18:47:58+03:00

0.766.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix distincts  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0871985 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-22 17:17:03+03:00

0.765.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Remove some remaining external internet accesses in the dockerfiles  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5ef1cbb ]
 * A minor dev-dependency correction                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2534629 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * dls client timeout + disable dls tests by default  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0e38bec ]
 * lim-off in async distinct                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f3b441f ]
 * add limit / offset for distincts                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/94ccee2 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added dataset options to response, BI-1042  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e367e95 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-22 16:20:22+03:00

0.764.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1069] mistype fix in CSV preview  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/83da9f1 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * More notes                                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/becedb1 ]
 * yasm_config: token notice                                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/81bc9a7 ]
 * releaser dev-dep                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/af95828 ]
 * Put the external binary libraries into a single submodule                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6067575 ]
 * Remove the unworkable '.env' from makefile                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dfc0547 ]
 * docker-cache handling fixes                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/efbd077 ]
 * MSSQL debs from a submodule, to avoid requesting microsoft.com from the tests  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2d9bacf ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c696c5f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-21 13:44:59+03:00

0.763.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1069] yandex-bi-common[db]==6.5.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/82ed1f1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-20 23:16:23+03:00

0.762.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1069] yandex-bi-common[db]==6.4.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3f517d2 ]
 * [BI-1069] yandex-bi-common[db]==6.3.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d699b83 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-20 21:49:25+03:00

0.761.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1069] Fix us auth mode for external installation  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c3dbe08 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-20 21:13:40+03:00

0.760.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1069] US_MASTER_TOKEN to granular settings            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6ac0841 ]
 * [BI-1069] US_MASTER_TOKEN from env instead of app config  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3d6d587 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-20 20:36:24+03:00

0.759.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1069] Exc message fix                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5b10b81 ]
 * [BI-1069] Mostly migrated on US manager and bicommon6  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/872e992 ]
 * [BI-1069] Mostly migrated on US manager and bicommon6  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/850014e ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yasm_config: the changes that were applied to production just now anyway  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9cdfdb7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-20 19:36:13+03:00

0.758.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Auxiliary script: diff_with_prod  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a0359af ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-1095] do not fail if exbuilder does not exist  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/440f5d0 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed dataset loading when native_type is None, BI-1124  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0aa7f72 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-19 18:59:51+03:00

0.757.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed usage of filter_args=None       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f09e812 ]
 * Fixed usage of allow_none for values  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7d80a42 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-18 19:23:22+03:00

0.756.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0a4ab3b ]
 * Added error for data sources without raw_schema, BI-1098  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7dc30d1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-15 16:07:46+03:00

0.755.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==4.328.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/236c452 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed validaion test                                                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/130d22d ]
 * Updated bi-common                                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9082459 ]
 * Empty error list for successful formula validation                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/05c6e3b ]
 * Implemented off switch for sample data source when data sources are updated, BI-1055  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/63c8e23 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-14 18:55:01+03:00

0.754.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added 'valid' attribute to source and avatar API, added title conflict errors  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7f9b6ef ]
 * Validation errors for title conflicts                                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0e22302 ]
 * Replaced 'regular' with 'data' in /preview                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9fb6663 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-13 16:46:46+03:00

0.753.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[db]==4.325.0, sqlalchemy-statface-api==0.27.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d5a2deb ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-13 14:34:08+03:00

0.752.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Allow null in values in WHERE  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/180af8d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-12 17:36:27+03:00

0.751.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * return 400 on logic error  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bd4a3c4 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-12 17:07:25+03:00

0.750.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-formula  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/da6623e ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-12 14:01:42+03:00

0.749.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-545] yandex-bi-formula==3.97.0 (quantile, quantile_approx)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/85a7011 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-11 15:59:37+03:00

0.748.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added test, fixed validation with raw_schema = None, BI-1045  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/62cfc56 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-07 19:06:37+03:00

0.747.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * sqlalchemy-metrika-api==1.15.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b5ecabb ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-07 18:22:22+03:00

0.746.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1094] Sentry configured for public DS API  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/998b3ef ]
 * [BI-1094] sentry-sdk to requirements           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/412d380 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-07 17:22:48+03:00

0.745.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * allow None for raw_schema in marshmallow  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/27e377c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-07 15:52:02+03:00

0.744.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e789e90 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-07 10:19:09+03:00

0.743.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1090] yandex-bi-common[db]==4.322.0             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d1df369 ]
 * [BI-1065] Fix tests-internal US client auth params  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a028c73 ]
 * [BI-1065] yandex-bi-common[db]==4.320.0             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/002bfb3 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-06 19:53:33+03:00

0.742.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.26.0                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1d1bc7e ]
 * put `certifi-yandex` last to, hopefully, make it override `certifi` properly  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0a7d45e ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-06 14:29:30+03:00

0.741.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * YQL_TOKEN environment variables and YQL utility functions was removed  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8f42bf1 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed connection replacer                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/07fac2c ]
 * Removed legacy schemas and fields, BI-1012  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/413e17e ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * statinfra-clickhouse-sqlalchemy==0.0.10.9  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/760ef7c ]
 * connections schemas cleanup                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/00df0dc ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-05 20:51:36+03:00

0.740.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed logging in replace_connection if table doesn't exist  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/181ebac ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-11-01 12:48:36+03:00

0.739.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed application of multiple autofixes that result from a single validation action, BI-1079  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/13d3974 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-31 18:51:10+03:00

0.738.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * sqlalchemy-metrika-api==1.14.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e3b7434 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-31 18:44:57+03:00

0.737.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * sqlalchemy-metrika-api==1.13.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/553c596 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-31 18:17:43+03:00

0.736.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix eqe timeout + up uwsgi  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/845ab07 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-31 14:47:56+03:00

0.735.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4be5dbd ]
 * added row_count_hard_limit parameter to async api  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/569d1ea ]
 * row count limit: changed err status code           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e83dba0 ]
 * [BI-1048] max row count                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6783828 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-30 20:26:41+03:00

0.734.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * log rls filters  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e5268c7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-30 13:55:21+03:00

0.733.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3b53320 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-29 19:34:36+03:00

0.732.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Say NO to legacy, BI-1012                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/45b6199 ]
 * Fixed role usage                                                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e80bb86 ]
 * Removed legacy attributes from dataset getter endpoint, removed legacy source replacer  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7787e62 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[db]==4.314.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/70928e5 ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix error formatting  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6cd8736 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-29 18:54:33+03:00

0.731.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1059] yandex-bi-common[db]==4.313.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d8faf92 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-28 21:07:31+03:00

0.730.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * remove celery  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7c007db ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-28 19:24:33+03:00

0.729.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.312.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/294ea7e ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-1046] calculate materialization status by fixed source id list  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f3bd880 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added comment about hack                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/31146cb ]
 * Added replace_connection action, BI-1045  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/220cad4 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-28 15:55:34+03:00

0.728.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3f88197 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-25 16:57:55+03:00

0.727.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed support for GenericProvider connection  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b619206 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-25 14:51:09+03:00

0.726.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.307.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cca370d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-23 18:26:05+03:00

0.725.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon                                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ec50b81 ]
 * changed conn preview format                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b7185b1 ]
 * added profiling to get connection preview api handler     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ceddbce ]
 * get preview data through data_source.get_raw_data_stream  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9767075 ]
 * [BI-402] get connection preview api handler               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0c32b95 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-23 01:06:36+03:00

0.724.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.299.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9d12238 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-21 18:37:11+03:00

0.723.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common and formula, updated usage of subqueries, BI-814, BI-1037  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f09c5be ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-21 18:12:46+03:00

0.722.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * disable retries in MaterializerClient  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8bc1f57 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-21 12:16:04+03:00

0.721.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * statfacereportsql distinctss fix and tests  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d192b04 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-18 17:41:09+03:00

0.720.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common==4.296.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/17697c8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-18 16:44:21+03:00

0.719.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-formula==3.93.0: statface_report_api quoting fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7250723 ]
 * sqlalchemy-statface-api==0.24.0: quoting fix (preliminary)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bb1d8f8 ]
 * dataset_cli quickstart example and conveniences             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cd2a6b0 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-18 16:10:26+03:00

0.718.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.294.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/eb0a94a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-18 12:50:57+03:00

0.717.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * requirements up                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3c76d06 ]
 * [BI-885] Statface Report SQL connection  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/16f2212 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * status for DatasetConfigurationError  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/091bf76 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-18 12:14:50+03:00

0.716.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-17 13:30:41+03:00

0.715.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * statinfra-clickhouse-sqlalchemy==0.0.10.6  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/271514b ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated test                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0abd8f1 ]
 * Added usage of ALLOW_SUBQUERY_IN_PREVIEW variable, BI-814  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5a0a4f0 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-17 11:26:13+03:00

0.714.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4eab78b ]
 * Moved select limits to const.py                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a6e703e ]
 * Added usage of subquery mode in preview from origin, BI-814  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0d28ec8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-16 15:23:46+03:00

0.713.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/25f3a21 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-16 14:46:21+03:00

0.712.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/72d2394 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f9b190b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-16 11:30:31+03:00

0.711.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * docker: reinstall certifi-yandex to ensure it is done after certifi  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ed0ed17 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed call to Dataset.quote  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/91ed506 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/80cbd4f ]
 * [BI-1036] moved preview_enabled property to datasource  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/60dd97b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-14 19:21:45+03:00

0.710.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0ee4c18 ]
 * Added role argument to all calls of get_data_source      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6cc2bfa ]
 * Fixed usage of get_parameters_hash                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7eaea23 ]
 * Renamed dataset execute conmmand to dataset execute-raw  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/70da1ae ]
 * Added dataset execute command to dataset-cli             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/320dcb0 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-11 18:55:59+03:00

0.709.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common                                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e420ad8 ]
 * Moved saving of dataset to before materialization task  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d13d9d6 ]
 * Added source_type to source updater                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d938575 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-11 14:50:56+03:00

0.708.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * statinfra-clickhouse-sqlalchemy==0.0.10.4  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0bafa15 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Switched to using Dataset.resolve_role explicitly  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5933ab5 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-11 13:05:54+03:00

0.707.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * forgotten settings BACK_SUPERUSER_OAUTH BACK_SUPERUSER_IAM_KEY_DATA  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/be583a9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-10 22:59:50+03:00

0.706.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed handling of update_source action by excluding the source itself from source_can_be_added  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cc9e5c7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-10 16:58:30+03:00

0.705.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.282.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3465906 ]
 * status_code for DatabaseUnavailable   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ce8e053 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-10 13:52:12+03:00

0.704.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Switched all tests to new dataset creation API  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d44bfdc ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon & sqlalchemy-metrika-api                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/10f48ed ]
 * [BI-1028] added metrica api connection accuracy parameter  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/19fe60e ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-09 14:44:30+03:00

0.703.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/de1d658 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-08 18:07:59+03:00

0.702.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1021] Fix type_  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fdaca77 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-08 16:14:35+03:00

0.701.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-1021] Refactoring                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a76f5ca ]
 * [BI-1021] New generic provider connection schema in connection handles        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/df405da ]
 * [BI-1021] GenericProviderConnection schema + ConnectionSchema generalization  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/52bc66f ]
 * yandex-bi-common[celery,db]==4.275.0                                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e35d22d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-08 12:00:49+03:00

0.700.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed creation of preview task                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a16a6f2 ]
 * Added join type check to validation                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d32c43d ]
 * Fixed feature turning off                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6719abe ]
 * Fixed validation of formulas with feature functions  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5d95008 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-08 11:38:40+03:00

0.699.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-978] Inactual TODO was removed                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0bebb49 ]
 * [BI-978] Apply new error processing mechanics to handling middlewares  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/67c6a31 ]
 * [BI-978] exc.BillingActionNotAllowed: bad request instead of 402       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/160b38e ]
 * [BI-978] New API error handling methods: extraction and redesign       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dae2394 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.273.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/704585b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-07 13:06:52+03:00

0.698.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * exc.BillingActionNotAllowed: bad request instead of 402  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0a0b588 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-04 18:39:37+03:00

0.697.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed field generation                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0155def ]
 * Fixed result_schema field generation  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b720f95 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-04 18:30:15+03:00

0.696.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.272.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a4efff9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-04 17:10:31+03:00

0.695.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.271.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cbf0444 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-04 13:50:06+03:00

0.694.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.270.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bdcf9d1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-03 19:32:50+03:00

0.693.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed merge error                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5e38a8d ]
 * Imp,emented hiding of non-cached fields in preview  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/87d7eac ]
 * Added select command to dataset-cli                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4437f1b ]
 * Added feature status to result_schema               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7d21ec7 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon                                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5516dca ]
 * [BI-384] return null (None) instead "null" for invalid geovalues  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/555863a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-03 16:34:12+03:00

0.692.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/35143ab ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-02 17:12:47+03:00

0.691.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * statface distincts icontains/istartswith  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/64615a2 ]
 * statface dataset updated tests (WIP)      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ee98026 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.266.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3729b66 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * added component public-dataset-api to releaser config  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/78b4fbd ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed 500 error in try_generate_conditions_from_columns  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4000abd ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-01 19:23:15+03:00

0.690.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Reverted field selection by title  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/11ecf50 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-01 13:55:51+03:00

0.689.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * fixed added geocoding feature usages collecting  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9ad1515 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-10-01 01:54:35+03:00

0.688.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * Make empty details & debug keys in public api  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f5d5a27 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-30 21:35:22+03:00

0.687.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon                                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/76d7ba2 ]
 * collect features usages for all features                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/82fe342 ]
 * [BI-932] schedule geocache update task on dataset update  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9e3c0e2 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-30 20:00:48+03:00

0.686.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed comparison of old and updated source  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e63b86d ]
 * Fixed refreshing of ref sources             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b83f537 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-30 19:10:05+03:00

0.685.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Removed deletion of fields on raw_schema update  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/56e144d ]
 * Removed GROUP BY hack for ClikHouse              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2eb9201 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-30 17:02:34+03:00

0.684.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-988] request id appending for public api + app prefix for internal request id                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f41fb08 ]
 * [BI-988] yandex-bi-common[celery,db]==4.262.0 (headers logging + request id appending for aiohttp)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/91a5895 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added feature cache status validation                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ee9ed8a ]
 * Updated select_data calls to use only ExpressionCtx  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d93d36e ]
 * Fixed test_source_deduplication                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2c4bf4d ]
 * Fixed validation with ID in tests                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9f1bdfc ]
 * Small fixes for empty dataset                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7094980 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-30 15:24:22+03:00

0.683.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added a workaround for ClickHouse GROUP BY problem            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e7a105a ]
 * Fixed test_access_mode_switch                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9a7383e ]
 * Fixed turning features on and off when switching access mode  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/66286c4 ]
 * Only id is required for refresh_source action, BI-996         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d5e3373 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-29 22:30:09+03:00

0.682.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-967] Gunicorn workers recycling to mitigate memory leaks  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d6f50f2 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-28 20:25:36+03:00

0.681.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix billing_host  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4fcc677 ]
 * up cfg            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c5e3b14 ]
 * billing checks    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6adc338 ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.259.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5950ab2 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-28 19:29:53+03:00

0.680.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-987] Caches env switch for public api  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/33fc8d7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-28 17:25:22+03:00

0.679.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-998] yandex-bi-common[celery,db]==4.256.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/945c631 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-28 10:17:56+03:00

0.678.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added autogenerated conditions, BI-997  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/948d8e3 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-27 20:58:59+03:00

0.677.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed auto-updating of direct fields when avatars and sources are modified/deleted  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5a87dea ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon                                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/798cb91 ]
 * replaced usage of us_cli.list_all_entries to Dataset.collection  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/00c4c47 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-27 18:47:35+03:00

0.676.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-967] Public granular settings fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0ccb13b ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added refresh_source action to validation, BI-996  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/117d0e8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-27 17:47:49+03:00

0.675.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-967] Public API forwarding was removed  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ae36609 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-27 15:06:38+03:00

0.674.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed loading of dataset in endpoints with ID, BI-995                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/aaba8d5 ]
 * Added source compatibility checks to validation and access_mode switch  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7ff0f84 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-27 13:51:08+03:00

0.673.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-967] Async dataset fields handler                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/63eaf7c ]
 * [BI-967] TODOs for syncronization sync and async version  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/55cdcb0 ]
 * [BI-967] Range async handler                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e91eb04 ]
 * [BI-967] Refactoring                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e5be476 ]
 * [BI-967] yandex-bi-common[celery,db]==4.253.0             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/70067a1 ]
 * [BI-967] Distinct async handler                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/49271a6 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-27 12:57:35+03:00

0.672.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-967] AIOHTTP update + log exceptions in application signal handlers  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ab03d79 ]
 * [BI-967] Configurable number of workers in gunicorn                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/001a172 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed handling of missing avatars when making exbuilder, BI-993  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b3039af ]
 * Removed featured field check                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/70ecfe4 ]
 * Fixed validation for GEOCODE,                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7c28c36 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-27 11:20:46+03:00

0.671.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-967] yandex-bi-common[celery,db]==4.252.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/605d441 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed test_geocoding  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c7d60df ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-26 17:58:32+03:00

0.670.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/618941b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-26 13:26:38+03:00

0.669.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-967] Fix .get() to [] in select_data handles for defaulted schema fields                                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/df835c3 ]
 * [BI-967] Fix async get result                                                                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/52c6720 ]
 * [BI-967] Cleanup                                                                                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9773d95 ]
 * [BI-967] Remove body from DSView.build_bi_query_for_result()                                                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/12a031c ]
 * [BI-967] BIQuery for distinct handler                                                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4d71811 ]
 * [BI-967] Hide redis password from repr in async app settings                                                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/656ff32 ]
 * [BI-967] Fix caches settings                                                                                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/13089f8 ]
 * [BI-967] All available methods of DatasetResource become class methods. Updating result schema for in async dataset result handler  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ac93414 ]
 * [BI-967] Redis settings fix                                                                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/39c0edd ]
 * [BI-967] Redis setup for aiohttp app + server header override                                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f7c4c46 ]
 * [BI-967] Public dataset view generification                                                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ff8987b ]
 * [BI-967] Flag to switch public api mode (forward-to-sync/direct)                                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a448709 ]
 * [BI-967] Initial version of error handling in async API (with constant HTTP codes)                                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0d4da2f ]
 * [BI-967] Initial version of public async dataset result                                                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/97b2f30 ]
 * [BI-967] make_ds_view, make_ds_validator, make_result becomes class methods                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b54d690 ]
 * [BI-967] Public forwarder now use US manager created by middleware                                                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6d61428 ]
 * [BI-967] BIQuery building for result in DataSetView                                                                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6e267c7 ]
 * [BI-967] Adaptation for creation service US manager in middleware                                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c61cb00 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-26 13:02:11+03:00

0.668.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common                                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0d6dfc5 ]
 * Added handling of datetime values with zeroes for date  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cf1c129 ]
 * Fixed validation tests                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d43e230 ]
 * Added case-insensitive filters, BI-868                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a4f0dbf ]
 * Added local Oracle DB to fix tests                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/284a57a ]
 * Added checks of managed_by attributes                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cde2a52 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * filter fields managed by feature from preview  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/462a8d9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-25 22:27:38+03:00

0.667.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon and statinfra-clickhouse-sqlalchem         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3b21543 ]
 * [BI-931] added distinct api handler copy without rls check  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cc81bda ]
 * fixed make_literal_value_ctx for geo types                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a609f08 ]
 * disabled flask verbose 404 error messages                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ed68c78 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-25 14:26:55+03:00

0.666.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8cfff92 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-23 17:20:13+03:00

0.665.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.18.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d1dee44 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-23 15:42:48+03:00

0.664.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed virtual flag field, BI-977  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e1a7c15 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-23 14:15:12+03:00

0.663.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-23 13:03:24+03:00

0.662.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.240.0 & appnope remove  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3addbf3 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.17.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ab320da ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added handling of DatasetConfigurationError in validation           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6146b33 ]
 * Added hiding of non-user-managed fields in suggestions and /fields  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1578bf5 ]
 * Added virtual flag to API objects                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bfdd088 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-23 13:00:49+03:00

0.661.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated usage of feature manager's API  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9dd54b4 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-20 16:13:45+03:00

0.660.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Removed debug stuff                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1ab0d27 ]
 * Fixed geocoding, added test, BI-934          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/efdcb51 ]
 * Added basic geocoding functionality, BI-934  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/60cf3b7 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon                                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f2ed860 ]
 * enabled logging from urllib3.connectionpool (to be able to monitor request retries)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d455b96 ]
 * [BI-623] moved err_code prefixes to bi-api                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8d5427e ]
 * [BI-623] changed err_code prefix                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b2460f9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-20 14:39:28+03:00

0.659.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-937] add GET /api/v1/datasets/{ds_id}/fields to public URLs                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7f3793c ]
 * [BI-937] Fix context passing in async us mgr & response header forwarding fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1239f53 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-19 22:49:14+03:00

0.658.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a3f6bc1 ]
 * Changed usage of own_raw_schema to saved_raw_schema in sources     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6e9518b ]
 * Added launching of materialization tasks for multisource handlers  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/05ecd70 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-19 19:02:37+03:00

0.657.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * bi-common 4.235.0                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e76d62f ]
 * More lock_aggregation passaround for correctness  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8dd7f9c ]
 * statface add_avatar test (non-working)            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fabbc85 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-19 17:49:55+03:00

0.656.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * bi-common update, statface test fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/aad8ba4 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-19 15:25:39+03:00

0.655.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-971] Migration to internal version of ylog  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/431e65d ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * bi-common 4.230.0                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/feb3cc1 ]
 * tests/dsmaker lock_aggregation                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8d4903a ]
 * statface user_url validation error test (non-working)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a4dd32e ]
 * statface_report lock_aggregation passaround and test   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/af219be ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added test for MetricaAPI  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cf62935 ]
 * Updated formula            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/72f3a58 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-19 14:44:40+03:00

0.654.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-623] rename fields in err response  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/12a7b84 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-18 15:24:13+03:00

0.653.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common[async,celery,db]==4.226.0                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b289866 ]
 * [BI-623] inherit more exceptions from DLBaseException               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e97b042 ]
 * [BI-623] moved _prepare_api_exception_info from bicommon to bi-api  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4b130a6 ]
 * [BI-623] api errors unified format                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fe301f5 ]
 * [BI-623] api errors unified format                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2a02f69 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-18 13:59:31+03:00

0.652.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * yandex-bi-common[async,celery,db]==4.221.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/60a6737 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-17 17:11:09+03:00

0.651.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-969] yandex-bi-common[async,celery,db]==4.220.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/61fc874 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-17 14:15:02+03:00

0.650.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-917] datasets_publicity_checker  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b4045c6 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added title to source API, BI-964              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f43fe2f ]
 * Updated usage of new attributes in raw_schema  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1b13f15 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-17 13:59:20+03:00

0.649.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-384] result data postprocessing: return None as None (null), not string  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6acdbd8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-16 14:12:41+03:00

0.648.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed statface source schema                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e5d0653 ]
 * Added deduplication of data sources for action add_source  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2c40c6c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-13 19:50:48+03:00

0.647.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed preview                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5d50cb8 ]
 * Fixed creation of CHYT datasets  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/01d378d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-13 10:28:32+03:00

0.646.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dda08b1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-12 20:29:38+03:00

0.645.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.15.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9ce43f5 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-12 17:40:27+03:00

0.644.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * bi-common: Disable the context critical-requirement  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8c7ad74 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added ability to combine dataset configurations from US and API in /preview  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f6f7784 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-12 16:45:10+03:00

0.643.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed schema for raw_schema in sources  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c76f583 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-12 11:24:40+03:00

0.642.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common[async,celery,db]==4.210.0            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/22a428b ]
 * MaterializerSchedulerActionConfig context passaround  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/80b7f90 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-11 18:48:24+03:00

0.641.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * add type to /fields  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3e0a87a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-11 17:58:25+03:00

0.640.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-11 17:29:19+03:00

0.639.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added parameter_hash attribute to data source API                                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d72c244 ]
 * Implemented autofix of direct result_schema fields when raw_schema is updated in multisource API  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9e7857b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-11 14:43:14+03:00

0.638.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-11 14:33:34+03:00

0.637.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added support for legacy actions in OneOfSchema  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4d3a224 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-11 13:55:25+03:00

0.636.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[async,celery,db]==4.208.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/00f7f8a ]
 * [BI-956] fix rls                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/847dba4 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-11 12:22:47+03:00

0.635.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[async,celery,db]==4.206.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b34f331 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-10 19:06:19+03:00

0.634.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed usage of join_type in avatar relations  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c97dd50 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-10 18:54:17+03:00

0.633.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Implemented more precise schemas for update actions using OneOfSchema, BI-954  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b0ac003 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-10 18:29:53+03:00

0.632.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added missing context argument to create_from_dict  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0e15f1b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-10 15:08:53+03:00

0.631.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * _test_in_devenv: minor fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3c72905 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added used of updates in /values enpoints  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/19e31c2 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-10 14:02:17+03:00

0.630.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-937] Improve async app settings security  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/421c771 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yandex-bi-common==4.200.0                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/324cf11 ]
 * Minor notes improvements                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/abb392c ]
 * test_in_devenv notes                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/db70deb ]
 * tox: disable warings that flood out the results  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3155252 ]
 * Context passaround                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cf2f687 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3476e9a ]
 * [BI-948] return db error in intranet  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f9bd3a1 ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * uwsgi eqe: http -> http-socket  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0aa31c0 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-10 12:30:25+03:00

0.629.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-09 17:16:29+03:00

0.628.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-937] Dataset config forwarding handle was removed                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0d3cf18 ]
 * [BI-937] Body forwarding fix                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/df35b5a ]
 * [BI-937] Folder ID substitution in forwarded header + request body forwarding  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/57e8163 ]
 * [BI-937] Required settings/environment final fixes                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/46f6a80 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-09 17:15:37+03:00

0.627.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-937] yandex-bi-common[async,celery,db]==4.200.0 (headers masking fix)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4234988 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-946] validate geo fields values in dataset result api  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cf508cc ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-09 14:41:10+03:00

0.626.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-937] Add folder ID header handling in case of master key auth  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5ab15ec ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-06 20:23:02+03:00

0.625.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-937] US public API token was added to settings                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bfcfe38 ]
 * [BI-937] Tokens from async to sync pass-throw + allow to use master token in sync datset api in case of public  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/77d4fe8 ]
 * [BI-937] Auth schema for sync public bi-api                                                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9bdb8d4 ]
 * [BI-937] Public API settings to granular settings                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5e1f248 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Dataset.get_locked_entry_cm context  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/72cdbf8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-06 19:32:31+03:00

0.624.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-937] New profiler middleware initialization      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1fe5866 ]
 * [BI-937] yandex-bi-common[async,celery,db]==4.197.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/966cacd ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added join_type to relations and fixed meta for source schemas, BI-933  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/53948aa ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-06 16:49:09+03:00

0.623.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3a2f78a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-06 12:58:39+03:00

0.622.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-937] Public API Key env var name fix               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/beb4b8e ]
 * [BI-937] Public API key check implementation in async  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b08de1e ]
 * [BI-937] Fix public API key auth in sync app           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/676c3bc ]
 * [BI-937] Public API proxy initial implementation       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dc9dd40 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.14.0                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e0501be ]
 * launcher script: non-bashism                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3fc6f3a ]
 * shellcheck                                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4772c09 ]
 * Reduce the amount of workers in testing to reduce the memory use  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e6c6046 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added suppression of errors to suggestion endpoint                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9ae17ee ]
 * Added support for schema_name in sources, removed PolyField, BI-651  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8e73008 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-06 11:38:12+03:00

0.621.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-937] yandex-bi-common[async,celery,db] in setup.py  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c9c3c67 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4eb95dd ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-05 11:24:25+03:00

0.620.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-937] Async API application framework  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/97599b2 ]
 * [BI-937] Async requirements               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/92110b9 ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * sqlalchemy-statface-api==0.13.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f01f0dd ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-04 18:14:34+03:00

0.619.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9ad1a28 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-04 15:53:58+03:00

0.618.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/597bf02 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-04 15:15:48+03:00

0.617.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added dataset.build_source(...) calls to data endpoints, added test for /result with multisource dataset  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6ac6b6b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-04 11:29:19+03:00

0.616.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-885] statface report url parsing error-wrap and improvements  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/033ff74 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-919]: result: add option for ignoring filters with unknown fields  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b1a5776 ]
 * [BI-935] catch bicommon.exc.DatabaseQueryError                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c1da150 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added generation and validation of avatar relation expressions to expressions.py  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/844ff6f ]
 * Simplified the logic in expressions a little                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/08f48d3 ]
 * Removed some legacy code from field_resolution                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b3fc46d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-09-03 19:26:38+03:00

0.615.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e629d09 ]
 * Removed duplicate code from /store endpoint      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/626e228 ]
 * Added shortcuts for updating common and formula  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/77e8d87 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-30 17:57:50+03:00

0.614.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed resolution of avatars and sources for direct fields, BI-916  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1470d39 ]
 * Removed some direct usages of Dataset.data                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/062dcee ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-30 16:57:12+03:00

0.613.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Consolidated DatasetView/Validator creation code, result_schema loading and title-guid conversions in formulas  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/80a705b ]
 * Added usage of set_access_mode in materialization management                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7ef3240 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-29 19:33:45+03:00

0.612.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added /preview for dynamic datasets  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/faa7c32 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-29 10:10:31+03:00

0.611.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * Fix build scheduler URL in README                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c2c74d5 ]
 * [BI-881] Extracting title from RKeeper table definition during dataset construction  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8933391 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix /fields  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/96ccf1b ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added NOTCONTAINS filter, BI-802  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fd7288d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-28 19:04:48+03:00

0.610.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [CHARTS-1452] add 'hidden' to /fields  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/30c4728 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-28 17:15:00+03:00

0.609.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed field validation for dynamic datasets  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/880e7e3 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-28 13:18:13+03:00

0.608.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula req to version with fixed suggestions, updated test                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cb167e3 ]
 * Added support for dynamically created datasets in suggestions and formula validation  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d272129 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-27 19:20:03+03:00

0.607.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added allow_none to table_name parameter in source schemas  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4b79bb2 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.182.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ba3c342 ]
 * increase uwsgi harakiri timeouts      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/66cc2ee ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-26 18:00:37+03:00

0.606.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.181.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/db524f6 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed type hint for context                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7fded5e ]
 * Removed hack for db_version saver in dataset POST  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0ddb1ad ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-26 13:40:40+03:00

0.605.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * has_data_sources()     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/97c3109 ]
 * fix csv source update  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/492aa6e ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-23 18:54:54+03:00

0.604.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common, updated test that checks root avatar resetting                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a81c1cc ]
 * Added support for empty datasets in GET                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bf453c2 ]
 * Added RLS to validation input/output, added saving of all props in multsource POST for dataset  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3c5042f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-22 13:23:03+03:00

0.603.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * An extra assert                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/926668b ]
 * [BI-885] statfacereport: title: fixed tests  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ded360e ]
 * [BI-885] statfacereport: title               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/97d3ef1 ]
 * Rsync in the container                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/69c53cb ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-22 11:43:53+03:00

0.602.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-21 16:40:53+03:00

0.601.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-885] statfacereport: deps update, add origin, disable preview  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/526eae8 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added is_root property to source avatars  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4bc84e9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-21 16:11:25+03:00

0.600.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-770] disable group by for queries without measures  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9f8ec38 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-21 15:08:10+03:00

0.599.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated PUT /datasets/{id}/... endpoint, added dsmaker module for "shorthand" notation in tests, several API fixes  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fa8a1ce ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-21 14:04:57+03:00

0.598.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-885] statfacereport: more tests, updated sqlalchemy dialect  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/347e0aa ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.173.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bd54d6a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-20 20:02:48+03:00

0.597.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-885] statfacereport distincts and fielddate minmax  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/af39311 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.171.0     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/310ac4d ]
 * \+USE_US_PUBLIC_TOKEN in testing-public  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bf4c75c ]
 * fix schema updating for conns with dstc  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b36d3b1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-20 18:17:49+03:00

0.596.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed usage of context in from_db calls  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f480872 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-20 16:26:37+03:00

0.595.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-885] statfacereport source fixes  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f3f841a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-20 11:09:31+03:00

0.594.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Renamed source relations to avatar relations  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c6b8b5b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-19 20:06:39+03:00

0.593.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.169.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2f49279 ]
 * [BI-902] add master_token option      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8514330 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Implemented source_relation validation actions                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f04096a ]
 * New version of dataset creation handle that creates an empty dataset  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8a7b73b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-19 17:47:35+03:00

0.592.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-885] requirements up                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7d9d7e0 ]
 * [BI-885] attr.s-typed Context model                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0bdf445 ]
 * Statface report test external_ipv6 marker                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8dfef13 ]
 * sqlalchemy-statface-api requirement bump                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e42c315 ]
 * [BI-885] Statface Report (API) dataset: minimally-working version           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4e3a16d ]
 * WIP: [BI-885] Statface Report (API) dataset: additional non-working tests   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4562998 ]
 * WIP: [BI-885] Statface Report (API) dataset: minimal kind-of-working tests  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/57335ef ]
 * WIP: [BI-885] Statface Report (API) dataset creation; dataset context       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e58e2ac ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * configs for public testing  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ded259c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-19 15:40:50+03:00

0.591.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * get rid of sentinel in tests  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/da1bdc7 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-formula==3.76.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0bb8677 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-19 11:41:42+03:00

0.590.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.163.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2fb624f ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Implemented source avatar validation actions  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d15da0b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-16 17:23:10+03:00

0.589.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added update actions for data sources                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/002fbdc ]
 * Removed cast autofix for new fields if cast is defined, BI-877  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/40b9e73 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.161.0       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0f9fb96 ]
 * remove db_name from ds if from conn        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bea9e7d ]
 * do not store db_name from conn in dataset  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7d43b1b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-16 15:48:41+03:00

0.588.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed setting of default string type for invalid fields, BI-849  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f3ec578 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-15 16:03:49+03:00

0.587.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added default string for cast of invalid fields, BI-849          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ac13aea ]
 * Removed name from anonymous dataset in validation                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9f586be ]
 * Stabilized basic usage of /api/v1/datasets/validators/dataset    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/522ae3d ]
 * Replaced Field class with BIField from bicommon                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7eece45 ]
 * WIP                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c2a0795 ]
 * Create anonymous DS with save=False                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5025075 ]
 * Added more TODOs                                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8b57273 ]
 * Started adding more validation actions for multisource datasets  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/82a36a6 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-14 17:02:06+03:00

0.586.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-884] Pre-creation of empty table definitions in RKeeper connection was removed  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8feca05 ]
 * [BI-884] yandex-bi-common[celery,db]==4.152.0                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ecbfce9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-09 19:38:50+03:00

0.585.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.151.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/756f32c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-09 18:28:46+03:00

0.584.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-734] Pointless comma was removed                                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c595bf8 ]
 * [BI-734] Fix duplicated table_connection_id in DS creation kwargs for RKeeper                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ae7159e ]
 * [BI-734] Raw schema generation fix for not filled native type                                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/73eaf81 ]
 * [BI-734] Fix: convert RKeeper connection raw schema to list of dicts                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/704ac45 ]
 * [BI-734] Validation for CreateFrom.RKEEPER                                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/99c0641 ]
 * [BI-883] Upgrade pip before installation; install requirements before copying code to speed-up docker build  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8d91d05 ]
 * [BI-883] Internal git-related files was added to .dockerignore                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b5ef997 ]
 * [BI-883] Docker-ignore                                                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/42c8573 ]
 * [BI-734] Ability to create dataset over RKeeper connection                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/144ef42 ]
 * [BI-734] List tables in RKeeper connections                                                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c2d9f4a ]
 * [BI-734] RKeeper schema required fields become really required                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a5d3134 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-06 17:55:04+03:00

0.583.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon and bi_formula           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a91ed5a ]
 * [BI-857] added geopoint geopolygon types  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5b36fab ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-02 13:39:07+03:00

0.582.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-734] Folder ID to RKeeper connection schema                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c3e6b51 ]
 * [BI-734] RKeeper Connection fix 500 on connection testing                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/20ac42d ]
 * [BI-734] RKeeper Connection meta: state normalization, title was removed                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a14eea7 ]
 * [BI-734] Table name fix for Rkeeper connections                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0a3ba2d ]
 * [BI-734] Fix delete for rkeeper connection                                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/acf9292 ]
 * [BI-734] Updating RKeeper connection with PUT                                                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4081d5c ]
 * [BI-734] TODO for generic schema in GET connection                                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/286ed7f ]
 * [BI-734] Schema for RKeeper connection user settings                                                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/824ed6e ]
 * [BI-734] 400 on RKeeper schema validation fail                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/74f83fc ]
 * [BI-734] Additional tables                                                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c9dfe23 ]
 * [BI-734] Ability to create rkeeper connections                                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3eb0021 ]
 * [BI-874] Test fix: create PG conn                                                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3f0cf22 ]
 * [BI-876] Do not fail connection deletion in case of exception on materialization tables deletion scheduling  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e9a4f69 ]
 * [BI-874] Connections listing handle was removed                                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8fb5ba6 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-08-02 13:06:25+03:00

0.581.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-864] Update yandex-bi-common (interpret clickhouse enums as strings)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/98a4200 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-30 18:33:06+03:00

0.580.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-864] Update statinfra-clickhouse-sqlalchemy  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/03806b1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-30 13:21:23+03:00

0.579.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-734] yandex-bi-common[celery,db]==4.144.0       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/be951b5 ]
 * [BI-734] Schema mapping for RKeeper data source     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cffe7e6 ]
 * [BI-734] temp yandex-bi-common[celery,db]==5.0.0a6  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0c0035c ]
 * [BI-734] temp yandex-bi-common[celery,db]==5.0.0a5  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2174454 ]
 * [BI-734] temp yandex-bi-common[celery,db]==5.0.0a4  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9b61161 ]
 * [BI-734] temp yandex-bi-common[celery,db]==5.0.0a3  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d3890a8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-26 17:44:25+03:00

0.578.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-734] yandex-bi-common[celery,db]==4.142.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c8bb1d4 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-25 20:05:44+03:00

0.577.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-734] yandex-bi-common[celery,db]==4.138.0                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/97fd325 ]
 * [BI-734] Fix pip-compile incompatibility with new pip                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ce25d2d ]
 * Get back remote python interpreter & make compose dev env looks like bi-common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/efaa006 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-25 13:28:36+03:00

0.576.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * sqlalchemy-metrika-api==1.10.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5ec7e57 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-23 17:43:14+03:00

0.575.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * removed unnecessary "TODO" comment       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/127220d ]
 * [BI-853] one more fields validation fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/56ae352 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-23 14:39:24+03:00

0.574.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common==4.136.0       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/aea750f ]
 * logs api fields validation fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8942dca ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-23 13:11:36+03:00

0.573.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.135.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b8de83f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-22 18:54:19+03:00

0.572.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common==4.134.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0e82d38 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-22 16:56:40+03:00

0.571.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added source avatars and relations to dataset response  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/19d1272 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/89a26f0 ]
 * fixed appmetrica logs test connection api handler  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a99bb1b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-21 13:11:04+03:00

0.570.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-common to 4.131.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b8b9415 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-19 14:10:38+03:00

0.569.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-19 13:06:36+03:00

0.568.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-common to 4.130.0               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a98a7d9 ]
 * Fixed group_by ids for aggregation checks  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ef54b40 ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.129.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ee7a7f0 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-18 21:16:17+03:00

0.567.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.128.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c8ac215 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-18 18:45:08+03:00

0.566.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-794] return info about us errors  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b6e08c9 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * more forgotten metrika_logs specific places  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5502a9c ]
 * appmetrica_conn_create_fix                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a7c5cc9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-18 18:08:35+03:00

0.565.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * moar                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0ed5ff8 ]
 * [CHARTS-1224] increase uwsgi buffer-size  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/703e567 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-18 13:25:20+03:00

0.564.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Full integration of result fields with source avatars  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3f7f7df ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common==4.123.0        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e524484 ]
 * appmetrica_logs_ds_creation_fix  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/adcd31d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-18 12:46:39+03:00

0.563.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * fixed marshmallow pre_load  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c009a22 ]
 * removed yt and yql legacy   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9ad5f40 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-17 18:26:08+03:00

0.562.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * 400 on raw_schema getting fail             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dea37e0 ]
 * statinfra-clickhouse-sqlalchemy==0.0.7.29  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/549b925 ]
 * yandex-bi-common[celery,db]==4.73.2        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/44ab39f ]
 * psycopg2 from sources                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/46152bd ]
 * yandex-bi-common[celery,db]==4.73.1        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/28a1d63 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common==4.122.0                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/203e413 ]
 * dataset creation api - replaced asserts to abort(400) calls                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0ae382c ]
 * appmetrica logs api dataset creation                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/20029bc ]
 * appmetricaLogsApi connection and dataset schemas                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/43cab56 ]
 * yandex-bi-formula==3.72.0                                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f97fce7 ]
 * fixed logging config                                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/08eba9b ]
 * Added logging for sqlalchemy_metrika_api. Removed duplicating logging configs.  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/67c7f12 ]
 * sqlalchemy-metrika-api==1.6.0                                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/35b3317 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-formula  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6f81cba ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-17 15:01:48+03:00

0.561.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * statinfra-clickhouse-sqlalchemy==0.0.7.36  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4d6294a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-17 13:51:20+03:00

0.560.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * statinfra-clickhouse-sqlalchemy==0.0.7.35  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8e38ebb ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-17 13:12:33+03:00

0.559.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * [BI-82] Updated bi-common for LowCardinality, introdced pip-compile-here helper script  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fa3e435 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common==4.117.0                                              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/de6c9c5 ]
 * Read lock_aggregation flag from raw_schema and write to result_schema  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7b9a6e8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-16 18:52:28+03:00

0.558.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added title and group fields to DataSourceTemplateResponseSchema  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3454993 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-15 16:51:03+03:00

0.557.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added GET /<connection_id>/info/sources entrypoint  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/886341a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-12 16:39:44+03:00

0.556.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-common                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9f02d79 ]
 * Added default dialect in case materialization is not ready  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/19f7a09 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-12 13:35:56+03:00

0.555.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.113.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/379c8b7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-11 19:50:06+03:00

0.554.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.112.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9ee46ae ]
 * yandex-bi-common[celery,db]==4.111.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/be7ab5d ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added raw_schema to sources                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/59c16ac ]
 * Added sources to dataset GET handler, updated marshmallow, BI-299  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/10a5d26 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-11 18:53:38+03:00

0.553.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed getting of raw schema  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/35fda1b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-11 14:22:11+03:00

0.552.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * sqlalchemy-metrika-api==1.9.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/77f8db2 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-11 13:20:32+03:00

0.551.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common==4.109.0               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b5a2e88 ]
 * [BI-831] catch MetrikaHttpApiException  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b9237b6 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-11 11:44:27+03:00

0.550.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d50622e ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-10 16:25:07+03:00

0.549.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-639] add new fields to result_schema on source update  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6b895c2 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Separated EnumField and EnumNameField  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c35d620 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-10 14:15:07+03:00

0.548.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common==4.104.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8c5f915 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-10 13:04:50+03:00

0.547.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/47fafbc ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-09 14:30:43+03:00

0.546.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-09 13:18:19+03:00

0.545.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added handling of FormulaError in request handlers, BI-824  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/90e912e ]
 * Updated bi-common                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d5504eb ]
 * Added better support for ISO-8601                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ccb3f41 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-08 20:23:32+03:00

0.544.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.101.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d6dd01c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-08 17:57:53+03:00

0.543.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.100.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/608d86e ]
 * fix dls config                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e85d6d0 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * sqlalchemy-metrika-api==1.8.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3f2afe9 ]
 * sqlalchemy-metrika-api==1.7.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fa994ca ]
 * yandex-bi-formula==3.72.0      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8677b2c ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * More fixes for the upcoming data source migration                                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3ac9a38 ]
 * Removed usage of legacy properties                                               [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f4935df ]
 * Fixed configuration of local PG for mat status                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9f00bf2 ]
 * Fixed some of the tests, replaced usage of external services with internal ones  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/39543f6 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-08 17:05:02+03:00

0.542.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.98.0            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fb8c888 ]
 * [BI-697] rls: generate filter expr in ds view  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cbe58b2 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-04 17:07:37+03:00

0.541.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated requirements                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8995c8b ]
 * Updates for new data source API, BI-712  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d158142 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-04 12:59:22+03:00

0.540.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * sqlalchemy-metrika-api==1.6.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/256013a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-03 17:02:24+03:00

0.539.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * sqlalchemy-metrika-api==1.5.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/921e7dd ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-03 15:22:03+03:00

0.538.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * temporary limit preview from all sources  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/59a8f69 ]
 * fix py version && update requests         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b363477 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-02 19:26:48+03:00

0.537.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update chsqla  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c34d576 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-01 18:17:03+03:00

0.536.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-formula version, BI-811  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/938327d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-01 15:37:43+03:00

0.535.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.88.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/63f4517 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-07-01 12:16:51+03:00

0.534.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * psycopg2 from sources                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/603991f ]
 * yandex-bi-common[celery,db]==4.86.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/60e5f75 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-28 20:26:19+03:00

0.533.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.85.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/649ba87 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Removed the usage of the sql_connection property for CHYT  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9e962d6 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-27 19:55:54+03:00

0.532.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-697] rls api: fix missing rls params  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1d7d4f3 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-26 13:18:17+03:00

0.531.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-772] Default caches TTL was changed to int instead of float  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/31df355 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-25 19:50:46+03:00

0.530.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-772] Default caches TTL was increased up to 3 minutes  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/aedf9eb ]
 * [BI-772] Configuration of caches TTL                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dc9624a ]
 * [BI-772] bi-common==4.79.0                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e103539 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-25 18:21:55+03:00

0.529.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-697] rls: move api to ds views  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c562584 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-25 17:41:12+03:00

0.528.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Auxiliary: yasm configurer update  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3e73723 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update chsqla  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/61567d6 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * fixed logging config                                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0fc4f89 ]
 * Added logging for sqlalchemy_metrika_api. Removed duplicating logging configs.  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2efd1f5 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Removed last usage of table link  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5ceed5a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-25 17:14:39+03:00

0.527.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-735] delete conn mat tables on conn.delete       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/56610ae ]
 * [BI-735] delete ds: use get_own_materialized_tables  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8026ea1 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated sqlalchemy-metrika-api  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/34f8ace ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-19 23:53:30+03:00

0.526.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.73.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/709406a ]
 * [BI-697] rls: fix empty config       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3c36711 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-19 17:36:49+03:00

0.525.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.72.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0297d8d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-19 15:27:57+03:00

0.524.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.69.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0069ef2 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-19 14:24:14+03:00

0.523.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix chyt new datasets  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a761df1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-18 17:38:52+03:00

0.522.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.68.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fe4a6f8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-18 15:43:52+03:00

0.521.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * yandex-bi-common[celery,db]==4.66.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/09808f8 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-formula, BI-771  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/af5f28e ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-17 17:37:24+03:00

0.520.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-494] dataset: execute + check permissions on some apis  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/544a0f6 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-17 14:44:57+03:00

0.519.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * added minimal validation on dataset creation for CreateDSFrom.CH_TABLE  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f77e87e ]
 * [BI-714] appmetrica dialect                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f6dbb5b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-17 13:36:53+03:00

0.518.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon to 4.63.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/622da22 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-15 23:19:56+03:00

0.517.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * python 3.7  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/94f33af ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-14 21:02:57+03:00

0.516.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * datasource replacer: keep access_mode  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e47d3d2 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-14 17:24:32+03:00

0.515.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-480] Handles to switch compress algorithm  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cd58860 ]
 * [BI-480] bicommon==4.53.1                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4e898ce ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-14 14:18:30+03:00

0.514.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * set access_mode on mat_sched init  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0cd3086 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-11 14:43:26+03:00

0.513.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-480] bicommon==4.53.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/13ae880 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-670] fixed dir_path for connections creation  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f5def08 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-10 14:42:07+03:00

0.512.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * new conn api: secure param for ch          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/037ff80 ]
 * [BI-697] retries in dls client             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/75ae5c0 ]
 * [BI-697] 400 with non-existing logins      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/56bfc86 ]
 * [BI-697]: 400 on rls config parsing error  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d09f000 ]
 * [BI-697] check permission                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/794a748 ]
 * change api url                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3279ee6 ]
 * [BI-757] 400 on metrika NotSupportedError  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1bd01ba ]
 * [BI-697] RLS api                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/57d9c7b ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * fixed logs api connection mat_sched_config saving  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/13008e0 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-07 17:15:59+03:00

0.511.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * do not fail on invalid datasets                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8f7a2e9 ]
 * [BI-756] update sources in result_schema on datasource replacing  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0195c38 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-06 19:16:33+03:00

0.510.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * [BI-756] add titles to raw_schema schema  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8bc21da ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-06 14:00:17+03:00

0.509.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Auxiliary: yasm/juggler: disable US alerts   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cfb0d91 ]
 * Auxiliary: yasm enconfigurer script: tuning  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/488601e ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added endpoint for min/max values, BI-710  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2c86009 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-670] moving connections out from "__system/connections" folder  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3785be5 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-06-05 17:11:29+03:00

0.508.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * bicommon: 4.35.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/981e899 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-31 19:32:25+03:00

0.507.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * handle access_mode on materializations  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b8b93c8 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-31 19:07:25+03:00

0.506.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update bicommon: 4.34.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/db02692 ]
 * update bicommon: 4.33.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/6ed5bb7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-31 18:12:02+03:00

0.505.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-480] bicommon==4.32.0, conditional cache-redis initialization on app start-up  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a729e44 ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update readme: delete obsolete build method  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/87c29a7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-31 16:21:42+03:00

0.504.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-480] New redis clusters configuration for production envs                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0cf4b25 ]
 * [BI-480] bicommon==4.27.0                                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4e0a258 ]
 * Adding endpoint codes to log context in resources (to be fetched in dataset logging)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e20c621 ]
 * [BI-480] Caches Redis password was extracted to cross-env config                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dd5617f ]
 * [BI-480] Caches config for prod environments                                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/abef7f5 ]
 * [BI-480] Cache usage control for preview and distinct                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/82e038e ]
 * [BI-480] bicommon==4.26.0                                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/794f57f ]
 * [BI-480] Redis initialization, configs, on/off lever                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f7deb43 ]
 * [BI-480] Tests image entry-point override                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0941687 ]
 * [BI-480] Python interpreter container in compose                                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/64bb61b ]

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * Auxilary: int_production panel key  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5fe8d2e ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added handling of QueryConstructorError in api decorator          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b31fa9f ]
 * Added passing of user types of select_expressions to select_data  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4178bb3 ]
 * Removed usage of the stringify parameter in select_data           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/919515f ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-238] updated sqlalchemy-metrika-api  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/247c429 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-31 13:50:54+03:00

0.503.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added handler for distinct values, BI-238  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2fbcb01 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-28 12:12:35+03:00

0.502.0
-------

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * allow none for cluster in yt schema  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/39eb884 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-27 19:45:00+03:00

0.501.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed adding of fields with errors, BI-720  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5580b44 ]
 * Removed test_update_dataset                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ae2f106 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-24 16:44:26+03:00

0.500.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed filtering of fields in DatasetView  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3603ebd ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * bi-494: raise exc instead of direct abort calling  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8ddb165 ]
 * BI-494: check 'execute' on connections             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/26951e3 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-24 13:02:26+03:00

0.499.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e1814e9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-23 17:47:49+03:00

0.498.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula, added REQ_CONDITION flag to translation of filter expression, BI-709  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9c30b47 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-23 16:45:56+03:00

0.497.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * BI-494: remove legacy api and switch tests to new mat status api  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bba0a9e ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-583] Removed dataset marshmellow schema validation         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ea71cae ]
 * bicommon up                                                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9d3b41d ]
 * removed require_ch_db_config decorator for perfomance reasons  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0930789 ]
 * updated sqlalchemy-metrika-api version                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/da1399e ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update bicommon  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7823f1d ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-23 14:21:58+03:00

0.496.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common                                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/251e74f ]
 * Moved OrderingDirection to enums from bi-common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dc085d6 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-22 19:38:02+03:00

0.495.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e480393 ]
 * Fixed resolution of datasource properties  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/30d8b5e ]
 * Added more filters to /result, BI-599      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/3e1d9a1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-22 13:51:41+03:00

0.494.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * yandex-bi-common==0.4.9  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/158e6b9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-20 20:17:08+03:00

0.493.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated common  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0aca962 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-20 18:50:32+03:00

0.492.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-formula and bi-common versions  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/12d3e29 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-20 18:09:52+03:00

0.491.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * bicommon==0.4.6                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f30bce7 ]
 * access_mode = direct for csv datasets            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d9b488f ]
 * write access_mode on dataset creation            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/68638d9 ]
 * don't copy csv raw_schema to dataset             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/87bc059 ]
 * [BI-583] use csv raw schema on dataset creation  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/2c94ef0 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-20 14:01:19+03:00

0.490.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update bicommon                                           [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d985887 ]
 * fix regex                                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5a2341c ]
 * BI-676: datasource replacer: fix yt (and all the others)  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ee7329a ]
 * drop tables in delete view; use ch replica macros         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4f7dcb5 ]
 * BI-655: drop table in materializer-worker                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7120297 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-common, Added handling of SourceNotReady                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/cb0f5e3 ]
 * Removed direct usage of data sources, BI-474                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d42a8d1 ]
 * Fixed marshmallow validation of connection schemas in case of data=None  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/370d0e7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-16 19:31:36+03:00

0.489.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * updated bicommon to 3.59  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/17cde0c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-16 14:46:15+03:00

0.488.0
-------

* [Konstantin Chupin](http://staff/kchupin@yandex-team.ru)

 * [BI-381] yandex-bi-common==0.3.57. Profiling for dataset result  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/11d9e13 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * fixed hostname validation for Logs API. Added hostname validation for other sql connections  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d124c09 ]

* [Ivan Moskvin](http://staff/hans@yandex-team.ru)

 * Add CD/CI section  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/80fd542 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-13 17:00:36+03:00

0.487.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * bicommon version up                                                 [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d3adba5 ]
 * yandex-bi-common==0.3.54                                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/949eb96 ]
 * [BI-497] trying to get dataset result schema title from raw schema  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9d0fe23 ]
 * [BI-479] materializer scheduler config for LogsAPI connections      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/63cfe66 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-05-07 17:31:46+03:00

0.486.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-formula package              [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a412b3b ]
 * Fixed 500 error in suggestions, BI-602  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c76496c ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-30 15:49:11+03:00

0.485.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added completions for suggestions, BI-602                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5c006bd ]
 * Chnged dataset formula validation to work in GUID mode, BI-667  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/14760ed ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-26 10:08:00+03:00

0.484.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * description for humans                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/08b7f1c ]
 * BI-659: result: 400 on non-existing field request  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/857c6d1 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-19 17:39:52+03:00

0.483.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Fixed compilatiion of query using the correct dialog, BI-646  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e32ccb9 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-19 17:02:06+03:00

0.482.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-17 16:16:51+03:00

0.481.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bi-common and bi-formula  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e451704 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * it's not meant to be here  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/210e213 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-15 14:24:55+03:00

0.480.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update bicommon  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/51de505 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-12 17:14:53+03:00

0.479.0
-------

* [Anton Vasilyev](http://staff/hhell@yandex-team.ru)

 * yasm_config: remove alerts for nonexistent resources  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/064a2ee ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update bicommon  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c11a3b0 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-12 16:11:27+03:00

0.478.0
-------

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update chsqla  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0335a4a ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-11 17:45:49+03:00

0.477.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated bicommon to version with fixed MS, added tds packages and configs to build, BI-613  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9fe052f ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-11 16:08:59+03:00

0.476.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * mask_secret_value: fix for empty value  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f1d7364 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-11 07:31:07+03:00

0.475.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-600] hide secret data in logs - fixed for empty dict  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/d87b1cc ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-10 21:31:08+03:00

0.474.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * BI-600: hide secret data in logs  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/8a007b3 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-10 20:05:54+03:00

0.473.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * fix 'too many rows' message from materializer  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/233aa34 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-10 19:21:31+03:00

0.472.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-614] updated bicommon  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7de0a12 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-10 18:18:45+03:00

0.471.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * handle ExtQueryExecuterException  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9208537 ]
 * update bicommon                   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/49b007b ]
 * handle 409 from us                [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1c8b706 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-10 17:48:13+03:00

0.470.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * datasource replacer: fix db_name setter  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/78fd629 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-10 16:21:40+03:00

0.469.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * add metrika to source replacer  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/5f2a785 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-10 15:41:21+03:00

0.468.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update chsqla  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/735b7e0 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-10 13:55:54+03:00

0.467.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Switched to pyodbc, BI-613  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bef142b ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-10 12:06:18+03:00

0.466.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-09 17:24:16+03:00

0.465.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * mat delete: drop table without us lock  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/003d9e2 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-09 12:53:50+03:00

0.464.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update bicommon  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/0421261 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-08 19:30:06+03:00

0.463.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * ext query: no workers for intra  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/87e5952 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-08 16:52:11+03:00

0.462.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * query-executer: workers: 3 -> 16  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/613e9ba ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-08 15:49:41+03:00

0.461.0
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update changelog after manual builds                         [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a4ed9b4 ]
 * mat delete: check curr mode & use mat_table_link explicitly  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7f077f2 ]

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added a hard limit on row count in /preview, BI-608                                       [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/1701588 ]
 * Updated bi-common to v with disabled autoload, revived the TABLE_NOT_READY error, BI-567  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/dfecb36 ]
 * Added 'updates' parameter to /result endpoint, BI-596                                     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4966298 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-08 14:53:43+03:00

0.460.0
-------

* [robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru)

 * update bicommon

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-05 14:22:13+03:00

0.459.0
-------

* [robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru)

 * update bicommon

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-05 14:22:13+03:00

0.458.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-579] fixed mat_start_date logs api param  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/ab23148 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-05 14:22:13+03:00

0.457.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Added more optional for cprofiler  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/be4f074 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * [BI-579] send mat_start_date to materializer as string  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e3ca464 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-05 11:46:16+03:00

0.456.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-04 15:07:07+03:00

0.455.0
-------

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * fixed logs api materialisation condition     [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/642d618 ]
 * temporary reverted mat_sched_config changes  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9ddcde4 ]
 * updated bi-common & sqlalchemy-metrika-api   [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a72ba5a ]
 * rm debug logging                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fbdb385 ]
 * Logs API regular materialization settings    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/c49a85e ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-04 00:50:03+03:00

0.454.0
-------

* [Grigory Statsenko](http://staff/gstatsenko@yandex-team.ru)

 * Updated formula  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bd69340 ]
 * Updated formula  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4023807 ]

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * conftest: use test_user  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/bf534fa ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-03 18:16:38+03:00

0.453.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-02 20:08:17+03:00

0.452.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-02 19:53:59+03:00

0.451.0
-------

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-02 19:42:03+03:00

0.450.0
-------

* [robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru)

 * releasing version 0.450.0  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/fbd7031 ]
 * releasing version 0.258.2  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/252e235 ]
 * releasing version 0.258.1  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/abd9ec4 ]

* [Andrey Snytin](http://staff/asnytin@yandex-team.ru)

 * added conn_type and host to log_context  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/9451a77 ]

* [Ivan Moskvin](http://staff/hans@yandex-team.ru)

 * release to int-testing by default        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/28fd68a ]
 * undo test changelog                      [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/daec098 ]
 * add changelog                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/02d6df3 ]
 * add initial changelog                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/362370b ]
 * [BI-*] Automate build&deploy to testing  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/7f9ee50 ]
 * [BI-*] Automate build&deploy to testing  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/275be89 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2019-04-02 19:29:39+03:00

0.449.0
-------
* All previous releases

0.258.2
-------

* [Dmitry Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * minus one crutch for old formulas                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/029e233 ]
 * fix filtering by measure not presented in request columns  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/4783293 ]
 * fix aggrs                                                  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/f3ad35a ]
 * app for ext queries                                        [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/a7744a3 ]
 * one more crutch for old formula                            [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/35e13d6 ]
 * query_executer                                             [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/67e8d7d ]

* [Dmitrii Fedorov](http://staff/dmifedorov@yandex-team.ru)

 * update clickhouse-sqlalchemy  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/b9c0488 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2018-10-31 16:48:04+00:00

0.258.1
-------

* [Ivan Moskvin](http://staff/hans@yandex-team.ru)

 * add initial changelog                    [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/476fb24 ]
 * [BI-*] Automate build&deploy to testing  [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/be6533d ]
 * Update bicommon                          [ ssh://git@bb.yandex-team.ru/statbox/bi-api/commit/e37dff7 ]

[robot-statinfra (STATBOX_RELOOSER)](http://staff/statinfra-dev@yandex-team.ru) 2018-10-31 13:57:31+00:00

0.258.0
-------
  All previous releases
