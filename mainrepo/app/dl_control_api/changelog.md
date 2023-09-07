0.1998.0
--------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * BI-4860: remove ConnectorsSettingsByType and CONNECTORS field from the bi api config (#395)  [ https://github.com/datalens-tech/datalens-backend-private/commit/0187da4f ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-07 16:19:59+00:00

0.1997.0
--------

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * BI-4229 Remove old-style connector availability configs (#337)  [ https://github.com/datalens-tech/datalens-backend-private/commit/7828b9e7 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-07 16:07:17+00:00

0.1996.0
--------

* [Sergei Borodin](http://staff/seray@yandex-team.ru)

 * BI-4428 secure-reader via TCP (#394)  [ https://github.com/datalens-tech/datalens-backend-private/commit/451c9f58 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-07 15:58:12+00:00

0.1995.0
--------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * did you know that tuple[str] is a tuple with exactly one string? (#402)  [ https://github.com/datalens-tech/datalens-backend-private/commit/1e61fbf7 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Removed make_foreign_native_type_conversion from TypeTransformer (#382)  [ https://github.com/datalens-tech/datalens-backend-private/commit/086c6ca8 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-07 15:50:05+00:00

0.1994.0
--------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * BI-4511: remove yt semaphores (#381)  [ https://github.com/datalens-tech/datalens-backend-private/commit/422e9fb2 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-07 15:12:58+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-07 15:07:16+00:00

0.1992.0
--------

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * fix-detect-affected-packages-to-handle-tests-deps (#390)  [ https://github.com/datalens-tech/datalens-backend-private/commit/3ec751ff ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-07 13:31:31+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-07 13:25:08+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-06 16:44:43+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-05 10:37:28+00:00

0.1988.0
--------

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * single mypy (#296)  [ https://github.com/datalens-tech/datalens-backend-private/commit/57787881 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-04 12:51:21+00:00

0.1987.0
--------

* [vallbull](http://staff/33630435+vallbull@users.noreply.github.com)

 * BI-4791: Delete port check (#295)  [ https://github.com/datalens-tech/datalens-backend-private/commit/3c616d15 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-04 12:30:15+00:00

0.1986.0
--------

* [vallbull](http://staff/33630435+vallbull@users.noreply.github.com)

 * BI-4776: Fix typo in code (#300)  [ https://github.com/datalens-tech/datalens-backend-private/commit/f7892a5d ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-04 12:15:21+00:00

0.1985.0
--------

* [vallbull](http://staff/33630435+vallbull@users.noreply.github.com)

 * BI-4776: Chyt forms (#256)  [ https://github.com/datalens-tech/datalens-backend-private/commit/01ecf70 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Implemented virtual FS editor and dry run for repmanager (#287)  [ https://github.com/datalens-tech/datalens-backend-private/commit/78f6aa8 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-04 11:46:57+00:00

0.1984.0
--------

* [KonstantAnxiety](http://staff/58992437+KonstantAnxiety@users.noreply.github.com)

 * Add missing dependencies to app/bi_api (#283)  [ https://github.com/datalens-tech/datalens-backend-private/commit/74c7546 ]

* [Valentin Gologuzov](http://staff/evilkost@users.noreply.github.com)

 * added script to distribute common mypy settings across repo (#286)  [ https://github.com/datalens-tech/datalens-backend-private/commit/4b34547 ]

* [Grigory Statsenko](http://staff/altvod@users.noreply.github.com)

 * Switching to fully managed FS access in repmanager (#278)  [ https://github.com/datalens-tech/datalens-backend-private/commit/f326b39 ]
 * Fixes for connectorization of bi_formula_ref (#267)        [ https://github.com/datalens-tech/datalens-backend-private/commit/3f5c290 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-04 08:51:11+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-01 21:24:46+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-01 14:07:02+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-09-01 12:03:44+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-31 15:33:51+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-25 13:41:58+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-25 10:14:31+00:00

0.1977.0
--------

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-24 14:03:55+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-24 13:53:14+00:00

0.1975.0
--------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * some more mypy fixes (#93)  [ https://github.com/datalens-tech/datalens-backend-private/commit/96fa442 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-24 10:16:25+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-24 07:59:50+00:00

0.1973.0
--------

* [Nick Proskurin](http://staff/42863572+MCPN@users.noreply.github.com)

 * fix typing (#91)  [ https://github.com/datalens-tech/datalens-backend-private/commit/0d95eb3 ]

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-23 16:16:48+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-23 15:22:07+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-23 13:47:51+00:00

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

[Continuous Integration](http://staff/username@users.noreply.github.com) 2023-08-21 18:08:43+00:00

0.1969.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-18 07:01:16+00:00

0.1968.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 16:11:05+00:00

0.1967.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 15:44:18+00:00

0.1966.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 14:38:27+00:00

0.1965.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 13:31:02+00:00

0.1964.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 11:50:04+00:00

0.1963.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 11:41:10+00:00

0.1962.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-17 00:15:09+00:00

0.1961.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-16 23:22:40+00:00

0.1960.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-16 14:29:08+00:00

0.1959.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-15 16:48:19+00:00

0.1958.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-15 15:34:05+00:00

0.1957.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-14 15:51:38+00:00

0.1956.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-14 14:39:00+00:00

0.1955.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-14 11:44:09+00:00

0.1954.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-11 14:13:51+00:00

0.1953.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-11 10:35:44+00:00

0.1952.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-10 15:19:14+00:00

0.1951.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-09 10:53:06+00:00

0.1950.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-08 16:51:01+00:00

0.1949.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-04 08:43:09+00:00

0.1948.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 20:18:33+00:00

0.1947.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 18:57:39+00:00

0.1946.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 18:32:01+00:00

0.1945.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 15:32:47+00:00

0.1944.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 12:43:34+00:00

0.1943.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-03 10:13:38+00:00

0.1942.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 19:40:54+00:00

0.1941.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 18:30:51+00:00

0.1940.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 15:26:11+00:00

0.1939.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 14:29:42+00:00

0.1938.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 09:27:15+00:00

0.1937.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-02 08:51:54+00:00

0.1936.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-01 11:05:23+00:00

0.1935.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-08-01 09:00:59+00:00

0.1934.0
--------

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-31 14:35:12+00:00

0.1933.0
--------

* [konstasa](http://staff/konstasa)

 * BI-4782 OS meta & apps: control-api, data-api  [ https://a.yandex-team.ru/arc/commit/12046241 ]

[robot-yc-arcadia](http://staff/robot-yc-arcadia) 2023-07-29 16:36:23+00:00

0.1932.0
--------

* Sync version
