1.25.0
------

[robot-statinfra](http://staff/robot-statinfra) 2020-09-03 15:20:58+03:00

1.24.0
------

* [asnytin](http://staff/asnytin)

 * lighten metrika tests to avoid flaps  [ https://a.yandex-team.ru/arc/commit/7177057 ]

* [hhell](http://staff/hhell)

 * tier1 venv-preparer, commonized all-local-libs installation  [ https://a.yandex-team.ru/arc/commit/7170013 ]
 * sqlalchemy-metrika-api: tests                                [ https://a.yandex-team.ru/arc/commit/6989680 ]
 * recurse for libraries                                        [ https://a.yandex-team.ru/arc/commit/6980795 ]
 * build-and-release in readmes                                 [ https://a.yandex-team.ru/arc/commit/6974155 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-05 16:38:23+03:00

1.23.0
------

* [hhell](http://staff/hhell)

 * [BI-1540] sqlalchemy-metrika-api over releaser  [ https://a.yandex-team.ru/arc/commit/6974094 ]
 * Make setup.py work in a clean environment       [ https://a.yandex-team.ru/arc/commit/6926983 ]

* [nslus](http://staff/nslus)

 * ARCADIA-2258 [migration] bb/STATBOX/sqlalchemy-metrika-api  [ https://a.yandex-team.ru/arc/commit/6927175 ]

* [asnytin](http://staff/asnytin)

 * 1.22.0                                                                                       [ https://a.yandex-team.ru/arc/commit/6926982 ]
 * added tests, Added [not]contains, [not]startswith, [not]endswith                             [ https://a.yandex-team.ru/arc/commit/6926980 ]
 * [BI-1274] supported operators NOT, NOT LIKE, NOT IN                                          [ https://a.yandex-team.ru/arc/commit/6926979 ]
 * 1.21.0                                                                                       [ https://a.yandex-team.ru/arc/commit/6926978 ]
 * changed exceptions imports, because bicommon imports MetrikaApiException from api_client...  [ https://a.yandex-team.ru/arc/commit/6926977 ]
 * 1.20.0                                                                                       [ https://a.yandex-team.ru/arc/commit/6926976 ]
 * [BI-1239] metrika exceptions                                                                 [ https://a.yandex-team.ru/arc/commit/6926975 ]
 * 1.19.0                                                                                       [ https://a.yandex-team.ru/arc/commit/6926974 ]
 * [BI-1387] deleted silverlight/java/flash-related fields                                      [ https://a.yandex-team.ru/arc/commit/6926973 ]
 * 1.18.0                                                                                       [ https://a.yandex-team.ru/arc/commit/6926972 ]
 * fixed wrong column alias processing in case of double type_coerce                            [ https://a.yandex-team.ru/arc/commit/6926971 ]
 * 1.17.0                                                                                       [ https://a.yandex-team.ru/arc/commit/6926970 ]
 * fixed offset processing                                                                      [ https://a.yandex-team.ru/arc/commit/6926969 ]
 * 1.16.0                                                                                       [ https://a.yandex-team.ru/arc/commit/6926968 ]
 * [BI-1162] like operator (=*)                                                                 [ https://a.yandex-team.ru/arc/commit/6926967 ]
 * [BI-1084] fixed filtering by datetime fields                                                 [ https://a.yandex-team.ru/arc/commit/6926966 ]
 * log sample_share                                                                             [ https://a.yandex-team.ru/arc/commit/6926965 ]
 * fixed limit setting                                                                          [ https://a.yandex-team.ru/arc/commit/6926964 ]
 * 1.13.0                                                                                       [ https://a.yandex-team.ru/arc/commit/6926963 ]
 * wrap all requests exceptions into MetrikaHttpApiException                                    [ https://a.yandex-team.ru/arc/commit/6926962 ]
 * MAX_LIMIT_VALUE                                                                              [ https://a.yandex-team.ru/arc/commit/6926961 ]
 * 1.12.0                                                                                       [ https://a.yandex-team.ru/arc/commit/6926960 ]
 * [BI-1028] added accuracy parameter                                                           [ https://a.yandex-team.ru/arc/commit/6926958 ]
 * 1.11.0                                                                                       [ https://a.yandex-team.ru/arc/commit/6926957 ]
 * renamed ym:s:sourceEngineName metrika field (title)                                          [ https://a.yandex-team.ru/arc/commit/6926956 ]
 * raise pretty metrica api errors if possible                                                  [ https://a.yandex-team.ru/arc/commit/6926955 ]
 * fixed fetchmany                                                                              [ https://a.yandex-team.ru/arc/commit/6926953 ]
 * appmetrica fields: fixed "hour" field type                                                   [ https://a.yandex-team.ru/arc/commit/6926952 ]
 * appmetrica fields: fixed "monthName" -> "month"                                              [ https://a.yandex-team.ru/arc/commit/6926951 ]
 * [BI-714] AppMetrica fields: fixed socdem                                                     [ https://a.yandex-team.ru/arc/commit/6926950 ]
 * [BI-714] AppMetrica fields                                                                   [ https://a.yandex-team.ru/arc/commit/6926949 ]
 * fixed datetime casting                                                                       [ https://a.yandex-team.ru/arc/commit/6926948 ]
 * [BI-562] casts: added BOOLEAN, return python datetime objects                                [ https://a.yandex-team.ru/arc/commit/6926947 ]
 * version 1.2.0                                                                                [ https://a.yandex-team.ru/arc/commit/6926946 ]
 * [BI-714] AppMetrica fields                                                                   [ https://a.yandex-team.ru/arc/commit/6926945 ]
 * [BI-714] appmetrica fixes                                                                    [ https://a.yandex-team.ru/arc/commit/6926944 ]
 * updated version                                                                              [ https://a.yandex-team.ru/arc/commit/6926942 ]
 * appmetrica fixes                                                                             [ https://a.yandex-team.ru/arc/commit/6926941 ]
 * appmetrica installs fields fixes                                                             [ https://a.yandex-team.ru/arc/commit/6926940 ]
 * appmetrica fixes                                                                             [ https://a.yandex-team.ru/arc/commit/6926939 ]
 * appmetrica dbapi                                                                             [ https://a.yandex-team.ru/arc/commit/6926938 ]
 * appmetrica installs fields description                                                       [ https://a.yandex-team.ru/arc/commit/6926936 ]
 * [BI-714] appmetrica                                                                          [ https://a.yandex-team.ru/arc/commit/6926935 ]
 * [BI-562] casts                                                                               [ https://a.yandex-team.ru/arc/commit/6926934 ]
 * tests/conftest.py                                                                            [ https://a.yandex-team.ru/arc/commit/6926933 ]
 * tests                                                                                        [ https://a.yandex-team.ru/arc/commit/6926932 ]
 * [BI-238] select distinct for dimension fields                                                [ https://a.yandex-team.ru/arc/commit/6926931 ]
 * fixed handling type_coerce in group by fields                                                [ https://a.yandex-team.ru/arc/commit/6926930 ]
 * bulk fixed fields config                                                                     [ https://a.yandex-team.ru/arc/commit/6926929 ]
 * switched off silent group by fields ignoring                                                 [ https://a.yandex-team.ru/arc/commit/6926928 ]
 * fixed fields config                                                                          [ https://a.yandex-team.ru/arc/commit/6926927 ]
 * adv fields info fixes                                                                        [ https://a.yandex-team.ru/arc/commit/6926926 ]
 * deleted visits fields "ym:s:yan..." - with permission_scope=partner_money                    [ https://a.yandex-team.ru/arc/commit/6926925 ]
 * visits fields - more info fixes                                                              [ https://a.yandex-team.ru/arc/commit/6926924 ]
 * visits fields info fixes                                                                     [ https://a.yandex-team.ru/arc/commit/6926923 ]
 * version bump                                                                                 [ https://a.yandex-team.ru/arc/commit/6926922 ]
 * fixed packaging                                                                              [ https://a.yandex-team.ru/arc/commit/6926921 ]
 * version up                                                                                   [ https://a.yandex-team.ru/arc/commit/6926920 ]
 * added fields info for other counter_sources                                                  [ https://a.yandex-team.ru/arc/commit/6926919 ]
 * version bump                                                                                 [ https://a.yandex-team.ru/arc/commit/6926918 ]
 * trying to guess that user needs available min/max dates for the counter                      [ https://a.yandex-team.ru/arc/commit/6926917 ]
 * between operator for date/datetime fields                                                    [ https://a.yandex-team.ru/arc/commit/6926916 ]
 * version bump                                                                                 [ https://a.yandex-team.ru/arc/commit/6926915 ]
 * supported labels in WHERE. Many fixes                                                        [ https://a.yandex-team.ru/arc/commit/6926914 ]
 * fixed has_table. Limited group by fields count                                               [ https://a.yandex-team.ru/arc/commit/6926913 ]
 * fixed name in setup.py                                                                       [ https://a.yandex-team.ru/arc/commit/6926912 ]
 * fixed api field types info                                                                   [ https://a.yandex-team.ru/arc/commit/6926911 ]
 * labels in group by                                                                           [ https://a.yandex-team.ru/arc/commit/6926910 ]
 * Supported select labels (AS). Many fixes..                                                   [ https://a.yandex-team.ru/arc/commit/6926909 ]
 * Switched to metrika data table API handle. Added column_info support.                        [ https://a.yandex-team.ru/arc/commit/6926908 ]
 * [BI-293] get_table_names                                                                     [ https://a.yandex-team.ru/arc/commit/6926907 ]
 * added makefile                                                                               [ https://a.yandex-team.ru/arc/commit/6926906 ]
 * fixed related imports                                                                        [ https://a.yandex-team.ru/arc/commit/6926905 ]
 * setup.py fix                                                                                 [ https://a.yandex-team.ru/arc/commit/6926904 ]
 * [BI-293] sqlalchemy-metrika-api. Initial commit                                              [ https://a.yandex-team.ru/arc/commit/6926903 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-15 17:47:27+03:00

1.22.0
---

Releaser-based versioning.

[Anton Vasilyev](http://staff/hhell@yandex-team.ru) 2020-06-15 16:53:39+03:00

