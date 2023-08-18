7.47.0
------

* [gstatsenko](http://staff/gstatsenko)

 * Updated geofunc docs                                [ https://a.yandex-team.ru/arc/commit/7265070 ]
 * Removed formula suggestions and formatting BI-1686  [ https://a.yandex-team.ru/arc/commit/7227138 ]
 * Implemented DB_CAST function, BI-1611               [ https://a.yandex-team.ru/arc/commit/7206684 ]

* [hhell](http://staff/hhell)

 * BI-1723: use BIGINT wherever possible [run large tests]                                                                                                            [ https://a.yandex-team.ru/arc/commit/7251976 ]
 * BI-1703: fix for constants in compeng queries [run large tests]                                                                                                    [ https://a.yandex-team.ru/arc/commit/7238850 ]
 * minor style/cleanup                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/7222976 ]
 * DEVTOOLSSUPPORT-2799: PGCTLTIMEOUT everywhere because apparently sandbox environment is sloooooooooow and the default 60 seconds is not enough to stop a postgres  [ https://a.yandex-team.ru/arc/commit/7221482 ]
 * BI-1121: bi-billing tier0 tests [run large tests]                                                                                                                  [ https://a.yandex-team.ru/arc/commit/7210643 ]
 * assorted fixes                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/7206805 ]
 * makefiles cleanup and commonize                                                                                                                                    [ https://a.yandex-team.ru/arc/commit/7206735 ]
 * BI-1478: datetimetz preliminary implementation [run large tests]                                                                                                   [ https://a.yandex-team.ru/arc/commit/7182670 ]
 * switching to py38 in tier1 tests                                                                                                                                   [ https://a.yandex-team.ru/arc/commit/7178546 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-09-03 15:20:47+03:00

7.46.0
------

* [gstatsenko](http://staff/gstatsenko)

 * Minor fixes for function docs                                                  [ https://a.yandex-team.ru/arc/commit/7177365 ]
 * Fixed fractional seconds in literal datetimes for MSSQL and MYSQL, DLHELP-371  [ https://a.yandex-team.ru/arc/commit/7098715 ]

* [hhell](http://staff/hhell)

 * Replace unwrap_args with a subclass, for better static checking in the future [run large tests]  [ https://a.yandex-team.ru/arc/commit/7161176 ]
 * tests: bi-formula tier1 tests fix, pin major version of all extra dependencies                   [ https://a.yandex-team.ru/arc/commit/7159272 ]
 * BI-1121: bi-formula tests: initdb waiter, host container [run large tests]                       [ https://a.yandex-team.ru/arc/commit/7155629 ]
 * BI-1121: simplify settings-resource loading                                                      [ https://a.yandex-team.ru/arc/commit/7148779 ]
 * Minor cleanups                                                                                   [ https://a.yandex-team.ru/arc/commit/7141980 ]
 * BI-1121: yet another attempt at oracle+mssql tests [RUN LARGE TESTS]                             [ https://a.yandex-team.ru/arc/commit/7122483 ]
 * [BI-1651] fix oracle unicode in tests                                                            [ https://a.yandex-team.ru/arc/commit/7108786 ]
 * Reorder the tier1 tests                                                                          [ https://a.yandex-team.ru/arc/commit/7105113 ]
 * BI-1121 / DEVTOOLSSUPPORT-1780: compile po files on the fly [RUN LARGE TESTS]                    [ https://a.yandex-team.ru/arc/commit/7089763 ]

* [kchupin](http://staff/kchupin)

 * [BI-1121] Common requirements in remote debug docker-container  [ https://a.yandex-team.ru/arc/commit/7153842 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-08-05 16:38:47+03:00

7.45.0
------

* [gstatsenko](http://staff/gstatsenko)

 * Wrapped original_test and position into NodeMeta  [ https://a.yandex-team.ru/arc/commit/7087451 ]

* [hhell](http://staff/hhell)

 * Minor dependencies tuning  [ https://a.yandex-team.ru/arc/commit/7087432 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-07 21:43:04+03:00

7.44.0
------

* [hhell](http://staff/hhell)

 * Supporting sqlalchemy 1.2.x for now               [ https://a.yandex-team.ru/arc/commit/7087289 ]
 * tests: minor: an extra oracle-encoding pre-check  [ https://a.yandex-team.ru/arc/commit/7085429 ]
 * return db retries for now                         [ https://a.yandex-team.ru/arc/commit/7081341 ]
 * BI-1121: minor tests tuning [RUN LARGE TESTS]     [ https://a.yandex-team.ru/arc/commit/7072474 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added aggregations any, argMin, argMax, BI-1053, BI-1118  [ https://a.yandex-team.ru/arc/commit/7086163 ]
 * Hotfix for double-agent functions (agg+win)               [ https://a.yandex-team.ru/arc/commit/7075621 ]
 * Minor doc updates                                         [ https://a.yandex-team.ru/arc/commit/7072187 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-07-07 19:38:01+03:00

7.43.0
------

* [gstatsenko](http://staff/gstatsenko)

 * Changed direction of some window functions, uipdated docs and examples, BI-1631  [ https://a.yandex-team.ru/arc/commit/7069282 ]
 * Added more info to aggregate functions that can be used as window ones, BI-1583  [ https://a.yandex-team.ru/arc/commit/7068567 ]
 * Updated window function docs in bi-formula, BI-1583                              [ https://a.yandex-team.ru/arc/commit/7049566 ]

* [hhell](http://staff/hhell)

 * auxiliary: minor fix                                          [ https://a.yandex-team.ru/arc/commit/7067665 ]
 * SUBBOTNIK-5296: common docker-tests config [RUN LARGE TESTS]  [ https://a.yandex-team.ru/arc/commit/7067274 ]
 * tests: db_for: wait_for_up                                    [ https://a.yandex-team.ru/arc/commit/7051412 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-30 22:12:47+03:00

7.42.0
------

* [gstatsenko](http://staff/gstatsenko)

 * Fixed FormulaItem.replace_at_index  [ https://a.yandex-team.ru/arc/commit/7040819 ]
 * Added test_iter_index               [ https://a.yandex-team.ru/arc/commit/7040424 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-27 11:32:16+03:00

7.41.0
------

* [gstatsenko](http://staff/gstatsenko)

 * Implemented node hierarchy indexing, BI-633  [ https://a.yandex-team.ru/arc/commit/7040086 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-26 15:51:05+03:00

7.40.0
------

* [gstatsenko](http://staff/gstatsenko)

 * Added is_aggregated_above_sub_node function to bi_formula.inspect.expression, BI-633  [ https://a.yandex-team.ru/arc/commit/7037358 ]

* [hhell](http://staff/hhell)

 * minor docstring fix                                      [ https://a.yandex-team.ru/arc/commit/7036619 ]
 * attempt to fix large tests                               [ https://a.yandex-team.ru/arc/commit/7013072 ]
 * refactor some definitions back into SQL_ALL              [ https://a.yandex-team.ru/arc/commit/7008696 ]
 * BI-1121: bi-formula tier0 with unit-tests                [ https://a.yandex-team.ru/arc/commit/7008166 ]
 * return the images                                        [ https://a.yandex-team.ru/arc/commit/6988414 ]
 * compose tests: pin the images, refactor the ch settings  [ https://a.yandex-team.ru/arc/commit/6988237 ]
 * recurse for libraries                                    [ https://a.yandex-team.ru/arc/commit/6980795 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-25 20:51:16+03:00

7.39.0
------

* [hhell](http://staff/hhell)

 * fix  [ https://a.yandex-team.ru/arc/commit/6973744 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-15 16:21:39+03:00

7.38.0
------

* [hhell](http://staff/hhell)

 * fix                                                                                                                                                                                                                                   [ https://a.yandex-team.ru/arc/commit/6973653 ]
 * [BI-1540] bi-formula over releaser                                                                                                                                                                                                    [ https://a.yandex-team.ru/arc/commit/6973558 ]
 * Merge pull request #285 in STATBOX/bi-formula from hhell/stuff to master

\* commit '7794210a778a560bd0fb4325f589c0603f805146':
  Tests: non-containerized unittests env                                                               [ https://a.yandex-team.ru/arc/commit/6924278 ]
 * minor styling fix                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924276 ]
 * [BI-1581] back to mysqldb in tests                                                                                                                                                                                                    [ https://a.yandex-team.ru/arc/commit/6924261 ]
 * 7.37.0                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6924260 ]
 * Fixes and tests                                                                                                                                                                                                                       [ https://a.yandex-team.ru/arc/commit/6924257 ]
 * [BI-1574] Tune the postgresql str concat to support asyncpg / postgresql prepared statements                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/6924256 ]
 * Plug strings plus into concat func                                                                                                                                                                                                    [ https://a.yandex-team.ru/arc/commit/6924255 ]
 * fixes and styling                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924253 ]
 * [BI-1478] Implement 'datetime(value, tzname)' for pg, mimicking current CH behavior                                                                                                                                                   [ https://a.yandex-team.ru/arc/commit/6924252 ]
 * Represent NO_VERSION as an empty tuple                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6924243 ]
 * [DLHELP-468] 'no version' should not be in 'and_below'                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6924242 ]
 * Some convenience '__repr__'s                                                                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/6924241 ]
 * [DLHELP-468] preliminary fix of materialized len(str)                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/6924240 ]
 * Fixes                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/6924233 ]
 * [BI-1478] CH str(datetime) tz-awareness notes                                                                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/6924232 ]
 * [BI-1478] FuncDatetime2FromCHStuff tests (test_datetime2)                                                                                                                                                                             [ https://a.yandex-team.ru/arc/commit/6924231 ]
 * [BI-1478] FuncDatetime2FromCHStuff                                                                                                                                                                                                    [ https://a.yandex-team.ru/arc/commit/6924230 ]
 * Merge pull request #266 in STATBOX/bi-formula from hhell/stuff to master

\* commit 'e9ba2932186cb1d4fd46a23eea15fec8dedfc8d7':
  Deprecate the temporary flag                                                                         [ https://a.yandex-team.ru/arc/commit/6924216 ]
 * 7.28.0                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6924187 ]
 * [BI-1410] hack to fix the statfacereport fielddate range                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6924185 ]
 * Merge pull request #247 in STATBOX/bi-formula from hhell/stuff to master

\* commit '0cc15808a5388ca92213b2374f44418de44fb922':
  Markup: rename concat_markup() to markup() to make it the primary way of turning string into markup  [ https://a.yandex-team.ru/arc/commit/6924149 ]

* [nslus](http://staff/nslus)

 * ARCADIA-2258 [migration] bb/STATBOX/bi-formula  [ https://a.yandex-team.ru/arc/commit/6927150 ]

* [gstatsenko](http://staff/gstatsenko)

 * Added newline in doc                                                                                                                                                                                                                                                                                                                                                                       [ https://a.yandex-team.ru/arc/commit/6924274 ]
 * Minor fix of dict inheritance                                                                                                                                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6924272 ]
 * Fixed typo in text                                                                                                                                                                                                                                                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/6924271 ]
 * Window function descriptions for doc, BI-1583                                                                                                                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6924269 ]
 * Merge pull request #283 in STATBOX/bi-formula from doc-no-include to master

\* commit 'b68f293911164561d26b1917575a966d1e0def65':
  Removed replaced includes with content in function doc generator, BI-1586                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6924268 ]
 * Minor internal refactoring of func doc generator, BI-1586                                                                                                                                                                                                                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/6924265 ]
 * Merge pull request #281 in STATBOX/bi-formula from window-func-docs to master

\* commit 'b5d1df39bc423b3272db783ee82ac451d2bac694':
  Added/updated window function docs, BI-1583                                                                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/6924264 ]
 * Text updates                                                                                                                                                                                                                                                                                                                                                                               [ https://a.yandex-team.ru/arc/commit/6924250 ]
 * Fixed typo                                                                                                                                                                                                                                                                                                                                                                                 [ https://a.yandex-team.ru/arc/commit/6924249 ]
 * Updated texts a little                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924248 ]
 * 7.36.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924246 ]
 * Merge pull request #275 in STATBOX/bi-formula from mfuncs to master

\* commit '5b6ba25352707fbf24da210f5d1a929c8012478f':
  Implemented all the main M-funcs, BI-1517                                                                                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/6924245 ]
 * Added conditional aggregation window functions, BI-1517                                                                                                                                                                                                                                                                                                                                    [ https://a.yandex-team.ru/arc/commit/6924237 ]
 * Merge pull request #273 in STATBOX/bi-formula from win-rfuncs to master

\* commit '000b14c323bf24ad25093ce42f5d7012ff1d56d4':
  Added RCOUNT, RMIN, RMAX, RAVG window functions, BI-1517                                                                                                                                                                                                   [ https://a.yandex-team.ru/arc/commit/6924235 ]
 * Merge pull request #269 in STATBOX/bi-formula from remove-over-hack to master

\* commit 'b269c8c7af0e4300ca9bb3145bfbbceede0470d3':
  Added comments
  Removed hacking of the Over clause element for insertion of partition_by and default order_by, BI-1517                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6924228 ]
 * Merge pull request #270 in STATBOX/bi-formula from msum to master

\* commit 'a6ff67f58baf8afdf7506ad3b6a1394b0a7c60cd':
  Added NumericDataType alias
  Added the first M-function MSUM, BI-1517                                                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/6924226 ]
 * Renamed base classes for window functions, BI-1517                                                                                                                                                                                                                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/6924222 ]
 * Merge pull request #268 in STATBOX/bi-formula from win-agg-functions to master

\* commit '0f813708aefa6b41a0402891e62d49d895a32f2f':
  Added common aggregation functions as window functions, BI-1517                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924220 ]
 * Merge pull request #267 in STATBOX/bi-formula from rank-percentile to master

\* commit '3e1f022618117a52a3126b7213773fcfa82b8a08':
  Added RANK_PERCENTILE function, BI-554                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6924219 ]
 * 7.35.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924212 ]
 * Unpinned chsa                                                                                                                                                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6924211 ]
 * 7.34.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924210 ]
 * Pinned down clickhouse-sqlalchemy and mady some style fixes                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6924207 ]
 * 7.33.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924206 ]
 * Fixed + for datetime, BI-1421                                                                                                                                                                                                                                                                                                                                                              [ https://a.yandex-team.ru/arc/commit/6924204 ]
 * 7.31.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924200 ]
 * Fixed slicing of desc nodes, BI-1404                                                                                                                                                                                                                                                                                                                                                       [ https://a.yandex-team.ru/arc/commit/6924198 ]
 * 7.30.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924197 ]
 * Merge pull request #261 in STATBOX/bi-formula from utc-datetime-fixes to master

\* commit 'cfe56e1c44349281577838df939ce36cf96b9b44':
  UTC datetime fixes for ClickHouse, BI-1241                                                                                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/6924196 ]
 * Added Descending formula node, BI-1404                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924193 ]
 * 7.29.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924192 ]
 * Merge pull request #258 in STATBOX/bi-formula from aux-nodes to master

\* commit '16b9b6fb264409a5a5970431ba2ef3f5c6fb7310':
  Made ErrorNode extractable
  Changed base class of ErrorNode to Null
  Replaced NamedItem with ParenthesizedExpr as the base class of Aliased node and added test_translate_parenthesized_expressions
  Added Aliased and ErrorNode formula nodes, BI-1394  [ https://a.yandex-team.ru/arc/commit/6924191 ]
 * Merge pull request #257 in STATBOX/bi-formula from rank-1-2 to master

\* commit 'd2c038ff450195620b7e834d845797a4435f4198':
  Added arg names for RANK*
  RANK* functions now accept only 1 or 2 args                                                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/6924184 ]
 * Added arg names for RANK*                                                                                                                                                                                                                                                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/6924181 ]
 * RANK* functions now accept only 1 or 2 args                                                                                                                                                                                                                                                                                                                                                [ https://a.yandex-team.ru/arc/commit/6924180 ]
 * 7.25.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924173 ]
 * Fixed slicing of ordered window functions                                                                                                                                                                                                                                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/6924171 ]
 * 7.24.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924170 ]
 * Added constants_neutral option to agg and winfunc level boundaries                                                                                                                                                                                                                                                                                                                         [ https://a.yandex-team.ru/arc/commit/6924168 ]
 * PR fixes                                                                                                                                                                                                                                                                                                                                                                                   [ https://a.yandex-team.ru/arc/commit/6924160 ]
 * Merge branch 'master' into update-docs                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924159 ]
 * 7.22.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924158 ]
 * Merge pull request #250 in STATBOX/bi-formula from window-validation to master

\* commit '955816b932610176307a9f2ce7ae0015457ba8d7':
  Added WindowFunctionChecker to validation                                                                                                                                                                                                           [ https://a.yandex-team.ru/arc/commit/6924157 ]
 * 7.21.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924156 ]
 * Fixed docstring typos                                                                                                                                                                                                                                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/6924154 ]
 * Added LevelSlice.is_lazy() method and some docstrings                                                                                                                                                                                                                                                                                                                                      [ https://a.yandex-team.ru/arc/commit/6924153 ]
 * 7.20.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924147 ]
 * Merge pull request #242 in STATBOX/bi-formula from parse-value-errors to master

\* commit 'e616fc96f6deda444e92a07a28db1e57ee87dac9':
  Added handling of invalud date and datetime values in formula parser                                                                                                                                                                               [ https://a.yandex-team.ru/arc/commit/6924146 ]
 * Added handling of invalud date and datetime values in formula parser                                                                                                                                                                                                                                                                                                                       [ https://a.yandex-team.ru/arc/commit/6924135 ]
 * 7.18.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924134 ]
 * Merge pull request #240 in STATBOX/bi-formula from fix-grouping-node-copy to master

\* commit 'b0b0605545bcf6c39460cf0d771a0daf488a8d1e':
  Fixed copying of WindowGroupingWithDimensions nodes                                                                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/6924133 ]
 * Merged release/7.11                                                                                                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6924131 ]
 * Post-merge fix                                                                                                                                                                                                                                                                                                                                                                             [ https://a.yandex-team.ru/arc/commit/6924125 ]
 * Merged again                                                                                                                                                                                                                                                                                                                                                                               [ https://a.yandex-team.ru/arc/commit/6924124 ]
 * Merged again                                                                                                                                                                                                                                                                                                                                                                               [ https://a.yandex-team.ru/arc/commit/6924123 ]
 * Merged release/7.11                                                                                                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6924116 ]
 * 7.17.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924111 ]
 * Fixed validation of aggregations for expressions with window functions                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924109 ]
 * 7.16.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924108 ]
 * Fixed inspection aggregations in expressions with winow functions                                                                                                                                                                                                                                                                                                                          [ https://a.yandex-team.ru/arc/commit/6924106 ]
 * 7.15.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924105 ]
 * Merge pull request #230 in STATBOX/bi-formula from more-win-funcs to master

\* commit '6fd591949f0f1e5e38c0ce90ccd813401e0ae18c':
  Fixed typos
  Fixed RANK, added RANK_DENSE, RANK_UNIQUE, SUM, RSUM window functions, updated doc generator for compatibility with window functions                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924104 ]
 * Fixed apply_mutations and FormulaItem.replace_nodes                                                                                                                                                                                                                                                                                                                                        [ https://a.yandex-team.ru/arc/commit/6924101 ]
 * 7.14.0                                                                                                                                                                                                                                                                                                                                                                                     [ https://a.yandex-team.ru/arc/commit/6924099 ]
 * Merge pull request #232 in STATBOX/bi-formula from validation-checkers to master

\* commit '766e8b506ee1d870ea4589acdda55156a79fa638':
  Refactored validation: Added Checker base class and AggregationChecker                                                                                                                                                                            [ https://a.yandex-team.ru/arc/commit/6924098 ]
 * Fixed type inference for window functions                                                                                                                                                                                                                                                                                                                                                  [ https://a.yandex-team.ru/arc/commit/6924095 ]

* [asnytin](http://staff/asnytin)

 * 7.32.0                                                                                           [ https://a.yandex-team.ru/arc/commit/6924203 ]
 * [BI-1274] metrika dialect supports [not]contains, startswith, endswith and logical OR, AND, NOT  [ https://a.yandex-team.ru/arc/commit/6924201 ]
 * 7.23.0                                                                                           [ https://a.yandex-team.ru/arc/commit/6924167 ]
 * [BI-1295] merged classes ExternalTranslationFunction and GeoFunction                             [ https://a.yandex-team.ru/arc/commit/6924165 ]
 * [BI-1295] removed docstring duplication                                                          [ https://a.yandex-team.ru/arc/commit/6924164 ]
 * [BI-1295] updated translations                                                                   [ https://a.yandex-team.ru/arc/commit/6924163 ]
 * [BI-1295] TOPONYM_TO_GEOPOINT, TOPONYM_TO_GEOPOLYGON functions                                   [ https://a.yandex-team.ru/arc/commit/6924162 ]

* [dmifedorov](http://staff/dmifedorov)

 * 7.26.0                            [ https://a.yandex-team.ru/arc/commit/6924177 ]
 * bi dialect for sa DefaultDialect  [ https://a.yandex-team.ru/arc/commit/6924174 ]

* [koshachy](http://staff/koshachy)

 * Merge pull request #246 in STATBOX/bi-formula from koshachy/doc_basepy-1584377115143 to master

\* commit 'c757e35802fb14b4feb645766ad4dd11d0d13d8e':
  doc_base.py edited online with Bitbucket  [ https://a.yandex-team.ru/arc/commit/6924148 ]

[robot-statinfra](http://staff/robot-statinfra) 2020-06-15 15:57:14+03:00

7.37.0
---

Releaser-based versioning.

[Anton Vasilyev](http://staff/hhell@yandex-team.ru) 2020-06-15 14:59:24+03:00

