# formula-cli

is a tool for basic formula troubleshooting and experimentation


## Installation

Requires:
- `Python 3.5.2` or above

The `datalens-formula` package has to be installed
(it is recommended to use a `virtualenv` for this):

1. checkout from git (ssh://git@bb.yandex-team.ru/statbox/bi-formula.git)
and install the package from the checked out folder
```
pip install -e . --index-url https://pypi.yandex-team.ru/simple/
```
2. or install from the local PyPI repo:
```
pip install datalens-formula --index-url https://pypi.yandex-team.ru/simple/
```

To run `bi-formula-cli doc *` commands be sure to prepare translation files
(requires the source code from git - installation option #1):
```
make translations
```
(see `Localization` in the [main README](../../README.md) for more info)


## Commands

### parse

Parse a formula and print its internal representation

```
bi-formula-cli parse 'SUM([MY Field]) + MAX([Other Field])'
```

For a pretty-printed output add the `--pretty` flag:
```
bi-formula-cli parse --pretty 'SUM([MY Field]) + MAX([Other Field])'
```
You will get something like this:
```
Formula(
  expr=Binary.make(
    name='+',
    left=FuncCall.make(name='sum', args=[Field(name='MY Field')]),
    right=FuncCall.make(name='max', args=[Field(name='Other Field')]),
  ),
)
```


### split

Split formula into parts (tree nodes) and print them at the corresponding nesting level.
Useful for troubleshooting and extending the formula parser.

```
bi-formula-cli split --diff 'IF [n0] * 5 THEN MY_FUNC(NULL, NOT [n2], "qwerty") ELSE [SOMETHING] AND [Q1] IN (12, 34) END'
```
Output:
```
 0: ▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫
 1: ▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫ ELSE ▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫ END
 2: IF ▫▫▫▫▫▫▫▫ THEN ▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫      ▫▫▫▫▫▫▫▫▫▫▫ AND ▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫
 3:    ▫▫▫▫ * ▫      MY_FUNC(▫▫▫▫, ▫▫▫▫▫▫▫▫, ▫▫▫▫▫▫▫▫)      [SOMETHING]     ▫▫▫▫ IN ▫▫▫▫▫▫▫▫
 4:    [n0]   5              NULL  NOT ▫▫▫▫  "qwerty"                       [Q1]    (▫▫, ▫▫)
 5:                                    [n2]                                          12  34                               [n2]                                         12  34
```


### translate

Translate `DataLens` formula into a raw SQL expression

The command requires a `--dialect <dialect_name>` argument.

Same formula with different dialects
```
bi-formula-cli translate --dialect clickhouse 'SUBSTR([MY Field], 6, 4)'
bi-formula-cli translate --dialect postgresql 'SUBSTR([MY Field], 6, 4)'
bi-formula-cli translate --dialect mysql 'SUBSTR([MY Field], 6, 4)'
```
produces different SQL code:
```
substring("MY Field", CAST(6 AS UInt8), CAST(4 AS UInt8))
SUBSTRING("MY Field" FROM 6 FOR 4)
SUBSTRING(`MY Field`, 6, 4)
```


### graph

Build GraphViz graphs for formula trees.

Generate raw graph code:
```
bi-formula-cli graph 'SUBSTR([MY Field], 6, 4)'
```
output:
```
// Formula
digraph {
        node_6b7bbe2c081646029ea8ee943a7af6cc [label=Formula shape=plaintext]
        node_3c93912d4db14a5eb0628a87102a35a3 [label="SUBSTR(<arg0>, <arg1>, <arg2>)" fillcolor="#bde6b1" shape=oval style=filled]
        node_09b970ce747446198c2792344fdb55fc [label="[MY Field]" fillcolor="#e6cccc" margin=0 shape=rectangle style=filled]
        node_3c93912d4db14a5eb0628a87102a35a3 -> node_09b970ce747446198c2792344fdb55fc [label=<arg0>]
        node_f11e8a17ad4c4a7ea135d99700940b7f [label=6 fillcolor="#b2dee6" margin=0 shape=rectangle style=filled]
        node_3c93912d4db14a5eb0628a87102a35a3 -> node_f11e8a17ad4c4a7ea135d99700940b7f [label=<arg1>]
        node_bec1eed4c8eb40e38180e05966549f1c [label=4 fillcolor="#b2dee6" margin=0 shape=rectangle style=filled]
        node_3c93912d4db14a5eb0628a87102a35a3 -> node_bec1eed4c8eb40e38180e05966549f1c [label=<arg2>]
        node_6b7bbe2c081646029ea8ee943a7af6cc -> node_3c93912d4db14a5eb0628a87102a35a3 [label=expr]
}
```

Create and save image to file:
```
bi-formula-cli graph --render-to formula.png 'SUBSTR([MY Field], 6, 4)'
```
The value of `--render-to` must end in `.png`

Generate and show image in GUI (requires `graphviz` to be installed):
```
bi-formula-cli graph --view 'SUBSTR([MY Field], 6, 4)'
```


### functions

List all supported functions for connection's SQL dialect.

```
bi-formula-cli functions --dialect mysql,postgresql
```
Prints a table like this:
```
| FUNCTION(args)        | POSTGRESQL_9_3   | MYSQL_5_6   | MYSQL_8_0_12   |
|:---------------------:|:----------------:|:-----------:|:--------------:|
| ABS(1)                | X                | X           | X              |
| ACOS(1)               | X                | X           | X              |
| ASCII(1)              | X                | X           | X              |
| ASIN(1)               | X                | X           | X              |
| ATAN(1)               | X                | X           | X              |
| ATAN2(2)              | X                | X           | X              |
| AVG(1)                | X                | X           | X              |
| CEILING(1)            | X                | X           | X              |
| CHAR(1)               | X                | X           | X              |
| CONTAINS(2)           | X                | X           | X              |
...
```
The format is Markdown-friendly, so it can be pasted into MD documents (just in case)

Use `--dialect all` for the full comparison table


### goto

Opens the PyCharm editor at the definition of the given function in the source code

Requires the `PYCHARM` variable to be set to path to the PyCharm binary;
otherwise it defaults to `pycharm`.

```
bi-formula-cli goto ifnull
```
works for operators too (`"%"`, `"/"`, `notbetween`, etc.).


### doc func

Generate function documentation files.

Use `--locale <locale name>` to set translation locale

```
bi-formula-cli doc func ./functions
```
will generate files for each function grouped by category (subfolders) inside the `.functions` directory


### doc toc

Generate function documentation table of contents (TOC).

Use `--locale <locale name>` to set translation locale

```
bi-formula-cli doc toc ./functions
```
will print the TOC to output


### doc list

Generate function list with short descriptions.

Use `--locale <locale name>` to set translation locale

```
bi-formula-cli doc list ./functions
```
will print the TOC to output


### doc availability

Generate a table with dialect support info for each function.

```
bi-formula-cli doc availability ./functions
```
will print the TOC to output


### doc locales

List all available locales (one per line)

```
bi-formula-cli doc locales
```
prints
```
en
ru
```

### doc todo

List functions that are not yet fully documented

Use optional `--category` argument to narrow down the list to a specific category

```
bi-formula-cli doc todo --category aggregation
```
prints
```
Missing description:
    AVG, COUNT, MAX, MEDIAN, MIN, STDEV, STDEVP
    SUM, VAR, VARP
Optional argument not documented:
    COUNT
```


### slice

Slice formula into execution levels.

```
bi-formula-cli slice "456 + SUM(FUNC(123)) + MAX([coeff] * AVG(456 + [qwe] + [rty] + [uio]) - 1)" --levels aggregate,aggregate,toplevel --diff
```
Output:
```
toplevel       ▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫ + MAX(▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫)
aggregate      456 + SUM(▫▫▫▫▫▫▫▫▫)       ▫▫▫▫▫▫▫ * AVG(▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫) - 1
aggregate                FUNC(123)        [coeff]       456 + [qwe] + [rty] + [uio]
```
