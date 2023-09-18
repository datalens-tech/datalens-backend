# Formula Syntax

## Conditional blocks

- `IF` block

Basic syntax:
```
IF <condition_1> THEN <result_1>
    [ ELSEIF <condition_2> THEN <result_2>
      ... ]
    ELSE <else_result>
END
```
An `IF` block can have multiple or no `ELSIF` clauses.

Example:
```
IF [Sky] = "rainy" THEN "outdoors with umbrella" ELSIF [Temp] > 10 THEN "outdoors" ELSE "indoors" END
```

- `CASE` block

Basic syntax:
```
CASE <expr>
    WHEN <value_1> THEN <result_1>
    [ WHEN <value_2> THEN <result_2>
      ... ]
    ELSE <else_result>
END
```
Example:
```
CASE [Series] WHEN 2 THEN "Tennant" WHEN 5 THEN "Smith" WHEN 8 THEN "Capaldi" ELSE "dunno" END
```

## Logical Operations

- `NOT`
```
NOT <value>
```

- `AND`
```
<value_1> AND <value_2>
```

- `OR`
```
<value_1> OR <value_2>
```

- `IN`
```
<value_1> [ NOT ] IN (<alternative_1>, ...)
```

- `IS`
```
<value> IS [ NOT ] { NULL | TRUE | FALSE }
```

Pattern matching:

- `LIKE`
```
<value_1> [NOT] LIKE <value_2>
```

- Comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`.
These are a bit different from other binary operators in that they can be chained:
```
1 < val_1 < 3 = val_2 = val_3 <= 4 != val_4
```
In this case they are interpreted in pairs, so the expression is eqivalent to the following:
```
(1 < val_1) AND (val_1 < 3) AND (3 = val_2) AND (val_2 = val_3) AND (val_3 <= 4) AND (4 != val_4)
```

- `BETWEEN`
```
<value_1> [ NOT ] BETWEEN <value_2> AND <value_3>
```

## Arithmetic Operations

- Multiplication and division:
```
val_1 * val_2 / val_3
```

- Addition and subtraction:
```
val_1 + val_2 - val_3
```

## Other Operations

- Modulo:
```
val_1 % val_2
```

- Power:
```
val_1 ^ val_2
```

## Database Fields

References to database fields or columns are made using square brackets (`[]`):
```
[My Field Name]
```

## Functions

Basic function call syntax looks like this:
```
FUNCTION_NAME(<argument_1>, <arguemnt_2>, ...)
```

Functions might have any number of arguments, as well as none at all, e.g. the mathematical function `PI()`.

Trailing commas are considered to be invalid syntax.

Example:
```
IIF([Value] > 5, REPLACE("the result is RES", "RES", [Result]), "value is too low")
```
