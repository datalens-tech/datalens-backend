# ANTLR4-Based Parser

## Parser generation

### CLI

Run
```bash
task gen:antlr
```

### PyCharm

Parser files can be generated via PyCharm.

PyCharm provides syntax highlighting and runtime validation of ANTLR grammars.

#### Configure ANTLR4 in PyCharm

1. Install the `ANTLR v4` plugin
1. Open the grammar file `dl_formula/parser/antlr/DataLens.g4` in PyCharm.
1. Configure generator:
    - right click in file editor -> **Configure ANTLR...**.
    - set **Output directory ...** to `<path/to/project>/backend/lib/dl_formula`;
    - set **Package/namespace ...** to `dl_formula.parser.antlr.gen`;
    - set **Language** to `Python3`;
    - uncheck **generate parse tree listener**;
    - check **generate parse tree visitor**.

#### Running the generator

1. Open grammar.
2. Right click in file editor -> **Generate ANTLR Recognizer**
