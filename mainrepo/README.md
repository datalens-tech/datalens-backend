## Management tasks

Running tasks:
```
task <task_name>
```


### Environment (`env:`)

- `task env:devenv`:
  Create development/testing environment (run it from a package dir)
- `task env:devenv-d`:
  Create development/testing environment in detached mode (run it from a package dir)
- `task env:ensure_venv`: Command to create virtual env for the mainrepo tools. 
  It requires presence of .env in the mainrepo/tools.


### Generation (`gen:`)

- `task gen:antlr`:
  (Re-)generate ANTLR code files for formula


### Code quality (`cq:`)

Experimental tasks to check and fix source files. 
 
- `task cq:fix_changed`:
  Apply all auto-fixes
- `task cq:check_changed`:
  Check for any non-conformity in code style/format/lint
- `task cq:fix_dir -- {single dir}`:
  Apply all auto-fixes to the given dir absolute path
- `task cq:check_dir -- {single dir}`:
  Check for any non-conformity in code style/format/lint in the given dir abs path
