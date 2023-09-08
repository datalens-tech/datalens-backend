# Local env and task file

Install Taskfile (https://taskfile.dev/) tools and set your .env file.
You could try use Taskfile to create a boilerplate with 'task env:mk_dot_env'. You only need to correctly set path to Python binary in your system.
Virtualenv would be created on firtst run of any command, or you could do it explicitly:
`task env:ensure_env`

## Useful commands

- task cq:fix_changed : applies all auto-fixes
- task cq:check_changed : check for any non-conformity in code style/format/lint

### Pre-commit hooks

You could install a git hook to automate checks or/and fixes using following task cmd. 
Note: currently any of this command completely replaces git hook content. 

- task cq:install_pre_commit_codestyle_checks : would run checks and block commit if any of them failed
- task cq:install_pre_commit_codestyle_checks_non_blocking : would run checks but would not block commit
- task cq:install_pre_commit_codestyle_autofix : would auto-fix errors and block commit if encounter any non-fixable error