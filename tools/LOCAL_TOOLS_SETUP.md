Setting up code linting & formatting tools with pycharm integration.
---

# Install tools
Simplest way to use code tools is to re-use venv created in `mainrepo/tools/.venv`
(if you used default setting for mainrepo/tools/.env $VENV_PATH).
Otherwise, create/use alternative venv and substitute venv path in snippets bellow.
With activated venv run:
    
    pip install isort 'black[d]' ruff

# ISORT, RUFF

- Get path to commands with an activate virtualenv:
    
      which isort
      which ruff

## [OPTION 1] Create a file watch 
- Go to Pycharm preferences Tools > File watcher > +
  - Name, description as you like
  - Paste absolute path for cmd in the Program field
  - Select/copy-paste into Arguments:  $FilePath$
  - Set Working directory to the repo root: $ProjectFileDir$
  - Uncheck option "Autosave edited files ...": to avoid tool run on incomplete change, which could often screw up your changes 

## [OPTION 2] Create external tool configuration for manual run. 
- Go to Pycharm preferences Tools > External tools > +
  - Name, description as you like
  - Paste absolute path for cmd in the Program field
  - Select/copy-paste into Arguments:  $FilePath$
  - Set Working directory to the repo root: $ProjectFileDir$
-  Setup keybindings
  - Fine "Tools>External tools"
  - There should be at least two sub records "External tool" which represents two tools which you just added
  - Assign shortcuts as you like

# BLACK

See instructions at https://black.readthedocs.io/en/stable/integrations/editors.html
I would suggest either using built-in plugin in recent pycharm or setup blackd for near instant formatting.

