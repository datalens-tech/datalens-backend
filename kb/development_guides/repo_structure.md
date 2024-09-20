# Project structure and package types

## Auxiliary directories

These directories contain tools and configurations that are not a part of the main codebase:
- `ci` – various tools for CI
- `docker_build` – docker bake files and scripts used to build applications
- `kb` – this knowledge base, contains guides and diagrams about the project
- `terrarium` – various tools for repository management and CI
- `tools` – contains taskfiles and other tools for local development and testing
- `lib/testenv-common` – contains common dockerfiles and entrypoints that are used for testing across other packages

## The `lib` directory

All packages under `lib/` can be divided into the following groups by their meaning:
- sqlalchemy dialect libraries – they contain extensions of basic sqlalchemy dialects for specific DL sources or
    custom dialects, they are typically named like `dl_sqlalchemy_*`
- connector packages – they contain connector plugins and are typically named like `dl_connector_*`,
    you can read more about connector packages on the ["Connector development"](connector_development.md) page
- testing base packages – they contain test suits and helpers for tests in other packages,
    examples of this type of packages are `dl_core_testing`, `dl_api_lib_testing`
- the so-called app-libs – an app-lib package contains means to configure, extend and run an application,
    defines its settings, views and an app factory, these packages typically contain "api"/"worker" and "lib" in
    their name, e.g. `dl_api_lib`, `dl_auth_api_lib`
- other packages basically isolate certain logic and can't be further divided into meaningful groups

Dependency management recommendations:
- with this amount of packages it is important to make an effort to avoid transitive
    dependencies – make sure that each imported package is listed among the package dependencies and vice versa
- sqlalchemy dialect libraries generally should only be used as dependencies in connector packages
    where they are needed, this is important
- app-lib packages generally should not be used as dependencies in packages under `lib`, they should only be used in `app`

## The `app` directory

Each package under `app` defines a specific implementation of one application, using a single app-lib,
extends its settings class and app factory if needed, provides means to instantiate, build (a Dockerfile) and run the app.
