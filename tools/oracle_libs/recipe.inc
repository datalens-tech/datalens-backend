DEPENDS(datalens/backend/tools/oracle_libs)
USE_RECIPE(datalens/backend/tools/oracle_libs/oracle_libs_recipe)

DATA(
    # sbr://1595899029  # bionic, almost-full deps
    sbr://1608374221  # xenial, minimal deps (libaio1)
)
