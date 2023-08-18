# Common includable for datalens/backend large tests with docker.

# # NOTE: `DOCKER_COMPOSE_FILE` and related config must be set *before* including.
IF(DOCKER_COMPOSE_FILE)
    # # Notable configuration:
    # SET(DOCKER_TEST_HOST ...)
    # SET(DOCKER_COMPOSE_FILE .../docker-compose.yml)
    # SET(DOCKER_CONTEXT_FILE .../docker-context.yml)

    # Sometimes the environment is *very* slow.
    ENV(COMPOSE_HTTP_TIMEOUT=600)

    INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)
ENDIF()

SIZE(LARGE)

TAG(
    ya:fat
    ya:force_sandbox
    ya:sandbox_coverage

    # Should reduce random failures due to timeouts,
    # and actually potentially reduce CPU usage
    sb:ssd

    # # To consider / set in actual tests:
    # ya:external
    # ya:dirty
    # ya:noretries
)

REQUIREMENTS(
    container:1435059545    # LXC_CONTAINER_WITH_DOCKER + xenial + arcadia-devtools at registry.yandex.net (actively used)
    # container:1603638470  # LXC_CONTAINER_WITH_DOCKER + xenial + arcadia-devtools at registry.yandex.net + mssql+oracle libs
    # container:1615660315  # LXC_CONTAINER_WITH_DOCKER + bionic + arcadia-devtools at registry.yandex.net + mssql+oracle libs

    cpu:all  # no neighboring processes
    ram:16
    dns:dns64  # registry access, at least
)

# Avoid depencency on `/etc/yandex/environment.*`
ENV(YENV_TYPE=development)
ENV(YENV_NAME=localhost)
