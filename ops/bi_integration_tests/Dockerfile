FROM registry.yandex.net/statinfra/base/bi_mess:latest

RUN mkdir -p /code

ENV RUN_DEVHOST_TESTS=1

COPY . .

# REMINDER: Logrotate fixes: if any logrotate configs change is done in here,
# apply the commands from the base mess.

WORKDIR /app

#We run integration tests inside doker with single dl_env=DLEnv.dynamic
#So we replace pytest dl_env parameter values list with [DLEnv.dynamic]
RUN perl -0777pi -e "s/(@pytest\.mark\.parametrize\s*\(\s*['\"]dl_env['\"].*?\[).*?]/\$1DLEnv.dynamic\]/sg" bi_integration_tests_tests/test_*.py

ENTRYPOINT ["./bi_integration_tests_bin"]
