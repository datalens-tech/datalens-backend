FROM registry.yandex.net/statinfra/base/bi_mess:latest
# ^ https://bb.yandex-team.ru/projects/STATBOX/repos/docker/browse/bi_base_mess/Dockerfile

RUN mkdir -p /code

COPY etc /etc

# REMINDER: Logrotate fixes: if any logrotate configs change is done in here,
# apply the commands from the base mess.

COPY /bi_test_project_worker /code/app/
RUN /code/app/bi_test_project_worker subcommands_to_bin
