FROM registry.yandex.net/statinfra/base/bi_mess:latest
COPY remote-python-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# yep, we override python3.9 from /trunk/arcadia/datalens/backend/ops/docker-base-images/base_ubuntu_runit/Dockerfile
# by python3.10 from ppa
# beacause there is no python3.10 in yandex mirror for ubuntu 20.04
RUN echo 'deb http://ppa.launchpad.net/deadsnakes/ppa/ubuntu focal main' > /etc/apt/sources.list.d/deadsnakes.list
RUN apt-get update && apt-get install -y python3.10 python3.10-dev python3-pip python3.10-venv

RUN ln -sf python3.10 /usr/bin/python && ln -sf python3.10 /usr/bin/python3

RUN mkdir /bitools
COPY requirements /bitools/requirements

COPY find_ssh_agent_socket.py /usr/local/bin/find_ssh_agent_socket.py
RUN chmod +x /usr/local/bin/find_ssh_agent_socket.py

EXPOSE 51042
