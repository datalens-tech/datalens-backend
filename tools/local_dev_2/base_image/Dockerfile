FROM cr.yandex/crpsufj53lhl0via9iam/datalens_base_ci:latest


# RUN mv /venv /backed_venv
COPY make_requirements_editable.py /make_requirements_editable.py
COPY remote-python-entrypoint.sh /entrypoint.sh
COPY local_install.sh /local_install.sh
COPY find_ssh_agent_socket.py /find_ssh_agent_socket.py

RUN mv /venv /backed_venv

RUN chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]
