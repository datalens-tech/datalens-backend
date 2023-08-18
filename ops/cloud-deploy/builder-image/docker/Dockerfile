FROM registry.yandex.net/cloud/packer:2020-08-25

COPY ./src /etc/cloud-builder

RUN chmod +x /etc/cloud-builder/*.sh

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip -o awscliv2.zip >/dev/null
RUN ./aws/install --update
