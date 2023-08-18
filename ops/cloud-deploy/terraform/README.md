
## */script/prepare_init.sh ##

Враппер для `terraform init`,
который извлекает из yav ключи сервисного аккаунта,
нужные для доступа в S3 бакет с шареным стейтом terraform,
и добавляет соответствующие опции в аргументы.
Ключи попадают в открытом виде в `.terraform/terraform.tfstate`.
Использование: `./scripts/terraform_init.sh`


## */script/prepare_env.sh ##

YCP провайдер может использовать настройки из `~/.config/ycp/config.yaml`.
Для этого в env'е должна быть переменная `YCP_PROFILE`,
в которой должен быть указан профиль из конфига.
Использование: `. ./script/prepare_env.sh`


# Установка #

## Terraform ##

Скачать ZIP и положить куда-нибудь в `$PATH`
https://releases.hashicorp.com/terraform/0.12.29/terraform_0.12.29_darwin_amd64.zip
Нужна именно 0.12.x, так как с 0.13.0 несовместим YCP провайдер.

linux-вариант:

    cd "$(mktemp -d)"
    wget https://releases.hashicorp.com/terraform/0.12.29/terraform_0.12.29_linux_amd64.zip
    unzip *.zip
    sudo cp terraform /usr/local/bin/


## YCP provider ##

https://wiki.yandex-team.ru/cloud/devel/terraform-ycp/

    curl https://mcdev.s3.mds.yandex.net/terraform-provider-ycp/install.sh | bash


# Использование #

Из папки окружения:

`terraform plan` – проанализировать, что надо сделать с окружением,
чтобы привести его в соответствие со спецификацией.

`terraform apply` – привести окружение в соотвествие со спецификацией.
