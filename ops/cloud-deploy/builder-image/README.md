# Cloud deploy
[![version](https://badger.yandex-team.ru/custom/[Version]/[v2-2020-09-30-12-23][blue]/badge.svg)](.version)

Проект создан с целью упростить сборку и деплой приложений в [Яндекс.Облако](https://cloud.yandex.ru).

Это набор bash-скриптов и конфигурационных файлов. Поставляется в виде docker-образа.
Пример запуска:
```
sudo docker run -it -w="/etc/cloud-builder" \
-v ./preprod:/etc/configs \
-e "DOCKER_USERNAME=40in" \
-e "DOCKER_TOKEN=sldfkj" \
-e "BUILD_IMAGE_URL=https://registry.yandex.net/dataui/testimage:3242" \
-e "YAV_TOKEN=***" \
-e "SSH_PRIVATE_KEY_FILE=/Users/imperator/.ssh/sa-key.json" \
registry.yandex.net/data-ui/cloud-builder:v2-2020-08-19-16-22 \
/bin/bash "build.sh /etc/configs ."
```

## API
### Build
Для сборки образа нужно вызвать 
```
sh build.sh <config-folder> <manifest-folder>
```

`config-folder` путь к папке с конфигурацией сборки

`manifest-folder` путь к папке, куда будет записан файл `manifest.json` с результаом сборки

#### ENV переменные
##### DOCKER_USERNAME
Логин для авторизации в docker
##### DOCKER_TOKEN
Токен для авторизации в docker
##### BUILD_IMAGE_URL
url docker-образа приложения, который будет запускаться внутри ВМ 
##### YAV_TOKEN
Токен из секретницы для прокачки секретов утилитой skm
##### SSH_PRIVATE_KEY_FILE
Путь к файлу сервисного аккаунта в облаке. Из под этого СА будут производиться все действия в облаке.

#### build-settings.yaml
Здесь описываются настройки **сборки** вашего приложения.

##### CLOUD_ID
id облака, где будет происходить процесс сборки
##### FOLDER_ID
id фолдера, где будет происходить процесс сборки
##### SUBNET_ID
id подсети, где будет происходить процесс сборки
##### ZONE
Зона: ru-central1-a | ru-central1-b | ru-central1-c
##### BUILD_ENVIRONMENT
Окружение: prod | preprod
##### BUCKET
Имя бакета для экспорта образа в s3, например project-packer-images.
У сервисного аккаунта, который используется для авторизации в облаке должны быть права на запись в бакет. 
##### DISABLE_USING_OLD_DNS
В качестве DNS прописан `2a02:6b8:0:3400::5005` по умолчанию. Для использования настроек DNS из базового образа нужно проставить этот фалг в `true`

## Авторизация в Соломоне
1. Даем права на проект в соломоне на запись для robot-cloud-metrics
1. Копируем себе настройку секрета из [коммита](https://github.yandex-team.ru/data-ui/cloud-console/commit/0bdb7a27b856efca5561f2d23ce6e376a6d92f37)
1. Используем версию registry.yandex.net/data-ui/cloud-builder:v2-2020-09-30-12-23

Если нужен свой робот, то соответсвенно сами получаете для него токен для доступа в соломон, складываете в yav и прописывает свой secret_id во 2 шаге

## Обновление базового образа
Актуальные версии базового образа можно найти 
[здесь](https://bb.yandex-team.ru/projects/CLOUD/repos/paas-images/browse/paas-base-g2/CHANGELOG.md)
