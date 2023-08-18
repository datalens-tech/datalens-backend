# Transfer US entries into CH

Скрипт используется в качестве Sandbox-ресурса, запускаемого кубиком в нирване.

Параметры запуска (кредсы баз, настройки скрипта) передаются в виде аргументов командной строки.

Переносит маппинг `encode_id(entry_id) - display_key` из United Storage в ClickHouse. В зависимости от параметров запуска переносит все объекты или те объекты, которые обновились с прошлого успешного выполнения скрипта, которое он получает из ClickHouse и записывает после выполнения. Обновление существующих объектов достигается использованием движка `ReplacingMergeTree` в ClickHouse, который после добавления записей в течение неопределенного времени в фоне удаляет дубликаты по уникальности `encoded_entry_id`.

## Nirvana & Sandbox

Тип ресурса: DATALENS_LOAD_USENTRIES_KEYS_TO_CH ([файл с классом Sandbox-ресурса](https://arcanum.yandex-team.ru/arc_vcs/sandbox/projects/datalens/resources/__init__.py))

Материалы в нирване: [папка](https://reactor.yandex-team.ru/browse?selected=10642289), [кубик](https://nirvana.yandex-team.ru/operation/34cbce66-0603-4958-829b-c25e46d2379a).
