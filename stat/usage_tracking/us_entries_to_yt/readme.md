# Transfer US entries into YT

Скрипт используется в качестве Sandbox-ресурса, запускаемого кубиком в нирване.

Параметры запуска (кредсы баз, настройки скрипта) передаются в виде аргументов командной строки.

Переносит маппинг `encode_id(entry_id) - display_key` из ClickHouse на YT.

## Nirvana & Sandbox

Тип ресурса: DATALENS_LOAD_USENTRIES_KEYS_TO_YT ([файл с классом Sandbox-ресурса](https://arcanum.yandex-team.ru/arc_vcs/sandbox/projects/datalens/resources/__init__.py))

Материалы в нирване: [папка](https://reactor.yandex-team.ru/browse?selected=10642289), [кубик](https://nirvana.yandex-team.ru/operation/7a59bf44-123c-41d9-8a56-a4fa5587c044).
