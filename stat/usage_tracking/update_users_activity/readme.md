# Update users activity

Скрипт используется в качестве Sandbox-ресурса, запускаемого кубиком в нирване.

Параметры запуска (пути к таблицам) передаются в виде аргументов командной строки.

С помощью Nile скрипт обновляет таблицу, где каждому `user_id` сопоставлен timestamp его последней замеченной активности по логам. Для обновления используется таблица, полученная скриптом `../prepare_logs`.

## Nirvana & Sandbox

Тип ресурса: DATALENS_UPDATE_USERS_ACTIVITY ([файл с классом Sandbox-ресурса](https://arcanum.yandex-team.ru/arc_vcs/sandbox/projects/datalens/resources/__init__.py))

Материалы в нирване: [реакция](https://reactor.yandex-team.ru/browse?selected=12994934), [кубик](https://nirvana.yandex-team.ru/operation/a2e7a2d1-d442-49cd-b7ec-7d12f5d3bb23/options), [артефакт для запуска](https://reactor.yandex-team.ru/browse?selected=12994868).
