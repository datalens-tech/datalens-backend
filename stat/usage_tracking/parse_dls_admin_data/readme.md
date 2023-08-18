# Parse DLS admin data

Скрипт используется в качестве Sandbox-ресурса, запускаемого кубиком в нирване.

Параметры запуска (входные и выходные таблицы) передаются в виде аргументов командной строки и проверяется соответствие пути к таблице на YT с этими аргументами.

С помощью Nile по полученным таблицам entry -> admin (групповой или личный) и group -> member генерируются таблицы entry -> puid админа для дашей и остальных сущностей.

## Nirvana & Sandbox

Тип ресурса: DATALENS_PARSE_DLS_ADMIN_DATA ([файл с классом Sandbox-ресурса](https://arcanum.yandex-team.ru/arc_vcs/sandbox/projects/datalens/resources/__init__.py))

Материалы в нирване: [кубик](https://nirvana.yandex-team.ru/operation/69d066d0-91fa-498b-b643-e9013fcc6bac), [граф](https://nirvana.yandex-team.ru/flow/cee0d5ae-c1c2-413d-aa7f-d7e69ab5edc6/8e250038-23d4-42a9-a353-d84b4a5d62c7/graph), [реакция](https://nirvana.yandex-team.ru/browse?selected=13716551).
