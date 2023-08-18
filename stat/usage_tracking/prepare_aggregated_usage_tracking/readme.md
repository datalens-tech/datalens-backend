# Prepare aggregated Usage Tracking

Скрипт используется в качестве Sandbox-ресурса, запускаемого кубиком в нирване.

Параметры запуска (входные таблицы и дата) передаются в виде аргументов командной строки.

С помощью Nile скрипт подготавливает агрегированную статистику по логам пользователей (т.е. схлопывает записи с одинаковым набором ключевых полей в одну) для дальнейней заливки во [внутренний кликхаус](https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/7ce87c2f-0701-4a5c-989a-d4faf6220c77/view).

## Nirvana & Sandbox

Тип ресурса: DATALENS_PREPARE_AGGREGATED_USAGE_TRACKING ([файл с классом Sandbox-ресурса](https://arcanum.yandex-team.ru/arc_vcs/sandbox/projects/datalens/resources/__init__.py))

Материалы в нирване: [реакция](https://nirvana.yandex-team.ru/browse?selected=13590733), [артефакт для запуска](https://nirvana.yandex-team.ru/browse?selected=13590721), [кубик](https://nirvana.yandex-team.ru/operation/75a4f942-4721-4e6a-bfb5-2376d9c6586d), [граф](https://nirvana.yandex-team.ru/flow/e70d1aa9-45ac-4004-9212-618508ceb750/5f52b635-5e15-4760-bea9-7832fade23e1/graph).
