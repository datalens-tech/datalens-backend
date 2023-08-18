# Update users activity

Скрипт используется в качестве Sandbox-ресурса, запускаемого кубиком в нирване.

Параметры запуска (кредсы для кликхауса и параметры трансфера) передаются в виде аргументов командной строки.

Скрипт запускает операцию Yandex Data Transfer, перекидывающую новую таблицу для usage tracking из YT в CH. Трансфер происходит во временную таблицу, и в случае успеха партиция из нее сдвигается в общую для usage tracking таблицу в CH. Больше подробностей можно найти в https://st.yandex-team.ru/BI-3765#630772878cef430a88cad11f.

## Nirvana & Sandbox

Тип ресурса: DATALENS_YC_CH_USAGE_TRACKING_TRANSFER ([файл с классом Sandbox-ресурса](https://arcanum.yandex-team.ru/arc_vcs/sandbox/projects/datalens/resources/__init__.py))

Материалы в нирване: [реакция](https://nirvana.yandex-team.ru/browse?selected=13353391), [кубик](https://nirvana.yandex-team.ru/operation/08e31d18-4ca7-4aea-b5f4-0674a0040419/options), [граф](https://nirvana.yandex-team.ru/flow/585b0842-ac85-4102-b537-5915cfe99567), [артефакт для запуска](https://reactor.yandex-team.ru/browse?selected=12994868).
