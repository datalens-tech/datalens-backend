# Поддержка процессов в нирване

## Обновление версии ресурса

Для обновления ресурса необходимо собрать его скрипт (из-под мака могут быть проблемы, можно все это делать на линуксовой машинке в облаке):
```sh
resource_name$ ya make -A .
```

После чего загрузить полученный бинарь в ресурс. В параметр `type` нужно записать соответствующий тип [Sandbox-ресурса](https://arcanum.yandex-team.ru/arc_vcs/sandbox/projects/datalens/resources/__init__.py), параметр `ttl` задает время жизни ресурса при отсутствии обращений к нему:
```sh
resource_name$ ya upload resource_name --type SANDBOX_RESOURCE_TYPE --owner DATALENS --ttl 30
```

Далее необходимо дождаться загрузки и получить ссылку на ресурс, т.е. вывода следующего вида:
```sh
Created resource id is 0000000000
	TTL          : 30 days
	Resource link: https://sandbox.yandex-team.ru/resource/0000000000/view
	Download link: https://proxy.sandbox.yandex-team.ru/0000000000
```

Перейти по ссылке Resource link, далее в раздел DEPENDANT TASKS и перейти на таску MDS_UPLOAD (кликнуть по Task id), выбрать раздел HISTORY, дождаться появления записи со статусом SUCCESS, после чего нажать release (галочка возле название ресурса), выбрать тип релиза и описание.

После появления записи со статусом RELEASED ресурс будет готов к использованию в нирване.
