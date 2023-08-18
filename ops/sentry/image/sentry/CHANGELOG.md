CHANGELOG
=========

### 9.1.1-yandex.3

* Включили возможность использовать кастомные фильтры
* atomic file change
* cache unistat

### 9.1.1-yandex.2

* fix typo and new path for backend.py

### 9.1.1-yandex.1

* Обновили версию Sentry до 9.1.1
[Полный список изменений](https://github.com/getsentry/sentry/releases/tag/9.1.0)

### 9.0.0-yandex.7

* Починили сборку Docker-образа
* Починили работу с django_pgaas драйвером

### 9.0.0-yandex.7

* Ldap группы можно перечислять через запятую и авторизовываться при вхождении в любую из них 

### 9.0.0-yandex.6

* django_pgaas support

### 9.0.0-yandex.5

* Отключили автоматическую загрузку файлов по пользовательским урлам

### 9.0.0-yandex.4

* SENTRYSUP-35: Вернули ca-certificates.crt

### 9.0.0-yandex.3

* Корректно разложили сертификаты
* Добавили настраиваемую cron-задачу для очистки старых данных

### 9.0.0-yandex.2

* Dump environment e.g. for use in juggler checks

### 9.0.0-yandex.1

* Обновили версию Sentry до 9.0.0 :tada:  
[Полный список изменений](https://github.com/getsentry/sentry/releases/tag/9.0.0)

### 8.21.0-yandex.3

* Добавили возможность задавать количество CELERY воркеров переменной окружения SENTRY_CELERY_CONCURRENCY

### 8.21.0-yandex.2

* Увеличили время жизни коннектов к базе данных
* Увеличили максимальный размер загружаемых файлов до 40Мб

### 8.21.0-yandex.1

* Разделили настройки Sentry на отдельные файлы
* Заменили sentry-ldap-auth на исправленную версию
* Обновили Sentry до версии 8.21.0

### 8.14.1-yandex.15

* Настроили загрузку артефактов в S3

### 8.14.1-yandex.14

* Увеличили максимально возможный размер для загружаемых файлов

### 8.14.1-yandex.13

* Добавили возможность интеграции со Slack'ом

### 8.14.1-yandex.12

* Обновили заголовок `Content-Security-Policy`, чтобы загружались аватарки.

### 8.14.1-yandex.11

* Исправили заголовок `Content-Security-Policy`, чтобы загружались аватарки.

### 8.14.1-yandex.10

* Добавили заголовок `Content-Security-Policy` для интерфейса.

### 8.14.1-yandex.9

* Исправили утекание паролей в логи при возникновении ошибок. Портировали фикс https://github.com/getsentry/sentry/issues/5933

### 8.14.1-yandex.8

* Зафиксирована версия `sentry-ldap-auth` на `2.1`. Исправили ошибку авторизации через LDAP (https://github.com/Banno/getsentry-ldap-auth/issues/15).

### 8.14.1-yandex.7

* В пакете redis `2.10.6` внесли breaking change. Зафиксировали зависимость на `2.10.5`.

### 8.14.1-yandex.6

* Уровень логирования изменен с `INFO` на `WARNING`
* В момент старта контейнера ждем инициализацию Redis, прежде чем запускать Sentry
