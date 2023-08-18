#!/bin/bash

CELERY_CONCURRENCY=${SENTRY_CELERY_CONCURRENCY:-${QLOUD_CPU_CORES%%.*}}

declare -px > /etc/container_environment.sh


wait_for_redis ()
{
    until [ `redis-cli ping | grep -c PONG` = 1 ]; do
        echo "Waiting 1s for Redis to load";
        sleep 1;
    done
}

# https://github.com/getsentry/sentry/issues/13242
sed -i -- 's/SOUTH_DATABASE_ADAPTERS"/SOUTH_DATABASE_ADAPTERS1"/g' /usr/local/lib/python2.7/site-packages/south/db/__init__.py
# наш драйвер не содержит postgres и на этом sentry ломается
sed -i -- "s/if 'postgres' in engine:/if 'postgres' in engine or 'backend' in engine:/g" /usr/local/lib/python2.7/site-packages/sentry/search/django/backend.py
sed -i -- "s/return 'postgres' in engine/return 'postgres' in engine or 'backend' in engine/g" /usr/local/lib/python2.7/site-packages/sentry/utils/db.py
sed -i -- "s/if 'postgres' in engine:/if 'postgres' in engine or 'pgaas' in engine:/g" /usr/local/lib/python2.7/site-packages/sentry/db/models/fields/array.py
sed -i -- "s/if 'postgres' in engine:/if 'postgres' in engine or 'pgaas' in engine:/g" /usr/local/lib/python2.7/site-packages/sentry/db/models/fields/bounded.py
sed -i -- "s/if 'postgres' in engine:/if 'postgres' in engine or 'pgaas' in engine:/g" /usr/local/lib/python2.7/site-packages/sentry/db/models/fields/citext.py
sed -i -- "s/if 'postgres' in engine:/if 'postgres' in engine or 'pgaas' in engine:/g" /usr/local/lib/python2.7/site-packages/sentry/db/models/fields/uuid.py

service redis-server start
wait_for_redis
/etc/init.d/xinetd start
start-stop-daemon -Sb --exec /usr/local/bin/sentry -- run worker --autoreload --no-color --logfile /proc/self/fd/1 --concurrency ${CELERY_CONCURRENCY}
start-stop-daemon -Sb --exec /usr/local/bin/sentry -- run cron --autoreload --no-color --logfile /proc/self/fd/1
QLOUD_CPU_CORES=${QLOUD_CPU_CORES%%.*} envsubst "\$QLOUD_HTTP_PORT \$QLOUD_CPU_CORES" < /etc/nginx/nginx.template > /etc/nginx/nginx.conf
nginx
sentry run web --upgrade --workers ${QLOUD_CPU_CORES%%.*}
