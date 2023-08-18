from __future__ import annotations

from typing import cast

from yadls.settings import settings as base

DATABASES = dict(
    default=dict(
        ENGINE='django.db.backends.postgresql',
        # ENGINE='django_pgaas.backend',
        HOST=base.DB_HOST,
        PORT=base.DB_PORT,
        USER=base.DB_USER,
        PASSWORD=base.DB_PASSWORD,
        NAME=base.DB_NAME,

        ATOMIC_REQUESTS=True,
        AUTOCOMMIT=True,
        CONN_MAX_AGE=None,
        DISABLE_SERVER_SIDE_CURSORS=True,
        OPTIONS=dict(
            connect_timeout=2,
            # sslmode='verify-full', sslrootcert='/etc/ssl/certs/ca-certificates.crt',
            target_session_attrs='read-write',
        ),
        # TEST=dict(CHARSET=None, COLLATION=None, MIRROR=None, NAME='test_yadls'),
        # TIME_ZONE=None,
    ),
)

DATABASES.update(
    slave=dict(
        DATABASES['default'],
        OPTIONS=dict(
            cast(dict, DATABASES['default']['OPTIONS']),
            target_session_attrs='any',
        ),
        # TEST=dict(DATABASES['default']['TEST'], MIRROR='default'),
    ),
    default_direct=dict(
        DATABASES['default'],
        ATOMIC_REQUESTS=False,
        # TEST=dict(DATABASES['default']['TEST'], MIRROR='default'),
    ),
)


SECRET_KEY = 'dev value'

INSTALLED_APPS = [
    'django_extensions',
    'yadls.dj.dls',
]


DEBUG = True

# Force init logging
from yadls import dbg
dbg.init_logging(app_name='dlsdj')
