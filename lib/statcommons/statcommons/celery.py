# coding: utf8
"""
celery commons
"""

from __future__ import division, absolute_import, print_function, unicode_literals

import logging


LOGGER = logging.getLogger(__name__)


def get_queue_size_active(celery_app, queue):
    with celery_app.connection_or_acquire() as conn:
        queue_info = conn.default_channel.queue_declare(queue=queue)
        return queue_info.message_count


def get_queue_size_passive_i(conn, queue):
    from amqp.exceptions import ChannelError
    try:
        queue_info = conn.default_channel.queue_declare(
            queue=queue,
            passive=True,
        )
    except ChannelError as exc:
        if exc.reply_code != '404':
            raise
        return 0
    return queue_info.message_count


def get_queue_size_passive(celery_app, queue):
    with celery_app.connection_or_acquire() as conn:
        return get_queue_size_passive_i(conn=conn, queue=queue)


def get_queue_size_redis_leg(celery_app, queue):
    """
    redis backend only, left here for reference.
    """
    # redis backend
    try:
        # func = celery_app.backend.client.llen
        conn = celery_app.connection_or_acquire().__enter__()
        cli = conn.channel().client
        func = cli.llen
    except AttributeError:
        pass
    else:
        return func(queue)
    LOGGER.warning("get_queue_size: unsupported celery backend: %r", celery_app.backend)
    return None
