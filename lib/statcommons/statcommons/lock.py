# coding: utf8
"""
...
"""

from __future__ import division, absolute_import, print_function, unicode_literals

import multiprocessing


class LockWrap(object):
    """ Minor convenience Lock wrap: CM and timeout """

    class LockNotAcquired(Exception):
        """ ... """

    @staticmethod
    def make_lock():
        lock = multiprocessing.Lock()
        setattr(lock, 'owner_name', None)
        return lock

    def __init__(self, timeout=2.0, lock=None, name=None):
        self.timeout = timeout
        if lock is None:
            lock = self.make_lock()
        self.lock = lock
        self.name = name

    def __enter__(self):
        result = self.lock.acquire(block=True, timeout=self.timeout)
        if not result:
            raise self.LockNotAcquired()
        setattr(self.lock, 'owner_name', self.name)

    def __exit__(self, *args, **kwargs):
        setattr(self.lock, 'owner_name', None)
        self.lock.release()


def test_lock_guard(cls=LockWrap):
    import random
    import threading
    import time

    print("LockWrap test...")

    def get_tid():
        return threading.currentThread().ident

    lock_obj = cls.make_lock()

    def locking_thread_fn(name, lock, duration, timeout):
        title = '%r: %r' % (get_tid(), name)
        while True:  # do the work once, but also wake up each `timeout` seconds until then.
            try:
                print('%s waits for lock for up to %.3fs ....' % (title, timeout))
                with cls(lock=lock, timeout=timeout, name=name):
                    print('%s begins to work for %.3fs ...' % (title, duration))
                    time.sleep(duration)  # the 'work'
                    print('%s finished.' % (title,))
                    return  # did it once, did it all.
            except cls.LockNotAcquired:
                print('%s woke up from waiting for %r;' % (title, lock.owner_name))

    _threads = []
    _total_d = 0
    for idx in range(3):
        _duration = random.randrange(500, 3000) / 1000.0
        _timeout = random.randrange(700, 2000) / 1000.0
        _threads.append(threading.Thread(
            target=locking_thread_fn,
            args=('thread_%d' % idx, lock_obj, _duration, _timeout)))
        _total_d += _duration

    ts0 = time.time()

    for thr in _threads:
        thr.start()
    for thr in _threads:
        thr.join()

    ts1 = time.time()
    td1 = ts1 - ts0

    print('duration: %.3f sec / expected: %.3f (%.1f%%)' % (
        td1, _total_d, 100 / _total_d * td1))


if __name__ == '__main__':
    test_lock_guard()
