import logging
import random
from datetime import datetime, timedelta
import enum
import json
import os.path
import atexit

from gevent.lock import Semaphore

from .models import Proxy

logger = logging.getLogger(__name__)


class GET_STRATEGY(enum.Enum):
    RANDOM = 'get_random'


class InsufficientProxiesError(RuntimeError):
    pass


class ProxyMaxRetriesExceeded(RuntimeError):
    pass


class ProxyList:
    def __init__(self, fetcher=None, min_size=50, max_failed=3, max_simultaneous=2,
                 filename=None, atexit_save=False):
        if min_size <= 0:
            raise ValueError('min_size must be positive')
        if fetcher:
            fetcher.add = self.add
        self.fetcher = fetcher
        self.min_size = min_size
        self.max_failed = max_failed
        self.max_simultaneous = max_simultaneous

        self.ready = Semaphore()
        self.active_proxies = {}
        self.blacklist_proxies = {}

        if filename and os.path.exists(filename):
            self.load(filename)
        if atexit_save:
            if atexit_save is True:
                assert filename
                atexit_save = filename
            atexit.register(self.save, atexit_save)

        self.maybe_update()

        # Dictionary to use shared connection pools between sessions
        self.proxy_pool_manager = {}

    @property
    def need_update(self):
        return len(self.active_proxies) < self.min_size

    def maybe_update(self, wait=False):
        if not len(self.active_proxies) and self.fetcher:
            self.ready.acquire(blocking=False)
        else:
            raise InsufficientProxiesError()
        if self.need_update and self.fetcher and self.fetcher.ready:
            self.fetcher()
        if wait and self.fetcher and self.ready.locked():
            self.ready.wait()

    def add(self, proxy):
        if proxy.url in self.active_proxies:
            self.active_proxies[proxy.url].merge_meta(proxy)
            return False
        elif proxy.url in self.blacklist_proxies:
            self.blacklist_proxies[proxy.url].merge_meta(proxy)
            return False
        else:
            self.active_proxies[proxy.url] = proxy
            if self.ready.locked:
                self.ready.release()
            return True

    def fail(self, proxy):
        proxy.failed_at = datetime.utcnow()
        proxy.failed += 1
        proxy.in_use -= 1
        assert proxy.in_use >= 0
        if proxy.url in self.active_proxies:
            if proxy.failed >= self.max_failed:
                self.blacklist(proxy)

    def blacklist(self, proxy):
        if proxy.url in self.active_proxies:
            del self.active_proxies[proxy.url]
        self.blacklist_proxies[proxy.url] = proxy
        if proxy.url in self.proxy_pool_manager:
            self.proxy_pool_manager[proxy.url].close()
            del self.proxy_pool_manager[proxy.url]
        self.maybe_update()

    def succees(self, proxy):
        proxy.succeed_at = datetime.utcnow()
        proxy.failed_at = None
        proxy.failed = 0
        proxy.in_use -= 1
        assert proxy.in_use >= 0

    def get(self, strategy, **kwargs):
        return getattr(self, strategy.value)(**kwargs)

    def get_random(self, exclude=[]):
        self.maybe_update(wait=True)
        try:
            proxy = random.choice([p for p in self.active_proxies.values()
                                   if p.in_use < p.max_simultaneous and p.url not in exclude])
        except IndexError:
            raise InsufficientProxiesError()
        proxy.in_use += 1
        return proxy

    def forget_blacklist(self, before):
        if isinstance(before, timedelta):
            before = datetime.utcnow() - timedelta
        for proxy in tuple(self.blacklist_proxies.values()):
            if proxy.failed_at < before:
                del self.blacklist_proxies[proxy.url]

    def load(self, filename):
        with open(filename, 'w') as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            for key in ('active_proxies', 'blacklist_proxies'):
                proxies = getattr(self, key)
                for proxy in data[key]:
                    proxies[proxy.url] = Proxy.from_json(proxy)
        else:
            for proxy in data:
                self.add(Proxy.from_json(proxy))

    def save(self, filename):
        data = {}
        for key in ('active_proxies', 'blacklist_proxies'):
            data[key] = tuple(p.to_json() for p in getattr(self, key))
        with open(filename, 'w') as fh:
            json.dump(fh)
