import logging
import random
from datetime import datetime, timedelta
import enum
import json
import os.path
import atexit

from gevent.lock import Semaphore

from .models import Proxy
from .utils import CompositeContains

logger = logging.getLogger(__name__)


class GET_STRATEGY(enum.Enum):
    RANDOM = 'get_random'


class InsufficientProxiesError(RuntimeError):
    pass


class ProxyMaxRetriesExceeded(RuntimeError):
    pass


class ProxyList:
    def __init__(self, fetcher=None, min_size=50, max_fail=3, max_simultaneous=2,
                 filename=None, atexit_save=False):
        if min_size <= 0:
            raise ValueError('min_size must be positive')
        self.fetcher = fetcher
        self.min_size = min_size
        self.max_fail = max_fail
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

        if fetcher:
            fetcher.proxy = self.proxy
            if fetcher.checker:
                blacklist = CompositeContains(self.active_proxies,
                                              self.blacklist_proxies)
                fetcher.checker.blacklist = blacklist
        self.maybe_update()

        # Dictionary to use shared connection pools between sessions
        self.proxy_pool_manager = {}

    @property
    def need_update(self):
        return len(self.active_proxies) < self.min_size

    def maybe_update(self, wait=False):
        if not len(self.active_proxies):
            if self.fetcher:
                self.ready.acquire(blocking=False)
                assert self.need_update
            else:
                raise InsufficientProxiesError()
        if self.need_update and self.fetcher and self.fetcher.ready:
            self.fetcher()
        if wait and self.fetcher and self.ready.locked():
            self.ready.wait()

    def proxy(self, proxy):
        if proxy.fail_at and proxy.fail_at > proxy.success_at:
            self.blacklist(proxy)

        elif proxy.addr in self.active_proxies:
            self.active_proxies[proxy.addr].merge_meta(proxy)

        elif proxy.addr in self.blacklist_proxies:
            self.blacklist_proxies[proxy.addr].merge_meta(proxy)

        else:
            self.active_proxies[proxy.addr] = proxy
            if self.ready.locked:
                self.ready.release()

    def fail(self, proxy, exc=None, resp=None):
        proxy.fail_at = datetime.utcnow()
        proxy.fail += 1
        proxy.in_use -= 1
        assert proxy.in_use >= 0
        if proxy.addr in self.active_proxies:
            if proxy.fail >= self.max_fail:
                self.blacklist(proxy)

    def blacklist(self, proxy):
        if proxy.addr in self.active_proxies:
            del self.active_proxies[proxy.addr]
        self.blacklist_proxies[proxy.addr] = proxy
        # TODO: there is urls in proxy_pool_manager!
        if proxy.url in self.proxy_pool_manager:
            self.proxy_pool_manager[proxy.url].clear()
            del self.proxy_pool_manager[proxy.url]
        self.maybe_update()

    def success(self, proxy):
        proxy.success_at = datetime.utcnow()
        proxy.fail_at = None
        proxy.fail = 0
        proxy.in_use -= 1
        assert proxy.in_use >= 0

    def get(self, strategy, **kwargs):
        return getattr(self, strategy.value)(**kwargs)

    def get_random(self, exclude=[], preserve=None):
        self.maybe_update(wait=True)
        if preserve:
            proxy = self.active_proxies.get(preserve, None)
            if proxy and proxy.in_use < self.max_simultaneous:
                proxy.in_use += 1
                return proxy
        try:
            proxy = random.choice([p for p in self.active_proxies.values()
                                   if p.in_use < self.max_simultaneous and
                                   p.addr not in exclude])
        except IndexError:
            raise InsufficientProxiesError()
        proxy.in_use += 1
        return proxy

    def forget_blacklist(self, before):
        if isinstance(before, timedelta):
            before = datetime.utcnow() - timedelta
        for proxy in tuple(self.blacklist_proxies.values()):
            if proxy.fail_at < before:
                del self.blacklist_proxies[proxy.addr]

    def load(self, filename):
        with open(filename, 'r') as fh:
            print(fh)
            data = json.load(fh)
        if isinstance(data, dict):
            for key in ('active_proxies', 'blacklist_proxies'):
                proxies = getattr(self, key)
                for proxy in data[key]:
                    proxy = Proxy.from_json(proxy)
                    proxies[proxy.addr] = proxy
        else:
            for proxy in data:
                self.add(Proxy.from_json(proxy))

    def save(self, filename):
        data = {}
        for key in ('active_proxies', 'blacklist_proxies'):
            data[key] = tuple(p.to_json() for p in getattr(self, key).values())
        with open(filename, 'w') as fh:
            json.dump(data, fh)
