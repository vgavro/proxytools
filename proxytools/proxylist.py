import logging
import random
from datetime import datetime, timedelta
import enum
import json
import os.path
import atexit

from gevent.lock import Semaphore
from gevent.thread import get_ident

from .models import Proxy
from .utils import CompositeContains

logger = logging.getLogger(__name__)


class GET_STRATEGY(enum.Enum):
    RANDOM = '_get_random'
    FASTEST = '_get_fastest'


class InsufficientProxiesError(RuntimeError):
    pass


class ProxyMaxRetriesExceeded(RuntimeError):
    pass


class ProxyList:
    def __init__(self, fetcher=None, min_size=50, max_fail=3, max_simultaneous=2,
                 rest=0, filename=None, atexit_save=False):
        if min_size <= 0:
            raise ValueError('min_size must be positive')
        self.fetcher = fetcher
        self.min_size = min_size
        self.max_fail = max_fail
        self.max_simultaneous = max_simultaneous
        self.rest = 0  # timeout for proxy to rest after success

        self.next_proxy_lock = Semaphore()
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
                fetcher.blacklist = CompositeContains(self.active_proxies,
                                                      self.blacklist_proxies)

        # Dictionary to use shared connection pools between sessions
        self.proxy_pool_manager = {}

        if self.need_update and self.fetcher and self.fetcher.ready:
            logger.info('Start fetch %s', self._stats_str)
            self.fetcher()

    @property
    def need_update(self):
        return len(self.active_proxies) < self.min_size

    @property
    def _stats_str(self):
        return ('(active:{} blacklist:{} fetcher:{})'
                .format(len(self.active_proxies), len(self.blacklist_proxies),
                        self.fetcher.ready and 'ready' or 'working'))

    def maybe_update(self):
        if not len(self.active_proxies) and not self.fetcher:
            raise InsufficientProxiesError('Insufficient proxies {}'
                                           .format(self._stats_str))
        if self.need_update and self.fetcher and self.fetcher.ready:
            logger.info('Start fetch %s', self._stats_str)
            self.fetcher()

    def proxy(self, proxy):
        if proxy.fail_at and proxy.fail_at > proxy.success_at:
            self.blacklist(proxy)

        elif proxy.addr in self.active_proxies:
            self.active_proxies[proxy.addr].merge_meta(proxy)

        elif proxy.addr in self.blacklist_proxies:
            self.blacklist_proxies[proxy.addr].merge_meta(proxy)

        else:
            self.active_proxies[proxy.addr] = proxy
            self.next_proxy_lock.release()

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
        if proxy.url in self.proxy_pool_manager:
            self.proxy_pool_manager[proxy.url].clear()
            del self.proxy_pool_manager[proxy.url]
        logger.debug('Blacklist: %s %s', proxy.addr, self._stats_str)
        self.maybe_update()

    def success(self, proxy):
        proxy.success_at = datetime.utcnow()
        proxy.fail_at = None
        proxy.fail = 0
        proxy.in_use -= 1
        assert proxy.in_use >= 0

    def get_ready_proxies(self, rest=None, exclude=[]):
        now = datetime.utcnow()
        rest = self.rest if rest is None else rest
        return {
            addr: p
            for addr, p in self.active_proxies.items()
            if p.in_use < self.max_simultaneous and
            addr not in exclude and
            (now - p.success_at).total_seconds() > rest
        }

    def get(self, strategy, rest=None, exclude=[], preserve=None, wait=True):
        if isinstance(strategy, enum.Enum):
            strategy = getattr(self, strategy.value)

        self.maybe_update()
        while True:
            ready_proxies = self.get_ready_proxies(rest, exclude)
            if ready_proxies:
                break
            else:
                if not self.fetcher or self.fetcher.ready:
                    raise InsufficientProxiesError('Insufficient proxies {}'
                                                   .format(self._stats_str))
                elif wait:
                    logger.info('Wait proxy (thread %s) %s', get_ident(),
                                self._stats_str)
                    self.next_proxy_lock.acquire(blocking=False)
                    self.next_proxy_lock.wait(None if wait is True else wait)
                else:
                    break

        if preserve:
            preserve = ready_proxies.get(preserve, None)
            if preserve:
                preserve.in_use += 1
                return preserve
        proxy = strategy(ready_proxies)
        if proxy:
            proxy.in_use += 1
            return proxy
        raise InsufficientProxiesError('Insufficient proxies {}'
                                       .format(self._stats_str))

    def _get_random(self, proxies):
        try:
            return random.choice(tuple(proxies.values()))
        except IndexError:
            return None

    def _get_fastest(self, proxies):
        for proxy in sorted(proxies.values(), reverse=True,
                            key=lambda p: p.speed or 0 / (p.in_use + 1)):
            return proxy

    def get_fastest(self, **kwargs):
        return self.get(self._get_fastest, **kwargs)

    def get_random(self, **kwargs):
        return self.get(self._get_random, **kwargs)

    def forget_blacklist(self, before):
        if isinstance(before, timedelta):
            before = datetime.utcnow() - timedelta
        for proxy in tuple(self.blacklist_proxies.values()):
            if proxy.fail_at < before:
                del self.blacklist_proxies[proxy.addr]

    def load(self, filename):
        with open(filename, 'r') as fh:
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
