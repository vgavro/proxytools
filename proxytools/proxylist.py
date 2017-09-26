import logging
import random
from datetime import datetime, timedelta
import enum
import json
import os.path
import atexit

from gevent import GreenletExit, Timeout, sleep
from gevent.event import Event
from gevent.thread import get_ident

from .models import Proxy
from .proxyfetcher import ProxyFetcher
from .utils import CompositeContains, repr_response

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
                 success_timeout=0, fail_timeout=0,
                 filename=None, atexit_save=False):
        if min_size <= 0:
            raise ValueError('min_size must be positive')
        self.min_size = min_size
        self.max_fail = max_fail
        self.max_simultaneous = max_simultaneous
        self.success_timeout = success_timeout
        self.fail_timeout = fail_timeout

        # Event is set each time proxy is added or released
        self.proxy_avail = Event()

        self.active_proxies = {}
        self.blacklist_proxies = {}
        self.waiting = {}

        # Dictionary to use shared connection pools between sessions
        self.proxy_pool_manager = {}

        if isinstance(fetcher, dict):
            fetcher = ProxyFetcher(proxylist=self, **fetcher)
        if fetcher:
            fetcher.proxy = self.proxy
            if fetcher.checker:
                fetcher.blacklist = CompositeContains(self.active_proxies,
                                                      self.blacklist_proxies)
        self.fetcher = fetcher

        if filename and os.path.exists(filename):
            self.load(filename)
        if atexit_save:
            # NOTE: atexit works only on SIGINT (not SIGTERM or SIGKILL)
            if atexit_save is True:
                assert filename
                atexit_save = filename
            atexit.register(self.save, atexit_save)
        self.atexit_save = atexit_save

        if self.need_update and fetcher and fetcher.ready:
            logger.info('Start fetch %s', self._stats_str)
            fetcher()

    @property
    def need_update(self):
        return len(self.active_proxies) < self.min_size

    @property
    def _stats_str(self):
        return ('(active:{} blacklist:{} wait:{} fetch:{})'
                .format(len(self.active_proxies), len(self.blacklist_proxies),
                        len(self.waiting), not self.fetcher and 'no' or
                        (self.fetcher.ready and 'ready' or 'working')))

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
            self.proxy_avail.set()

    def fail(self, proxy, exc=None, resp=None, timeout=None):
        if exc:
            reason = ' exception: {!r}'.format(exc)
        elif resp:
            reason = ' response not matched: {}'.format(repr_response(resp))
        else:
            reason = ''
        logger.debug('Failed: %s%s %s', proxy.addr, reason, self._stats_str)
        proxy.fail_at = datetime.utcnow()
        proxy.fail += 1
        proxy.in_use -= 1
        assert proxy.in_use >= 0
        if proxy.addr in self.active_proxies:
            if proxy.fail >= self.max_fail:
                self.blacklist(proxy)
            else:
                timeout = self.fail_timeout if timeout is None else timeout
                if timeout:
                    rest_till = proxy.fail_at + timedelta(seconds=timeout)
                    if not proxy.rest_till or proxy.rest_till < rest_till:
                        proxy.rest_till = rest_till
                self.proxy_avail.set()  # TODO: consider rest_till?
                sleep(0)  # switch to other greenlet for fair play

    def blacklist(self, proxy):
        if proxy.addr in self.active_proxies:
            del self.active_proxies[proxy.addr]
        self.blacklist_proxies[proxy.addr] = proxy
        if proxy.url in self.proxy_pool_manager:
            self.proxy_pool_manager[proxy.url].clear()
            del self.proxy_pool_manager[proxy.url]
        logger.debug('Blacklist: %s %s', proxy.addr, self._stats_str)
        self.maybe_update()

    def success(self, proxy, timeout=None):
        proxy.success_at = datetime.utcnow()
        proxy.fail_at = None
        proxy.fail = 0
        proxy.in_use -= 1
        assert proxy.in_use >= 0
        timeout = self.success_timeout if timeout is None else timeout
        if timeout:
            rest_till = proxy.success_at + timedelta(seconds=timeout)
            if not proxy.rest_till or proxy.rest_till < rest_till:
                proxy.rest_till = rest_till
        self.proxy_avail.set()  # TODO: consider rest_till?
        sleep(0)  # switch to other greenlet for fair play

    def rest(self, proxy, timeout, resp=None):
        proxy.in_use -= 1
        assert proxy.in_use >= 0
        rest_till = datetime.utcnow() + timedelta(seconds=timeout)
        if not proxy.rest_till or proxy.rest_till < rest_till:
            proxy.rest_till = rest_till
        reason = resp and ' response: {}'.format(repr_response(resp)) or ''
        logger.debug('Rest: %s%s till %s %s', proxy.addr, reason, proxy.rest_till, self._stats_str)

    @property
    def in_use(self):
        return sum([p.in_use for p in self.active_proxies])

    def get_ready_proxies(self, exclude=[], countries=None):
        now = datetime.utcnow()
        return {
            addr: p
            for addr, p in self.active_proxies.items()
            if p.in_use < self.max_simultaneous and
            addr not in exclude and
            (not p.rest_till or p.rest_till < now) and
            (not countries or p.country in countries)
        }

    def get(self, strategy, exclude=[], persist=None, wait=True, countries=None):
        if not callable(strategy):
            if isinstance(strategy, str):
                strategy = getattr(self, GET_STRATEGY[strategy].value)
            elif isinstance(strategy, enum.Enum):
                strategy = getattr(self, strategy.value)

        self.maybe_update()

        ident = get_ident()
        while True:
            ready_proxies = self.get_ready_proxies(exclude, countries)
            if ready_proxies:
                break
            elif wait is False or ((not self.fetcher or self.fetcher.ready) and not self.in_use):
                raise InsufficientProxiesError('Insufficient proxies {}'
                                               .format(self._stats_str))
            else:
                # logger.info('Wait proxy (thread %s) %s', ident, self._stats_str)
                self.proxy_avail.clear()
                if ident not in self.waiting:
                    #TODO: maybe more stats here
                    self.waiting[ident] = datetime.utcnow()
                elif (wait is not True and wait is not None and
                     (datetime.utcnow() - self.waiting[ident]).total_seconds() > wait):
                    del self.waiting[ident]
                    raise Timeout(wait)
                try:
                    self.proxy_avail.wait(None if wait is True else wait)
                except (Timeout, GreenletExit):
                    del self.waiting[ident]
        if ident in self.waiting:
            del self.waiting[ident]

        if persist:
            persist = ready_proxies.get(persist, None)
            if persist:
                persist.in_use += 1
                return persist
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
        logger.info('Loaded proxies status %s %s', filename, self._stats_str)

    def save(self, filename=None):
        filename = filename or self.atexit_save
        if not filename:
            raise ValueError('Please specify filename or '
                             'init ProxyList with atexit_save attribute')
        data = {}
        for key in ('active_proxies', 'blacklist_proxies'):
            data[key] = tuple(p.to_json() for p in getattr(self, key).values())
        logger.info('Saving proxies status %s %s', filename, self._stats_str)
        with open(filename, 'w') as fh:
            json.dump(data, fh)
