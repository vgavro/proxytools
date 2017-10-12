import logging
import random
from datetime import datetime, timedelta
import enum
import json
import os.path
import atexit

from gevent import Timeout, sleep, spawn
from gevent.event import Event
from gevent.thread import get_ident

from .models import Proxy, PROXY_RESULT_TYPE
from .proxyfetcher import ProxyFetcher
from .utils import JSONEncoder, CompositeContains, repr_response, import_string

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
                 success_timeout=0, fail_timeout=0, history=0, update_on=None,
                 update_timeout=10 * 60, filename=None, atexit_save=False, json_encoder={}):
        if min_size <= 0:
            raise ValueError('min_size must be positive')
        self.min_size = min_size
        self.max_fail = max_fail
        self.max_simultaneous = max_simultaneous
        self.success_timeout = success_timeout
        self.fail_timeout = fail_timeout
        self.history = history

        # Event is set each time proxy is added or released
        self.proxy_ready = Event()
        self._proxy_ready_at = None
        self._proxy_ready_notify_worker = None

        self.active_proxies = {}
        self.blacklist_proxies = {}
        self.waiting = {}

        # Dictionary to use shared connection pools between sessions
        self.proxy_pool_manager = {}

        self.update_timeout = timedelta(seconds=update_timeout)
        if isinstance(update_on, str):
            update_on = import_string(update_on)
        self.update_on = update_on

        if isinstance(fetcher, dict):
            fetcher = ProxyFetcher(proxylist=self, **fetcher)
        if fetcher:
            fetcher.proxy = self.proxy
            if fetcher.checker:
                fetcher.blacklist = CompositeContains(self.active_proxies,
                                                      self.blacklist_proxies)
        self.fetcher = fetcher

        if isinstance(json_encoder, dict):
            json_encoder = JSONEncoder(**json_encoder)
        self.json_encoder = json_encoder

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
        return bool(len(self.active_proxies) < self.min_size or
                    (self.update_on and self.update_on(self)))

    @property
    def _stats_str(self):
        return ('(active:{} blacklist:{} wait:{} fetch:{})'
                .format(len(self.active_proxies), len(self.blacklist_proxies),
                        len(self.waiting), not self.fetcher and 'no' or
                        (self.fetcher.ready and 'ready' or 'working')))

    def _proxy_ready_notify_at(self, proxy_ready_at):
        """
        Spawns ProxyList._proxy_ready_notify for ProxyList.proxy_ready event invoke
        on Proxy.rest_till expiration.
        """
        if self._proxy_ready_at:
            if self._proxy_ready_at > proxy_ready_at:
                self._proxy_ready_notify_worker.kill()
                self._proxy_ready_at = proxy_ready_at
                self._proxy_ready_notify_worker = spawn(self._proxy_ready_notify)
        else:
            self._proxy_ready_at = proxy_ready_at
            assert (not self._proxy_ready_notify_worker or
                    self._proxy_ready_notify_worker.ready())
            self._proxy_ready_notify_worker = spawn(self._proxy_ready_notify)

    def _proxy_ready_notify(self):
        now = datetime.utcnow()
        while self._proxy_ready_at:
            assert self._proxy_ready_at > now
            sleep((self._proxy_ready_at - now).total_seconds())
            now = datetime.utcnow()
            try:
                self._proxy_ready_at = min(p.rest_till for p in self.active_proxies.values()
                                           if p.rest_till and p.rest_till > now)
            except ValueError:
                # no rest_till in future
                self._proxy_ready_at = None
            self.proxy_ready.set()

    def maybe_update(self):
        if not len(self.active_proxies) and not self.fetcher:
            raise InsufficientProxiesError('No proxies and no fetcher {}'
                                           .format(self._stats_str))
        if self.fetcher and self.fetcher.ready and self.need_update:
            now = datetime.utcnow()
            if (self.fetcher.started_at is None or
               (self.fetcher.started_at + self.update_timeout) <= now):
                logger.info('Start fetch %s', self._stats_str)
                self.fetcher()
            else:
                logger.warning('Update needed, but timeout not expired (%s), %s',
                               now - self.fetcher.started_at, self._stats_str)

    def proxy(self, proxy, load=False):
        if proxy.addr in self.blacklist_proxies:
            if proxy.success_at and (not proxy.fail_at or proxy.success_at > proxy.fail_at):
                # recheck - we should unblacklist it
                self.unblacklist(proxy)
            else:
                # fetch from other source
                self.blacklist_proxies[proxy.addr].merge_meta(proxy)

        elif proxy.blacklist:
            # loading
            self.blacklist(proxy, load=load)

        elif not load and proxy.fail_at and (not proxy.success_at or
                                             proxy.fail_at > proxy.success_at):
            # check or recheck and it was failed
            self.blacklist(proxy)

        elif proxy.addr in self.active_proxies:
            # fetch from other source
            self.active_proxies[proxy.addr].merge_meta(proxy)

        else:
            # loading or fetch
            self.active_proxies[proxy.addr] = proxy
            if load and proxy.rest_till and proxy.rest_till > datetime.utcnow():
                self._proxy_ready_notify_at(proxy.rest_till)
            else:
                self.proxy_ready.set()

    def fail(self, proxy, timeout=None, exc=None, resp=None, request_ident=None):
        proxy.fail_at = datetime.utcnow()
        proxy.fail += 1
        proxy.in_use -= 1
        assert proxy.in_use >= 0
        reason = (exc and repr(exc)) or (resp is not None and repr_response(resp) or None)
        if self.history:
            proxy.set_history(proxy.fail_at, PROXY_RESULT_TYPE.FAIL, reason,
                              request_ident, self.history)

        logger.debug('Failed: %s%s%s %s', proxy.addr,
                     request_ident and ' ' + request_ident or '',
                     reason and ' ' + reason or '', self._stats_str)
        if proxy.addr in self.active_proxies:
            if proxy.fail >= self.max_fail:
                self.blacklist(proxy)
            else:
                timeout = self.fail_timeout if timeout is None else timeout
                if timeout:
                    proxy.set_rest_till(proxy.fail_at + timedelta(seconds=timeout))
                    self._proxy_ready_notify_at(proxy.rest_till)
                else:
                    self.proxy_ready.set()
                    sleep(0)  # switch to other greenlet for fair play

    def blacklist(self, proxy, load=False):
        proxy.blacklist = True
        if proxy.addr in self.active_proxies:
            del self.active_proxies[proxy.addr]
        self.blacklist_proxies[proxy.addr] = proxy
        if proxy.url in self.proxy_pool_manager:
            self.proxy_pool_manager[proxy.url].clear()
            del self.proxy_pool_manager[proxy.url]
        if not load:
            logger.debug('Blacklist: %s %s', proxy.addr, self._stats_str)
        self.maybe_update()

    def unblacklist(self, proxy):
        proxy.blacklist = False
        if proxy.addr in self.blacklist_proxies:
            del self.blacklist_proxies[proxy.addr]
        if proxy.addr not in self.active_proxies:
            self.active_proxies[proxy.addr] = proxy
            self.proxy_ready.set()

    def success(self, proxy, timeout=None, resp=None, request_ident=None):
        proxy.success_at = datetime.utcnow()
        proxy.fail = 0
        proxy.in_use -= 1
        assert proxy.in_use >= 0
        if self.history:
            proxy.set_history(proxy.success_at, PROXY_RESULT_TYPE.SUCCESS,
                              resp is not None and repr_response(resp) or None,
                              request_ident, self.history)
        timeout = self.success_timeout if timeout is None else timeout
        if timeout:
            proxy.set_rest_till(proxy.success_at + timedelta(seconds=timeout))
            self._proxy_ready_notify_at(proxy.rest_till)
        else:
            self.proxy_ready.set()
            sleep(0)  # switch to other greenlet for fair play

    def rest(self, proxy, timeout, resp=None, request_ident=None):
        proxy.in_use -= 1
        assert proxy.in_use >= 0
        now = datetime.utcnow()
        proxy.set_rest_till(now + timedelta(seconds=timeout))
        self._proxy_ready_notify_at(proxy.rest_till)
        reason = resp is not None and repr_response(resp) or None
        if self.history:
            proxy.set_history(now, PROXY_RESULT_TYPE.REST, reason,
                              request_ident, self.history)
        logger.debug('Rest: %s%s%s till %s %s', proxy.addr,
                     request_ident and ' ' + request_ident or '',
                     reason and ' ' + reason or '', proxy.rest_till, self._stats_str)

    @property
    def in_use(self):
        return sum([p.in_use for p in self.active_proxies.values()])

    def get_ready_proxies(self, exclude=[], countries=None, countries_exclude=None,
                          min_speed=None):
        now = datetime.utcnow()
        return {
            addr: p
            for addr, p in self.active_proxies.items()
            if p.in_use < self.max_simultaneous and
            addr not in exclude and
            (not p.rest_till or p.rest_till < now) and
            (not countries or p.country in countries) and
            (not countries_exclude or p.country not in countries_exclude) and
            (not min_speed or p.speed >= min_speed)
        }

    def get(self, strategy, persist=None, wait=True, request_ident=None, **proxy_params):
        if not callable(strategy):
            if isinstance(strategy, str):
                strategy = getattr(self, GET_STRATEGY[strategy].value)
            elif isinstance(strategy, enum.Enum):
                strategy = getattr(self, strategy.value)

        self.maybe_update()

        ident = get_ident()  # unique integer id for greenlet
        while True:
            ready_proxies = self.get_ready_proxies(**proxy_params)
            if ready_proxies:
                break
            elif not wait or ((not self.fetcher or self.fetcher.ready) and not self.in_use):
                raise InsufficientProxiesError('No ready proxies {} {}{}'
                    .format(proxy_params, request_ident and request_ident + ' ' or '',
                            self._stats_str))
            else:
                # logger.info('Wait proxy (thread %s) %s', ident, self._stats_str)
                self.proxy_ready.clear()
                if ident not in self.waiting:
                    # Storing extra data for superproxy monitoring
                    self.waiting[ident] = dict(since=datetime.utcnow(),
                        request_ident=request_ident, params=proxy_params)
                elif (wait is not True and
                     (datetime.utcnow() - self.waiting[ident]['since']).total_seconds() > wait):
                    del self.waiting[ident]
                    raise InsufficientProxiesError('Ready proxies wait timeout({}) {} {}{}'
                        .format(wait, proxy_params, request_ident and request_ident + ' ' or '',
                                self._stats_str))
                try:
                    self.proxy_ready.wait(None if wait is True else wait)
                except Timeout:
                    continue
                except:
                    del self.waiting[ident]
                    raise
        if ident in self.waiting:
            del self.waiting[ident]

        if persist:
            proxy = ready_proxies.get(persist, None)
            if proxy:
                proxy.in_use += 1
                return proxy
        proxy = strategy(ready_proxies)
        if proxy:
            proxy.in_use += 1
            return proxy
        raise InsufficientProxiesError('No proxies from {} ready with {} strategy {}{}'
            .format(len(ready_proxies), strategy, request_ident and request_ident + ' ' or '',
                    self._stats_str))

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

    def get_by_addr(self, addr):
        return self.active_proxies.get(addr) or self.blacklist_proxies.get(addr)

    def load(self, filename):
        with open(filename, 'r') as fh:
            data = json.load(fh)
        for proxy in data:
            self.proxy(Proxy.from_json(proxy), load=True)
        logger.info('Loaded proxies status %s %s', filename, self._stats_str)

    def save(self, filename=None):
        filename = filename or self.atexit_save
        if not filename:
            raise ValueError('Please specify filename or '
                             'init ProxyList with atexit_save attribute')
        content = self.json_encoder.dumps(tuple(self.active_proxies.values()) +
                                          tuple(self.blacklist_proxies.values()))
        logger.info('Saving proxies status %s %s', filename, self._stats_str)
        with open(filename, 'w') as fh:
            fh.write(content)
