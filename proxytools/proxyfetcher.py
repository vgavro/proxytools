import logging
from datetime import datetime, timedelta

from .models import Proxy, AbstractProxyProcessor
from .proxychecker import ProxyChecker
from .utils import EntityLoggerAdapter, classproperty, str_to_enum, import_string


logger = logging.getLogger(__name__)


class ProxyFetcher(AbstractProxyProcessor):
    def __init__(self, fetchers='*', checker=None,
                 proxy=None, pool=None, pool_size=None, blacklist=None,
                 **kwargs):
        super().__init__(proxy, pool, pool_size)
        # TODO: implement stop on limit!

        if isinstance(checker, dict):
            checker = ProxyChecker(**checker)
        self.checker = checker
        if self.checker:
            # TODO: NOTE!!! this is not set on lazy add setting
            # implement __setattr__ (or other magic method) for setting
            # proxyfetcher.proxy after initialization
            # to set it also on checker
            self.checker.proxy = self.checked_proxy

        if fetchers == '*':
            fetchers = self.registry
        self.fetchers = []
        fetcher_kwargs = {name: kwargs.pop(name, {}) for name in self.registry}
        for fetcher in fetchers:
            if isinstance(fetcher, str):
                if fetcher in self.registry:
                    fetcher = self.registry[fetcher]
                else:
                    fetcher = self.register(fetcher)
            if isinstance(fetcher, type):
                fetcher = fetcher(**fetcher_kwargs[fetcher.name],
                                  proxy=self.process_proxy,
                                  pool=self.pool, **kwargs)
            else:
                if fetcher_kwargs[fetcher.name]:
                    raise ValueError('{} already initialized'.format(fetcher.name))
                fetcher.pool = self.pool
                fetcher.proxy = self.process_proxy
            self.fetchers.append(fetcher)

    def __call__(self, join=False):
        for fetcher in self.fetchers:
            fetcher()
        if join:
            for fetcher in self.fetchers:
                fetcher.workers.join()
            if self.checker:
                self.checker.workers.join()

    def checked_proxy(self, proxy):
        # proxy after checking
        # TODO: where to filter?
        self.proxy(proxy)

    def process_proxy(self, proxy):
        if proxy.url not in self.blacklist:
            if self.checker:
                self.checker(proxy)
            else:
                self.proxy(proxy)

    @property
    def ready(self):
        return (all(f.ready for f in self.fetchers) and
                (not self.checker or self.checker.ready))

    _registry = None

    @classproperty
    def registry(cls):
        if cls._registry is None:
            # Lazy loading fetchers from package
            from .fetchers import __all__
            cls._registry = {fetcher_cls.name: fetcher_cls
                             for fetcher_cls in __all__}
        return cls._registry

    @classmethod
    def register(cls, fetcher_cls):
        if isinstance(fetcher_cls, str):
            fetcher_cls, fetcher_path = import_string(fetcher_cls), fetcher_cls
        if not issubclass(fetcher_cls, ConcreteProxyFetcher):
            raise TypeError('fetcher_cls must be ConcreteProxyFetcher subclass')
        cls.registry[fetcher_cls.name] = fetcher_cls
        cls.registry[fetcher_path] = fetcher_cls
        return fetcher_cls


class ConcreteProxyFetcher(AbstractProxyProcessor):
    def __init__(self, proxy=None, pool=None, pool_size=None, blacklist=None,
                 types=None, countries=None, anonymities=None, success_delta=None,
                 session=None, proxylist=None, session_params={}):
        super().__init__(proxy, pool, pool_size, blacklist)

        self.types = types and set(str_to_enum(t, Proxy.TYPE) for t in types) or None
        self.countries = countries
        self.anonymities = anonymities
        self.success_delta = success_delta

        self.logger = EntityLoggerAdapter(logger, self.name)
        self.session = session or self.create_session(proxylist, **session_params)

    @classproperty
    def name(cls):
        return cls.__name__.lower().replace('proxyfetcher', '')

    def create_session(self, proxylist, **params):
        # Lazy import requests because of gevent.monkey_patch
        from .requests import ConfigurableSession, ProxyListSession
        params.setdefault('timeout', 10)
        params.setdefault('random_user_agent', True)
        if proxylist:
            params.setdefault('allow_no_proxy', True)
            session = ProxyListSession(proxylist, **params)
        else:
            session = ConfigurableSession(**params)
        return session

    def filter(self, proxy, now=None):
        now = now or datetime.utcnow()
        if self.countries and proxy.country not in self.countries:
            return False
        if self.anonymities and proxy.anonymity not in self.anonymities:
            return False
        if self.types and not proxy.types.intersection(self.types):
            return False
        if (self.success_delta and proxy.success_at < (now - self.success_delta)):
            return False
        return True

    def process_worker(self, worker, *args, **kwargs):
        result = worker(*args, **kwargs)
        if not result:
            return
        now = datetime.utcnow()
        for proxy in worker(*args, **kwargs):
            assert isinstance(proxy, Proxy)

            if isinstance(proxy.success_at, int):
                proxy.success_at = now - timedelta(seconds=proxy.success_at)
            elif isinstance(proxy.success_at, timedelta):
                proxy.success_at = now - proxy.success_at

            if self.filter(proxy, now=now):
                proxy.fetch_at = now
                if proxy.success_at:
                    assert now >= proxy.success_at, ('Proxy success_at in future: {}'
                                                     .format(proxy))
                proxy.fetch_sources.add(self.name)
                self.logger.debug('Fetched: %s', proxy.addr)
                self.process_proxy(proxy)
            else:
                self.logger.debug('Filtered: %s', proxy.addr)
