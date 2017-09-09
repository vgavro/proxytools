import sys
from datetime import datetime
from collections import OrderedDict
import enum

import click

# from .cli import cli
from .models import Proxy, AbstractProxyProcessor
from .proxychecker import ProxyChecker
from .utils import classproperty, import_string


class ProxyFetcher(AbstractProxyProcessor):
    def __init__(self, fetchers, checker=None,
                 proxy=None, pool=None, pool_size=None,
                 **kwargs):
        super().__init__(proxy, pool, pool_size)
        # TODO: implement stop on limit!

        self.checker = checker
        if self.checker:
            # TODO: NOTE!!! this is not set on lazy add setting
            # implement __setattr__ (or other magic method) for setting
            # proxyfetcher.proxy after initialization
            # to set it also on checker
            self.checker.proxy = self.proxy

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
                    raise ValueError(f'{fetcher.name} already initialized')
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

    def process_proxy(self, proxy):
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
    def __init__(self, proxy=None, pool=None, pool_size=10,
                 session=None, types=None, countries=None, anonymities=None,
                 success_delta=None):
        super().__init__(proxy, pool, pool_size)

        self.types = set(isinstance(t, enum.Enum) and t or Proxy.TYPE[t.upper()]
                         for t in types)
        self.countries = countries
        self.anonymities = anonymities
        self.success_delta = success_delta

        self.session = session or self.create_session()

    @classproperty
    def name(cls):
        return cls.__name__.lower().replace('proxyfetcher', '')

    def create_session(self):
        # TODO: create session using current proxylist
        # Lazy import requests because of gevent.monkey_patch
        import requests
        session = requests.Session()
        session.headers['User-Agent'] = ('Mozilla/5.0 (X11; Linux x86_64) '
                                         'AppleWebKit/537.36 (KHTML, like Gecko) '
                                         'Chrome/59.0.3071.86 Safari/537.36')
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
            if self.filter(proxy, now=now):
                proxy.fetch_at = now
                assert proxy.fetch_at > proxy.success_at, f'Proxy success_at in future: {proxy}'
                proxy.fetch_sources.add(self.name)
                self.process_proxy(proxy)


@click.command()
@click.option('-c', '--config', default=None, help='YAML config file.',
              envvar=['PROXYTOOLS_CONFIG'])
@click.option('-o', '--options', default='{}',
              help='YAML config override string (will be merged with file if supplied).')
@click.option('--check/--no-check', default=False,
              help='Run local checker on fetched proxies or not.')
@click.option('-s', '--save', required=True, help='Save(JSON) proxies to file; "-" for stdout.')
@click.pass_context
def main(ctx, config, options, check, save):
    # TODO: move it to .cli module
    from .cli import load_config, configure_logging, gevent_monkey_patch, JSONEncoder
    config = load_config(config, options, 'proxyfetcher')
    configure_logging(config.get('logging', {}))
    gevent_monkey_patch()
    ctx.obj = {}
    ctx.obj['config'] = config
    ctx.obj['json_encoder'] = JSONEncoder(**config.get('json', {}))

    # TODO: move parameters to options
    checker = check and ProxyChecker(http_check=False, https_force_check=True) or None

    proxies = OrderedDict()

    def proxy(proxy):
        if proxy.url in proxies:
            proxies[proxy.url].merge_meta(proxy)
        else:
            proxies[proxy.url] = proxy

    conf = ctx.obj['config'].get('proxyfetcher', {})

    fetchers = conf.pop('fetchers', None)
    if fetchers in ('*', None):
        fetchers = ProxyFetcher.registry

    fetcher = ProxyFetcher(fetchers, checker=checker, proxy=proxy, **conf)

    fetcher(join=True)

    ctx.obj['json_encoder'].dump(tuple(proxies.values()),
                                 (save == '-') and sys.stdout or save)
