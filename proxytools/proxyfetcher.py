from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import datetime
from collections import OrderedDict
import sys
import os
import json

import gevent
import gevent.pool
import click

from .models import Proxy
from .utils import classproperty, gevent_monkey_patch, import_string


class AbstractProxyFetcher(metaclass=ABCMeta):
    @abstractmethod
    def __call__(self, join=False):
        """Start proxy fetching"""
        pass

    @abstractproperty
    def ready(self):
        """If proxy fetching is finished (or not started yet)"""
        pass

    def add(self, proxy):
        """You must implement this method (or pass it to __init__)"""
        print(proxy)


class MultiProxyFetcher(AbstractProxyFetcher):
    def __init__(self, fetchers, add=None, pool=None, pool_size=20, **kwargs):
        self.pool = pool or gevent.pool.Pool(pool_size)
        self.add = add or self.add

        self.fetchers = []
        fetcher_kwargs = {name: kwargs.pop(name, {}) for name in self.registry}
        for fetcher in fetchers:
            if isinstance(fetcher, str):
                if fetcher in self.registry:
                    fetcher = self.registry[fetcher]
                else:
                    fetcher = self.register(fetcher)
            if isinstance(fetcher, type):
                fetcher = fetcher(**fetcher_kwargs[fetcher.name], add=self.add,
                                  pool=self.pool, **kwargs)
            else:
                if fetcher_kwargs[fetcher.name]:
                    raise ValueError(f'{fetcher.name} already initialized')
                fetcher.pool = self.pool
                fetcher.add = self.add
            self.fetchers.append(fetcher)

    def __call__(self, join=False):
        for fetcher in self.fetchers:
            fetcher()
        if join:
            for fetcher in self.fetchers:
                fetcher.workers.join()

    @property
    def ready(self):
        return all(f.ready for f in self.fetchers)

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
        if not issubclass(fetcher_cls, ProxyFetcher):
            raise TypeError('fetcher_cls must be ProxyFetcher subclass')
        cls.registry[fetcher_cls.name] = fetcher_cls
        cls.registry[fetcher_path] = fetcher_cls
        return fetcher_cls


class ProxyFetcher(AbstractProxyFetcher):
    def __init__(self, add=None, pool=None, pool_size=10, session=None,
                 types=None, countries=None, anonymities=None, checked_delta=None):
        self.pool = pool or gevent.pool.Pool(pool_size)
        self.add = add or self.add
        self.workers = gevent.pool.Group()

        self.types = types
        self.countries = countries
        self.anonymities = anonymities
        self.checked_delta = checked_delta

        self.session = session or self.create_session()

    def __call__(self, join=False):
        self.spawn(self.worker)
        if join:
            self.workers.join()

    @property
    def ready(self):
        return not len(self.workers)

    @classproperty
    def name(cls):
        return cls.__name__.lower().replace('proxyfetcher', '')

    def create_session(self):
        # TODO: create session using current proxylist
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
        if self.types and proxy.type not in self.types:
            return False
        if (self.checked_delta and proxy.checked_at < (now - self.checked_delta)):
            return False
        return True

    def process_worker(self, worker, *args, **kwargs):
        now = datetime.utcnow()
        for proxy in worker(*args, **kwargs):
            assert isinstance(proxy, Proxy)
            if self.filter(proxy, now=now):
                proxy.fetched_at = now
                proxy.fetched_sources.add(self.name)
                proxy.types = set(proxy.types)
                self.add(proxy)

    def spawn(self, worker, *args, **kwargs):
        self.workers.add(self.pool.spawn(self.process_worker, worker, *args, **kwargs))

    def worker(self):
        # NOTE: worker may spawn another workers, and so on
        raise NotImplementedError()


@click.command()
@click.option('-c', '--config', default=None, help='YAML config file.',
              envvar=['PROXYFETCHER_CONFIG', 'PROXYTOOLS_CONFIG'])
@click.option('-o', '--override', default=None,
              help='YAML config override string (will be merged with file if supplied).')
@click.option('-s', '--save', default=None, help='Save proxies to file (JSON).')
@click.option('-p', '--print', is_flag=True, help='Print proxies to stdout.')
def main(config, override, save, print):
    if not any((save, print)):
        raise click.BadArgumentUsage('You must supply --save or --print arguments.')
    gevent_monkey_patch()

    proxies = OrderedDict()

    def add(proxy):
        if print:
            sys.stdout.write(str(proxy) + os.linesep)
            sys.stdout.flush()
        if proxy.url in proxies:
            proxies[proxy.url].merge_meta(proxy)
        else:
            proxies[proxy.url] = proxy

    fetcher = MultiProxyFetcher(MultiProxyFetcher.registry, add=add)
    fetcher(join=True)
    if save:
        with open(save, 'w') as fh:
            json.dump([p.to_json() for p in proxies.values()], fh,
                      indent=2, separators=(',', ': '))
