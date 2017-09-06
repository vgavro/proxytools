from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import datetime

# TOOD: move it in other place
# from gevent import monkey  # noqa
# monkey.patch_all()  # noqa

import gevent
import gevent.pool

from .models import Proxy
from .utils import classproperty


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
    def __init__(self, fetchers, add=None, pool=None, pool_size=10):
        self.pool = pool or gevent.pool.Pool(pool_size)
        self.add = add or self.add

        for fetcher in fetchers:
            fetcher.pool = self.pool
            fetcher.add = self.add
        self.fetchers = fetchers

    def __call__(self, join=False):
        for fetcher in self.fetchers:
            fetcher()
        if join:
            for fetcher in self.fetchers:
                fetcher.workers.join()

    @property
    def ready(self):
        return all(f.ready for f in self.fetchers)


class ProxyFetcher(AbstractProxyFetcher):
    def __init__(self, add=None, pool=None, pool_size=10, session=None,
                 types=None, countries=None, anonymities=None, succeed_since=None):
        self.pool = pool or gevent.pool.Pool(pool_size)
        self.add = add or self.add
        self.workers = gevent.pool.Group()

        self.types = types
        self.countries = countries
        self.anonymities = anonymities
        # TODO: add succeed_since and add to self.filter

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

    def filter(self, proxy):
        if self.countries and proxy.country not in self.countries:
            return False
        if self.anonymities and proxy.anonymity not in self.anonymities:
            return False
        if self.types and proxy.type not in self.types:
            return False
        return True

    def process_worker(self, worker, *args):
        now = datetime.utcnow()
        for proxy in worker(*args):
            assert isinstance(proxy, Proxy)
            if self.filter(proxy):
                proxy.fetched_at = now
                proxy.fetched_sources.add(self.name)
                self.add(proxy)

    def spawn(self, worker, *args, **kwargs):
        self.workers.add(self.pool.spawn(self.process_worker, worker, *args, **kwargs))

    def worker(self):
        # NOTE: worker may spawn another workers, and so on
        raise NotImplementedError()


def main():
    from .utils import gevent_monkey_patch
    gevent_monkey_patch()
    from .fetchers.hidester import HidesterProxyFetcher
    # from .fetchers.hidemyname import HidemyNameProxyFetcher
    import pickle

    proxies = []

    def add(proxy):
        print(proxy)
        proxies.append(proxy)

    fetcher = MultiProxyFetcher([HidesterProxyFetcher()],  # HidemyNameProxyFetcher()],
                                add=add)
    fetcher(join=True)
    pickle.dump(proxies, open('proxies', 'wb'))
