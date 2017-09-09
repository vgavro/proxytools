import enum
from collections import OrderedDict
from urllib.parse import urlparse

import gevent.pool

from .utils import from_isoformat, to_isoformat


class Proxy:
    class TYPE(enum.Enum):
        HTTP = 'http'
        HTTPS = 'https'
        SOCKS4 = 'socks4'
        SOCKS5 = 'socks5'

    class ANONYMITY(enum.Enum):
        # NOTE: only for HTTP, others are HIGH in any case
        HIGH = 1
        ANONYMOUS = 2
        TRANSPARENT = 3

    __slots__ = ('url types anonymity country speed fetch_at fetch_sources '
                 'success_at fail_at fail in_use').split()

    def __init__(self, url, types, anonymity=None, country=None, speed=None,
                 fetch_at=None, fetch_sources=None,
                 success_at=None, fail_at=None, fail=0, in_use=0):

        scheme = urlparse(url).scheme
        if not scheme:
            if self.TYPE.HTTP in types or self.TYPE.HTTPS in types:
                url = 'http://' + url
            elif self.TYPE.SOCKS5 in types:
                url = 'socks5://' + url
            elif self.TYPE.SOCKS4 in types:
                url = 'socks4://' + url
            else:
                raise ValueError('Can\'t determine proxy url scheme by types: {}'
                                 .format(types))
        known_schemes = ['http', 'https', 'socks4', 'socks5']
        assert urlparse(url).scheme in known_schemes, f'Unknown scheme: {scheme}'

        if self.TYPE.HTTP not in types:
            anonymity = self.ANONYMITY.HIGH

        (self.url, self.types, self.anonymity, self.country, self.speed,
         self.fetch_at, self.fetch_sources,
         self.success_at, self.fail_at, self.fail, self.in_use) = \
            (url, set(types), anonymity, country, speed,
             fetch_at, fetch_sources and set(fetch_sources) or set(),
             success_at, fail_at, fail, in_use)

    def __hash__(self):
        return self.url

    def __repr__(self):
        attrs = ', '.join('{}={}'.format(k, v) for k, v in self.to_json().items())
        return '<Proxy({})>'.format(attrs)

    __str__ = __repr__

    @property
    def is_checked(self):
        return self.success_at and (not self.fetch_at or (self.fetch_at < self.success_at))

    @property
    def parsed(self):
        return urlparse(self.url)

    def merge_meta(self, proxy):
        # hidester not showing if proxy type also https, for example
        # so we may want to merge this info from more verbose sources
        self.types.update(proxy.types)
        self.fetch_sources.update(proxy.fetch_sources)
        if not self.country:
            self.country = proxy.country
        if not self.anonymity:
            self.anonymity = proxy.anonymity
        if not self.speed:
            self.speed = proxy.speed

    def to_json(self):
        return OrderedDict((
            ('url', self.url),
            ('types', [type_.name for type_ in self.types]),
            ('anonymity', self.anonymity and self.anonymity.name),
            ('country', self.country),
            ('speed', self.speed),
            ('fetch_at', self.fetch_at and to_isoformat(self.fetch_at)),
            ('fetch_sources', tuple(self.fetch_sources)),
            ('success_at', self.success_at and to_isoformat(self.success_at)),
            ('fail_at', self.fail_at and to_isoformat(self.fail_at)),
            ('fail', self.fail),
        ))

    @classmethod
    def from_json(cls, data):
        data = data.copy()
        for key, value in data.items():
            if value and key in ('success_at', 'fetch_at', 'fail_at'):
                data[key] = from_isoformat(value)
            elif value and key == 'fetch_sources':
                data[key] = set(value)
            elif value and key == 'anonymity':
                data[key] = Proxy.ANONYMITY[value]
            elif key == 'types':
                data[key] = set(Proxy.TYPE[type_] for type_ in value)
        return cls(**data)

#    def to_csv(self):
#        # Maybe implement it someday? :-)
#        # It wouldn't be very useful without cross-project loading
#        def to_text(value):
#            if value is None:
#                return ''
#            if isinstance(value, (tuple, list, set)):
#                return ' '.join([to_text(v) for v in value])
#            if isinstance(value, enum.Enum):
#                return value.name
#            if isinstance(value, datetime):
#                to_isoformat(value)
#            return str(value)
#        return [to_text(value) for value in self.to_json().values()]


class AbstractProxyProcessor:
    POOL_SIZE_DEFAULT = 10

    def __init__(self, proxy=None, pool=None, pool_size=None):
        self.pool = pool or gevent.pool.Pool(pool_size or self.POOL_SIZE_DEFAULT)
        # Using separated pool and group to manage workers status on shared pool
        self.workers = gevent.pool.Group()
        if proxy:
            self.proxy = proxy

    def __call__(self, join=False):
        self.spawn(self.worker)
        if join:
            self.workers.join()

    def ready(self):
        """If proxy fetching is finished or not started yet"""
        return not len(self.workers)

    def spawn(self, worker, *args, **kwargs):
        spawned = self.pool.spawn(self.process_worker, worker, *args, **kwargs)
        self.workers.add(spawned)

    def process_worker(self, worker, *args, **kwargs):
        result = worker(*args, **kwargs)
        if result:
            for proxy in result:
                assert isinstance(proxy, Proxy)
                self.process_proxy(proxy)

    def process_proxy(self, proxy):
        self.proxy(proxy)

    def proxy(self, proxy):
        raise NotImplementedError('You must implement this method '
                                  'or pass callback to __init__')

    def worker(self):
        """
        Worker may spawn another workers, and so on.
        Returns None or proxy iterator
        """
        raise NotImplementedError()
