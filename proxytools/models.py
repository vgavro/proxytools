import enum
from collections import OrderedDict

import gevent.pool

from .utils import from_isoformat, to_isoformat, str_to_enum


class TYPE(enum.Enum):
    HTTP = 'http'
    HTTPS = 'https'  # "CONNECT" tunneling
    SOCKS4 = 'socks4'
    SOCKS5 = 'socks5'


SOCKS_TYPES = set([TYPE.SOCKS4, TYPE.SOCKS5])
HTTP_TYPES = set([TYPE.HTTP, TYPE.HTTPS])


class ANONYMITY(enum.Enum):
    # NOTE: only for HTTP, others are HIGH in any case
    HIGH = 1
    ANONYMOUS = 2
    TRANSPARENT = 3


class PROXY_RESULT_TYPE(enum.Enum):
    # For Proxy.history
    FAIL = 0
    SUCCESS = 1
    REST = 2


class Proxy:
    TYPE = TYPE
    ANONYMITY = ANONYMITY

    __slots__ = ('addr types anonymity country speed fetch_at fetch_sources '
                 'success_at fail_at fail in_use rest_till blacklist history').split()

    def __init__(self, addr, types, anonymity=None, country=None, speed=None,
                 fetch_at=None, fetch_sources=None,
                 success_at=None, fail_at=None, fail=0, in_use=0, rest_till=None,
                 blacklist=False, history=None):

        types = set(str_to_enum(t, TYPE) for t in types)
        if types.intersection(SOCKS_TYPES):
            assert not types.intersection(HTTP_TYPES), 'Proxy incompatible types: ' + addr

        if TYPE.HTTP not in types:
            anonymity = ANONYMITY.HIGH
        else:
            anonymity = anonymity and str_to_enum(anonymity, ANONYMITY) or None

        (self.addr, self.types, self.anonymity, self.country, self.speed,
         self.fetch_at, self.fetch_sources, self.success_at, self.fail_at,
         self.fail, self.in_use, self.rest_till, self.blacklist, self.history) = \
            (addr, set(types), anonymity, country, speed,
             fetch_at, fetch_sources and set(fetch_sources) or set(),
             success_at, fail_at, fail, in_use, rest_till, blacklist, history)

    def __hash__(self):
        return self.addr

    def __repr__(self):
        attrs = ', '.join('{}={}'.format(k, v) for k, v in self.to_json().items())
        return '<Proxy({})>'.format(attrs)

    __str__ = __repr__

    @property
    def url(self):
        """Returns http URL for http/https, highest protocol for socks"""
        if TYPE.SOCKS5 in self.types:
            return 'socks5://' + self.addr
        if TYPE.SOCKS4 in self.types:
            return 'socks4://' + self.addr
        return 'http://' + self.addr

    def get_url(self, type_, rdns=None, ignore_types=False):
        if rdns is not None:
            raise NotImplemented('Should return socks4a (if rdns and supported) '
                                 'or socks5h (if rnds=True)')
        type_ = str_to_enum(type_, TYPE)
        if not ignore_types and type_ not in self.types:
            raise ValueError('Proxy {self.addr} has no type {type_}')
        if ignore_types:
            return type_.value + '://' + self.addr
        if type_ in SOCKS_TYPES:
            return TYPE.SOCKS5.value + '://' + self.addr
        return 'http://' + self.addr

    @property
    def is_checked(self):
        # is checked locally
        return self.success_at and (not self.fetch_at or (self.fetch_at < self.success_at))

    @property
    def used_at(self):
        # NOTE: we're not considering rest, because we're not storing rest_since
        if self.fail_at and (not self.success_at or self.fail_at >= self.success_at):
            return self.fail_at
        return self.success_at or self.fail_at

    def set_rest_till(self, rest_till):
        if not self.rest_till or self.rest_till < rest_till:
            self.rest_till = rest_till

    def set_history(self, time, result_type, reason, request_ident, max_history):
        self.history = ([[time, result_type, reason, request_ident]] +
                        (self.history or []))[:max_history]

    def merge_meta(self, proxy):
        # hidester not showing if proxy type also https, for example
        # so we may want to merge this info from more verbose sources
        if self.types.intersection(HTTP_TYPES):
            if proxy.types.intersection(SOCKS_TYPES):
                return
        elif self.types.intersection(SOCKS_TYPES):
            if proxy.types.intersection(HTTP_TYPES):
                return

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
            ('addr', self.addr),
            ('types', [type_.name for type_ in self.types]),
            ('anonymity', self.anonymity and self.anonymity.name),
            ('country', self.country),
            ('speed', self.speed),
            ('fetch_at', self.fetch_at and to_isoformat(self.fetch_at)),
            ('fetch_sources', tuple(self.fetch_sources)),
            ('success_at', self.success_at and to_isoformat(self.success_at)),
            ('fail_at', self.fail_at and to_isoformat(self.fail_at)),
            ('fail', self.fail),
            ('rest_till', self.rest_till and to_isoformat(self.rest_till)),
            ('blacklist', self.blacklist),
            ('history', self.history and [(to_isoformat(h[0]), h[1].name, *h[2:])
                                          for h in self.history] or None),
        ))

    @classmethod
    def from_json(cls, data):
        data = data.copy()
        for key, value in data.items():
            if value and key in ('success_at', 'fetch_at', 'fail_at', 'rest_till'):
                data[key] = from_isoformat(value)
            elif value and key == 'fetch_sources':
                data[key] = set(value)
            elif value and key == 'anonymity':
                data[key] = ANONYMITY[value.upper()]
            elif key == 'types':
                data[key] = set(TYPE[type_.upper()] for type_ in value)
            elif value and key == 'history':
                data[key] = [(from_isoformat(h[0]), PROXY_RESULT_TYPE[h[1]], *h[2:])
                             for h in value]

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

    def __init__(self, proxy=None, pool=None, pool_size=None, blacklist=None):
        self.pool = pool or gevent.pool.Pool(pool_size or self.POOL_SIZE_DEFAULT)
        # Using separated pool and group to manage workers status on shared pool
        self.workers = gevent.pool.Group()
        if proxy:
            self.proxy = proxy
        self.blacklist = blacklist is not None and blacklist or {}

    def __call__(self, join=False):
        self.spawn(self.worker)
        if join:
            self.workers.join()

    @property
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
        if proxy.addr not in self.blacklist:
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
