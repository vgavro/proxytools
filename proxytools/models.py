import enum
from collections import OrderedDict
from urllib.parse import urlparse

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

    __slots__ = ('url types anonymity country speed fetched_at fetched_sources '
                 'succeed_at failed_at failed in_use').split()

    def __init__(self, url, types, anonymity=None, country=None, speed=None,
                 fetched_at=None, fetched_sources=None,
                 succeed_at=None, checked_at=None, failed_at=None, failed=0, in_use=0):
        # Note that checked and succeed are same variable

        if not urlparse(url).scheme:
            if self.TYPE.HTTP in types or self.TYPE.HTTPS in types:
                url = 'http://' + url
            elif self.TYPE.SOCKS5 in types:
                url = 'socks5://' + url
            elif self.TYPE.SOCKS4 in types:
                url = 'socks4://' + url
            else:
                raise ValueError('Can\'t determine proxy url scheme by types: {}'
                                 .format(types))

        if self.TYPE.HTTP not in types:
            anonymity = self.ANONYMITY.HIGH

        (self.url, self.types, self.anonymity, self.country, self.speed,
         self.fetched_at, self.fetched_sources,
         self.succeed_at, self.failed_at, self.failed, self.in_use) = \
            (url, set(types), anonymity, country, speed,
             fetched_at, fetched_sources and set(fetched_sources) or set(),
             succeed_at or checked_at, failed_at, failed, in_use)

    def __hash__(self):
        return self.url

    def __repr__(self):
        attrs = ', '.join('{}={}'.format(k, v) for k, v in self.to_json().items())
        return '<Proxy({})>'.format(attrs)

    __str__ = __repr__

    @property
    def checked_at(self):
        return self.succeed_at

    @checked_at.setter
    def checked_at(self, checked_at):
        self.succeed_at = checked_at

    @property
    def local_succeed(self):
        return self.succeed_at and (not self.fetched_at or (self.fetched_at < self.succeed_at))

    @property
    def parsed(self):
        return urlparse(self.url)

    def merge_meta(self, proxy):
        # hidester not showing if proxy type also https, for example
        # so we may want to merge this info from more verbose sources
        self.types.update(proxy.types)
        self.fetched_sources.update(proxy.fetched_sources)
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
            ('fetched_at', self.fetched_at and to_isoformat(self.fetched_at)),
            ('fetched_sources', tuple(self.fetched_sources)),
            ('succeed_at', self.succeed_at and to_isoformat(self.succeed_at)),
            ('failed_at', self.failed_at and to_isoformat(self.failed_at)),
            ('failed', self.failed),
        ))

    @classmethod
    def from_json(cls, data):
        data = data.copy()
        for key, value in data.items():
            if value and key in ('succeed_at', 'fetched_at', 'failed_at'):
                data[key] = from_isoformat(value)
            elif value and key == 'fetched_sources':
                data[key] = set(value)
            elif value and key == 'anonymity':
                data[key] = Proxy.ANONYMITY[value]
            elif key == 'types':
                data[key] = set(Proxy.TYPES[type_] for type_ in value)
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
