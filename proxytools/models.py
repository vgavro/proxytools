import enum
from urllib.parse import urlparse


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
                 'succeed_at in_use failed_at failed').split()

    def __init__(self, url, types, anonymity=None, country=None, speed=None,
                 fetched_at=None, fetched_sources=set(),
                 succeed_at=None, in_use=0, failed_at=None, failed=0):

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
         self.succeed_at, self.in_use, self.failed_at, self.failed) = \
            (url, types, anonymity, country, speed,
             fetched_at, fetched_sources,
             succeed_at, in_use, failed_at, failed)

    def __hash__(self):
        return self.url

    def __str__(self):
        return '{}({} {})'.format(self.url, self.country, ','.join(self.fetched_sources))

    # TODO
    __repr__ = __str__

    @property
    def local_succeed(self):
        return self.succeed_at and (not self.fetched_at or (self.fetched_at < self.succeed_at))

    @property
    def parsed(self):
        return urlparse(self.url)

    def merge_meta(self, proxy):
        self.fetched_sources.update(proxy.fetched_sources)
        if not self.country:
            self.country = proxy.country
        if not self.anonymity:
            self.anonymity = proxy.anonymity
        if not self.speed:
            self.speed = proxy.speed
