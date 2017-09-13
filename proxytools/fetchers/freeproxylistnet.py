import re
from datetime import timedelta

from lxml import html

from ..proxyfetcher import ConcreteProxyFetcher, Proxy


class FreeProxyListNet(ConcreteProxyFetcher):
    HTTP_URL = 'https://free-proxy-list.net'
    SOCKS_URL = 'https://www.socks-proxy.net'
    US_URL = 'https://www.us-proxy.org'
    GB_URL = 'https://free-proxy-list.net/uk-proxy.html'

    ANONYMITY_MAP = {
        'elite proxy': Proxy.ANONYMITY.HIGH,
        'anonymous': Proxy.ANONYMITY.ANONYMOUS,
        'transparent': Proxy.ANONYMITY.TRANSPARENT,
    }

    HTTPS_TYPES_MAP = {
        'yes': (Proxy.TYPE.HTTP, Proxy.TYPE.HTTPS),
        'no': (Proxy.TYPE.HTTP,)
    }

    SOCKS_TYPES_MAP = {
        'Socks4': (Proxy.TYPE.SOCKS4,),
        'Socks5': (Proxy.TYPE.SOCKS5,),
    }

    TIME_REGEXPS = (
        re.compile('()()(\d+) seconds? ago'),
        re.compile('()(\d+)() minutes? ago'),
        re.compile('(\d+) hours?()() ago'),
        re.compile('(\d+) hours? (\d+)() minutes? ago'),
    )

    def _parse_time(self, value):
        for regexp in self.TIME_REGEXPS:
            match = regexp.match(value)
            if match:
                h, m, s = (int(x or 0) for x in match.groups())
                return timedelta(hours=h, minutes=m, seconds=s)
        self.logger.warn('Time not matched: %s', value)

    def _parse_country(self, value):
        return value if value != 'Unknown' else None

    def _parse_http_proxy_row(self, tr):
        return Proxy(
            tr[0].text + ':' + tr[1].text,
            types=self.HTTPS_TYPES_MAP[tr[6].text],  # "Https" field
            country=self._parse_country(tr[2].text),
            anonymity=self.ANONYMITY_MAP[tr[4].text],
            success_at=self._parse_time(tr[7].text),
        )

    def _parse_socks_proxy_row(self, tr):
        return Proxy(
            tr[0].text + ':' + tr[1].text,
            types=self.SOCKS_TYPES_MAP[tr[4].text],
            country=self._parse_country(tr[2].text),
            success_at=self._parse_time(tr[7].text),
        )

    def worker(self):
        if not self.countries or 'GB' in self.countries:
            self.spawn(self.page_worker, self.GB_URL, 'http')
        if not self.countries or 'US' in self.countries:
            self.spawn(self.page_worker, self.US_URL, 'http')
        if (not self.types or
           self.types.intersection([Proxy.TYPE.SOCKS4, Proxy.TYPE.SOCKS5])):
            self.spawn(self.page_worker, self.SOCKS_URL, 'socks')
        return self.page_worker(self.HTTP_URL, 'http')

    def page_worker(self, url, proxy_type):
        resp = self.session.get(url)
        resp.raise_for_status()
        doc = html.fromstring(resp.text)

        for tr in doc.cssselect('table#proxylisttable tbody')[0]:
            if proxy_type == 'http':
                yield self._parse_http_proxy_row(tr)
            else:
                yield self._parse_socks_proxy_row(tr)
