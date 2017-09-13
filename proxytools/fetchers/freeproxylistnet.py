import re
from datetime import timedelta

from lxml import html

from ..proxyfetcher import ConcreteProxyFetcher, Proxy


class FreeProxyListNet(ConcreteProxyFetcher):
    URL = 'https://free-proxy-list.net'

    ANONYMITY_MAP = {
        'elite proxy': Proxy.ANONYMITY.HIGH,
        'anonymous': Proxy.ANONYMITY.ANONYMOUS,
        'transparent': Proxy.ANONYMITY.TRANSPARENT,
    }

    TYPES_MAP = {
        'yes': (Proxy.TYPE.HTTP, Proxy.TYPE.HTTPS),
        'no': (Proxy.TYPE.HTTP,)
    }

    TIME_REGEXPS = (
        re.compile('()()(\d+) seconds? ago'),
        re.compile('()(\d+)() minutes? ago'),
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

    def _parse_proxy_row(self, tr):
        return Proxy(
            tr[0].text + ':' + tr[1].text,
            types=self.TYPES_MAP[tr[6].text],  # "Https" field
            country=self._parse_country(tr[2].text),
            anonymity=self.ANONYMITY_MAP[tr[4].text],
            success_at=self._parse_time(tr[7].text),
        )

    def worker(self):
        resp = self.session.get(self.URL)
        resp.raise_for_status()
        doc = html.fromstring(resp.text)

        for tr in doc.cssselect('table#proxylisttable tbody')[0]:
            yield self._parse_proxy_row(tr)
