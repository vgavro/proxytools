import re
from datetime import datetime, timedelta
from lxml import html

from ..proxyfetcher import ProxyFetcher, Proxy


class HidemyNameProxyFetcher(ProxyFetcher):
    URL = 'https://hidemy.name/en/proxy-list/'

    ANONYMITY_MAP = {
        'High': Proxy.ANONYMITY.HIGH,
        'Medium': Proxy.ANONYMITY.ANONYMOUS,
        'Low': Proxy.ANONYMITY.ANONYMOUS,
        'No': Proxy.ANONYMITY.TRANSPARENT,
    }

    TIME_REGEXPS = (re.compile('()(\d+) minute?s'),
                    re.compile('(\d+) h\. (\d+) min\.'))

    def __init__(self, *args, pages=None, **kwargs):
        self.pages = pages
        super().__init__(*args, **kwargs)

    def worker(self):
        resp = self.session.get(self.URL)
        resp.raise_for_status()
        doc = html.fromstring(resp.text)

        pages = self.pages or self.parse_pages_count(doc)
        for i in range(1, pages):
            self.spawn(self.page_worker, i * 64)

        return self.parse_proxies(doc)

    def page_worker(self, start):
        resp = self.session.get(self.URL + '?start={}'.format(start))
        resp.raise_for_status()
        return self.parse_proxies(html.fromstring(resp.text))

    def parse_pages_count(self, doc):
        ul = doc.cssselect('div.proxy__pagination ul')[0]
        return int(ul[-1][0].text)

    def parse_proxies(self, doc):
        tbody = doc.cssselect('table.proxy__t tbody')[0]
        for tr in tbody:
            types = [Proxy.TYPE[t.strip()] for t in tr[4].text.upper().split(',')]
            assert types

            _span_cls = tr[2][0][0].attrib['class']
            assert _span_cls.startswith('flag-icon'), _span_cls
            assert _span_cls[-3] == '-', _span_cls
            country = _span_cls[-2:].upper()

            for regexp in self.TIME_REGEXPS:
                match = regexp.match(tr[6].text)
                if match:
                    succeed_at = datetime.utcnow() - timedelta(hours=int(match.group(1) or 0),
                                                               minutes=int(match.group(2)))
                    break
            else:
                raise AssertionError('time not matched:{}'.format(tr[6].text))

            yield Proxy(tr[0].text + ':' + tr[1].text, types=types, succeed_at=succeed_at,
                        country=country, anonymity=self.ANONYMITY_MAP[tr[5].text])
