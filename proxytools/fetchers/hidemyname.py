from lxml import html
from pytimeparse.timeparse import timeparse

from ..proxyfetcher import ConcreteProxyFetcher, Proxy


class HidemyNameProxyFetcher(ConcreteProxyFetcher):
    # NOTE: looks like working ok only from UA ip?
    URL = 'https://hidemy.name/en/proxy-list/'

    ANONYMITY_MAP = {
        'High': Proxy.ANONYMITY.HIGH,
        'Medium': Proxy.ANONYMITY.ANONYMOUS,
        'Low': Proxy.ANONYMITY.ANONYMOUS,
        'No': Proxy.ANONYMITY.TRANSPARENT,
    }

    def __init__(self, *args, pages=None, **kwargs):
        self.pages = pages
        super().__init__(*args, **kwargs)

    def worker(self):
        resp = self.session.get(self.URL)
        resp.raise_for_status()
        doc = html.fromstring(resp.text)

        pages = self.pages or self.parse_pages_count(doc)
        for i in range(1, pages):
            if not self.session.request_wait:
                self.spawn(self.page_worker, i * 64)
            else:
                for p in self.page_worker(i * 64):
                    yield p
        for p in self.parse_proxies(doc):
            yield p

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

            yield Proxy(
                tr[0].text + ':' + tr[1].text, types=types,
                anonymity=self.ANONYMITY_MAP[tr[5].text],
                country=country, success_at=timeparse(tr[6].text.replace('.', ''))
            )
