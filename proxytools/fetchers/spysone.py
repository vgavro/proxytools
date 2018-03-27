import re

from lxml import html
from pytimeparse.timeparse import timeparse
import js2py

from ..proxyfetcher import ConcreteProxyFetcher, Proxy


class SpysOneProxyFetcher(ConcreteProxyFetcher):
    COUNTRIES_URL = 'http://spys.one/en/proxy-by-country/'
    COUNTRY_URL = 'http://spys.one/free-proxy-list/{}/'

    JS_CONTEXT_REGEXP = re.compile('<script type="text/javascript">'
                                   '(eval\(function\(p\,r\,o\,x\,y\,s\).*?)'
                                   '</script>', re.MULTILINE | re.DOTALL)

    ANONYMITY_MAP = {
        'HIA': Proxy.ANONYMITY.HIGH,
        'ANM': Proxy.ANONYMITY.ANONYMOUS,
        'NOA': Proxy.ANONYMITY.TRANSPARENT,
    }

    def create_session(self, *args, **kwargs):
        kwargs.setdefault('retry_response', lambda r: r.status_code == 503)
        kwargs.setdefault('retry_count', 1)
        kwargs.setdefault('request_wait', 1)
        return super().create_session(*args, **kwargs)

    def worker(self):
        resp = self.session.get(self.COUNTRIES_URL)
        resp.raise_for_status()
        doc = html.fromstring(resp.content)
        for a in doc.cssselect('a[href*="/free-proxy-list/"][title*="proxy servers list."]'):
            url = a.attrib['href']
            assert url.endswith('/')
            code = url.split('/')[-2]
            assert (len(code) == 2 and code.upper() == code), 'No country code in {}'.format(url)
            self.spawn(self.country_worker, code)

    def country_worker(self, code):
        # Note - actually we get only last 500 proxies here, but it's ok for now.
        # May improve it later.
        url = self.COUNTRY_URL.format(code)
        resp = self.session.post(url, data={'xpp': 5, 'xf1': 0, 'xf2': 0, 'xf4': 0, 'xf5': 0},
                                 headers={'Referer': url})
        for proxy in self._resp_parser(resp, code):
            yield proxy

    def _resp_parser(self, resp, country):
        resp.raise_for_status()
        match = self.JS_CONTEXT_REGEXP.search(resp.text)
        assert match, 'Proxy page not recognized for {}'.format(country)
        js_context = js2py.EvalJs()
        js_context.execute(match.group(1))

        doc = html.fromstring(resp.content)
        counter = 0
        for selector in ['tr.spy1x', 'tr.spy1xx']:
            for tr in doc.cssselect(selector):
                if len(tr) != 9 or tr[0].text_content() == 'Proxy address:port':
                    continue
                yield self._row_parser(tr, country, js_context)
                counter += 1
        assert counter, 'No proxies found for {}'.format(country)

    def _row_parser(self, tr, country, js_context):
        addr_elem = tr[0][1]
        assert addr_elem.tag == 'font'
        assert addr_elem[0].tag == 'script'
        ip = addr_elem.text
        port = js_context.eval(addr_elem[0].text.replace('document.write', ''))
        port = int(html.fromstring(port).text_content().replace(':', ''))
        try:
            types = [tr[1][0].text_content()]
        except IndexError:
            types = [tr[1].text_content()]
            if 'HTTPS' in types:
                types.append('HTTP')
        anonymity = self.ANONYMITY_MAP[tr[2].text_content().strip()]
        success_at = timeparse(tr[8][0][1].text.strip()[1:-1].replace(' ago', '').strip())
        return Proxy('{}:{}'.format(ip, port), types=types, anonymity=anonymity,
                     country=country, success_at=success_at)
