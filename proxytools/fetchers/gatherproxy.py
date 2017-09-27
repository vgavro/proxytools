import re
# import json
# from datetime import timedelta

from lxml import html
from pytimeparse.timeparse import timeparse

from ..proxyfetcher import ConcreteProxyFetcher, Proxy
from ..utils import country_name_to_alpha2


class GatherproxyProxyFetcher(ConcreteProxyFetcher):
    HTTP_BY_ANONYMITY_URL = 'http://www.gatherproxy.com/proxylist/anonymity/?t={}'
    SOCKS_COUNTRIES_URL = 'http://www.gatherproxy.com/sockslist/sockslistbycountry'
    SOCKS_COUNTRY_URL = 'http://www.gatherproxy.com/sockslist/country/?c={}'

    ANONYMITY_MAP = {
        'Elite': Proxy.ANONYMITY.HIGH,
        'Anonymous': Proxy.ANONYMITY.ANONYMOUS,
        'Transparent': Proxy.ANONYMITY.TRANSPARENT,
    }

    JS_FUNCTION_ARG_REGEXP = re.compile('.*\(\'(.*)\'\)')

    def __init__(self, *args, pages=None, **kwargs):
        self.pages = pages
        super().__init__(*args, **kwargs)

    def create_session(self, *args, **kwargs):
        kwargs.setdefault('retry_response', lambda r: r.status_code == 403)
        kwargs.setdefault('retry_count', 1)
        kwargs.setdefault('request_wait', 0.1)
        return super().create_session(*args, **kwargs)

    def worker(self):
        for proxy in self.http_proxies_worker():
            yield proxy
        self.socks_proxies_worker()

    def http_proxies_worker(self):
        for anonymity in self.ANONYMITY_MAP.keys():
            url = self.HTTP_BY_ANONYMITY_URL.format(anonymity)

            data = {'Type': anonymity.lower(), 'PageIdx': 1, 'Uptime': 0}
            resp = self.session.post(url, data, headers={'Referer': url})
            for proxy in self.parse_proxies(resp, is_socks=False):
                yield proxy

            doc = html.fromstring(resp.content)
            a = doc.cssselect('div.pagenavi')[0][-1]
            assert a.tag == 'a'
            pages = self.pages and min([int(a.text), self.pages]) or int(a.text)

            for page in range(2, pages + 1):
                data = {'Type': anonymity.lower(), 'PageIdx': page, 'Uptime': 0}
                self.spawn(self.page_worker, url, data, is_socks=False)

    def socks_proxies_worker(self):
        resp = self.session.get(self.SOCKS_COUNTRIES_URL)
        doc = html.fromstring(resp.content)

        countries = []
        for li in doc.cssselect('ul.pc-list')[0]:
            countries.append(li.text_content().split('(')[0].strip())
        assert countries, 'Socks proxy countries not found'

        for country in countries:
            code = country_name_to_alpha2(country)
            if self.countries and code not in self.countries:
                continue
            url = self.SOCKS_COUNTRY_URL.format(country)
            data = {'Country': country}
            self.spawn(self.page_worker, url, data, is_socks=True)

    def page_worker(self, url, data, is_socks):
        resp = self.session.post(url, data, headers={'Referer': url})
        resp.raise_for_status()
        return self.parse_proxies(resp, is_socks)

    def parse_proxies(self, resp, is_socks):
        doc = html.fromstring(resp.content)
        for tr in doc.cssselect('table#tblproxy')[0][2:]:
            success_at = timeparse(tr[0].text.replace(' ago', ''))
            ip = self.JS_FUNCTION_ARG_REGEXP.match(tr[1].text_content()).group(1)
            port = self.JS_FUNCTION_ARG_REGEXP.match(tr[2].text_content()).group(1)
            if is_socks:
                anonymity = None
                country = country_name_to_alpha2(tr[3].text_content())
                types = [tr[5].text]
                if tr[5].text == 'SOCKS4/5':
                    types = ['SOCKS4', 'SOCKS5']
            else:
                port = int(port, 16)  # port obfuscated with HEX for http proxies
                anonymity = self.ANONYMITY_MAP[tr[3].text]
                country = country_name_to_alpha2(tr[4].text)
                types = [Proxy.TYPE.HTTP]
            yield Proxy('{}:{}'.format(ip, port), types=types, anonymity=anonymity,
                        country=country, success_at=success_at)

        # Looks like this obfuscation method is used on get requests only,
        # so we don't need it.

        # PROXY_REGEXP = re.compile('gp\.insertPrx\((\{.*\})\)')
        # for p in self.PROXY_REGEXP.findall(resp.text):
        #     p = json.loads(p)
        #     m, s = [int(x) for x in p['PROXY_LAST_UPDATE'].split()]
        #     success_at = timedelta(minutes=m, seconds=s)
        #     port = int(p['PROXY_PORT'], 16)  # port obfuscated with HEX
        #     yield Proxy('{}:{}'.format(p['PROXY_IP'], port), [Proxy.TYPE.HTTP],
        #                 anonymity=self.ANONYMITY_MAP[p['PROXY_TYPE']],
        #                 country=country_name_to_alpha2(p['PROXY_COUNTRY']),
        #                 success_at=success_at)
