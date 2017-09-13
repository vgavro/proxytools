import re
from datetime import timedelta
from shutil import which

from gevent.subprocess import run, PIPE
from lxml import html

from ..proxyfetcher import ConcreteProxyFetcher, Proxy
from ..utils import country_name_to_alpha2


class TorVpnProxyFetcher(ConcreteProxyFetcher):
    ROOT_URL = 'https://www.torvpn.com'
    PROXY_URL = ROOT_URL + '/en/proxy-list'

    TIME_REGEXPS = (
        re.compile('()()(\s+)s'),
        re.compile('()(\d+)()m'),
    )

    def _parse_time(self, value):
        for regexp in self.TIME_REGEXPS:
            match = regexp.match(value)
            if match:
                h, m, s = (int(x or 0) for x in match.groups())
                return timedelta(hours=h, minutes=m, seconds=s)
        self.logger.warn('Time not matched: %s', value)

    def __init__(self, *args, **kwargs):
        self.convert = kwargs.pop('convert', which('convert'))
        self.gocr = kwargs.pop('gocr', which('gocr'))
        super().__init__(*args, **kwargs)

    def worker(self):
        if not self.convert or not self.gocr:
            self.logger.warn('Dependencies not found: convert: %s, gocr: %s',
                             self.convert, self.gocr)
            return
        resp = self.session.get(self.PROXY_URL)
        resp.raise_for_status()
        doc = html.fromstring(resp.text)

        tbody = doc.cssselect('table.table tbody')
        assert len(tbody) == 1, 'Can\'t find proxy table'
        for tr in tbody[0][1:]:
            # skipping first because it's header
            ip_url = tr[1][0].attrib['src']
            port = tr[2].text

            country = tr[3][0].text
            country = country != 'Unknown' and country_name_to_alpha2(country) or None

            types, capabilities = [], tr[5].text_content().strip()
            if 'HTTP' in capabilities:
                types.append(Proxy.TYPE.HTTP)
            if 'CONNECT' in capabilities:
                types.append(Proxy.TYPE.HTTPS)
            if not types:
                # TODO: add socks proxy
                self.logger.warn('Unknown capabilities: %s', capabilities)
                continue
            # assert types, 'Unknown capabilities: {}'.format(capabilities)

            success_at = self._parse_time(tr[9].text)

            self.spawn(self.proxy_worker, ip_url, port, country, types, success_at)

    def proxy_worker(self, ip_url, port, country, types, success_at):
        assert ip_url.startswith('/')
        resp = self.session.get(self.ROOT_URL + ip_url)
        resp.raise_for_status()
        assert resp.headers['content-type'] == 'image/png'
        cmd = run('{} - pbm:- | {} -C "0-9." -'.format(self.convert, self.gocr),
                  stdout=PIPE, input=resp.content, shell=True, check=True)
        # "_" is default for unrecognizable, this is always "7"
        ip = cmd.stdout.strip().decode().replace('_', '7')
        yield Proxy(ip + port, types=types, country=country, success_at=success_at)
