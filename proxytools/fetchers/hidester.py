from datetime import datetime

from ..proxyfetcher import ProxyFetcher, Proxy
from ..utils import get_country_alpha_2_by_name  # noqa


class HidesterProxyFetcher(ProxyFetcher):
    URL = 'https://hidester.com/proxydata/php/data.php?mykey=csv&gproxy=2'
    REFERER = 'https://hidester.com/proxylist/'

    ANONYMITY_MAP = {
        'Elite': Proxy.ANONYMITY.HIGH,
        'Anonymous': Proxy.ANONYMITY.ANONYMOUS,
        'Transparent': Proxy.ANONYMITY.TRANSPARENT,
    }

    def worker(self):
        resp = self.session.get(self.URL, headers={'Referer': self.REFERER})
        resp.raise_for_status()
        for proxy in resp.json():
            types = [Proxy.TYPE[proxy['type'].upper()]]
            if proxy['type'] == 'http':
                # it's better to check it?
                types.append(Proxy.TYPE.HTTPS)

            yield Proxy('{type}://{IP}:{PORT}'.format(**proxy), types=types,
                        country=get_country_alpha_2_by_name(proxy['country']),
                        anonymity=self.ANONYMITY_MAP[proxy['anonymity']],
                        succeed_at=datetime.utcfromtimestamp(int(proxy['latest_check'])))
