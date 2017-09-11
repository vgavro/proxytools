from datetime import datetime

from ..proxyfetcher import ConcreteProxyFetcher, Proxy
from ..utils import country_name_to_alpha2


class HidesterProxyFetcher(ConcreteProxyFetcher):
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
            yield Proxy(
                '{IP}:{PORT}'.format(**proxy),
                # NOTE: hidester just doesn't show if it's HTTPS or not
                types=[Proxy.TYPE[proxy['type'].upper()]],
                country=country_name_to_alpha2(proxy['country']),
                anonymity=self.ANONYMITY_MAP[proxy['anonymity']],
                success_at=datetime.utcfromtimestamp(int(proxy['latest_check']))
            )
