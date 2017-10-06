from datetime import datetime

from ..proxyfetcher import ConcreteProxyFetcher, Proxy
from ..utils import country_name_to_alpha2


class HidesterProxyFetcher(ConcreteProxyFetcher):
    WEB_URL = 'https://hidester.com/proxylist/'
    JSON_URL = 'https://hidester.com/proxydata/php/data.php?mykey=csv&gproxy=2'

    ANONYMITY_MAP = {
        'Elite': Proxy.ANONYMITY.HIGH,
        'Anonymous': Proxy.ANONYMITY.ANONYMOUS,
        'Transparent': Proxy.ANONYMITY.TRANSPARENT,
    }

    def worker(self):
        # not needed with last hidester.com fixes
        # resp = self.session.get(self.WEB_URL)  # getting required cookies
        resp = self.session.get(self.JSON_URL, headers={'Referer': self.WEB_URL})
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
