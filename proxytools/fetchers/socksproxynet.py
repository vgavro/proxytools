from ..models import Proxy
from .freeproxylistnet import FreeProxyListNet as _FreeProxyListNet


class SocksProxyNet(_FreeProxyListNet):
    URL = 'https://socks-proxy.net'

    TYPES_MAP = {
        'Socks4': (Proxy.TYPE.SOCKS4,),
        'Socks5': (Proxy.TYPE.SOCKS5,),
    }

    def _parse_proxy_row(self, tr):
        return Proxy(
            tr[0].text + ':' + tr[1].text,
            types=self.TYPES_MAP[tr[4].text],
            country=self._parse_country(tr[2].text),
            success_at=self._parse_time(tr[7].text),
        )
