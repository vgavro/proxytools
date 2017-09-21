import logging
from urllib.parse import urlparse


logger = logging.getLogger(__name__)


HOP_BY_HOP_HEADERS = frozenset([
    'connection', 'keep-alive', 'proxy-authenticate',
    'proxy-authorization', 'te', 'trailers', 'transfer-encoding',
    'upgrade', 'proxy-connection', 'content-encoding'
])


def is_hop_by_hop(header):
    return header.lower() in HOP_BY_HOP_HEADERS


def reconstruct_url(environ):
    url = environ['PATH_INFO']
    # Fix ;arg=value in url
    if '%3B' in url:
        url, arg = url.split('%3B', 1)
        url = ';'.join([url, arg.replace('%3D', '=')])
    # Stick query string back in
    try:
        return url + '?' + environ['QUERY_STRING']
    except KeyError:
        return url


SUPERPROXY_HEADERS = {
    # decode, encode
    'timeout': (int, str),  # TODO: proxying only proxy_* kwargs
    'proxy_strategy': (lambda x: str(x).upper(), lambda x: str(x).upper()),
    'proxy_max_retries': (int, str),
    'proxy_rest': (int, str),
    'proxy_wait': (int, str),
    'proxy_preserve': (int, str),
    'proxy_countries': (lambda x: x.split(','), lambda x: ','.join(x)),
}


class WSGISuperProxy:
    def __init__(self, proxylist, **session_kwargs):
        from .requests import ProxyListSession
        self.session = ProxyListSession(proxylist, forgetful_cookies=True,
                                        **session_kwargs)

    def __call__(self, environ, start_response):
        # Allow simple proxy from PATH url
        path = environ['PATH_INFO']
        if path.startswith('/http://') or path.startswith('/https://'):
            environ['PATH_INFO'] = path[1:]
            environ['HTTP_HOST'] = urlparse(path[1:]).hostname

        method = environ['REQUEST_METHOD']
        url = reconstruct_url(environ)

        headers = {
            key.title(): value
            for key, value in (
                # This is a hacky way of getting the header names right
                (key[5:].replace('_', '-'), value)
                for key, value in environ.items()
                # Keys that start with HTTP_ are all headers
                if key.startswith('HTTP_') and not key.startswith('HTTP_X_SUPERPROXY_')
            )
            if not is_hop_by_hop(key)
        }
        try:
            headers['Content-Type'] = environ['CONTENT_TYPE']
        except KeyError:
            pass

        try:
            data = environ['wsgi.input'].read(int(environ['CONTENT_LENGTH']))
        except (KeyError, ValueError):
            data = None

        kwargs = {
            key: SUPERPROXY_HEADERS[key][0](value)
            for key, value in (
                (key[18:].lower(), value)
                for key, value in environ.items()
                if key.startswith('HTTP_X_SUPERPROXY_')
            )
            if key in SUPERPROXY_HEADERS
        }

        resp = self.session.request(method, url, data=data, headers=headers, **kwargs)
        start_response(
            '{0.status_code} {0.reason}'.format(resp),
            [(k, v) for k, v in resp.headers.items() if not is_hop_by_hop(k)] +
            [('X-Superproxy-Addr', resp._proxy.addr)]
        )
        return resp.iter_content()
