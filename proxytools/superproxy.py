import logging

from .utils import ResponseMatch


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
    # TODO: proxying only proxy_* kwargs
    'timeout': (int, str),
    'allow_no_proxy': (lambda x: bool(int(x)), lambda x: str(int(x))),
    'proxy_strategy': (lambda x: str(x).upper(), lambda x: str(x).upper()),
    'proxy_max_retries': (int, str),
    'proxy_wait': (lambda x: {'-1': False, '0': True}.get(x, int(x)),
                   lambda x: str({True: 0, False: -1}.get(x, x))),
    'proxy_persist': (str, str),
    'proxy_countries': (lambda x: x.split(','), lambda x: ','.join(x)),
    'proxy_countries_exclude': (lambda x: x.split(','), lambda x: ','.join(x)),
    'proxy_success_response': (ResponseMatch._from_superproxy_header,
                               lambda x: x._to_superproxy_header()),
    'proxy_success_timeout': (int, str),
    'proxy_rest_response': (ResponseMatch._from_superproxy_header,
                            lambda x: x._to_superproxy_header()),
    'proxy_rest_timeout': (int, str),
    'proxy_fail_timeout': (int, str),
}


class WSGISuperProxy:
    def __init__(self, proxylist, **session_kwargs):
        from .requests import ProxyListSession
        self.session = ProxyListSession(proxylist, forgetful_cookies=True,
                                        **session_kwargs)

    def __call__(self, environ, start_response):
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

        try:
            resp = self.session.request(method, url, data=data, headers=headers, **kwargs)
        except BaseException as exc:
            logger.error('%r', exc)
            content = repr(exc).encode('utf-8')
            start_response('500 Internal Server Error',
                           [('Content-Length', str(len(content)))])
            yield content
            return

        headers = []
        # http://docs.python-requests.org/en/master/user/quickstart/#response-headers
        for k in resp.headers:
            if not is_hop_by_hop(k) and k.lower() not in ('content-length',):
                for v in resp.raw.headers.getheaders(k):
                    # Requests merge same header (set-cookie for example), see link above
                    headers.append((k, v))
        start_response(
            '{0.status_code} {0.reason}'.format(resp),
            headers +
            [('Content-Length', str(len(resp.content))),
             ('X-Superproxy-Addr', resp._proxy.addr)]
        )
        yield resp.content
