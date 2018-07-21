import re

# https://www.etnetera.cz/en/what-we-do/ewa-cdn
# Etnetera Web Accelerator AntiDOS
EWA_ANTIDOS_REGEXP = re.compile('document.cookie="EWAAD=([\d\w]+);')
EWA_URLS = (
    'https://www.mzv.cz/',
)

ewaad_cache = {}


def proxy_request_wrapper(request):
    def wrapper(meth, url, headers=None, **kwargs):
        if not url.startswith(EWA_URLS):
            return request(meth, url, headers=headers, **kwargs)
        headers = headers or {}
        if 'Cookie' in headers:
            # It will be in conflict with cookies parameter,
            # will not raise exception but request would be malformed
            del headers['Cookie']
        proxy = kwargs.get('proxies', {}).get('https')
        ewaad = ewaad_cache.get(proxy)
        if ewaad:
            # logger.debug('EWAAD from cache %s %s', proxy, ewaad)
            kwargs['cookies'] = {'EWAAD': ewaad}
        resp = request(meth, url, headers=headers, **kwargs)
        match = EWA_ANTIDOS_REGEXP.search(resp.text)
        if not match:
            return resp
        ewaad = match.groups()[0]
        # logger.debug('EWAAD resolved %s %s', proxy, ewaad)
        ewaad_cache[proxy] = ewaad
        kwargs['cookies'] = {'EWAAD': ewaad}
        resp = request(meth, url, headers=headers, **kwargs)
        match = EWA_ANTIDOS_REGEXP.search(resp.text)
        if match:
            raise RuntimeError('EWAAD not resolved')
        return resp

    return wrapper
