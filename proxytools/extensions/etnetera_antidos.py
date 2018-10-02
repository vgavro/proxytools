import logging
import os.path
import json
import atexit
import re

from gevent.pool import Pool

from proxytools.requests import ProxyListSession
from proxytools.superproxy import WSGISuperProxy

logger = logging.getLogger(__name__)

# https://www.etnetera.cz/en/what-we-do/ewa-cdn
# Etnetera Web Accelerator AntiDOS
EWAAD_REGEXP = re.compile('document.cookie="EWAAD=([\d\w]+);')
ewaad_cache = {}


class EwaadSessionMixin:
    def __init__(self, *args, ewaad_urls=None, **kwargs):
        if ewaad_urls:
            self.ewaad_urls = tuple(ewaad_urls) if isinstance(ewaad_urls, list) else ewaad_urls
        else:
            raise ValueError('ewaad_urls required')
        super().__init__(*args, **kwargs)

    def request(self, meth, url, headers=None, **kwargs):
        if not url.startswith(self.ewaad_urls):
            return super().request(meth, url, headers=headers, **kwargs)
        headers = headers or {}
        if 'Cookie' in headers:
            # It will be in conflict with cookies parameter,
            # will not raise exception but request would be malformed
            del headers['Cookie']
        proxy = kwargs.get('proxies', self.proxies).get('https').split('://')[-1]
        ewaad = ewaad_cache.get(proxy)
        if ewaad:
            # logger.debug('EWAAD from cache %s %s', proxy, ewaad)
            kwargs['cookies'] = {'EWAAD': ewaad}
        resp = super().request(meth, url, headers=headers, **kwargs)
        match = EWAAD_REGEXP.search(resp.text)
        if not match:
            return resp
        elif ewaad:
            logger.warn('EWAAD cached failed %s %s', proxy, ewaad)

        ewaad = match.groups()[0]
        # logger.debug('EWAAD resolved %s %s', proxy, ewaad)
        ewaad_cache[proxy] = ewaad
        kwargs['cookies'] = {'EWAAD': ewaad}
        resp = super().request(meth, url, headers=headers, **kwargs)
        match = EWAAD_REGEXP.search(resp.text)
        if match:
            raise RuntimeError('EWAAD not resolved')
        return resp


class ProxyListSession(EwaadSessionMixin, ProxyListSession):
    pass


class WSGISuperProxy(WSGISuperProxy):
    def __init__(self, *args, **kwargs):
        self.ewaad_warm_workers = Pool(kwargs.pop('ewaad_warm_pool_size', None))
        ewaad_cache_filename = kwargs.pop('ewaad_cache_filename', None)
        if ewaad_cache_filename:
            if os.path.exists(ewaad_cache_filename):
                try:
                    ewaad_cache.update(json.loads(open(ewaad_cache_filename).read()))
                except Exception as exc:
                    logger.exception('Loading EWAAD cache failed %s %r',
                                     ewaad_cache_filename, exc)
                else:
                    logger.info('Loaded EWAAD cache %s %d', ewaad_cache_filename,
                                len(ewaad_cache))

            atexit.register(self.ewaad_cache_save, ewaad_cache_filename)

        kwargs['session_cls'] = ProxyListSession
        super().__init__(*args, **kwargs)

    def ewaad_cache_save(self, filename):
        with open(filename, 'w') as fh:
            json.dump(ewaad_cache, fh)
        logger.info('Saved EWAAD cache %s %d', filename, len(ewaad_cache))

    def _status(self):
        missed = len(set(self.proxylist.active_proxies.keys()) -
                     set(ewaad_cache.keys()))
        return {
            **super()._status(),
            'extra': ('EWAAD total=%d missed=%d warm_workers=%d' %
                      (len(ewaad_cache), missed, len(self.ewaad_warm_workers)))
        }

    def _ewaad_warm_worker(self, proxy):
        url = self.session.ewaad_urls
        url = url if isinstance(url, str) else url[0]
        try:
            self.session.get(url, proxy_request_ident='ewaad_warm',
                proxy_persist=proxy, proxy_max_retries=1)
        except Exception as exc:
            logger.error('EWAAD warm error: %r', exc)

    def action_ewaad_warm(self, data):
        for p in (set(self.proxylist.active_proxies.keys()) -
                  set(ewaad_cache.keys())):
            self.ewaad_warm_workers.spawn(self._ewaad_warm_worker, p)
