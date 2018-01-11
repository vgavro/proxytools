import sys
from collections import OrderedDict
import logging
import logging.config
import http.client

from click import command, option, echo, BadOptionUsage
import coloredlogs
import yaml

from .utils import dict_merge, JSONEncoder, CompositeContains, gevent_monkey_patch


DEFAULT_LOGGING_CONFIG = {
    'coloredlogs': {
        'level': 'info'
    }
}


def configure_logging(config):
    config = config.copy()
    if config.pop('http_debug', False):
        http.client.HTTPConnection.debuglevel = 1
    else:
        http.client.HTTPConnection.debuglevel = 0

    if config.get('coloredlogs'):
        conf = config.pop('coloredlogs').copy()
        conf['field_styles'] = dict_merge(coloredlogs.DEFAULT_FIELD_STYLES,
                                          conf.get('field_styles', {}))
        conf['level_styles'] = dict_merge(coloredlogs.DEFAULT_LEVEL_STYLES,
                                          conf.pop('level_styles', {}))
        coloredlogs.install(**conf)
    else:
        del config['coloredlogs']  # in case 'coloredlogs': null or {}

    config.setdefault('version', 1)
    logging.config.dictConfig(config)


def load_config(config_filename, override_str, override_key,
                root_keys=['logging', 'json', 'proxyfetcher', 'proxychecker', 'superproxy']):
    config = config_filename and yaml.load(open(config_filename)) or {}
    assert isinstance(config, dict), 'Wrong config format'
    override = yaml.load(override_str)
    assert isinstance(config, dict), 'Wrong override format'
    for key, value in override.items():
        if key in root_keys:
            dict_merge(config[key], override[key])
        else:
            dict_merge(config[override_key], override[key])
    return config


_cli_common_options = [
    option('-c', '--config', default=None, envvar=['PROXYTOOLS_CONFIG'],
        help='YAML config file.'),
    option('-o', '--options', default='{}',
        help='YAML config override string (will be merged with file if supplied).'),
    option('-v', '--verbose', is_flag=True,
        help='Show verbose logging.'),
]


def _cli_wrapper(func, config, options, verbose, **kwargs):
    config = load_config(config, options, func.__name__)

    conf = dict_merge(DEFAULT_LOGGING_CONFIG.copy(),
                      config.get('logging', {}))
    if verbose and 'coloredlogs' in conf:
        conf['coloredlogs']['level'] = 'debug'
    configure_logging(conf)

    gevent_monkey_patch()
    return func(config, **kwargs)


def cli(*options):
    def decorator(func):
        def wrapper(**kwargs):
            return _cli_wrapper(func, **kwargs)
        for option_wrapper in reversed(options):
            wrapper = option_wrapper(wrapper)
        for option_wrapper in reversed(_cli_common_options):
            wrapper = option_wrapper(wrapper)
        return command()(wrapper)
    return decorator


@cli(
    option('--list', 'show_list', is_flag=True,
        help='List all registered fetchers.'),
    option('--fetchers',
        help=('Comma-separated fetcher names or import strings. '
              'Use "*" for all registered.')),
    option('--check/--no-check', default=False,
        help='Run local checker on fetched proxies.'),
    option('-p', '--pool', 'pool_size', type=int, default=None,
        help='Pool size (defaults {ProxyFetcher.POOL_SIZE_DEFAULT}).'),
    option('--https-only', is_flag=True,
        help='Fetch only proxies with https support (with socks proxies).'),
    option('--no-socks', is_flag=True,
        help='Exclude socks4/socks5 proxies.'),
    option('--http-check-https', is_flag=True,
        help='Fetch also http proxies, and check it for https.'),
    option('-s', '--save', required=False,
        help='Save(JSON) proxies to file (stdout by default).')
)
def fetcher(config, show_list, fetchers, check, pool_size,
            https_only, http_check_https, no_socks, save):
    from .proxyfetcher import ProxyFetcher
    from .proxychecker import ProxyChecker

    if show_list:
        for fetcher in ProxyFetcher.registry.values():
            echo(fetcher.name +' '+ fetcher.__module__ + ':' + fetcher.__name__)
        return

    proxies = OrderedDict()

    checker = None
    if check:
        conf = config.get('proxyfetcher', {})
        if http_check_https:
            conf['https_force_check'] = True
        if pool_size:
            conf['pool_size'] = pool_size
        blacklist = conf.pop('blacklist', None)
        if not blacklist:
            conf['blacklist'] = proxies
        else:
            # Do not check already checked proxies
            conf['blacklist'] = CompositeContains(blacklist, proxies)

        checker = ProxyChecker(**conf)

    json_encoder = JSONEncoder(**config.get('json', {}))

    def proxy(proxy):
        if proxy.addr in proxies:
            proxies[proxy.addr].merge_meta(proxy)
        else:
            proxies[proxy.addr] = proxy

    conf = config.get('proxyfetcher', {})
    fetchers_ = conf.pop('fetchers', None)
    if fetchers == '*':
        fetchers_ = ProxyFetcher.registry
    elif fetchers:
        fetchers_ = fetchers.split(',')
    if not fetchers:
        raise BadOptionUsage('You should specify fetchers with option or in config.')

    types = set(t.upper() for t in
                conf.pop('types', ['HTTP', 'HTTPS', 'SOCKS4', 'SOCKS5']))
    if https_only and not http_check_https:
        types = set(('HTTPS', 'SOCKS4', 'SOCKS5'))
    if no_socks:
        types = types.difference(['SOCKS4', 'SOCKS5'])
    if not types:
        raise BadOptionUsage('Proxy types appears to be empty. '
                             'Check config and options compability.')
    if pool_size:
        conf['pool_size'] = pool_size

    fetcher = ProxyFetcher(fetchers_, checker=checker, proxy=proxy, types=types, **conf)
    fetcher(join=True)

    http_count, socks_count, sources = 0, 0, {}
    for p in proxies.values():
        if tuple(p.types)[0].name.startswith('HTTP'):
            http_count += 1
        else:
            socks_count += 1
        for source in p.fetch_sources:
            sources.setdefault(source, {'total': 0, 'uniq': 0})
            sources[source]['total'] += 1
            if len(p.fetch_sources) == 1:
                sources[source]['uniq'] += 1
    sources = ', '.join(['{0}:total={1[total]} uniq={1[uniq]}'.format(k, v)
                         for k, v in sources.items()])
    logging.info('Fetched %s proxies (http(s)=%s, socks=%s %s)',
        len(proxies), http_count, socks_count, sources)

    json_encoder.dump(proxies.values(), save or sys.stdout)


@cli(
    option('-l', '--listen', default=None,
        help='Listen host:port (defaults "0.0.0.0:8088").'),
    option('-p', '--pool', 'pool_size', type=int, default=None,
        help='Pool size (defaults "500").'),
    option('-d', '--dozer', is_flag=True,
        help='Enable dozer memory debugger.'),
)
def superproxy(config, listen, pool_size, dozer):
    import signal
    import sys
    # for graceful shutdown with saving proxies on atexit
    signal.signal(signal.SIGTERM, lambda *args: sys.exit(0))
    signal.signal(signal.SIGQUIT, lambda *args: sys.exit(0))

    from gevent.pywsgi import WSGIServer
    from gevent.pool import Pool
    from .superproxy import WSGISuperProxy
    from .proxylist import ProxyList

    # TODO: not properly working if no checker or fetcher specified in config?
    fetcher = config.get('proxyfetcher', {}).copy()
    checker = config.get('proxychecker', {}).copy()

    conf = config.get('superproxy', {})
    proxylist = ProxyList(fetcher=fetcher, checker=checker, **conf.pop('proxylist', {}))

    listen = listen or conf.pop('listen', '0.0.0.0:8088')
    pool_size = pool_size or conf.pop('pool_size', 500)
    dozer = conf.pop('dozer', False)
    iface, port = listen.split(':')
    app = WSGISuperProxy(proxylist, **conf)
    if dozer:
        from dozer import Dozer
        app = Dozer(app)
    server = WSGIServer((iface, int(port)), app, spawn=Pool(pool_size))
    server.serve_forever()
