import logging.config
import http.client

# import click
import coloredlogs
import yaml

from .utils import dict_merge, JSONEncoder, gevent_monkey_patch  # noqa


DEFAULT_LOGGING_CONFIG = {
    'coloredlogs': {
        'level': 'info'
    }
}


def configure_logging(config):
    config = dict_merge(DEFAULT_LOGGING_CONFIG.copy(), config)
    if config.pop('http_debug', False):
        http.client.HTTPConnection.debuglevel = 1
    else:
        http.client.HTTPConnection.debuglevel = 0

    if config.get('coloredlogs'):
        conf = config.pop('coloredlogs').copy()
        conf['field_styles'] = dict_merge(coloredlogs.DEFAULT_FIELD_STYLES,
                                          conf.get('field_styles', {}), copy=True)
        conf['level_styles'] = dict_merge(coloredlogs.DEFAULT_LEVEL_STYLES,
                                          conf.pop('level_styles', {}), copy=True)
        coloredlogs.install(**conf)

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


# TODO: WIP
# @click.group()
# @click.option('-c', '--config', default={}, help='YAML config file.',
#               envvar=['PROXYTOOLS_CONFIG'])
# @click.option('-o', '--options', default={},
#               help='YAML config override string (will be merged with file if supplied).')
# # @click.option('-l', '--load', default={},
# #               help='Load proxies from file (json)')
# # @click.option('-s', '--save', default={},
# #               help='Save proxies to file (json)')
# @click.pass_context
# def cli(ctx, config, override, load, save):
#     config = load_config(config, override, override_key=ctx.invoked_subcommand)
#     configure_logging(config.get('logging', {}))
#     gevent_monkey_patch()
#
#     ctx.obj['config'] = config
#     ctx.obj['json_encoder'] = JSONEncoder(**config.__get('json', {}))
