from importlib import import_module
import os.path

from ..proxyfetcher import ProxyFetcher


__all__ = []


for filename in os.listdir(os.path.dirname(__file__)):
    name, ext = os.path.splitext(filename)
    if name.startswith('_') or ext != '.py':
        continue
    module = import_module('.' + name, __name__)
    for attr in dir(module):
        if attr.startswith('_'):
            continue
        obj = getattr(module, attr)
        if issubclass(obj, ProxyFetcher) and obj is not ProxyFetcher:
            __all__.append(obj)
