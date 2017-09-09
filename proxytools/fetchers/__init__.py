from importlib import import_module
import os.path

from ..proxyfetcher import ConcreteProxyFetcher
from ..utils import get_subclasses_from_module


__all__ = []


for filename in os.listdir(os.path.dirname(__file__)):
    name, ext = os.path.splitext(filename)
    if name.startswith('_') or ext != '.py':
        continue
    module = import_module('.' + name, __name__)
    __all__.extend(get_subclasses_from_module(module, ConcreteProxyFetcher))
