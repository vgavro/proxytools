class ProxyListError(RuntimeError):
    pass


class InsufficientProxies(ProxyListError):
    pass


class ProxyMaxRetriesExceeded(ProxyListError):
    pass


ProxyListError.cls_map = {
    'InsufficientProxies': InsufficientProxies,
    'ProxyMaxRetriesExceeded': ProxyMaxRetriesExceeded,
}
