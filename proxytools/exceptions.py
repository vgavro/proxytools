class InsufficientProxies(Exception):
    pass


class ProxyMaxRetriesExceeded(Exception):
    def __init__(self, msg, fail_count=0, rest_count=0):
        # int required for deserealization
        self.msg, self.fail_count, self.rest_count = msg, int(fail_count), int(rest_count)
        super().__init__(msg, fail_count, rest_count)
