from pesto.wsgiutils import ClosingIterator
from . import environ_prefix

__all__ = 'AutoRollbackMiddleware',

class AutoRollbackMiddleware(object):
    """
    Middleware that rolls back any uncommitted transactions at the end of every
    request
    """

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        def rollback_stores():
            l = len(environ_prefix)
            for item in list(environ.keys()):
                if item[:l] == environ_prefix:
                    environ[item].rollback()
                    del environ[item]

        return ClosingIterator(self.app(environ, start_response),
                               rollback_stores)


