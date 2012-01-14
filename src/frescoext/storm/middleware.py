from pesto.wsgiutils import ClosingIterator

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
            for item in environ.keys():
                if item[:13] == 'fresco.storm.':
                    environ[item].rollback()

        return ClosingIterator(self.app(environ, start_response),
                               rollback_stores)


