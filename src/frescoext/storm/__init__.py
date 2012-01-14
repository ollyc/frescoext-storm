"""
WSGI service providing per request storm store objects
"""
from functools import wraps
from logging import getLogger
from weakref import WeakKeyDictionary

from fresco.core import context
from pesto import Request
from pesto.wsgiutils import ClosingIterator

logger = getLogger(__name__)

retryable_errors = tuple()
try:
    from psycopg2.extensions import TransactionRollbackError
    retryable_errors += (TransactionRollbackError,)
except ImportError:
    pass

try:
    from storm.database import create_database
    from storm.store import Store
    from storm.exceptions import DatabaseError
except ImportError:
    logger.warn("Storm package not available")

class StorePools(dict):
    """
    Collection of StorePool objects
    """

    def disconnect_all(self):
        """
        Call ``disconnect`` on all configured StorePools.
        """
        for item in self.values():
            item.disconnect()

class StorePool(object):
    """
    Store pool that maintains a single store per request context.
    """
    _pools = {}

    @classmethod
    def create(cls, dsn):
        """
        Factory method to return a StorePool object.

        Will always return the same StorePool object when called with the same
        dsn.
        """
        if dsn in cls._pools:
            return cls._pools[dsn]
        return cls(dsn)

    def __init__(self, dsn):
        self.dsn = dsn
        self.db = create_database(dsn)
        # Keep a handle on all store objects currently assigned
        self._all_stores = WeakKeyDictionary()
        self._context_attr = '_store_%d' % id(self)
        self.__class__._pools[dsn] = self

    def getstore(self):
        try:
            return getattr(context, self._context_attr)
        except AttributeError:
            s = Store(self.db)
            setattr(context, self._context_attr, s)
            self._all_stores[s] = None
            return s

    def disconnect(self):
        """
        Disconnect all stores.

        Any pending transactions will be rolled back and the stores'
        connections closed. Attempts to use objects bound to a store will raise
        an exception.

        Subsequent calls to ``getstore`` will return a fresh store object
        """
        active = list(self._all_stores)
        for s in active:
            del self._all_stores[s]
            s.rollback()
            s.close()
        try:
            delattr(context, self._context_attr)
        except AttributeError:
            pass

    def __repr__(self):
        return "<%s %r, active=%d>" % (self.__class__.__name__,
                                       self.dsn,
                                       len(list(s for s in self.all_stores if hasattr(s, 'store'))))

def getstore(environ=None, database=None, rollback=True):
    """
    Return a storm.store.Store object.

    If called twice with the same environ object, the same store object will be
    returned.

    :param environ: A WSGI environment or the fresco request object
    :param database: Name of database connection to use
    :param rollback: If False, the store will not be auto rolled back.

    """
    if database is None:
        database = default_connection

    if environ is None:
        try:
            environ = context.request.environ
        except AttributeError:
            pass

    elif isinstance(environ, Request):
        environ = environ.environ

    if environ:
        try:
            return environ['fresco.storm.%s' % database]
        except (TypeError, KeyError):
            pass

    store = store_pools[database].getstore()

    if rollback and environ is not None:
        store.rollback()
        environ['fresco.storm.%s' % database] = store
    return store


def autoretry(retry_on=None, database=None, always_commit=False, init_backoff=0.2, max_attempts=5):
    """
    If a TransactionRollbackError occurs due to a concurrent update, this
    decorator will re-run the request.

    :param retry_on: tuple of exception classes to retry on, or None for the default
    :param database: the database store name, as configured through ``add_connection``
    :param always_commit: If true, commit the store unless an exception is
                          raised. The default is to roll back if an error
                          response is returned.
    :param init_backoff: How long (in seconds) to wait before retrying the
                         function in the event of an error. The retry time will
                         be doubled at each unsuccessful attempt. The actual
                         time used is randomized to help avoid conflicts.
    :param max_attempts: How many attempts to make in total before rolling
                         back and re-raising the last error received

    """

    from time import sleep

    retry_on = retry_on or retryable_errors

    def decorator(func):
        @wraps(func)
        def decorated(*args, **kwargs):

            store = getstore(getattr(context, 'request', None), rollback=False, database=database)
            backoff = init_backoff
            attempts = max_attempts

            while True:
                try:
                    result = func(*args, **kwargs)
                    try:
                        successful = 200 <= result.status_code < 400
                    except AttributeError:
                        # Not a Response object?
                        successful = True

                    if successful or always_commit:
                        store.commit()
                    else:
                        store.rollback()
                    return result

                except retry_on as e:
                    store.rollback()
                    logger.warn('autoretry %r; backoff=%g', e, backoff)
                    sleep(random() * backoff * 2)
                    backoff *= 2
                    attempts -= 1
                    if attempts == 0:
                        raise

                except DatabaseError as e:
                    raise

                except Exception:
                    store.rollback()
                    raise

        return decorated
    return decorator

def autocommit(retry=True, retry_on=None, database=None):
    """
    Call store.commit() after a successful call to ``func``, or rollback in
    case of error.
    """

    from pesto import Response
    def decorator(func):
        if retry:
            func = autoretry(retry_on, database)(func)

        @wraps(func)
        def decorated(*args, **kwargs):
            store = getstore(getattr(context, 'request', None), rollback=False, database=database)
            try:
                result = func(*args, **kwargs)
            except Exception:
                store.rollback()
                raise
            if not isinstance(result, Response) or 200 <= result.status_code < 400:
                store.commit()
            return result

        return decorated
    return decorator

def add_connection(name, dsn, default=False):
    """
    Create a named StorePool for the given dsn.

    :param name: name for the connection
    :param dsn: storm connection string (eg 'postgres://user:pw@host/database')
    :param default: set this dsn to be the default connection

    """
    global store_pools, default_connection
    if default:
        default_connection = name
    store_pools[name] = StorePool.create(dsn)

def _remove_all_connections():
    """
    Reset the global state of this module.
    Convenience function for the test suite.
    """
    store_pools.disconnect_all()
    store_pools.clear()
    StorePool._pools.clear()
    default_connection = None

store_pools = StorePools()
