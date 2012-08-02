"""
WSGI service providing per request storm store objects
"""
import threading
from functools import wraps
from logging import getLogger
from random import random
from weakref import WeakValueDictionary

from fresco.core import context

from storm.database import create_database
from storm.store import Store
from storm.uri import URI
from storm.exceptions import DatabaseError, IntegrityError

logger = getLogger(__name__)

#: Name of the default connection
default_connection = None

#: Default list of errors to retry
retryable_errors = (IntegrityError,)

#: Prefix for key in WSGI environ dict
environ_prefix = 'frescoext.storm.'

try:
    from psycopg2.extensions import TransactionRollbackError
    retryable_errors += (TransactionRollbackError,)
except ImportError:
    pass


class StorePool(object):
    """
    Store pool that maintains a single store per request context.
    """

    def __init__(self):
        self._local = threading.local()
        self._all_stores = WeakValueDictionary()
        self._databases = {}
        self.uris = {}

    def add(self, name, uri):
        if not isinstance(uri, URI):
            uri = URI(uri)
        self.uris.setdefault(name, uri)
        self._databases.setdefault(name, create_database(uri))

    def getstore(self, name, fresh=False):
        try:
            stores = self._local.stores
        except AttributeError:
            stores = self._local.stores = WeakValueDictionary()

        if fresh:
            return self._getstore_fresh(name)

        try:
            return stores[name]
        except KeyError:
            return stores.setdefault(name, self._getstore_fresh(name))

    def _getstore_fresh(self, name):
        """
        Return a fresh store object
        """
        store = Store(self._databases[name])
        self._all_stores[id(store)] = store
        return store

    def disconnect(self):
        """
        Disconnect all stores.

        Any pending transactions will be rolled back and the stores'
        connections closed. Attempts to use objects bound to a store will raise
        an exception.

        Subsequent calls to ``getstore`` will return a fresh store object
        """
        self._local = threading.local()
        for key, store in self._all_stores.items():
            del self._all_stores[key]
            store.rollback()
            store.close()

    def __repr__(self):
        return "<%s %r, active=%d>" % (self.__class__.__name__,
                                       self.dsn,
                                       len(self._all_stores))


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

    else:
        environ = getattr(environ, 'environ', environ)

    if environ is None:
        store = store_pool.getstore(database)
        store.rollback()
        return store

    environ_key = environ_prefix + database

    try:
        return environ[environ_prefix + database]
    except (TypeError, KeyError):
        pass

    for c in context._contexts[context._ident_func()][:-1]:
        request = c.get('request')

        # A request higher up the context stack has already acquired a store.
        # Make sure this subrequest gets a fresh store
        if request is not None and environ_key in request.environ:
            store = store_pool.getstore(database, fresh=True)
            environ[environ_key] = store
            return store

    store = store_pool.getstore(database)
    environ[environ_key] = store
    store.rollback()
    return store


def autoretry(retry_on=None, database=None, always_commit=False,
              _getstore=None, init_backoff=0.2, max_attempts=5):
    """
    If a TransactionRollbackError occurs due to a concurrent update, this
    decorator will re-run the request.

    :param retry_on: tuple of exception classes to retry on, or None for the
                     default.
    :param database: the database store name, as configured through
                     ``add_connection``.
    :param _getstore: a callable returning a store object. overrides
                      ``database``.
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
    if _getstore is None:
        _getstore = lambda: getstore(getattr(context, 'request', None),
                                     rollback=False, database=database)

    def decorator(func):
        @wraps(func)
        def decorated(*args, **kwargs):

            store = _getstore()
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


def autocommit(retry=True, retry_on=None, database=None, _getstore=None):
    """
    Call store.commit() after a successful call to ``func``, or rollback in
    case of error.

    :param retry: If ``True`` conflicting changes will be retired automatically
    :param retry_on: List of exceptions to retry the request on, or None for
                     the defaults
    :param database: database connection name
    :param _getstore: callable returning a store object. overrides
                      ``database``.
    """

    from fresco import Response
    if _getstore is None:
        _getstore = lambda: getstore(getattr(context, 'request', None),
                                     rollback=False, database=database)

    def decorator(func):
        if retry:
            func = autoretry(retry_on, database, _getstore=_getstore)(func)

        @wraps(func)
        def decorated(*args, **kwargs):
            store = _getstore()
            try:
                result = func(*args, **kwargs)
            except Exception:
                store.rollback()
                raise
            if not isinstance(result, Response) \
               or 200 <= result.status_code < 400:
                store.commit()
            else:
                store.rollback()
            return result

        return decorated
    return decorator


def add_connection(name, uri, default=False):
    """
    Create a named StorePool for the given uri.

    :param name: name for the connection
    :param uri: connection uri (eg 'postgres://user:pw@host/database')
    :param default: set this uri to be the default connection

    """
    global store_pools, default_connection
    if default:
        default_connection = name
    store_pool.add(name, uri)


def _remove_all_connections():
    """
    Reset the global state of this module.
    Convenience function for the test suite.
    """
    global default_connection
    store_pool.disconnect()
    default_connection = None

store_pool = StorePool()
