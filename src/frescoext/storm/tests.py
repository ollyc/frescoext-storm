from frescoext.storm import add_connection, getstore, store_pools
from frescoext.storm import _remove_all_connections
from fresco.core import context, Request

from nose.tools import assert_raises
from storm.exceptions import ClosedError


class TestAddConnection(object):

    @classmethod
    def teardown_class(cls):
        _remove_all_connections()

    def test_add_connection_creates_new_connection(self):
        add_connection('mydb', 'sqlite:')
        store = getstore({}, 'mydb')
        assert hasattr(store, 'execute')

    def test_add_connection_sets_default(self):
        add_connection('mydb', 'sqlite:?conn=1')
        add_connection('mydb2', 'sqlite:?conn=2', default=True)
        assert getstore({}).get_database()\
                is getstore({}, 'mydb2').get_database()
        assert getstore({}).get_database()\
                is not getstore({}, 'mydb').get_database()


class TestPoolDisconnect(object):

    def tearDown(self):
        _remove_all_connections()

    def test_disconnect_all_removes_all_connections(self):

        add_connection('mydb', 'sqlite:?conn=1')
        add_connection('mydb2', 'sqlite:?conn=2')

        store1 = getstore({}, 'mydb')
        store2 = getstore({}, 'mydb2')

        # Check stores are alive
        store1.execute("SELECT 1")
        store2.execute("SELECT 1")

        store_pools.disconnect_all()

        assert_raises(ClosedError, store1.execute, "SELECT 1")
        assert_raises(ClosedError, store2.execute, "SELECT 1")

    def test_disconnect_doesnt_affect_other_stores(self):

        add_connection('mydb', 'sqlite:?conn=1')
        add_connection('mydb2', 'sqlite:?conn=2')

        store1 = getstore({}, 'mydb')
        store2 = getstore({}, 'mydb2')

        store_pools['mydb'].disconnect()
        assert_raises(ClosedError, store1.execute, "SELECT 1")
        store2.execute("SELECT 1")


class TestGetStorm(object):

    @classmethod
    def setup_class(cls):
        add_connection('mydb', 'sqlite:?conn=1', default=True)

    @classmethod
    def teardown_class(cls):
        _remove_all_connections()

    def test_getstore_returns_new_store_in_new_context(self):

        saved = context._ident_func

        s1 = getstore()
        # Patch _ident_func to ensue that context.push() creates an insulated
        # request context
        object.__setattr__(context, '_ident_func', lambda: None)
        try:
            context.push(request=Request({}))
            s2 = getstore()
            context.pop()
        finally:
            object.__setattr__(context, '_ident_func', saved)
        s3 = getstore()
        assert s1 is not s2
        assert s1 is s3

    def test_getstore_returns_request_bound_store(self):

        environ = {}
        assert getstore(environ) is environ['frescoext.storm.mydb']
