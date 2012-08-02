from frescoext.storm import add_connection, getstore, store_pool
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

    def test_disconnect_removes_all_connections(self):

        add_connection('mydb', 'sqlite:?conn=1')
        add_connection('mydb2', 'sqlite:?conn=2')

        store1 = getstore({}, 'mydb')
        store2 = getstore({}, 'mydb2')

        # Check stores are alive
        store1.execute("SELECT 1")
        store2.execute("SELECT 1")

        store_pool.disconnect()

        assert_raises(ClosedError, store1.execute, "SELECT 1")
        assert_raises(ClosedError, store2.execute, "SELECT 1")


class TestGetStorm(object):

    @classmethod
    def setup_class(cls):
        add_connection('mydb', 'sqlite:?conn=1', default=True)

    @classmethod
    def teardown_class(cls):
        _remove_all_connections()

    def test_getstore_returns_new_store_in_new_context(self):

        context.push(request=Request({}))
        s1 = getstore()
        context.push(request=Request({}))
        s2 = getstore()
        context.pop()
        s3 = getstore()
        context.pop()
        assert s1 is not s2
        assert s1 is s3

    def test_getstore_returns_request_bound_store(self):

        environ = {}
        assert getstore(environ) is environ['frescoext.storm.mydb']
