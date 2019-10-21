from testcontainers.mysql import MySqlContainer
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ticclat.flask_app.flask_app import app
from ticclat.ticclat_schema import Base
import pytest


@pytest.fixture(scope='session')
def engine():
    mysql = MySqlContainer('mysql:5.7')

    mysql.start()
    connection_url = mysql.get_connection_url() + '?charset=utf8mb4&local_infile=1'
    yield create_engine(connection_url)
    mysql.stop()


@pytest.yield_fixture()
def tables(engine):
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


@pytest.yield_fixture
def dbsession(engine, tables):
    """Returns an sqlalchemy session, and after the test tears down everything
    properly.
    """
    connection = engine.connect()
    # begin the nested transaction
    transaction = connection.begin()
    # use the connection with the already started transaction
    session = Session(bind=connection)

    yield session

    session.close()
    # roll back the broader transaction
    transaction.rollback()
    # put back the connection to the connection pool
    connection.close()


@pytest.fixture(scope='module')
def flask_test_client():
    flask_app = app

    # Flask provides a way to test your application by exposing the Werkzeug test Client
    # and handling the context locals for you.
    test_client = flask_app.test_client()

    # Establish an application context before running the tests.
    ctx = flask_app.app_context()
    ctx.push()

    yield test_client  # this is where the testing happens!

    ctx.pop()
