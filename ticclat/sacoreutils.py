"""SQLAlchemy core utility functionality

Functionality for faster bulk inserts without using the ORM.
More info: https://docs.sqlalchemy.org/en/latest/faq/performance.html
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

DBSession = scoped_session(sessionmaker())


def get_engine(user, password, dbname,
               dburl='mysql://{}:{}@localhost/{}?charset=utf8mb4'):
    """Returns an engine that can be used for fast bulk inserts
    """
    engine = create_engine(dburl.format(user, password, dbname), echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)

    return engine


def sql_insert(engine, table_object, to_insert):
    """Insert a list of objects into the database without using a session.

    This is a fast way of (mass) inserting objects. However, because no session
    is used, no relationships can be added automatically. So, use with care!

    Inputs:
        engine: SQLAlchemy created using init_db.
        table_object: object representing a table in the database (i.e., one
            of the objects from ticclat_schema)
        to_insert (list of dicts): list containg dictionary representations of
            the objects (rows) to be inserted
    """

    engine.execute(
        table_object.__table__.insert(),
        [obj for obj in to_insert]
    )
