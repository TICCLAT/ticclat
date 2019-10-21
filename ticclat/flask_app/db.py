import sqlalchemy
from sqlalchemy.orm import sessionmaker

from ticclat.flask_app import settings

engine = None
session_factory = None
md = None


def create_db():
    global engine
    global session_factory
    global md
    engine = sqlalchemy.create_engine(settings.DATABASE_URL)
    session_factory = sqlalchemy.orm.sessionmaker(bind=engine)
    md = sqlalchemy.MetaData()
