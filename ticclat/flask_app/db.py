import sqlalchemy
from sqlalchemy.orm import sessionmaker

from ticclat.flask_app import settings

engine = sqlalchemy.create_engine(settings.DATABASE_URL)
session_factory = sqlalchemy.orm.sessionmaker(bind=engine)
md = sqlalchemy.MetaData()
