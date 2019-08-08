import sqlalchemy
from sqlalchemy.orm import sessionmaker

from ticclat import settings

print(settings.DATABASE_URL)
engine = sqlalchemy.create_engine(settings.DATABASE_URL)
session_factory = sqlalchemy.orm.sessionmaker(bind=engine)
md = sqlalchemy.MetaData()
