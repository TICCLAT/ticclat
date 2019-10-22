import sqlalchemy

from ticclat.flask_app import settings


class Database:
    engine = None
    md = None
    session = None

    def setup(self, engine=None):
        self.engine = engine if engine else sqlalchemy.create_engine(settings.DATABASE_URL)
        self.md = sqlalchemy.MetaData()


database = Database()
