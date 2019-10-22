import sqlalchemy

from ticclat.flask_app import settings


class Database:
    engine = None
    session_factory = None
    md = None

    def setup(self, engine=None):
        self.engine = engine if engine else sqlalchemy.create_engine(settings.DATABASE_URL)
        self.md = sqlalchemy.MetaData()


database = Database()
