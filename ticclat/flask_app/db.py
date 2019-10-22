# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
"""Contains the :class:`Database` class and a 'singleton' instance called `database`."""
import os
from dataclasses import dataclass
import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


@dataclass
class Database:
    """Contains the (sqlalchemy) engine, current session and sqlalchemy metadata.

    Note:
        There should only be one instance (also in this module, called `database`).
    """

    engine: Engine = None
    md: MetaData = None
    session: Session = None

    def setup(self, engine: Engine = None) -> None:
        """Set up the database (engine, md, session attributes).

        If `engine` is not provided, it will be created using the environmental
        variable ``DATABASE_URL``.

        Example::
            DATABASE_URL="mysql://root:password@127.0.0.1:3306/ticclat_test?charset=utf8mb4&local_infile=1"`
        """
        if engine:
            self.engine = engine
        else:
            database_url = os.environ.get("DATABASE_URL", default='').strip()
            self.engine = sqlalchemy.create_engine(database_url)
        self.md = sqlalchemy.MetaData()


# The global database 'singleton'
database: Database = Database()
