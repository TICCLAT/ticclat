# -*- coding: utf-8 -*-
"""Contains the create_app function that creates a Flask instance."""

import sqlalchemy
from flask import Flask

from flask_sqlalchemy_session import flask_scoped_session
from sqlalchemy.orm import Session

from ticclat.flask_app.db import database
from ticclat.flask_app.routes import init_app


def create_app(dbsession: Session = None) -> Flask:
    """Create new Flask app

    :param dbsession: optional sqlalchemy Session, if `None`, a new one will be created.
    :return: Flask instance.
    """
    app = Flask(__name__)
    app.config.update()
    if not dbsession:
        # no session given, will setup an engine using DATABASE_URL env var, and create a session_factory from it
        database.setup()
        session_factory = sqlalchemy.orm.sessionmaker(bind=database.engine)
        database.session = flask_scoped_session(session_factory, app)
    else:
        # session given, this is for tests - session is setup in tests/conftest.py.
        database.setup(dbsession.bind.engine)
        database.session = dbsession

    # loads the routes
    init_app(app, database.session)

    return app
