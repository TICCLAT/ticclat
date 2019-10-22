# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
"""This is the entrypoint for flask.

Example:
    To run a develop server::

        $ export FLASK_ENV=development FLASK_DEBUG=1
        $ export FLASK_APP=ticclat.flask_app.wsgi:app
        $ export DATABASE_URL="mysql://root:password@127.0.0.1:3306/ticclat_test?charset=utf8mb4&local_infile=1"
        $ flask run

    To run a 'production' server using gunicorn::

        $ gunicorn ticclat.flask_app.wsgi:app --bind 0.0.0.0:8000 --max-requests 100 --workers 2 --timeout 30



"""
from ticclat.flask_app.flask_app import create_app

flask_app = app = create_app()

if __name__ == "__main__":
    app.run(host='0.0.0.0')
