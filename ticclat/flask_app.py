import os

import sqlalchemy
from flask import Flask, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ticclat import settings
from ticclat.ticclat_schema import Corpus

engine = create_engine(settings.DATABASE_URL)
session = sessionmaker(bind=engine)()
md = sqlalchemy.MetaData()

app = Flask(__name__)
app.config.update(

)

# error handler in production:
if os.environ.get('FLASK_ENV', 'development') == 'production':
    @app.errorhandler(Exception)
    def handle_bad_request(e):
        code = 500
        try:
            code = e.code
        except:
            pass
        return jsonify({
            'type': e.__class__.__name__,
            'message': str(e),
        }), code


@app.route('/')
def home():
    return 'Welcome to TICCLAT.'


@app.route('/tables')
def tables():
    return jsonify(engine.table_names())


@app.route('/tables/<table_name>')
def table_columns(table_name: str):
    table = sqlalchemy.Table(table_name, md, autoload=True, autoload_with=engine)
    return jsonify({i[0]: str(i[1].type) for i in table.c.items()})


@app.route('/corpora')
def corpora():
    def corpus_repr(corpus):
        return {
            'name': corpus.name,
            'num_documents': len(corpus.corpus_documents)
        }
    return jsonify(list(map(corpus_repr, session.query(Corpus).all())))