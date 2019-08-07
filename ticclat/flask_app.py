import json
import os

import pandas
import sqlalchemy
from flask import Flask, jsonify, request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from flask_sqlalchemy_session import flask_scoped_session

from ticclat import settings
from ticclat import raw_queries
from ticclat import queries
from ticclat.ticclat_schema import Corpus

engine = create_engine(settings.DATABASE_URL)
session_factory = sessionmaker(bind=engine)
md = sqlalchemy.MetaData()

app = Flask(__name__)
app.config.update()


# CORS
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = 'DELETE, GET, POST, PUT'
        headers = request.headers.get('Access-Control-Request-Headers')
        if headers:
            response.headers['Access-Control-Allow-Headers'] = headers
    return response


app.after_request(add_cors_headers)


# DB session
session = flask_scoped_session(session_factory, app)

# error handler in production:
if os.environ.get('FLASK_ENV', 'development') == 'production':
    @app.errorhandler(Exception)
    def handle_bad_request(e):
        code = 500
        try:
            code = e.code
        except AttributeError:
            pass
        return jsonify({
            'type': e.__class__.__name__,
            'message': str(e),
        }), code


@app.route('/')
def home():
    route_iterator = app.url_map.iter_rules()
    routes = [str(rule) for rule in route_iterator]
    return jsonify(sorted(routes))


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
        return {"name": corpus.name, "num_documents": len(corpus.corpus_documents), "id": corpus.corpus_id}

    return jsonify(list(map(corpus_repr, session.query(Corpus).all())))


@app.route("/word_frequency_per_year/<word_name>")
def word_frequency_per_year(word_name: str):
    corpus_id = request.args.get('corpus_id')
    if corpus_id:
        corpus_id = int(corpus_id)
    connection = engine.connect()
    query = raw_queries.query_word_frequency_per_year(corpus_id)
    df = pandas.read_sql(query, connection, params={'lookup_word': word_name})
    resp = jsonify(df.to_dict(orient='record'))
    resp.headers['X-QUERY'] = json.dumps(query)
    return resp


@app.route("/word_frequency_per_corpus/<word_name>")
def word_frequency_per_corpus(word_name: str):
    connection = engine.connect()
    query = raw_queries.query_word_frequency_per_corpus()
    df = pandas.read_sql(query, connection, params={'lookup_word': word_name})
    return jsonify(df.to_dict(orient='record'))


@app.route("/word_frequency_per_corpus_per_year/<word_name>")
@app.route("/word_frequency_per_corpus_per_year/<word_name>/<start_year>")
@app.route("/word_frequency_per_corpus_per_year/<word_name>/<start_year>/<end_year>")
@app.route("/word_frequency_per_corpus_per_year/<word_name>/0/<end_year>")
def word_frequency_per_corpus_per_year(word_name: str, start_year=None, end_year=None):
    r, md = queries.wordform_in_corpora_over_time(session, wf=word_name,
                                                  start_year=start_year,
                                                  end_year=end_year)

    return jsonify({'wordform': word_name, 'metadata': md, 'corpora': r})


@app.route("/word/<word_name>")
def word(word_name: str):
    connection = engine.connect()
    query = raw_queries.query_word_links()
    df = pandas.read_sql(query, connection, params={'lookup_word': word_name})
    lexicon_variants = df.to_dict(orient='records')

    query = raw_queries.query_anahash_links()
    df = pandas.read_sql(query, connection, params={'lookup_word': word_name})
    anahash_variants = df['wordform'].to_list()

    query = raw_queries.query_morph_links()
    df = pandas.read_sql(query, connection, params={'lookup_word': word_name})
    morph_variants = df['wordform'].to_list()

    return jsonify({
        'lexicon_variants': lexicon_variants,
        'anahash_variants': anahash_variants,
        'morph_variants': morph_variants,
    })


@app.route("/variants/<word_name>")
@app.route("/variants/<word_name>/<start_year>")
@app.route("/variants/<word_name>/<start_year>/<end_year>")
@app.route("/variants/<word_name>/0/<end_year>")
def variants(word_name: str, start_year=None, end_year=None):
    result = queries.get_wf_variants(session, word_name, start_year=start_year,
                                     end_year=end_year)
    return jsonify({'wordform': word_name,
                    'paradigms': result})


@app.route("/lexica/<word_name>")
def lexica(word_name: str):
    result = queries.get_lexica_data(session, word_name)
    return jsonify(result)


@app.route("/lemmas_for_wordform/<word_form>")
def lemmas_for_wordform(word_form: str):
    connection = engine.connect()
    query = raw_queries.find_lemmas_for_wordform()
    df = pandas.read_sql(query, connection, params={'lookup_word': word_form})
    return jsonify(df.to_dict(orient='record'))


@app.route("/morphological_variants_for_lemma/<paradigm_id>")
def morphological_variants_for_lemma(paradigm_id: int):
    connection = engine.connect()
    query = raw_queries.find_morphological_variants_for_lemma()
    df = pandas.read_sql(query, connection, params={'paradigm_id': paradigm_id})
    return jsonify(df.to_dict(orient='record'))


@app.route("/year_range")
def year_range():
    start, end = queries.get_corpora_year_range(session)

    return jsonify({'start': start, 'end': end})


@app.route("/regexp_search/<regexp>")
def regexp_search(regexp: str):
    connection = engine.connect()
    query = """SELECT SQL_CALC_FOUND_ROWS wordform FROM wordforms wf1 WHERE wf1.wordform REGEXP %(regexp)s LIMIT 500"""
    df = pandas.read_sql(query, connection, params={'regexp': regexp})
    words = df['wordform'].to_list()
    df = pandas.read_sql('SELECT FOUND_ROWS() AS rows', connection)
    return jsonify({
        'total': int(df['rows'][0]),
        'words': words
    })
