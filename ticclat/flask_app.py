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

print(settings.DATABASE_URL)
engine = create_engine(settings.DATABASE_URL)
session_factory = sessionmaker(bind=engine)
md = sqlalchemy.MetaData()

app = Flask(__name__)
app.config.update()

session = flask_scoped_session(session_factory, app)

# error handler in production:
if os.environ.get("FLASK_ENV", "development") == "production":

    @app.errorhandler(Exception)
    def handle_bad_request(e):
        code = 500
        try:
            code = e.code
        except Exception as e:
            pass
        return jsonify({"type": e.__class__.__name__, "message": str(e)}), code


@app.route("/")
def home():
    route_iterator = app.url_map.iter_rules()
    routes = [str(rule) for rule in route_iterator]
    return jsonify(sorted(routes))


@app.route("/tables")
def tables():
    return jsonify(engine.table_names())


@app.route("/tables/<table_name>")
def table_columns(table_name: str):
    table = sqlalchemy.Table(table_name, md, autoload=True, autoload_with=engine)
    return jsonify({i[0]: str(i[1].type) for i in table.c.items()})


@app.route("/corpora")
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
def word_frequency_per_corpus_per_year(word_name: str):
    r = queries.wordform_in_corpora_over_time(session, wf=word_name)
    r = r.dropna(subset=['pub_year'])
    r['normalized_term_frequency'] = r['term_frequency'] / r['num_words'] * 100.0

    # get domain and range
    min_year = r['pub_year'].min()
    max_year = r['pub_year'].max()

    min_freq = r['normalized_term_frequency'].min()
    max_freq = r['normalized_term_frequency'].max()

    md = {
        'min_year': min_year,
        'max_year': max_year,
        'min_freq': min_freq,
        'max_freq': max_freq
    }

    # create result
    result = []
    for name, data in r.groupby('name'):
        corpus_data = {'name': name, 'data': []}
        for row in data.iterrows():
            corpus_data['data'].append(
                {'year': row[1]['pub_year'], 
                 'freq': row[1]['normalized_term_frequency']})
        result.append(corpus_data)
    
    return jsonify({'metadata': md, 'data': result})


@app.route("/word/<word_name>")
def word(word_name: str):
    connection = engine.connect()
    query =raw_queries.query_word_links()
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


