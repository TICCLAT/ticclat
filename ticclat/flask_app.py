import json
import os

import pandas
import sqlalchemy
from flask import Flask, jsonify, request
from timeit import default_timer as timer

from flask_sqlalchemy_session import flask_scoped_session

from ticclat import raw_queries, queries, db
from ticclat.plots.blueprint import plots as plots_blueprint
from ticclat.ticclat_schema import Corpus

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

app.register_blueprint(plots_blueprint, url_prefix='/plots')

# DB session
session = flask_scoped_session(db.session_factory, app)

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
    return jsonify(db.engine.table_names())


@app.route('/tables/<table_name>')
def table_columns(table_name: str):
    table = sqlalchemy.Table(table_name, db.md, autoload=True, autoload_with=db.engine)
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
    connection = db.engine.connect()
    query = raw_queries.query_word_frequency_per_year(corpus_id)
    df = pandas.read_sql(query, connection, params={'lookup_word': word_name})
    resp = jsonify(df.to_dict(orient='record'))
    resp.headers['X-QUERY'] = json.dumps(query)
    return resp


@app.route("/word_frequency_per_corpus/<word_name>")
def word_frequency_per_corpus(word_name: str):
    connection = db.engine.connect()
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
    connection = db.engine.connect()
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
    paradigms, md = queries.get_wf_variants(session, word_name,
                                            start_year=start_year,
                                            end_year=end_year)
    return jsonify({'wordform': word_name,
                    'paradigms': paradigms,
                    'metadata': md})


@app.route("/lexica/<word_name>")
def lexica(word_name: str):
    result = queries.get_lexica_data(session, word_name)
    return jsonify(result)


@app.route("/lemmas_for_wordform/<word_form>")
def lemmas_for_wordform(word_form: str):
    connection = db.engine.connect()
    query = raw_queries.find_lemmas_for_wordform()
    df = pandas.read_sql(query, connection, params={'lookup_word': word_form})
    df = df.fillna(0)
    return jsonify(df.to_dict(orient='record'))


@app.route("/morphological_variants_for_lemma/<paradigm_id>")
def morphological_variants_for_lemma(paradigm_id: int):
    connection = db.engine.connect()
    query = raw_queries.find_morphological_variants_for_lemma()
    df = pandas.read_sql(query, connection, params={'paradigm_id': paradigm_id})
    df = df.fillna(0)
    return jsonify(df.to_dict(orient='record'))


@app.route("/year_range")
def year_range():
    start, end = queries.get_corpora_year_range(session)

    return jsonify({'start': start, 'end': end})


@app.route("/regexp_search/<regexp>")
def regexp_search(regexp: str):
    connection = db.engine.connect()
    query = """SELECT SQL_CALC_FOUND_ROWS wordform FROM wordforms wf1 WHERE wf1.wordform REGEXP %(regexp)s LIMIT 500"""
    df = pandas.read_sql(query, connection, params={'regexp': regexp})
    words = df['wordform'].to_list()
    df = pandas.read_sql('SELECT FOUND_ROWS() AS rows', connection)
    return jsonify({
        'total': int(df['rows'][0]),
        'words': words
    })


@app.route("/word_type_codes")
def word_type_codes():
    codes = queries.distinct_word_type_codes(session)

    codes = [c.code for c in codes]

    return jsonify(codes)


@app.route('/paradigm_count')
def _paradigm_count():
    connection = db.engine.connect()
    X = request.args.get('X', None)
    Y = request.args.get('Y', None)
    Z = request.args.get('Z', None)
    query = f"""
SELECT X,Y,Z, COUNT(W) AS num_paradigms FROM morphological_paradigms WHERE 1
{'AND X = %(X)s' if X else ''}
{'AND Y = %(Y)s' if Y else ''}
{'AND Z = %(Z)s' if Z else ''}
GROUP BY X,Y,Z
ORDER BY num_paradigms DESC
"""
    df = pandas.read_sql(query, connection, params={'X': X, 'Y': Y, 'Z': Z})
    return jsonify(df.to_dict(orient='record'))


@app.route("/ticcl_variants/<word_form>")
def ticcl_variants(word_form: str):
    # TODO: get lexicon_id from the frontend (it is now fixed to 7)
    lexicon_id = request.args.get('lexicon_id', 7)
    # TODO: get corpus_id from the frontend (it is now fixed to 2)
    corpus_id = request.args.get('corpus_id', 2)

    return jsonify(queries.get_ticcl_variants(session, word_form, lexicon_id, corpus_id))


@app.route("/suffixes/<suffix_1>")
@app.route("/suffixes/<suffix_1>/<suffix_2>")
def suffixes(suffix_1: str, suffix_2: str = ""):
    min_freq = request.args.get('min_freq', 10)
    start = timer()
    # TODO: refactor regexp_search to not return limited view and use that here
    connection = db.engine.connect()

    # search first suffix
    search_1 = "%" + suffix_1
    query = f"""
SELECT wordform, frequency
FROM wordform_frequency
WHERE frequency > {min_freq} AND wordform LIKE %(search_1)s
"""
    df = pandas.read_sql(query, connection, params={'search_1': search_1})
    words = df['wordform'].to_list()
    freqs = df['frequency'].to_list()

    half_way = timer()

    pairs = []

    # match with second suffix
    for word, freq in zip(words, freqs):
        word_2 = word[:-len(suffix_1)] + suffix_2
        query = f"""
SELECT wordform, frequency
FROM wordform_frequency
WHERE frequency > {min_freq} AND wordform = %(word_2)s
"""
        df = pandas.read_sql(query, connection, params={'word_2': word_2})
        if len(df['wordform']) == 1:
            pairs.append({'word1': word,
                          'word1_freq': int(freq),
                          'word2': word_2,
                          'word2_freq': int(df['frequency'][0])})

    end = timer()

    return jsonify({
        'runtime_seconds': {
            'total': end - start,
            'first_search': half_way - start,
            'second_search': end - half_way
        },
        'pairs': pairs
    })
