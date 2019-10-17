import json
import os

import pandas
import sqlalchemy
from flask import Flask, jsonify, request
from timeit import default_timer as timer

from flask_sqlalchemy_session import flask_scoped_session

from ticclat import raw_queries, queries, db
from ticclat.utils import chunk_df
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
    query = """
SELECT corpora.corpus_id, corpora.name, SUM(word_count) AS word_count, COUNT(d.document_id) AS document_count
FROM corpora LEFT JOIN corpusId_x_documentId cIxdI on corpora.corpus_id = cIxdI.corpus_id
LEFT JOIN documents d on cIxdI.document_id = d.document_id
GROUP BY corpora.corpus_id, corpora.name
    """
    connection = db.engine.connect()
    df = pandas.read_sql(query, connection)
    return jsonify(df.to_dict(orient='record'))


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


@app.route('/network/<wordform>')
def _network(wordform: str):
    connection = db.engine.connect()
    xyz_df = pandas.read_sql(raw_queries.get_wxyz(), connection, params={'wordform': wordform})
    # select first result (first paradigm for wordform)
    wxyz = xyz_df.iloc[0].to_dict()

    # select the top frequent X values for same Z,Y
    # df = pandas.read_sql(raw_queries.get_frequent_x_for_zy(), connection, params={'Z': xyz['Z'], 'Y': xyz['Y']})
    df = pandas.read_sql(raw_queries.get_min_dist_x_xyz(), connection, params={'Z': wxyz['Z'], 'Y': wxyz['Y'], 'X': wxyz['X']})
    x_values = df['X'].tolist()

    # append itself to the list of X values to query next
    if wxyz['X'] not in x_values:
        x_values.append(wxyz['X'])

    nodes = []
    links = []

    def flatten(nested_list):
        return [item for sublist in nested_list for item in sublist]

    nested_w = [
        pandas.read_sql(
            raw_queries.get_most_frequent_lemmas_for_xyz(),
            connection,
            params={'Z': wxyz['Z'], 'Y': wxyz['Y'], 'X': x, 'limit': 50}
        ).reset_index(drop=True).to_dict(orient='records') for x in x_values
    ]

    W_list = flatten(nested_w)

    for w in W_list:
        nodes.append({
            'id': str(w['wordform_id']),
            'tc_z': wxyz['Z'],
            'tc_y': wxyz['Y'],
            'tc_x': w['X'],
            'tc_w': w['W'],
            'type': 'w',
            'frequency': w['frequency'],
            'wordform': w['wordform']
        })

    for x in x_values:
        w_nodes_for_x = [node for node in nodes if node['tc_x'] == x]
        x_node = {
            'id': f'Z{wxyz["Z"]}Y{wxyz["Y"]}X{x}',
            'tc_z': wxyz['Z'],
            'tc_y': wxyz['Y'],
            'tc_x': x,
            'tc_w': w_nodes_for_x[0]['tc_w'],
            'type': 'x',
            'frequency': sum([node['frequency'] for node in w_nodes_for_x]),
            'wordform': w_nodes_for_x[0]['wordform']
        }
        nodes.append(x_node)

        if len(w_nodes_for_x) == 1:
            nodes.remove(w_nodes_for_x[0])
        else:
            for node in w_nodes_for_x:
                links.append({
                    'source': node['id'],
                    'target': x_node['id'],
                    'id': node['id'] + x_node['id'],
                    'type': 'XW'
                })

    root_node = list(filter(lambda node: node['tc_x'] == wxyz['X'] and node['type'] == 'x', nodes))[0]
    root_node['wordform'] = wordform
    root_node['tc_w'] = wxyz['W']

    other_x_nodes = filter(lambda node: node['type'] == 'x' and node['tc_x'] != wxyz['X'], nodes)

    for node in other_x_nodes:
        links.append({
            'source': node['id'],
            'target': root_node['id'],
            'id': node['id'] + root_node['id'],
            'type': 'XX'
        })

    return jsonify({
        'nodes': nodes,
        'links': links,
    })


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
    df['wordform2'] = df.apply(lambda row: row['wordform'][:-len(suffix_1)] + suffix_2, axis=1)

    half_way = timer()

    pairs = []

    # match with second suffix
    for chunk in chunk_df(df, batch_size=500):
        matches = queries.get_wordform_matches(session, chunk, min_freq)

        result = pandas.merge(chunk, matches, on='wordform2')
        result.columns = ['word1', 'word1_freq', 'word2', 'word2_freq']

        for pair in result.to_dict(orient='record'):
            pairs.append(pair)

    end = timer()

    return jsonify({
        'runtime_seconds': {
            'total': end - start,
            'first_search': half_way - start,
            'second_search': end - half_way
        },
        'num_results': {
            'first_search': df.shape[0],
            'second_search': len(pairs)
        },
        'pairs': pairs
    })


@app.route("/variants_by_wxyz")
def _variants_by_wxyz():
    W = request.args.get('w')
    X = request.args.get('x')
    Y = request.args.get('y')
    Z = request.args.get('z')
    connection = db.engine.connect()

    query = """
SELECT frequency, w.wordform FROM morphological_paradigms
LEFT JOIN wordforms w on morphological_paradigms.wordform_id = w.wordform_id
LEFT JOIN wordform_frequency ON w.wordform_id = wordform_frequency.wordform_id
WHERE morphological_paradigms.W = %(W)s
AND morphological_paradigms.X = %(X)s
AND morphological_paradigms.Y = %(Y)s
AND morphological_paradigms.Z = %(Z)s
AND word_type_code IN ('HCL', 'HCM')
    """

    df = pandas.read_sql(query, connection, params={'W': W, 'X': X, 'Y': Y, 'Z': Z})

    return jsonify(df.to_dict(orient="record"))

@app.route('/word_type_codes')
def _word_type_codes():
    return session.execute("SELECT DISTINCT word_type_code FROM morphological_paradigms")
