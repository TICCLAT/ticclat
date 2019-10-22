import json
import os
from timeit import default_timer as timer

import pandas
import sqlalchemy
from flask import jsonify, request

from ticclat.flask_app import raw_queries, queries
from ticclat.flask_app.db import database
from ticclat.flask_app.paradigm_network import paradigm_network
from ticclat.flask_app.plots.blueprint import plots as plots_blueprint
from ticclat.utils import chunk_df


# CORS
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = 'DELETE, GET, POST, PUT'
        headers = request.headers.get('Access-Control-Request-Headers')
        if headers:
            response.headers['Access-Control-Allow-Headers'] = headers
    return response


def init_app(app, session):
    app.after_request(add_cors_headers)

    app.register_blueprint(plots_blueprint, url_prefix='/plots')

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
        return jsonify(database.engine.table_names())


    @app.route('/tables/<table_name>')
    def table_columns(table_name: str):
        table = sqlalchemy.Table(table_name, database.md, autoload=True, autoload_with=database.engine)
        return jsonify({i[0]: str(i[1].type) for i in table.c.items()})


    @app.route('/corpora')
    def corpora():
        query = """
    SELECT corpora.corpus_id, corpora.name, SUM(word_count) AS word_count, COUNT(d.document_id) AS document_count
    FROM corpora LEFT JOIN corpusId_x_documentId cIxdI on corpora.corpus_id = cIxdI.corpus_id
    LEFT JOIN documents d on cIxdI.document_id = d.document_id
    GROUP BY corpora.corpus_id, corpora.name
        """
        df = pandas.read_sql(query, session.connection())
        return jsonify(df.to_dict(orient='record'))


    @app.route("/word_frequency_per_year/<word_name>")
    def word_frequency_per_year(word_name: str):
        corpus_id = request.args.get('corpus_id')
        if corpus_id:
            corpus_id = int(corpus_id)
        query = raw_queries.query_word_frequency_per_year(corpus_id)
        df = pandas.read_sql(query, session.connection(), params={'lookup_word': word_name})
        resp = jsonify(df.to_dict(orient='record'))
        resp.headers['X-QUERY'] = json.dumps(query)
        return resp


    @app.route("/word_frequency_per_corpus/<word_name>")
    def word_frequency_per_corpus(word_name: str):
        query = raw_queries.query_word_frequency_per_corpus()
        df = pandas.read_sql(query, session.connection(), params={'lookup_word': word_name})
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
        connection = session.connection()
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
        query = raw_queries.find_lemmas_for_wordform()
        df = pandas.read_sql(query, session.connection(), params={'lookup_word': word_form})
        df = df.fillna(0)
        return jsonify(df.to_dict(orient='record'))


    @app.route("/morphological_variants_for_lemma/<paradigm_id>")
    def morphological_variants_for_lemma(paradigm_id: int):
        query = raw_queries.find_morphological_variants_for_lemma()
        df = pandas.read_sql(query, session.connection(), params={'paradigm_id': paradigm_id})
        df = df.fillna(0)
        return jsonify(df.to_dict(orient='record'))


    @app.route("/year_range")
    def year_range():
        start, end = queries.get_corpora_year_range(session)

        return jsonify({'start': start, 'end': end})


    @app.route("/regexp_search/<regexp>")
    def regexp_search(regexp: str):
        connection = session.connection()
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
        df = pandas.read_sql(query, session.connection(), params={'X': X, 'Y': Y, 'Z': Z})
        return jsonify(df.to_dict(orient='record'))

    @app.route('/network/<wordform>')
    def _network(wordform: str):
        connection = session.connection()
        return jsonify(paradigm_network(connection, wordform))

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
        connection = session.connection()

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

        df = pandas.read_sql(query, session.connection(), params={'W': W, 'X': X, 'Y': Y, 'Z': Z})

        return jsonify(df.to_dict(orient="record"))
