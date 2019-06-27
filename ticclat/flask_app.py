import json
import os

import pandas
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
app.config.update()

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
    return "Welcome to TICCLAT."


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
        return {"name": corpus.name, "num_documents": len(corpus.corpus_documents)}

    return jsonify(list(map(corpus_repr, session.query(Corpus).all())))


@app.route("/word/<word_name>")
def word(word_name: str):
    connection = engine.connect()
    query = """
SELECT    wordforms.wordform, 
          lexicon_name 
FROM      wordform_links 
LEFT JOIN wordforms 
ON        wordform_links.wordform_to = wordforms.wordform_id 
LEFT JOIN source_x_wordform_link 
ON        wordform_links.wordform_from = source_x_wordform_link.wordform_from 
AND       wordform_links.wordform_to = source_x_wordform_link.wordform_to 
LEFT JOIN lexica 
ON        source_x_wordform_link.lexicon_id = lexica.lexicon_id 
WHERE     wordform_links.wordform_from = 
          ( 
                 SELECT wordform_id 
                 FROM   wordforms 
                 WHERE  wordform = %(lookup_word)s
          )
"""
    df = pandas.read_sql(query, connection, params={'lookup_word': word_name})
    lexicon_variants = df.to_dict(orient='records')

    query = """
SELECT wf2.wordform
FROM wordforms
       LEFT JOIN wordforms AS wf2 ON wordforms.anahash_id = wf2.anahash_id
WHERE wordforms.wordform = %(lookup_word)s
    """
    df = pandas.read_sql(query, connection, params={'lookup_word': word_name})
    anahash_variants = df['wordform'].to_list()

    query = """
SELECT m1.wordform FROM morphological_paradigms AS m1 LEFT JOIN morphological_paradigms AS m2 ON m1.X = m2.X AND m1.Y = m2.Y AND m1.Z = m2.Z AND m1.W = m2.W
    LEFT JOIN wordforms w on m1.wordform_id = w.wordform_id
WHERE m2.wordform_id = (SELECT wordform_id FROM wordforms WHERE wordform = %(lookup_word)s);
"""
    df = pandas.read_sql(query, connection, params={'lookup_word': word_name})
    morph_variants = df['wordform'].to_list()

    return jsonify({
        'lexicon_variants': lexicon_variants,
        'anahash_variants': anahash_variants,
        'morph_variants': morph_variants,
    })


