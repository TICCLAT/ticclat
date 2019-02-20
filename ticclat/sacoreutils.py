"""SQLAlchemy core utility functionality

Functionality for faster bulk inserts without using the ORM.
More info: https://docs.sqlalchemy.org/en/latest/faq/performance.html
"""
import os
import tempfile
import json
import scipy

import numpy as np
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from ticclat.ticclat_schema import Wordform, Corpus, Document, TextAttestation
from ticclat.dbutils import session_scope
from ticclat.tokenize import terms_documents_matrix_counters, nltk_tokenize
from ticclat.utils import chunk_df, write_json_lines, read_json_lines, \
    get_temp_file

DBSession = scoped_session(sessionmaker())


def get_engine(user, password, dbname,
               dburl='mysql://{}:{}@localhost/{}?charset=utf8mb4'):
    """Returns an engine that can be used for fast bulk inserts
    """
    engine = create_engine(dburl.format(user, password, dbname), echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)

    return engine


def sql_insert(engine, table_object, to_insert):
    """Insert a list of objects into the database without using a session.

    This is a fast way of (mass) inserting objects. However, because no session
    is used, no relationships can be added automatically. So, use with care!

    Inputs:
        engine: SQLAlchemy created using init_db.
        table_object: object representing a table in the database (i.e., one
            of the objects from ticclat_schema)
        to_insert (list of dicts): list containg dictionary representations of
            the objects (rows) to be inserted
    """

    engine.execute(
        table_object.__table__.insert(),
        [obj for obj in to_insert]
    )


def sql_insert_batches(engine, table_object, iterator, batch_size=10000):
    to_add = []
    for item in iterator:
        to_add.append(item)
        if len(to_add) == batch_size:
            sql_insert(engine, table_object, to_add)
            to_add = []
    sql_insert(engine, table_object, to_add)


def bulk_add_wordforms_core(engine, iterator, batch_size=50000):
    sql_insert_batches(engine, Wordform, iterator, batch_size)


def get_tas(corpus, doc_ids, wf_mapping, p):

    cx = scipy.sparse.coo_matrix(corpus)
    for i, j, v in zip(cx.row, cx.col, cx.data):
        word = p[j]
        freq = v
        yield {'wordform_id': wf_mapping[word],
               'document_id': doc_ids[i],
               'frequency': int(freq)}


def add_corpus_core(session, engine, texts_iterator, corpus_name):
    # get terms/document matrix of corpus and vectorizer containing vocabulary
    print('Tokenizing')
    tokenized_file = get_temp_file()
    write_json_lines(tokenized_file, texts_iterator)

    print('Creating the terms/document matrix')
    documents_iterator = read_json_lines(tokenized_file)
    corpus_m, v = terms_documents_matrix_counters(documents_iterator)

    wfs = pd.DataFrame()
    wfs['wordform'] = v.vocabulary_

    # Prepare the documents to be added to the database
    # FIXME: add proper metadata
    print('Creating document data')
    cx = scipy.sparse.csr_matrix(corpus_m)
    word_counts = cx.sum(axis=1)  # sum the rows

    wc_list = np.array(word_counts).flatten().tolist()

    documents = pd.DataFrame()
    documents['word_count'] = wc_list

    # add other metadata
    documents['pub_year'] = 2019
    documents['language'] = 'nl'

    # Determine which wordforms in the vocabulary need to be added to the
    # database
    num = 10000
    to_add = []

    print('Determine which wordforms need to be added')
    for chunk in chunk_df(wfs, num=num):
        # Find out which wordwordforms are not yet in the database
        wordforms = set(list(chunk['wordform']))

        q = session.query(Wordform)
        result = q.filter(Wordform.wordform.in_(wordforms)).all()

        existing_wfs = set(wf.wordform for wf in result)
        for wf in wordforms.difference(existing_wfs):
            to_add.append({'wordform': wf,
                           'wordform_lowercase': wf.lower()})

    # Create the corpus (in a session) and get the ID
    print('Creating the corpus')
    corpus = Corpus(name=corpus_name)
    session.add(corpus)

    # add the documents using ORM, because we need to link them to the
    # corpus
    print('Adding the documents')
    for doc in documents.to_dict(orient='records'):
        d = Document(**doc)
        d.document_corpora.append(corpus)
    session.flush()

    # Insert the wordforms that need to be added using SQLAlchemy core (much
    # faster than using the ORM)
    print('Adding the wordforms')
    if to_add != []:
        sql_insert(engine, Wordform, to_add)

    print('Prepare adding the text attestations')
    # make a mapping from
    df = pd.DataFrame.from_dict(v.vocabulary_, orient='index')
    df = df.reset_index()

    wf_mapping = {}

    for chunk in chunk_df(df, 50000):
        result = session.query(Wordform) \
                .filter(Wordform.wordform.in_(list(chunk['index']))).all()
        for wf in result:
            wf_mapping[wf.wordform] = wf.wordform_id

    # get doc_ids
    docs = session.query(Document).join(Corpus, Document.document_corpora).order_by(Document.document_id).all()
    doc_ids = [d.document_id for d in docs]

    # reverse mapping from wordform to id in the terms/document matrix
    p = dict(zip(v.vocabulary_.values(), v.vocabulary_.keys()))

    (fd, ta_file) = tempfile.mkstemp()
    os.close(fd)
    write_json_lines(ta_file, get_tas(corpus_m, doc_ids, wf_mapping, p))

    print('Adding the text attestations')
    to_add = []
    for ta in read_json_lines(ta_file):
        to_add.append(ta)
        if len(to_add) == 250000:
            sql_insert(engine, TextAttestation, to_add)
            to_add = []
    sql_insert(engine, TextAttestation, to_add)

    # remove temp data
    os.remove(tokenized_file)
    os.remove(ta_file)
