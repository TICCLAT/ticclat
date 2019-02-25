"""SQLAlchemy core utility functionality

Functionality for faster bulk inserts without using the ORM.
More info: https://docs.sqlalchemy.org/en/latest/faq/performance.html
"""
import os
import logging
import json
import scipy

import numpy as np
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.sql import select
from sqlalchemy.orm import scoped_session, sessionmaker

from ticclat.ticclat_schema import Wordform, Corpus, Document, \
    TextAttestation, corpusId_x_documentId
from ticclat.tokenize import terms_documents_matrix_counters
from ticclat.utils import chunk_df, write_json_lines, read_json_lines, \
    get_temp_file

logger = logging.getLogger(__name__)

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
        engine: SQLAlchemy engine or session
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
    # Doing the insert with an empty list results in adding a row with all
    # fields to the deafult values, or an error, if fields don't have a default
    # value. Se, we have to check whether to_add is empty.
    if to_add != []:
        sql_insert(engine, table_object, to_add)


def bulk_add_wordforms_core(engine, iterator, batch_size=50000):
    sql_insert_batches(engine, Wordform, iterator, batch_size)


def bulk_add_textattestations_core(engine, iterator, batch_size=50000):
    sql_insert_batches(engine, TextAttestation, iterator, batch_size)


def select_wordforms(session, iterator):
    for chunk in iterator:
        # Find out which wordwordforms are not yet in the database
        wordforms = list(chunk['wordform'])
        s = select([Wordform]).where(Wordform.wordform.in_(wordforms))
        result = session.execute(s).fetchall()

        # wf: (id, wordform, anahash_id, wordform_lowercase)
        for wf in result:
            yield {'wordform_id': wf[0],
                   'wordform': wf[1]}


def get_tas(corpus, doc_ids, wf_mapping, p):

    cx = scipy.sparse.coo_matrix(corpus)
    for i, j, v in zip(cx.row, cx.col, cx.data):
        word = p[j]
        freq = v
        yield {'wordform_id': wf_mapping[word],
               'document_id': doc_ids[i],
               'frequency': int(freq)}


def add_corpus_core(session, texts_iterator, corpus_name):
    logger.info('Tokenizing')
    tokenized_file = get_temp_file()
    write_json_lines(tokenized_file, texts_iterator)

    logger.info('Creating the terms/document matrix')
    documents_iterator = read_json_lines(tokenized_file)
    corpus_m, v = terms_documents_matrix_counters(documents_iterator)

    wfs = pd.DataFrame()
    wfs['wordform'] = v.vocabulary_

    # Prepare the documents to be added to the database
    # FIXME: add proper metadata
    logger.info('Creating document data')
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
    num = 50000

    logger.info('Determine which wordforms need to be added')
    wf_to_add_file = get_temp_file()
    with open(wf_to_add_file, 'w') as f:
        for chunk in chunk_df(wfs, num=num):
            # Find out which wordwordforms are not yet in the database
            wordforms = set(list(chunk['wordform']))
            s = select([Wordform]).where(Wordform.wordform.in_(wordforms))
            result = session.execute(s).fetchall()

            # wf: (id, wordform, anahash_id, wordform_lowercase)
            existing_wfs = set([wf[1] for wf in result])
            for wf in wordforms.difference(existing_wfs):
                f.write(json.dumps({'wordform': wf,
                                    'wordform_lowercase': wf.lower()}))
                f.write('\n')

    # Create the corpus (in a session) and get the ID
    logger.info('Creating the corpus')
    corpus = Corpus(name=corpus_name)
    session.add(corpus)

    # add the documents using ORM, because we need to link them to the
    # corpus
    logger.info('Adding the documents')
    for doc in documents.to_dict(orient='records'):
        d = Document(**doc)
        d.document_corpora.append(corpus)
    session.flush()
    corpus_id = corpus.corpus_id

    # Insert the wordforms that need to be added using SQLAlchemy core (much
    # faster than using the ORM)
    logger.info('Adding the wordforms')
    bulk_add_wordforms_core(session, read_json_lines(wf_to_add_file))

    logger.info('Prepare adding the text attestations')
    # make a mapping from
    df = pd.DataFrame.from_dict(v.vocabulary_, orient='index')
    df = df.reset_index()

    logger.info('\tGetting the wordform ids')
    wf_mapping = {}

    for chunk in chunk_df(df, 50000):
        to_select = list(chunk['index'])
        s = select([Wordform]).where(Wordform.wordform.in_(to_select))
        result = session.execute(s).fetchall()
        for wf in result:
            # wf: (id, wordform, anahash_id, wordform_lowercase)
            wf_mapping[wf[1]] = wf[0]

    logger.info('\tGetting the document ids')
    # get doc_ids
    s = select([corpusId_x_documentId.join(Corpus).join(Document)]) \
        .where(Corpus.corpus_id == corpus_id).order_by(Document.document_id)
    r = session.execute(s).fetchall()
    # row: (corpus_id, document_id, ...)
    doc_ids = [row[1] for row in r]

    logger.info('\tReversing the mapping')
    # reverse mapping from wordform to id in the terms/document matrix
    p = dict(zip(v.vocabulary_.values(), v.vocabulary_.keys()))

    logger.info('\tGetting the text attestations')
    ta_file = get_temp_file()
    write_json_lines(ta_file, get_tas(corpus_m, doc_ids, wf_mapping, p))

    logger.info('Adding the text attestations')
    bulk_add_textattestations_core(session, read_json_lines(ta_file),
                                   batch_size=250000)

    # remove temp data
    os.remove(tokenized_file)
    os.remove(wf_to_add_file)
    os.remove(ta_file)
