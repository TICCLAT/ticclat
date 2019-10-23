"""SQLAlchemy core utility functionality

Functionality for faster bulk inserts without using the ORM.
More info: https://docs.sqlalchemy.org/en/latest/faq/performance.html
"""
import logging
import json
import scipy

import numpy as np
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.sql import select
from sqlalchemy.orm import scoped_session, sessionmaker

from tqdm import tqdm

from ticclat.ticclat_schema import Wordform, Corpus, Document, \
    TextAttestation, Anahash, corpusId_x_documentId
from ticclat.utils import chunk_df, write_json_lines, read_json_lines, \
    get_temp_file, iterate_wf, chunk_json_lines, count_lines

LOGGER = logging.getLogger(__name__)

DB_SESSION = scoped_session(sessionmaker())


def get_engine(user, password, dbname,
               dburl='mysql://{}:{}@localhost/{}?charset=utf8mb4'):
    """Returns an engine that can be used for fast bulk inserts
    """
    engine = create_engine(dburl.format(user, password, dbname), echo=False)
    DB_SESSION.remove()
    DB_SESSION.configure(bind=engine, autoflush=False, expire_on_commit=False)

    return engine


def sql_insert(engine, table_object, to_insert):
    """Insert a list of objects into the database without using a session.

    This is a fast way of (mass) inserting objects. However, because no session
    is used, no relationships can be added automatically. So, use with care!

    This function is a simplified version of test_sqlalchemy_core from here:
    https://docs.sqlalchemy.org/en/13/faq/performance.html#i-m-inserting-400-000-rows-with-the-orm-and-it-s-really-slow

    Inputs:
        engine: SQLAlchemy engine or session
        table_object: object representing a table in the database (i.e., one
            of the objects from ticclat_schema)
        to_insert (list of dicts): list containg dictionary representations of
            the objects (rows) to be inserted
    """

    engine.execute(table_object.__table__.insert(), to_insert)


def sql_query_batches(engine, query, iterator, total=0, batch_size=10000):
    """
    Execute `query` on items in `iterator` in batches.

    Take care: no session is used, so relationships can't be added automatically.

    Inputs:
        total: used for tqdm, since iterator will often be a generator, which
               has no predefined length.
    """
    with tqdm(total=total, mininterval=2.0) as pbar:
        objects = []
        for item in iterator:
            objects.append(item)
            if len(objects) == batch_size:
                engine.execute(query, objects)
                objects = []
                pbar.update(batch_size)
        # Doing the insert with an empty list results in adding a row with all
        # fields to the default values, or an error, if fields don't have a default
        # value. Se, we have to check whether to_add is empty.
        if objects != []:
            engine.execute(query, objects)
            pbar.update(len(objects))


def sql_insert_batches(engine, table_object, iterator, total=0, batch_size=10000):
    """
    Insert items in `iterator` in batches into database table.

    Take care: no session is used, so relationships can't be added automatically.

    Inputs:
        table_object: the ticclat_schema object corresponding to the database
                      table.
        total: used for tqdm, since iterator will often be a generator, which
               has no predefined length.
    """
    with tqdm(total=total, mininterval=2.0) as pbar:
        to_add = []
        for item in iterator:
            to_add.append(item)
            if len(to_add) == batch_size:
                sql_insert(engine, table_object, to_add)
                to_add = []
                pbar.update(batch_size)
        # Doing the insert with an empty list results in adding a row with all
        # fields to the default values, or an error, if fields don't have a default
        # value. Se, we have to check whether to_add is empty.
        if to_add != []:
            sql_insert(engine, table_object, to_add)
            pbar.update(len(to_add))


def bulk_add_wordforms_core(engine, iterator, **kwargs):
    """
    Insert wordforms in `iterator` in batches into wordforms database table.

    Convenience wrapper around `sql_insert_batches` for wordforms.
    Take care: no session is used, so relationships can't be added automatically.
    """
    sql_insert_batches(engine, Wordform, iterator, **kwargs)


def bulk_add_textattestations_core(engine, iterator, **kwargs):
    """
    Insert text attestations in `iterator` in batches into text_attestations database table.

    Convenience wrapper around `sql_insert_batches` for text attestations.
    Take care: no session is used, so relationships can't be added automatically.
    """
    sql_insert_batches(engine, TextAttestation, iterator, **kwargs)


def bulk_add_anahashes_core(engine, iterator, **kwargs):
    """
    Insert anahashes in `iterator` in batches into anahashes database table.

    Convenience wrapper around `sql_insert_batches` for anagram hashes.
    Take care: no session is used, so relationships can't be added automatically.
    """
    sql_insert_batches(engine, Anahash, iterator, **kwargs)


def get_tas(corpus, doc_ids, wf_mapping, word_from_tdmatrix_id):
    """
    Get term attestation from wordform frequency matrix.

    Term attestation records the occurrence and frequency of a word in a given
    document.

    Inputs:
        corpus: the dense corpus term-document matrix, like from
                `tokenize.terms_documents_matrix_ticcl_frequency`
        doc_ids: list of indices of documents in the term-document matrix
        wf_mapping: dictionary mapping wordforms (key) to database wordform_id
        word_from_tdmatrix_id: mapping of term-document matrix column index
                               (key) to wordforms (value)
    """
    corpus_coo = scipy.sparse.coo_matrix(corpus)
    for tdmatrix_doc_ix, tdmatrix_word_ix, tdmatrix_value in zip(corpus_coo.row, corpus_coo.col, corpus_coo.data):
        word = word_from_tdmatrix_id[tdmatrix_word_ix]
        freq = tdmatrix_value
        yield {'wordform_id': wf_mapping[word],
               'document_id': doc_ids[tdmatrix_doc_ix],
               'frequency': int(freq)}


def add_corpus_core(session, corpus_matrix, vectorizer, corpus_name,
                    document_metadata=pd.DataFrame(), batch_size=50000):
    """
    Add a corpus to the database.

    A corpus is a collection of documents, which is a collection of words.
    This function adds all words as wordforms to the database, records their
    "attestation" (the fact that they occur in a certain document and with what
    frequency), adds the documents they belong to, adds the corpus and adds the
    corpus ID to the documents.

    Inputs:
        session: SQLAlchemy session (e.g. from `dbutils.get_session`)
        corpus_matrix: the dense corpus term-document matrix, like from
                       `tokenize.terms_documents_matrix_ticcl_frequency`
        vectorizer: the terms in the term-document matrix, as given by
                    `tokenize.terms_documents_matrix_ticcl_frequency`
        corpus_name: the name of the corpus in the database
        document_metadata: see `ticclat_schema.Document` for all the possible
                           metadata. Make sure the index of this dataframe
                           matches with the document identifiers in the term-
                           document matrix, which can be easily achieved by
                           resetting the index for a Pandas dataframe.
        batch_size: batch handling of wordforms to avoid memory issues.
    """
    with get_temp_file() as wf_file:
        write_json_lines(wf_file, iterate_wf(vectorizer.vocabulary_))

        # Prepare the documents to be added to the database
        LOGGER.info('Creating document data')
        corpus_csr = scipy.sparse.csr_matrix(corpus_matrix)
        word_counts = corpus_csr.sum(axis=1)  # sum the rows

        wc_list = np.array(word_counts).flatten().tolist()

        document_metadata['word_count'] = wc_list

        # Determine which wordforms in the vocabulary need to be added to the
        # database
        LOGGER.info('Determine which wordforms need to be added')
        with get_temp_file() as wf_to_add_file:
            with tqdm(total=count_lines(wf_file)) as pbar:
                for chunk in chunk_json_lines(wf_file, batch_size=batch_size):
                    # Find out which wordwordforms are not yet in the database
                    wordforms = {wf['wordform'] for wf in chunk}
                    select_statement = select([Wordform]).where(Wordform.wordform.in_(wordforms))
                    result = session.execute(select_statement).fetchall()

                    # wf: (id, wordform, anahash_id, wordform_lowercase)
                    existing_wfs = {wf[1] for wf in result}
                    for wordform in wordforms.difference(existing_wfs):
                        wf_to_add_file.write(json.dumps({'wordform': wordform,
                                                         'wordform_lowercase': wordform.lower()}))
                        wf_to_add_file.write('\n')
                    pbar.update(batch_size)

            # Create the corpus (in a session) and get the ID
            LOGGER.info('Creating the corpus')
            corpus = Corpus(name=corpus_name)
            session.add(corpus)

            # add the documents using ORM, because we need to link them to the
            # corpus
            LOGGER.info('Adding the documents')
            for doc in document_metadata.to_dict(orient='records'):
                document_obj = Document(**doc)
                document_obj.document_corpora.append(corpus)
            session.flush()
            corpus_id = corpus.corpus_id

            # Insert the wordforms that need to be added using SQLAlchemy core (much
            # faster than using the ORM)
            LOGGER.info('Adding the wordforms')
            bulk_add_wordforms_core(session, read_json_lines(wf_to_add_file))

    LOGGER.info('Prepare adding the text attestations')
    # make a mapping from
    df = pd.DataFrame.from_dict(vectorizer.vocabulary_, orient='index')
    df = df.reset_index()

    LOGGER.info('\tGetting the wordform ids')
    wf_mapping = {}

    for chunk in chunk_df(df, batch_size=batch_size):
        to_select = list(chunk['index'])
        select_statement = select([Wordform]).where(Wordform.wordform.in_(to_select))
        result = session.execute(select_statement).fetchall()
        for wordform in result:
            # wordform: (id, wordform, anahash_id, wordform_lowercase)
            wf_mapping[wordform[1]] = wordform[0]

    LOGGER.info('\tGetting the document ids')
    # get doc_ids
    select_statement = select([corpusId_x_documentId.join(Corpus).join(Document)]) \
        .where(Corpus.corpus_id == corpus_id).order_by(Document.document_id)
    result = session.execute(select_statement).fetchall()
    # row: (corpus_id, document_id, ...)
    doc_ids = [row[1] for row in result]

    LOGGER.info('\tReversing the mapping')
    # reverse mapping from wordform to id in the terms/document matrix
    word_from_tdmatrix_id = dict(zip(vectorizer.vocabulary_.values(),
                                     vectorizer.vocabulary_.keys()))

    LOGGER.info('\tGetting the text attestations')
    with get_temp_file() as ta_file:
        write_json_lines(ta_file, get_tas(corpus_matrix, doc_ids, wf_mapping, word_from_tdmatrix_id))

        LOGGER.info('Adding the text attestations')
        total = count_lines(ta_file)
        bulk_add_textattestations_core(session, read_json_lines(ta_file),
                                       total=total, batch_size=batch_size)
