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

from tqdm import tqdm

from ticclat.ticclat_schema import (
    Wordform,
    Corpus,
    Document,
    TextAttestation,
    Anahash,
    corpusId_x_documentId,
)
from ticclat.utils import (
    chunk_df,
    write_json_lines,
    read_json_lines,
    get_temp_file,
    iterate_wf,
    chunk_json_lines,
    count_lines,
)

logger = logging.getLogger(__name__)

DBSession = scoped_session(sessionmaker())


def get_engine(
    user, password, dbname, dburl="mysql://{}:{}@localhost/{}?charset=utf8mb4"
):
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

    engine.execute(table_object.__table__.insert(), [obj for obj in to_insert])


def sql_query_batches(engine, query, iterator, total=0, batch_size=10000):
    with tqdm(total=total, mininterval=2.0) as pbar:
        objects = []
        for item in iterator:
            objects.append(item)
            if len(objects) == batch_size:
                engine.execute(query, objects)
                objects = []
                pbar.update(batch_size)
        # Doing the insert with an empty list results in adding a row with all
        # fields to the deafult values, or an error, if fields don't have a default
        # value. Se, we have to check whether to_add is empty.
        if objects != []:
            engine.execute(query, objects)
            pbar.update(len(objects))


def sql_insert_batches(engine, table_object, iterator, total=0, batch_size=10000):
    with tqdm(total=total, mininterval=2.0) as pbar:
        to_add = []
        for item in iterator:
            to_add.append(item)
            if len(to_add) == batch_size:
                sql_insert(engine, table_object, to_add)
                to_add = []
                pbar.update(batch_size)
        # Doing the insert with an empty list results in adding a row with all
        # fields to the deafult values, or an error, if fields don't have a default
        # value. Se, we have to check whether to_add is empty.
        if to_add != []:
            sql_insert(engine, table_object, to_add)
            pbar.update(len(to_add))


def bulk_add_wordforms_core(engine, iterator, batch_size=50000):
    sql_insert_batches(engine, Wordform, iterator, batch_size=batch_size)


def bulk_add_textattestations_core(engine, iterator, total=0, batch_size=50000):
    sql_insert_batches(
        engine, TextAttestation, iterator, total=total, batch_size=batch_size
    )


def bulk_add_anahashes_core(engine, iterator, batch_size=50000):
    sql_insert_batches(engine, Anahash, iterator, batch_size=batch_size)


def select_wordforms(session, iterator):
    for chunk in iterator:
        # Find out which wordwordforms are not yet in the database
        wordforms = list(chunk["wordform"])
        s = select([Wordform]).where(Wordform.wordform.in_(wordforms))
        result = session.execute(s).fetchall()

        # wf: (id, wordform, anahash_id, wordform_lowercase)
        for wf in result:
            yield {"wordform_id": wf[0], "wordform": wf[1]}


def get_tas(corpus, doc_ids, wf_mapping, p):

    cx = scipy.sparse.coo_matrix(corpus)
    for i, j, v in zip(cx.row, cx.col, cx.data):
        word = p[j]
        freq = v
        yield {
            "wordform_id": wf_mapping[word],
            "document_id": doc_ids[i],
            "frequency": int(freq),
        }


def add_corpus_core(
    session,
    corpus_matrix,
    vectorizer,
    corpus_name,
    document_metadata=pd.DataFrame(),
    batch_size=50000,
):
    with get_temp_file() as wf_file:
        write_json_lines(wf_file, iterate_wf(vectorizer.vocabulary_))

        # Prepare the documents to be added to the database
        # FIXME: add proper metadata
        logger.info("Creating document data")
        cx = scipy.sparse.csr_matrix(corpus_matrix)
        word_counts = cx.sum(axis=1)  # sum the rows

        wc_list = np.array(word_counts).flatten().tolist()

        document_metadata["word_count"] = wc_list

        # Determine which wordforms in the vocabulary need to be added to the
        # database
        logger.info("Determine which wordforms need to be added")
        with get_temp_file() as wf_to_add_file:
            with tqdm(total=count_lines(wf_file)) as pbar:
                for chunk in chunk_json_lines(wf_file, batch_size=batch_size):
                    # Find out which wordwordforms are not yet in the database
                    wordforms = set([wf["wordform"] for wf in chunk])
                    s = select([Wordform]).where(Wordform.wordform.in_(wordforms))
                    result = session.execute(s).fetchall()

                    # wf: (id, wordform, anahash_id, wordform_lowercase)
                    existing_wfs = set([wf[1] for wf in result])
                    for wf in wordforms.difference(existing_wfs):
                        wf_to_add_file.write(
                            json.dumps(
                                {"wordform": wf, "wordform_lowercase": wf.lower()}
                            )
                        )
                        wf_to_add_file.write("\n")
                    pbar.update(batch_size)

            # Create the corpus (in a session) and get the ID
            logger.info("Creating the corpus")
            corpus = Corpus(name=corpus_name)
            session.add(corpus)

            # add the documents using ORM, because we need to link them to the
            # corpus
            logger.info("Adding the documents")
            for doc in document_metadata.to_dict(orient="records"):
                d = Document(**doc)
                d.document_corpora.append(corpus)
            session.flush()
            corpus_id = corpus.corpus_id

            # Insert the wordforms that need to be added using SQLAlchemy core (much
            # faster than using the ORM)
            logger.info("Adding the wordforms")
            bulk_add_wordforms_core(session, read_json_lines(wf_to_add_file))

    logger.info("Prepare adding the text attestations")
    # make a mapping from
    df = pd.DataFrame.from_dict(vectorizer.vocabulary_, orient="index")
    df = df.reset_index()

    logger.info("\tGetting the wordform ids")
    wf_mapping = {}

    for chunk in chunk_df(df, 50000):
        to_select = list(chunk["index"])
        s = select([Wordform]).where(Wordform.wordform.in_(to_select))
        result = session.execute(s).fetchall()
        for wf in result:
            # wf: (id, wordform, anahash_id, wordform_lowercase)
            wf_mapping[wf[1]] = wf[0]

    logger.info("\tGetting the document ids")
    # get doc_ids
    s = (
        select([corpusId_x_documentId.join(Corpus).join(Document)])
        .where(Corpus.corpus_id == corpus_id)
        .order_by(Document.document_id)
    )
    r = session.execute(s).fetchall()
    # row: (corpus_id, document_id, ...)
    doc_ids = [row[1] for row in r]

    logger.info("\tReversing the mapping")
    # reverse mapping from wordform to id in the terms/document matrix
    p = dict(zip(vectorizer.vocabulary_.values(), vectorizer.vocabulary_.keys()))

    logger.info("\tGetting the text attestations")
    with get_temp_file() as ta_file:
        write_json_lines(ta_file, get_tas(corpus_matrix, doc_ids, wf_mapping, p))

        logger.info("Adding the text attestations")
        total = count_lines(ta_file)
        bulk_add_textattestations_core(
            session, read_json_lines(ta_file), total=total, batch_size=batch_size
        )
