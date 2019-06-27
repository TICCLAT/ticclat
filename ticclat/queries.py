import logging

import pandas as pd

from sqlalchemy import select, text
from sqlalchemy.sql import func, distinct, and_, desc

from ticclat.ticclat_schema import (
    Lexicon,
    Wordform,
    Anahash,
    Document,
    Corpus,
    lexical_source_wordform,
    corpusId_x_documentId,
    TextAttestation,
)

logger = logging.getLogger(__name__)


def wordform_in_corpora(session, wf):
    """Given a wordform, return a list of corpora in which it occurs.

    Gives both the term frequency and document frequency.
    """
    q = (
        select(
            [
                Wordform.wordform_id,
                Wordform.wordform,
                Corpus.name,
                func.count(Document.document_id).label("document_frequency"),
                func.sum(TextAttestation.frequency).label("term_frequency"),
            ]
        )
        .select_from(
            Corpus.__table__.join(
                corpusId_x_documentId,
                Corpus.corpus_id == corpusId_x_documentId.c.corpus_id,
            )
            .join(Document, Document.document_id == corpusId_x_documentId.c.document_id)
            .join(TextAttestation)
            .join(Wordform)
        )
        .where(Wordform.wordform == wf)
        .group_by(Corpus.name, Wordform.wordform, Wordform.wordform_id)
    )

    logger.debug(f"Executing query:\n{q}")

    return session.execute(q)


def wordform_in_corpus_over_time(session, wf, corpus_name):
    """Given a wordform, and a corpus, return word frequencies over time.

    Gives both the term frequency and document frequency.
    """
    q = (
        select(
            [
                Wordform.wordform_id,
                Wordform.wordform,
                Document.pub_year,
                func.count(Document.document_id).label("document_frequency"),
                func.sum(TextAttestation.frequency).label("term_frequency"),
            ]
        )
        .select_from(
            Corpus.__table__.join(
                corpusId_x_documentId,
                Corpus.corpus_id == corpusId_x_documentId.c.corpus_id,
            )
            .join(Document, Document.document_id == corpusId_x_documentId.c.document_id)
            .join(TextAttestation)
            .join(Wordform)
        )
        .where(and_(Wordform.wordform == wf, Corpus.name == corpus_name))
        .group_by(Document.pub_year, Wordform.wordform, Wordform.wordform_id)
    )

    logger.debug(f"Executing query:\n{q}")

    return session.execute(q)


def wordform_in_corpora_over_time(session, wf):
    """Given a wordform, and a corpus, return word frequencies over time.

    Gives both the term frequency and document frequency.
    """
    q = (
        select(
            [
                Corpus.name,
                Document.pub_year,
                func.count(Document.document_id).label("document_frequency"),
                func.sum(TextAttestation.frequency).label("term_frequency"),
                func.sum(Document.word_count).label("num_words"),
            ]
        )
        .select_from(
            Corpus.__table__.join(
                corpusId_x_documentId,
                Corpus.corpus_id == corpusId_x_documentId.c.corpus_id,
            )
            .join(Document, Document.document_id == corpusId_x_documentId.c.document_id)
            .join(TextAttestation)
            .join(Wordform)
        )
        .where(Wordform.wordform == wf)
        .group_by(
            Corpus.name, Document.pub_year, Wordform.wordform, Wordform.wordform_id
        )
    )

    logger.debug(f"Executing query:\n{q}")

    return pd.read_sql(q, session.connection())


def wfs_min_num_lexica(session, num=2):
    """Select wordforms that occur in at least a number of lexicons.

    Inputs:
        session: SQLAlchemy session object.
        num (int): the number of lexicons a wordform should at least occur in

    Returns:
        SQLAlchemy query result.
    """
    subq = (
        select([Wordform, func.count("lexicon_id").label("num_lexicons")])
        .select_from(lexical_source_wordform.join(Wordform))
        .group_by(Wordform.wordform_id)
    )

    q = (
        select(["*"])
        .select_from(subq.alias())
        .where(text(f"num_lexicons >= {num}"))
        .order_by(text("num_lexicons"))
    )

    logger.debug(f"Executing query:\n{q}")

    return session.execute(q)


def count_wfs_in_lexica(session):
    """Count the number of wordforms in all lexicons in the database.

    Inputs:
        session: SQLAlchemy session object.

    Returns:
        SQLAlchemy query result.
    """
    q = (
        select(
            [
                Lexicon.lexicon_name,
                func.count(distinct(Wordform.wordform_id)).label("num_wordforms"),
            ]
        )
        .select_from(Wordform.__table__.join(lexical_source_wordform).join(Lexicon))
        .group_by(Lexicon.lexicon_name)
    )

    logger.debug(f"Executing query:\n{q}")

    return session.execute(q)


def count_wfs_per_document_corpus(session, corpus_name):
    """Count the number of wordforms in each document of a corpus.

    Inputs:
        session: SQLAlchemy session object.
        corpus_name: name of the corpus to count wordforms per document for.

    Returns:
        SQLAlchemy query result.
    """
    q = (
        select(
            [
                Document.title,
                func.count(distinct(Wordform.wordform_id)).label("tot_freq"),
            ]
        )
        .select_from(
            Corpus.__table__.join(corpusId_x_documentId)
            .join(Document)
            .join(TextAttestation)
            .join(Wordform)
        )
        .where(Corpus.name == corpus_name)
        .group_by(Document.title)
    )

    logger.debug(f"Executing query:\n{q}")

    return session.execute(q)


def count_wfs_per_document_corpus_and_lexicon(session, corpus_name, lexicon_name):
    """Count the number of wordforms per document that occurs in a lexicon.

    Inputs:
        session: SQLAlchemy session object.
        corpus_name: name of the corpus to count wordforms per document for.
        lexicon_name: name of the lexicon the wordforms should be in.

    Returns:
        SQLAlchemy query result.
    """
    q = (
        select(
            [
                Document.title,
                func.count(distinct(Wordform.wordform_id)).label("lexicon_freq"),
            ]
        )
        .select_from(
            Corpus.__table__.join(corpusId_x_documentId)
            .join(Document)
            .join(TextAttestation)
            .join(Wordform)
            .join(lexical_source_wordform)
            .join(Lexicon)
        )
        .where(and_(Corpus.name == corpus_name, Lexicon.lexicon_name == lexicon_name))
        .group_by(Document.title)
    )

    logger.debug(f"Executing query:\n{q}")

    return session.execute(q)


def anahash_of_wf(session, wf):
    q = (
        select([Wordform.wordform_id, Wordform.wordform, Anahash.anahash])
        .select_from(Wordform.__table__.join(Anahash))
        .where(Wordform.wordform == wf)
    )

    logger.debug(f"Executing query:\n{q}")

    return session.execute(q)


def num_wfs_per_anahash(session):
    """Count the number of wordforms for each anahash.

    Inputs:
        session: SQLAlchemy session object.

    Returns:
        SQLAlchemy query result.
    """
    subq = (
        select([Anahash, func.count("wordform_id").label("num_wf")])
        .select_from(Anahash.__table__.join(Wordform))
        .group_by(Anahash.anahash_id)
    )
    q = (
        select(["*"])
        .select_from(subq.alias())
        .where(text("num_wf > 1"))
        .order_by(desc("num_wf"))
    )

    logger.debug(f"Executing query:\n{q}")

    return session.execute(q)


def count_unique_wfs_in_corpus(session, corpus_name):
    q = (
        select([func.count(distinct(Wordform.wordform_id))])
        .select_from(
            Corpus.__table__.join(corpusId_x_documentId)
            .join(Document)
            .join(TextAttestation)
            .join(Wordform)
        )
        .where(Corpus.name == corpus_name)
    )

    logger.debug(f"Executing query:\n{q}")

    return session.execute(q)
