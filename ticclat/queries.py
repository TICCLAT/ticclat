import logging

import numpy as np
import pandas as pd

from sqlalchemy import select, text
from sqlalchemy.sql import func, distinct, and_, desc

from ticclat.ticclat_schema import Lexicon, Wordform, Anahash, Document, \
    Corpus, lexical_source_wordform, corpusId_x_documentId, TextAttestation, \
    MorphologicalParadigm, WordformLinkSource, WordformLink

logger = logging.getLogger(__name__)


def wordform_in_corpora(session, wf):
    """Given a wordform, return a list of corpora in which it occurs.

    Gives both the term frequency and document frequency.
    """
    q = select([Wordform.wordform_id, Wordform.wordform, Corpus.name,
                func.count(Document.document_id).label('document_frequency'),
                func.sum(TextAttestation.frequency).label('term_frequency')]) \
        .select_from(Corpus.__table__.join(corpusId_x_documentId,
                                           Corpus.corpus_id
                                           == corpusId_x_documentId.c.corpus_id)
                     .join(Document,
                           Document.document_id
                           == corpusId_x_documentId.c.document_id)
                     .join(TextAttestation).join(Wordform)) \
        .where(Wordform.wordform == wf) \
        .group_by(Corpus.name, Wordform.wordform, Wordform.wordform_id)

    logger.debug(f'Executing query:\n{q}')

    return session.execute(q)


def wordform_in_corpus_over_time(session, wf, corpus_name):
    """Given a wordform, and a corpus, return word frequencies over time.

    Gives both the term frequency and document frequency.
    """
    q = select([Wordform.wordform_id, Wordform.wordform, Document.pub_year,
                func.count(Document.document_id).label('document_frequency'),
                func.sum(TextAttestation.frequency).label('term_frequency')]) \
        .select_from(Corpus.__table__.join(corpusId_x_documentId,
                                           Corpus.corpus_id
                                           == corpusId_x_documentId.c.corpus_id)
                     .join(Document,
                           Document.document_id
                           == corpusId_x_documentId.c.document_id)
                     .join(TextAttestation).join(Wordform)) \
        .where(and_(Wordform.wordform == wf, Corpus.name == corpus_name)) \
        .group_by(Document.pub_year, Wordform.wordform, Wordform.wordform_id)

    logger.debug(f'Executing query:\n{q}')

    return session.execute(q)


def wordform_in_corpora_over_time(session, wf, start_year=None, end_year=None):
    """Given a wordform, and a corpus, return word frequencies over time.

    Gives both the term frequency and document frequency.
    """
    start_year, end_year = set_year_range(session, start_year, end_year)

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
        .where(
            and_(
                Wordform.wordform == wf,
                Document.pub_year >= start_year,
                Document.pub_year <= end_year
            )
        )
        .group_by(
            Corpus.name, Document.pub_year, Wordform.wordform, Wordform.wordform_id
        )
        .order_by(Document.pub_year)
    )

    logger.debug(f"Executing query:\n{q}")

    df = pd.read_sql(q, session.connection())
    df = df.dropna(subset=['pub_year'])
    df['normalized_tf'] = df['term_frequency'] / df['num_words'] * 100.0

    # get domain and range
    min_year = df['pub_year'].min()
    if np.isnan(min_year):
        min_year = 0
    max_year = df['pub_year'].max()
    if np.isnan(max_year):
        max_year = 0

    min_freq = df['normalized_tf'].min()
    if np.isnan(min_freq):
        min_freq = 0.0
    max_freq = df['normalized_tf'].max()
    if np.isnan(max_freq):
        max_freq = 0.0

    md = {
        'min_year': int(min_year),
        'max_year': int(max_year),
        'min_freq': float(min_freq),
        'max_freq': float(max_freq)
    }

    # create result
    result = []
    for name, data in df.groupby('name'):
        corpus_data = {'name': name, 'frequencies': []}
        for row in data.iterrows():
            corpus_data['frequencies'].append(
                {'year': row[1]['pub_year'],
                 'freq': row[1]['normalized_tf']})
        result.append(corpus_data)

    return result, md


def wfs_min_num_lexica(session, num=2):
    """Select wordforms that occur in at least a number of lexicons.

    Inputs:
        session: SQLAlchemy session object.
        num (int): the number of lexicons a wordform should at least occur in

    Returns:
        SQLAlchemy query result.
    """
    subq = select([Wordform, func.count('lexicon_id').label('num_lexicons')]) \
        .select_from(lexical_source_wordform.join(Wordform)) \
        .group_by(Wordform.wordform_id)

    q = select(['*']).select_from(subq.alias()) \
        .where(text(f'num_lexicons >= {num}')) \
        .order_by(text('num_lexicons'))

    logger.debug(f'Executing query:\n{q}')

    return session.execute(q)


def count_wfs_in_lexica(session):
    """Count the number of wordforms in all lexicons in the database.

    Inputs:
        session: SQLAlchemy session object.

    Returns:
        SQLAlchemy query result.
    """
    q = select([Lexicon.lexicon_name,
               func.count(distinct(Wordform.wordform_id))
               .label('num_wordforms')]) \
        .select_from(Wordform.__table__.join(lexical_source_wordform)
                     .join(Lexicon)) \
        .group_by(Lexicon.lexicon_name)

    logger.debug(f'Executing query:\n{q}')

    return session.execute(q)


def count_wfs_per_document_corpus(session, corpus_name):
    """Count the number of wordforms in each document of a corpus.

    Inputs:
        session: SQLAlchemy session object.
        corpus_name: name of the corpus to count wordforms per document for.

    Returns:
        SQLAlchemy query result.
    """
    q = select([Document.title,
                func.count(distinct(Wordform.wordform_id)).label('tot_freq')])
    q = q.select_from(
        Corpus.__table__
              .join(corpusId_x_documentId).join(Document)
              .join(TextAttestation).join(Wordform)
    )
    q = q.where(Corpus.name == corpus_name).group_by(Document.title)

    logger.debug(f'Executing query:\n{q}')

    return session.execute(q)


def count_wfs_per_document_corpus_and_lexicon(session, corpus_name,
                                              lexicon_name):
    """Count the number of wordforms per document that occurs in a lexicon.

    Inputs:
        session: SQLAlchemy session object.
        corpus_name: name of the corpus to count wordforms per document for.
        lexicon_name: name of the lexicon the wordforms should be in.

    Returns:
        SQLAlchemy query result.
    """
    q = select([Document.title,
                func.count(distinct(Wordform.wordform_id))
                    .label('lexicon_freq')]) \
        .select_from(Corpus.__table__.join(corpusId_x_documentId)
                     .join(Document).join(TextAttestation).join(Wordform)
                     .join(lexical_source_wordform).join(Lexicon)) \
        .where(and_(Corpus.name == corpus_name,
                    Lexicon.lexicon_name == lexicon_name)) \
        .group_by(Document.title)

    logger.debug(f'Executing query:\n{q}')

    return session.execute(q)


def anahash_of_wf(session, wf):
    q = select([Wordform.wordform_id, Wordform.wordform, Anahash.anahash]) \
        .select_from(Wordform.__table__.join(Anahash)) \
        .where(Wordform.wordform == wf)

    logger.debug(f'Executing query:\n{q}')

    return session.execute(q)


def num_wfs_per_anahash(session):
    """Count the number of wordforms for each anahash.

    Inputs:
        session: SQLAlchemy session object.

    Returns:
        SQLAlchemy query result.
    """
    subq = select([Anahash, func.count('wordform_id').label('num_wf')]) \
        .select_from(Anahash.__table__.join(Wordform)) \
        .group_by(Anahash.anahash_id)
    q = select(['*']) \
        .select_from(subq.alias()) \
        .where(text('num_wf > 1')) \
        .order_by(desc('num_wf'))

    logger.debug(f'Executing query:\n{q}')

    return session.execute(q)


def count_unique_wfs_in_corpus(session, corpus_name):
    q = select([func.count(distinct(Wordform.wordform_id))]) \
        .select_from(Corpus.__table__.join(corpusId_x_documentId)
                     .join(Document).join(TextAttestation).join(Wordform)) \
        .where(Corpus.name == corpus_name)

    logger.debug(f'Executing query:\n{q}')

    return session.execute(q)


def get_wf_variants(session, wf, start_year=None, end_year=None):
    start_year, end_year = set_year_range(session, start_year, end_year)

    paradigms = []
    for paradigm in get_wf_paradigms(session, wf).fetchall():
        c = f'Z{paradigm.Z:04}Y{paradigm.Y:04}X{paradigm.X:04}W{paradigm.W:08}'
        p = {'paradigm_code': c, 'lemma': None, 'variants': []}
        for variant in get_paradigm_variants(session, paradigm).fetchall():
            vd = {'wordform': variant.wordform}
            r, md = wordform_in_corpora_over_time(session, wf=variant.wordform,
                                                  start_year=start_year,
                                                  end_year=end_year)
            vd['corpora'] = r
            vd['word_type_code'] = variant.word_type_code
            vd['V'] = variant.V
            vd['word_type_number'] = variant.word_type_number

            p['variants'].append(vd)

            if variant.word_type_code == 'HCL':
                # We should have a single HCL for each paradigm. Warn if that
                # is not the case.
                if p['lemma'] is not None:
                    logger.warn(f'Found duplicate HCL for {variant.wordform}.')
                p['lemma'] = variant.wordform
        paradigms.append(p)

    return paradigms


def get_wf_paradigms(session, wf):
    q = select([Wordform.wordform_id,
                Wordform.wordform,
                MorphologicalParadigm.Z,
                MorphologicalParadigm.Y,
                MorphologicalParadigm.X,
                MorphologicalParadigm.W,
                MorphologicalParadigm.word_type_code]) \
        .select_from(MorphologicalParadigm.__table__.join(Wordform)) \
        .where(Wordform.wordform == wf)
    return session.execute(q)


def get_paradigm_variants(session, paradigm):
    q = select([Wordform.wordform,
                MorphologicalParadigm]) \
        .select_from(Wordform.__table__.join(MorphologicalParadigm)) \
        .where(and_(MorphologicalParadigm.Z == paradigm.Z,
                    MorphologicalParadigm.Y == paradigm.Y,
                    MorphologicalParadigm.X == paradigm.X,
                    MorphologicalParadigm.W == paradigm.W))
    return session.execute(q)


def get_lexica_data(session, wordform):

    # Get vocabularies (=lexica without links) with this word
    q = select([Wordform, Lexicon]) \
        .select_from(lexical_source_wordform.join(Wordform).join(Lexicon)) \
        .where(and_(Wordform.wordform == wordform,
                    Lexicon.vocabulary == True))  # noqa E712
    logger.debug(f'Executing query:\n{q}')
    result = session.execute(q).fetchall()
    lexicon_entries = [{'lexicon_name': row.lexicon_name,
                        'correct': True} for row in result]

    # Get lexica with links containing this word
    q = select([Wordform, Lexicon, WordformLinkSource.wordform_from_correct],
               distinct=True) \
        .select_from(Wordform.__table__.join(WordformLink,
            onclause=Wordform.wordform_id == WordformLink.wordform_from)
        .join(WordformLinkSource).join(Lexicon)) \
        .where(and_(Wordform.wordform == wordform,
                    Lexicon.vocabulary == False))  # noqa E712
    logger.debug(f'Executing query:\n{q}')
    result = session.execute(q).fetchall()

    for row in result:
        lexicon_entries.append({'lexicon_name': row.lexicon_name,
                                'correct': row.wordform_from_correct})

    # sort result alphabetically on lexicon name
    return sorted(lexicon_entries, key=lambda i: i['lexicon_name'])


def get_corpora_year_range(session):
    """Get the earliest and latest publication years over all the corpora.

    Does not yet include year ranges (represented using Document.year_from and
    Document.year_to).

    Args:
        session (sqlalchemy.orm.session.Session): SQLAlchemy session object.

    Returns:
        start (int): Earliest year
        end (int): Latest year
    """
    q = select([func.min(Document.pub_year).label('min_year'),
                func.max(Document.pub_year).label('max_year')]) \
        .select_from(Document)
    r = session.execute(q)
    return r.fetchone()


def set_year_range(session, start_year, end_year):
    """Change start_year and end_year to the database value if they are None.

    Args:
        session (sqlalchemy.orm.session.Session): SQLAlchemy session object.
        start_year (int or None): start year for which word frequency data
            should be retrieved.
        end_year (int or None): end year for which word frequency data should
            be retrieved.

    Returns:
        Returns:
        start (int): Earliest year
        end (int): Latest year
    """
    if start_year is None or end_year is None:
        s, e = get_corpora_year_range(session)
        if start_year is None:
            start_year = s
        if end_year is None:
            end_year = e
    return start_year, end_year
