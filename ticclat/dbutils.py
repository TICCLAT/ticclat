import numpy as np
import pandas as pd

from contextlib import contextmanager

from tqdm import tqdm_notebook as tqdm

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker

from ticclat.ticclat_schema import Wordform, Lexicon, Anahash, Document, \
                                   TextAttestation, Corpus
from ticclat.tokenize import nltk_tokenize


# source: https://docs.sqlalchemy.org/en/latest/orm/session_basics.html
@contextmanager
def session_scope(s):
    """Provide a transactional scope around a series of operations."""
    session = s()
    try:
        yield session
        session.commit()
    except:  # noqa: E722
        session.rollback()
        raise
    finally:
        session.close()


def get_session(user, password, dbname,
                dburl='mysql://{}:{}@localhost/{}?charset=utf8mb4'):
    engine = create_engine(dburl.format(user, password, dbname))

    return sessionmaker(bind=engine)


def get_or_create_wordform(session, wordform, has_analysis=False,
                           wordform_id=None):
    wf = None

    # does the wordform already exist?
    if wordform_id is not None:
        pass
    else:
        q = session.query(Wordform)
        wf = q.filter(Wordform.wordform == wordform).first()

    if wf is None:
        wf = Wordform(wordform_id=wordform_id,
                      wordform=wordform,
                      has_analysis=has_analysis,
                      wordform_lowercase=wordform.lower())
        session.add(wf)

    return wf


<<<<<<< HEAD
def bulk_add_wordforms(session, wfs, disable_pbar=False, num=10000):
    """wfs is pandas dataframe with the same column names as the database table
=======
def bulk_add_wordforms(session, wfs, num=10000):
    """
    wfs is pandas DataFrame with the same column names as the database table,
    in this case just "wordform"
>>>>>>> 1f17a02ae96066dae58ece2fb4eb8de85b3bb84d
    """
    if not wfs['wordform'].is_unique:
        raise ValueError('The wordform-column contains duplicate entries.')

    if wfs.shape[0] > num:
        n = wfs.shape[0] // num
    else:
        n = 1

    total = 0

    for chunk in tqdm(np.array_split(wfs, n), disable=disable_pbar):
        # Find out which wordwordforms are not yet in the database
        wordforms = list(chunk['wordform'])

        q = session.query(Wordform)
        result = q.filter(Wordform.wordform.in_(wordforms)).all()

        existing_wfs = [wf.wordform for wf in result]

        # Add wordforms that are not in the database
<<<<<<< HEAD
        if len(existing_wfs) < len(chunk):
            to_add = []
            for idx, row in chunk.iterrows():
                if row['wordform'] not in existing_wfs:
                    total += 1
                    to_add.append(
                        Wordform(wordform=row['wordform'],
                                 has_analysis=row['has_analysis'],
                                 wordform_lowercase=row['wordform'].lower())
                        )
            if to_add != []:
                session.bulk_save_objects(to_add)
=======
        to_add = []
        for idx, row in chunk.iterrows():
            if row['wordform'] not in existing_wfs:
                total += 1
                to_add.append(
                    Wordform(wordform=row['wordform'],
                            #  has_analysis=row['has_analysis'],
                             wordform_lowercase=row['wordform'].lower())
                )
        if to_add != []:
            session.bulk_save_objects(to_add)
>>>>>>> 1f17a02ae96066dae58ece2fb4eb8de85b3bb84d

    return total


def add_lexicon(session, lexicon_name, vocabulary, wfs, num=10000):
    """
    wfs is pandas DataFrame with the same column names as the database table,
    in this case just "wordform"
    """
    bulk_add_wordforms(session, wfs, num=num)

    lexicon = Lexicon(lexicon_name=lexicon_name, vocabulary=vocabulary)
    session.add(lexicon)

    wordforms = list(wfs['wordform'])

    q = session.query(Wordform).filter(Wordform.wordform.in_(wordforms)).all()
    print(len(q))
    lexicon.wordforms = q


def get_word_frequency_df(session):
    """Can be used as input for ticcl-anahash."""
    q = session.query(Wordform).filter(and_(Wordform.anahash_id is None)) \
        .with_entities(Wordform.wordform)
    df = pd.read_sql(q.statement, q.session.bind)
    df = df.set_index('wordform')
    df['frequency'] = 1

    return df


def bulk_add_anahashes(session, anahashes, num=10000):
    """anahashes is pandas dataframe with the column wordform (index), anahash
    """
    # Remove duplicate anahashes
    unique_hashes = anahashes.copy().drop_duplicates(subset='anahash')
    print(anahashes.shape)
    print(unique_hashes.shape)

    n = unique_hashes.shape[0] // num

    total = 0

    for chunk in tqdm(np.array_split(unique_hashes, n)):
        # Find out which anahashes are not yet in the database.
        ahs = list(chunk['anahash'])

        result = session.query(Anahash) \
            .filter(Anahash.anahash.in_(ahs)).all()

        existing_ahs = [ah.anahash for ah in result]

        # Add anahashes that are not in the database
        to_add = []
        for idx, row in chunk.iterrows():
            if row['anahash'] not in existing_ahs:
                total += 1
                to_add.append(Anahash(anahash=row['anahash']))
        if to_add != []:
            session.bulk_save_objects(to_add)

    return total


def connect_anahases_to_wordforms(session, anahashes):
    # get wordforms that have no anahash value
    # if we have a hash value now:
    # select hash value from database
    # and add to wf
    hashes = list(anahashes.index)
    wfs = session.query(Wordform).filter(and_(Wordform.anahash_id is None,
                                              Wordform.wordform.in_(hashes))).all()
    total = 0

    for wf in tqdm(wfs):
        # print(wf)
        h = anahashes.loc[wf.wordform]['anahash']
        # print(h)
        a = session.query(Anahash).filter(Anahash.anahash == h).first()
        # print(a)
        wf.anahash = a
        total += 1

    return total


def add_document(session, terms_vector, pub_year, language, corpus):
    """Add a document to the database.

    Inputs:
        session: SQLAlchemy session object.
        terms_vector (Counter): term-frequency vector representing the
            document.
        pub_year (int): year of publication of the document (metadata).
        language (str): language of the document (metadata).
        corpus (Corpus): corpus to which the document belongs.

    Returns:
        Document (document object)
    """
    # create document
    d = Document(word_count=sum(terms_vector.values()), pub_year=pub_year,
                 language=language)
    session.add(d)

    # make sure all wordforms exist in the database
    df = pd.DataFrame()
    df['wordform'] = terms_vector.keys()
    df['has_analysis'] = False
    bulk_add_wordforms(session, df, disable_pbar=True)

    # get the wordforms
    q = session.query(Wordform)
    result = q.filter(Wordform.wordform.in_(terms_vector.keys())).all()

    # add the wordforms to the document
    for wf in result:
        ta = TextAttestation(ta_document=d, ta_wordform=wf,
                             frequency=terms_vector[wf.wordform])
        session.add(ta)

    # set the corpus
    d.document_corpora.append(corpus)

    return d


def add_corpus(session, name, texts_file):
    """Add a corpus to the database.

    Inputs:
        session: SQLAlchemy session object.
        name (str): The name of the corpus.
        texts_file (str): Path to the file containing the texts. This file
            should contain one text per line.
    Returns:
        Corpus: The corpus object
    """
    # create corpus
    corpus = Corpus(name=name)
    session.add(corpus)

    for terms_vector in tqdm(nltk_tokenize(texts_file)):
        # FIXME: add proper metatadata for a document
        add_document(session, terms_vector, pub_year=2019, language='nl',
                     corpus=corpus)

    return corpus
