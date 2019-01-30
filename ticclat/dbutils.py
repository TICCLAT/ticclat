import numpy as np
import pandas as pd

from contextlib import contextmanager

from tqdm import tqdm_notebook as tqdm

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker

from ticclat.ticclat import Wordform, Lexicon, Anahash


# source: https://docs.sqlalchemy.org/en/latest/orm/session_basics.html
@contextmanager
def session_scope(s):
    """Provide a transactional scope around a series of operations."""
    session = s()
    try:
        yield session
        session.commit()
    except:
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


def bulk_add_wordforms(session, wfs, num=10000):
    """wfs is pandas dataframe with the same column names as the database table
    """
    n = wfs.shape[0] // num

    total = 0

    for chunk in tqdm(np.array_split(wfs, n)):
        # Find out which wordwordforms are not yet in the database
        wordforms = list(chunk['wordform'])

        q = session.query(Wordform)
        result = q.filter(Wordform.wordform.in_(wordforms)).all()

        existing_wfs = [wf.wordform for wf in result]

        # Add wordforms that are not in the database
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

    return total


def add_lexicon(session, lexicon_name, wfs, num=10000):
    """wfs is pandas dataframe with the same column names as the database table
    """
    bulk_add_wordforms(session, wfs, num=num)

    lexicon = Lexicon(lexicon_name=lexicon_name)
    session.add(lexicon)

    wordforms = list(wfs['wordform'])

    q = session.query(Wordform).filter(Wordform.wordform.in_(wordforms)).all()
    print(len(q))
    lexicon.wordforms = q


def get_word_frequency_df(session):
    """Can be used as input for ticcl-anahash."""
    q = session.query(Wordform).filter(and_(Wordform.anahash_id == None)) \
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
    wfs = session.query(Wordform).filter(and_(Wordform.anahash_id == None,
                                              Wordform.wordform.in_(hashes))).all()
    total = 0

    for wf in tqdm(wfs):
        #print(wf)
        h = anahashes.loc[wf.wordform]['anahash']
        #print(h)
        a = session.query(Anahash).filter(Anahash.anahash == h).first()
        #print(a)
        wf.anahash = a
        total += 1

    return total
