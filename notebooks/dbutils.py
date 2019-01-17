import numpy as np
import pandas as pd

from contextlib import contextmanager

from tqdm import tqdm

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker

from ticclat import Wordform, Lexicon


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
