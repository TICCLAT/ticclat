import logging
import pandas as pd

from contextlib import contextmanager

from tqdm import tqdm

from sqlalchemy import create_engine, select, and_, bindparam
from sqlalchemy.orm import sessionmaker

# for create_database:
import MySQLdb
from sqlalchemy_utils import database_exists
from sqlalchemy_utils.functions import drop_database

from ticclat.ticclat_schema import Base, Wordform, Lexicon, Anahash, Corpus, \
    lexical_source_wordform
from ticclat.utils import chunk_df, anahash_df, write_json_lines, \
    read_json_lines, get_temp_file
from ticclat.sacoreutils import bulk_add_wordforms_core, sql_insert
from ticclat.tokenize import nltk_tokenize

logger = logging.getLogger(__name__)


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


def bulk_add_wordforms(session, wfs, disable_pbar=False, num=10000):
    """
    wfs is pandas DataFrame with the same column names as the database table,
    in this case just "wordform"
    """
    logger.info('Bulk adding wordforms.')

    if not wfs['wordform'].is_unique:
        raise ValueError('The wordform-column contains duplicate entries.')

    total = 0

    progress_bar = tqdm(total=len(wfs))

    for chunk in chunk_df(wfs, num=num):
        # Find out which wordwordforms are not yet in the database
        wordforms = list(chunk['wordform'])

        s = select([Wordform]).where(Wordform.wordform.in_(wordforms))
        result = session.execute(s).fetchall()

        existing_wfs = [wf['wordform'] for wf in result]

        # Add wordforms that are not in the database
        if len(existing_wfs) < len(chunk):
            to_add = []
            for _, row in chunk.iterrows():
                if row['wordform'] not in existing_wfs:
                    total += 1
                    to_add.append(
                        {'wordform': row['wordform'],
                         'wordform_lowercase': row['wordform'].lower()}
                    )

            if to_add != []:
                bulk_add_wordforms_core(session, to_add)

        progress_bar.update(n=num)

    logger.info('{} wordforms have been added.'.format(total))

    return total


def add_lexicon(session, lexicon_name, vocabulary, wfs, num=10000):
    """
    wfs is pandas DataFrame with the same column names as the database table,
    in this case just "wordform"
    """
    logger.info('Adding lexicon.')

    bulk_add_wordforms(session, wfs, num=num)

    lexicon = Lexicon(lexicon_name=lexicon_name, vocabulary=vocabulary)
    session.add(lexicon)
    session.flush()
    lexicon_id = lexicon.lexicon_id

    logger.debug('Lexicon id: {}'.format(lexicon.lexicon_id))

    wordforms = list(wfs['wordform'])

    s = select([Wordform]).where(Wordform.wordform.in_(wordforms))
    result = session.execute(s).fetchall()

    logger.info('Adding {} wordforms to the lexicon.'.format(len(result)))
    session.execute(
        lexical_source_wordform.insert(),
        [{'lexicon_id': lexicon_id,
          'wordform_id': wf['wordform_id']} for wf in result]
    )

    logger.info('Lexicon was added.')

    return lexicon


def get_word_frequency_df(session):
    """Can be used as input for ticcl-anahash.

    Returns:
        Pandas DataFrame containing wordforms as index and a frequency value as
            column, or None if all wordforms in the database already are
            connected to an anahash value
    """
    logger.info('Selecting wordforms without anahash value.')
    q = session.query(Wordform).filter(Wordform.anahash == None)  # noqa: E711
    q = q.with_entities(Wordform.wordform)

    df = pd.read_sql(q.statement, q.session.bind)
    if df.empty:
        df = None
    else:
        df = df.set_index('wordform')
        df['frequency'] = 1

    return df


def bulk_add_anahashes(session, anahashes, num=10000):
    """anahashes is pandas dataframe with the column wordform (index), anahash
    """
    logger.info('Adding anahashes.')
    # Remove duplicate anahashes
    unique_hashes = anahashes.copy().drop_duplicates(subset='anahash')
    msg = 'The input data contains {} wordform/anahash pairs.'
    logger.debug(msg.format(anahashes.shape[0]))
    'There are {} unique anahash values.'
    logger.debug(msg.format(unique_hashes.shape[0]))

    total = 0

    for chunk in chunk_df(unique_hashes, num=num):
        # Find out which anahashes are not yet in the database.
        ahs = list(chunk['anahash'])

        s = select([Anahash]).where(Anahash.anahash.in_(ahs))
        result = session.execute(s).fetchall()

        existing_ahs = [ah[1] for ah in result]

        # Add anahashes that are not in the database

        to_add = []
        for _, row in chunk.iterrows():
            if row['anahash'] not in existing_ahs:
                total += 1
                to_add.append({'anahash': row['anahash']})
        if to_add != []:
            sql_insert(session, Anahash, to_add)

    logger.info('Added {} anahashes.'.format(total))

    return total


def get_anahashes(session, anahashes):
    for chunk in chunk_df(anahashes):
        hashes = list(chunk.index)
        s = select([Wordform]).where(
            and_(Wordform.anahash_id == None,  # noqa: E711
                 Wordform.wordform.in_(hashes)))
        wfs = session.execute(s).fetchall()
        for wf in wfs:
            # wf (wordform_id, wordform, anahash_id, wordform_lowercase)
            h = anahashes.loc[wf[1]]['anahash']
            s = select([Anahash]).where(Anahash.anahash == h)
            a = session.execute(s).fetchone()

            # SQLAlchemy doesn't allow the use of column names in update
            # statements, so we use something else.
            yield {'a_id': a[0], 'wf_id': wf[0]}


def connect_anahases_to_wordforms(session, anahashes):
    logger.info('Connecting anahashes to wordforms.')
    anahash_to_wf_file = get_temp_file()
    t = write_json_lines(anahash_to_wf_file, get_anahashes(session, anahashes))

    u = Wordform.__table__.update(). \
        where(Wordform.wordform_id == bindparam('wf_id')). \
        values(anahash_id=bindparam('a_id'))

    to_update = [o for o in read_json_lines(anahash_to_wf_file)]
    if to_update != []:
        session.execute(u, to_update)

    logger.info('Added the anahash of {} wordforms.'.format(t))

    return t


def update_anahashes(session, alphabet_file):
    df = get_word_frequency_df(session)

    anahashes = anahash_df(df, alphabet_file)

    bulk_add_anahashes(session, anahashes)

    connect_anahases_to_wordforms(session, anahashes)


def add_corpus(session, name, texts_file, n_documents=1000, n_wfs=1000):
    """Add a corpus to the database.

    Take care: this is a very slow method for adding a big corpus like the
    Dutch wikipedia. Also, this function is still untested.

    Inputs:
        session: SQLAlchemy session object.
        name (str): The name of the corpus.
        texts_file (str): Path to the file containing the texts. This file
            should contain one text per line.
    Returns:
        Corpus: The corpus object
    """
    # add all the wordforms in the corpus
    i = 0
    dfs = []

    for terms_vector in tqdm(nltk_tokenize(texts_file)):
        df = pd.DataFrame()
        df['wordform'] = terms_vector.keys()
        dfs.append(df)

        i += 1

        if i % n_documents == 0:
            r = pd.concat(dfs)
            r = r.drop_duplicates(subset='wordform')
            n = bulk_add_wordforms(session, r, disable_pbar=True)
            print('Added {} wordforms'.format(n))

            dfs = []

    # also add the final documents
    if len(dfs) > 0:
        r = pd.concat(dfs)
        r = r.drop_duplicates(subset='wordform')
        n = bulk_add_wordforms(session, r, disable_pbar=True, num=n_wfs)
        print('Added {} wordforms'.format(n))

    # create corpus
    corpus = Corpus(name=name)
    session.add(corpus)

    for terms_vector in tqdm(nltk_tokenize(texts_file)):
        # get the wordforms
        q = session.query(Wordform)
        wfs = q.filter(Wordform.wordform.in_(terms_vector.keys())).all()

        # FIXME: add proper metatadata for a document
        corpus.add_document(terms_vector, wfs)

    return corpus


def create_ticclat_database(delete_existing=False, dbname='ticclat', user="", passwd=""):
    db = MySQLdb.connect(user=user, passwd=passwd)
    engine = create_engine(f"mysql://{user}:{passwd}@localhost/{dbname}?charset=utf8mb4")

    with db.cursor() as cursor:
        try:
            cursor.execute(f"CREATE DATABASE {dbname} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;")
            result = cursor.fetchall()
        except MySQLdb.ProgrammingError as e:
            if database_exists(engine.url):
                if not delete_existing:
                    raise Exception(f"Database `{dbname}` already exists, delete it first before recreating.")
                else:
                    drop_database(engine.url)
                    cursor.execute(f"CREATE DATABASE {dbname} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;")
                    result = cursor.fetchall()
            else:
                raise e

    Session = sessionmaker(bind=engine)

    # create tables
    Base.metadata.create_all(engine)
