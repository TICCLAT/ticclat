import os
import json
import logging
import pandas as pd

from contextlib import contextmanager
from collections import defaultdict

from tqdm import tqdm

from sqlalchemy import create_engine, select, bindparam, and_
from sqlalchemy.orm import sessionmaker

# for create_database:
import MySQLdb
from sqlalchemy_utils import database_exists
from sqlalchemy_utils.functions import drop_database

from ticclat.ticclat_schema import Base, Wordform, Lexicon, Anahash, Corpus, \
    lexical_source_wordform, WordformLink, WordformLinkSource
from ticclat.utils import chunk_df, anahash_df, write_json_lines, \
    read_json_lines, get_temp_file, json_line
from ticclat.sacoreutils import bulk_add_wordforms_core, \
    bulk_add_anahashes_core, sql_query_batches, sql_insert_batches
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


def bulk_add_wordforms(session, wfs, disable_pbar=False, batch_size=10000):
    """
    wfs is pandas DataFrame with the same column names as the database table,
    in this case just "wordform"
    """
    logger.info('Bulk adding wordforms.')

    if not wfs['wordform'].is_unique:
        logger.info('The wordform-column contains duplicate entries. '
                    'Removing duplicates.')
        wfs = wfs.drop_duplicates(subset='wordform')

    total = 0

    with tqdm(total=len(wfs)) as progress_bar:
        for chunk in chunk_df(wfs, batch_size=batch_size):
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
                    bulk_add_wordforms_core(session, to_add, batch_size=batch_size)

            progress_bar.update(n=batch_size)

    logger.info('{} wordforms have been added.'.format(total))

    return total


def add_lexicon(session, lexicon_name, vocabulary, wfs, batch_size=10000):
    """
    wfs is pandas DataFrame with the same column names as the database table,
    in this case just "wordform"
    """
    logger.info('Adding lexicon.')

    bulk_add_wordforms(session, wfs, batch_size=batch_size)

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


def get_word_frequency_df(session, add_ids=False):
    """Can be used as input for ticcl-anahash.

    Returns:
        Pandas DataFrame containing wordforms as index and a frequency value as
            column, or None if all wordforms in the database already are
            connected to an anahash value
    """
    logger.info('Selecting wordforms without anahash value.')
    q = session.query(Wordform).filter(Wordform.anahash == None)  # noqa: E711
    if add_ids:
        q = q.with_entities(Wordform.wordform, Wordform.wordform_id)
    else:
        q = q.with_entities(Wordform.wordform)

    df = pd.read_sql(q.statement, q.session.bind)
    if df.empty:
        df = None
    else:
        df = df.set_index('wordform')
        df['frequency'] = 1

    return df


def get_wf_mapping(session, lexicon=None, lexicon_id=None):
    msg = 'Getting the wordform mapping of lexicon "{}"'

    if lexicon is not None:
        if lexicon.lexicon_id is None:
            raise ValueError('The lexicon does not (yet) have an ID. Please'
                             ' make sure the ID of the lexicon is set.')
        else:
            lexicon_id = lexicon.lexicon_id
        msg = msg.format(lexicon)
    elif lexicon_id is not None:
        msg = msg.format('lexicon id {}'.format(lexicon_id))
    # Add option lexicon name and get the lexicon_id from the database
    else:
        raise ValueError('Please specify the lexicon.')

    logger.info(msg)

    s = select([lexical_source_wordform.join(Lexicon).join(Wordform)]) \
        .where(Lexicon.lexicon_id == lexicon_id)
    logger.debug(s)
    result = session.execute(s).fetchall()

    wf_mapping = defaultdict(int)
    for r in result:
        wf_mapping[r['wordform']] = r[lexical_source_wordform.c.wordform_id]

    return wf_mapping


def bulk_add_anahashes(session, anahashes, tqdm=None, batch_size=10000):
    """anahashes is pandas dataframe with the column wordform (index), anahash
    """
    logger.info('Adding anahashes.')
    # Remove duplicate anahashes
    unique_hashes = anahashes.copy().drop_duplicates(subset='anahash')
    logger.debug(f'The input data contains {anahashes.shape[0]} wordform/anahash pairs.')
    logger.debug(f'There are {unique_hashes.shape[0]} unique anahash values.')

    total = 0
    anahashes_to_add_file = get_temp_file()

    if tqdm is not None:
        pbar = tqdm(total=unique_hashes.shape[0])
    with open(anahashes_to_add_file, 'w') as f:
        for chunk in chunk_df(unique_hashes, batch_size=batch_size):
            # Find out which anahashes are not yet in the database.
            ahs = set(list(chunk['anahash']))

            s = select([Anahash]).where(Anahash.anahash.in_(ahs))
            result = session.execute(s).fetchall()

            existing_ahs = set([ah[1] for ah in result])

            for ah in ahs.difference(existing_ahs):
                f.write(json.dumps({'anahash': ah}))
                f.write('\n')
                total += 1
            if tqdm is not None:
                pbar.update(chunk.shape[0])
    if tqdm is not None:
        pbar.close()

    bulk_add_anahashes_core(session, read_json_lines(anahashes_to_add_file))

    os.remove(anahashes_to_add_file)

    logger.info('Added {} anahashes.'.format(total))

    return total


def get_anahashes(session, anahashes, wf_mapping, batch_size=50000):
    unique_hashes = anahashes.copy().drop_duplicates(subset='anahash')

    with tqdm(total=unique_hashes.shape[0], mininterval=2.0) as pbar:
        ah_mapping = {}

        for chunk in chunk_df(unique_hashes, batch_size=batch_size):
            # Find out which anahashes are not yet in the database.
            ahs = set(list(chunk['anahash']))

            s = select([Anahash]).where(Anahash.anahash.in_(ahs))
            result = session.execute(s).fetchall()

            for ah in result:
                ah_mapping[ah[1]] = ah[0]
            pbar.update(chunk.shape[0])

    with tqdm(total=anahashes.shape[0], mininterval=2.0) as pbar:
        for wf, row in anahashes.iterrows():
            # SQLAlchemy doesn't allow the use of column names in update
            # statements, so we use something else.
            yield {'a_id': ah_mapping[row['anahash']], 'wf_id': wf_mapping[wf]}
            pbar.update(1)


def connect_anahashes_to_wordforms(session, anahashes, df, batch_size=50000):
    logger.info('Connecting anahashes to wordforms.')

    logger.debug('Getting wordform/anahash_id pairs.')
    anahash_to_wf_file = get_temp_file()
    t = write_json_lines(anahash_to_wf_file, get_anahashes(session, anahashes,
                                                           df))

    u = Wordform.__table__.update(). \
        where(Wordform.wordform_id == bindparam('wf_id')). \
        values(anahash_id=bindparam('a_id'))

    logger.debug('Adding the connections wordform -> anahash_id.')
    sql_query_batches(session, u, read_json_lines(anahash_to_wf_file), t,
                      batch_size)

    os.remove(anahash_to_wf_file)

    logger.info('Added the anahash of {} wordforms.'.format(t))

    return t


def update_anahashes(session, alphabet_file, tqdm=None, batch_size=50000):
    """Add anahashes for all wordforms that do not have an anahash value yet.

    Requires ticcl to be installed!

    Inputs:
        session: SQLAlchemy session object.
        alphabet_file (str): the path to the alphabet file for ticcl.
    """
    logger.info('Adding anahash values to wordforms without anahash.')
    df = get_word_frequency_df(session, add_ids=True)

    if df is None:
        logger.info('All wordforms have an anahash value.')
        return

    wf_mapping = df['wordform_id'].to_dict(defaultdict(int))

    anahashes = anahash_df(df[['frequency']], alphabet_file)

    bulk_add_anahashes(session, anahashes, tqdm=tqdm, batch_size=batch_size)

    connect_anahashes_to_wordforms(session, anahashes, wf_mapping, batch_size)


def write_wf_links_data(session, wf_mapping, links_df, wf_from_name,
                        wf_to_name, lexicon_id, wf_from_correct, wf_to_correct,
                        wfl_file, wfls_file):
    num_wf_links = 0
    num_wf_link_sources = 0
    with open(wfl_file, 'w') as links, open(wfls_file, 'w') as sources:
        wf_links = defaultdict(bool)
        for idx, row in tqdm(links_df.iterrows(), total=links_df.shape[0]):
            wf_from = wf_mapping[row[wf_from_name]]
            wf_to = wf_mapping[row[wf_to_name]]

            # Don't add links to self! and keep track of what was added,
            # because duplicates may occur
            if wf_from != wf_to and (wf_from, wf_to) not in wf_links:
                s = select([WordformLink]). \
                    where(and_(WordformLink.wordform_from == wf_from,
                               WordformLink.wordform_to == wf_to))
                r = session.execute(s).fetchone()
                if r is None:
                    # Both directions of the relationship need to be added.
                    links.write(json_line({'wordform_from': wf_from,
                                           'wordform_to': wf_to}))
                    links.write(json_line({'wordform_from': wf_to,
                                           'wordform_to': wf_from}))

                    num_wf_links += 2
                # The wordform link sources (in both directions) need to be
                # written regardless of the existence of the wordform links.
                line = json_line({'wordform_from': wf_from,
                                  'wordform_to': wf_to,
                                  'lexicon_id': lexicon_id,
                                  'wordform_from_correct': wf_from_correct,
                                  'wordform_to_correct': wf_to_correct})
                sources.write(line)
                line = json_line({'wordform_from': wf_to,
                                  'wordform_to': wf_from,
                                  'lexicon_id': lexicon_id,
                                  'wordform_from_correct': wf_to_correct,
                                  'wordform_to_correct': wf_from_correct})
                sources.write(line)
                num_wf_link_sources += 2

                wf_links[(wf_from, wf_to)] = True
                wf_links[(wf_to, wf_from)] = True

    return num_wf_links, num_wf_link_sources


def add_lexicon_with_links(session, lexicon_name, vocabulary, wfs, from_column,
                           to_column, from_correct, to_correct,
                           batch_size=50000):
    logger.info('Adding lexicon with links between wordforms.')

    # Make a dataframe containing all wordforms in the lexicon
    wordforms = pd.DataFrame()
    wordforms['wordform'] = wfs[from_column].append(wfs[to_column],
                                                    ignore_index=True)
    wordforms = wordforms.drop_duplicates(subset='wordform')

    # Create the lexicon (with all the wordforms)
    lexicon = add_lexicon(session, lexicon_name, vocabulary, wordforms,
                          batch_size=batch_size)

    wf_mapping = get_wf_mapping(session, lexicon_id=lexicon.lexicon_id)

    try:
        wfl_file = get_temp_file()
        logger.debug('Writing wordform links to add to "{}".'.format(wfl_file))

        wfls_file = get_temp_file()
        msg = 'Writing wordform link sources to add to "{}".'.format(wfls_file)
        logger.debug(msg)

        num_l, num_s = write_wf_links_data(session, wf_mapping, wfs,
                                           from_column, to_column,
                                           lexicon.lexicon_id,
                                           from_correct, to_correct,
                                           wfl_file, wfls_file)

        logger.info('Inserting {} wordform links.'.format(num_l))
        sql_insert_batches(session, WordformLink, read_json_lines(wfl_file),
                           batch_size=batch_size)

        logger.info('Inserting {} wordform link sources.'.format(num_s))
        sql_insert_batches(session, WordformLinkSource,
                           read_json_lines(wfls_file), batch_size=batch_size)
    finally:
        try:
            os.remove(wfl_file)
        except FileNotFoundError:
            pass
        try:
            os.remove(wfls_file)
        except FileNotFoundError:
            pass

    return lexicon


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
        n = bulk_add_wordforms(session, r, disable_pbar=True, batch_size=n_wfs)
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


def create_ticclat_database(delete_existing=False, dbname='ticclat', user="", passwd="", host="localhost"):
    db = MySQLdb.connect(user=user, passwd=passwd, host=host)
    engine = create_engine(f"mysql://{user}:{passwd}@{host}/{dbname}?charset=utf8mb4")

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

    # create tables
    Base.metadata.create_all(engine)
