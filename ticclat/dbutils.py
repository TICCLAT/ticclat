"""
Collection of database access functions.
"""

import os
import re
import json
import logging
from pathlib import Path
import tempfile
from contextlib import contextmanager
from collections import defaultdict

import numpy as np
import pandas as pd

import sh
from tqdm import tqdm

from sqlalchemy import create_engine, select, bindparam, and_
from sqlalchemy.orm import sessionmaker

# for create_database:
import MySQLdb
from sqlalchemy_utils import database_exists
from sqlalchemy_utils.functions import drop_database

from ticclat.ticclat_schema import Base, Wordform, Lexicon, Anahash, \
    lexical_source_wordform, WordformLink, WordformLinkSource, \
    MorphologicalParadigm, WordformFrequencies
from ticclat.utils import chunk_df, anahash_df, write_json_lines, \
    read_json_lines, get_temp_file, json_line, split_component_code, \
    morph_iterator, preprocess_wordforms
from ticclat.sacoreutils import bulk_add_anahashes_core, sql_query_batches, sql_insert_batches

LOGGER = logging.getLogger(__name__)


# source: https://docs.sqlalchemy.org/en/latest/orm/session_basics.html
@contextmanager
def session_scope(session_maker):
    """Provide a transactional scope around a series of operations."""
    session = session_maker()
    try:
        yield session
        session.commit()
    except:  # noqa: E722
        session.rollback()
        raise
    finally:
        session.close()


def get_engine(without_database=False):
    """
    Create an sqlalchemy engine using the DATABASE_URL environment variable.
    """
    url = os.environ.get('DATABASE_URL')
    if without_database:
        url = url.replace(get_db_name(), "")
    engine = create_engine(url)
    return engine


def get_session_maker():
    """
    Return an sqlalchemy sessionmaker object using an engine from get_engine().
    """
    return sessionmaker(bind=get_engine())


def get_session():
    """
    Return an sqlalchemy session object using a sessionmaker from get_session_maker().
    """
    return get_session_maker()()


def get_db_name():
    """
    Get the database name from the DATABASE_URL environment variable.
    """
    database_url = os.environ.get('DATABASE_URL')
    return re.match(r'.*/(.*?)($|(\?.*$))', database_url).group(1)


def get_or_create_wordform(session, wordform, has_analysis=False, wordform_id=None):
    """
    Get a Wordform object of wordform.

    The Wordform object is an sqlalchemy field defined in the ticclat schema.
    It is coupled to the entry of the given wordform in the wordforms database
    table.
    """
    # does the wordform already exist?
    if wordform_id is not None:
        wordform_object = None
    else:
        query = session.query(Wordform)
        wordform_object = query.filter(Wordform.wordform == wordform).first()

    # first() can also return None, so still need to check
    if wordform_object is None:
        wordform_object = Wordform(wordform_id=wordform_id,
                                   wordform=wordform,
                                   has_analysis=has_analysis,
                                   wordform_lowercase=wordform.lower())
        session.add(wordform_object)

    return wordform_object


def bulk_add_wordforms(session, wfs, preprocess_wfs=True):
    """
    wfs is pandas DataFrame with the same column names as the database table,
    in this case just "wordform"
    """
    LOGGER.info('Bulk adding wordforms.')

    if preprocess_wfs:
        wfs = preprocess_wordforms(wfs)

    # remove empty entries
    wfs['wordform'].replace('', np.nan, inplace=True)
    wfs.dropna(subset=['wordform'], inplace=True)

    if not wfs['wordform'].is_unique:
        LOGGER.info('The wordform-column contains duplicate entries. '
                    'Removing duplicates.')
        wfs = wfs.drop_duplicates(subset='wordform')

    wfs['wordform_lowercase'] = wfs['wordform'].apply(lambda x: x.lower())

    file_handler, file_name = tempfile.mkstemp()
    os.close(file_handler)

    wfs.to_csv(file_name, header=False, index=False, sep='\t')

    query = f"""
    LOAD DATA LOCAL INFILE :file_name INTO TABLE wordforms (wordform, wordform_lowercase);
    """
    result = session.execute(query, {'file_name': file_name})

    os.unlink(file_name)

    LOGGER.info('%s wordforms have been added.', result.rowcount)

    return result.rowcount


def add_lexicon(session, lexicon_name, vocabulary, wfs, preprocess_wfs=True):
    """
    wfs is pandas DataFrame with the same column names as the database table,
    in this case just "wordform"
    """
    LOGGER.info('Adding lexicon.')

    bulk_add_wordforms(session, wfs, preprocess_wfs=preprocess_wfs)

    lexicon = Lexicon(lexicon_name=lexicon_name, vocabulary=vocabulary)
    session.add(lexicon)
    session.flush()
    lexicon_id = lexicon.lexicon_id

    LOGGER.debug('Lexicon id: %s', lexicon.lexicon_id)

    wordforms = list(wfs['wordform'])

    select_statement = select([Wordform]).where(Wordform.wordform.in_(wordforms))
    result = session.execute(select_statement).fetchall()

    LOGGER.info('Adding %s wordforms to the lexicon.', len(result))
    session.execute(
        lexical_source_wordform.insert(),  # noqa pylint: disable=E1120
                                           # this is a known pylint/sqlalchemy issue, see
                                           # https://github.com/sqlalchemy/sqlalchemy/issues/4656
        [{'lexicon_id': lexicon_id,
          'wordform_id': wf['wordform_id']} for wf in result]
    )

    LOGGER.info('Lexicon was added.')

    return lexicon


def get_word_frequency_df(session, add_ids=False):
    """Can be used as input for ticcl-anahash.

    Returns:
        Pandas DataFrame containing wordforms as index and a frequency value as
            column, or None if all wordforms in the database already are
            connected to an anahash value
    """
    LOGGER.info('Selecting wordforms without anahash value.')
    query = session.query(Wordform).filter(Wordform.anahash == None)  # noqa E711 pylint: disable=singleton-comparison
    if add_ids:
        query = query.with_entities(Wordform.wordform, Wordform.wordform_id)
    else:
        query = query.with_entities(Wordform.wordform)

    df = pd.read_sql(query.statement, query.session.bind)
    if df.empty:
        df = None
    else:
        df = df.set_index('wordform')
        df['frequency'] = 1

    return df


def get_wf_mapping(session, lexicon=None, lexicon_id=None):
    """
    Create a dictionary with a mapping of wordforms to wordform_id.

    The keys of the dictionary are wordforms, the values are the IDs
    of those wordforms in the database wordforms table.
    """
    msg = 'Getting the wordform mapping of lexicon "{}"'

    if lexicon is not None:
        if lexicon.lexicon_id is None:
            raise ValueError('The lexicon does not (yet) have an ID. Please'
                             ' make sure the ID of the lexicon is set.')
        lexicon_id = lexicon.lexicon_id
        msg = msg.format(lexicon)
    elif lexicon_id is not None:
        msg = msg.format('lexicon id {}'.format(lexicon_id))
    # Add option lexicon name and get the lexicon_id from the database
    else:
        raise ValueError('Please specify the lexicon.')

    LOGGER.info(msg)

    select_statement = select([lexical_source_wordform.join(Lexicon).join(Wordform)]) \
        .where(Lexicon.lexicon_id == lexicon_id)
    LOGGER.debug(select_statement)
    result = session.execute(select_statement).fetchall()

    wf_mapping = defaultdict(int)
    for row in result:
        wf_mapping[row['wordform']] = row[lexical_source_wordform.c.wordform_id]

    # Make sure a KeyError is raised, if we try to look up a word that is not
    # in the database (because we preprocessed it)
    wf_mapping.default_factory = None

    return wf_mapping


def bulk_add_anahashes(session, anahashes, tqdm_factory=None, batch_size=10000):
    """anahashes is pandas dataframe with the column wordform (index), anahash
    """
    LOGGER.info('Adding anahashes.')
    # Remove duplicate anahashes
    unique_hashes = anahashes.copy().drop_duplicates(subset='anahash')
    LOGGER.debug('The input data contains %s wordform/anahash pairs.', anahashes.shape[0])
    LOGGER.debug('There are %s unique anahash values.', unique_hashes.shape[0])

    count_added = 0

    with get_temp_file() as anahashes_to_add_file:
        if tqdm_factory is not None:
            pbar = tqdm_factory(total=unique_hashes.shape[0])
        for chunk in chunk_df(unique_hashes, batch_size=batch_size):
            # Find out which anahashes are not yet in the database.
            ahs = set(list(chunk['anahash']))

            select_statement = select([Anahash]).where(Anahash.anahash.in_(ahs))
            result = session.execute(select_statement).fetchall()

            existing_ahs = {row[1] for row in result}

            for non_existing_ah in ahs.difference(existing_ahs):
                anahashes_to_add_file.write(json.dumps({'anahash': non_existing_ah}))
                anahashes_to_add_file.write('\n')
                count_added += 1
            if tqdm_factory is not None:
                pbar.update(chunk.shape[0])
        if tqdm_factory is not None:
            pbar.close()

        bulk_add_anahashes_core(session, read_json_lines(anahashes_to_add_file))

    LOGGER.info('Added %s anahashes.', count_added)

    return count_added


def get_anahashes(session, anahashes, wf_mapping, batch_size=50000):
    """
    Generator of dictionaries with anahash ID and wordform ID pairs.

    Given `anahashes`, a dataframe with wordforms and corresponding anahashes,
    yield dictionaries containing two entries each: key 'a_id' has the value
    of the anahash ID in the database, key 'wf_id' has the value of the
    wordform ID in the database.
    """
    unique_hashes = anahashes.copy().drop_duplicates(subset='anahash')

    with tqdm(total=unique_hashes.shape[0], mininterval=2.0) as pbar:
        ah_mapping = {}

        for chunk in chunk_df(unique_hashes, batch_size=batch_size):
            # Find out which anahashes are not yet in the database.
            ahs = set(list(chunk['anahash']))

            select_statement = select([Anahash]).where(Anahash.anahash.in_(ahs))
            result = session.execute(select_statement).fetchall()

            for row in result:
                ah_mapping[row[1]] = row[0]
            pbar.update(chunk.shape[0])

    with tqdm(total=anahashes.shape[0], mininterval=2.0) as pbar:
        for wordform, row in anahashes.iterrows():
            # SQLAlchemy doesn't allow the use of column names in update
            # statements, so we use something else.
            yield {'a_id': ah_mapping[row['anahash']], 'wf_id': wf_mapping[wordform]}
            pbar.update(1)


def connect_anahashes_to_wordforms(session, anahashes, df, batch_size=50000):
    """
    Create the relation between wordforms and anahashes in the database.

    Given `anahashes`, a dataframe with wordforms and corresponding anahashes,
    create the relations between the two in the wordforms and anahashes tables
    by setting the anahash_id foreign key in the wordforms table.
    """
    LOGGER.info('Connecting anahashes to wordforms.')

    LOGGER.debug('Getting wordform/anahash_id pairs.')
    with get_temp_file() as anahash_to_wf_file:
        total_lines_written = write_json_lines(anahash_to_wf_file,
                                               get_anahashes(session, anahashes, df))

        update_statement = Wordform.__table__.update(). \
            where(Wordform.wordform_id == bindparam('wf_id')). \
            values(anahash_id=bindparam('a_id'))

        LOGGER.debug('Adding the connections wordform -> anahash_id.')
        sql_query_batches(session, update_statement, read_json_lines(anahash_to_wf_file),
                          total_lines_written, batch_size)

    LOGGER.info('Added the anahash of %s wordforms.', total_lines_written)

    return total_lines_written


def update_anahashes_new(session, alphabet_file):
    """
    Add anahashes for all wordforms that do not have an anahash value yet.

    Requires ticcl to be installed!

    Inputs:
        session: SQLAlchemy session object.
        alphabet_file (str): the path to the alphabet file for ticcl.
    """
    tmp_file_path = str(Path(tempfile.tempdir) / 'mysql/wordforms.csv')

    LOGGER.info("Exporting wordforms to file")
    if os.path.exists(tmp_file_path):
        os.remove(tmp_file_path)

    session.execute(f"""
SELECT wordform, 1 INTO OUTFILE '{tmp_file_path}'
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
FROM wordforms
WHERE anahash_id IS NULL;
    """)

    LOGGER.info("Generating anahashes")
    try:
        sh.TICCL_anahash(['--list', '--alph', alphabet_file, tmp_file_path])
    except sh.ErrorReturnCode as exception:
        raise ValueError('Running TICCL-anahash failed: {}'.format(exception.stdout))

    ticcled_file_path = tmp_file_path + '.list'

    # drop old table if it's there
    session.execute("DROP TABLE IF EXISTS ticcl_import")

    # create temp table
    session.execute("""
CREATE TEMPORARY TABLE ticcl_import (
    wordform VARCHAR(255),
    anahash BIGINT
);
    """)

    LOGGER.info("Loading ticcled file into temp table")
    session.execute("""
LOAD DATA LOCAL INFILE :file_path INTO TABLE ticcl_import
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
(wordform, anahash)
    """, {'file_path': ticcled_file_path})

    if os.path.exists(tmp_file_path):
        os.remove(tmp_file_path)

    LOGGER.info("Storing new anahashes")
    session.execute("""INSERT IGNORE INTO anahashes(anahash) SELECT anahash FROM ticcl_import""")

    LOGGER.info("Setting wordform anahash_ids")
    session.execute("""
UPDATE ticcl_import
LEFT JOIN wordforms ON ticcl_import.wordform = wordforms.wordform
LEFT JOIN anahashes ON ticcl_import.anahash = anahashes.anahash
SET wordforms.anahash_id = anahashes.anahash_id WHERE 1
    """)


def update_anahashes(session, alphabet_file, tqdm_factory=None, batch_size=50000):
    """Add anahashes for all wordforms that do not have an anahash value yet.

    Requires ticcl to be installed!

    Inputs:
        session: SQLAlchemy session object.
        alphabet_file (str): the path to the alphabet file for ticcl.
    """
    LOGGER.info('Adding anahash values to wordforms without anahash.')
    df = get_word_frequency_df(session, add_ids=True)

    if df is None:
        LOGGER.info('All wordforms have an anahash value.')
        return

    wf_mapping = df['wordform_id'].to_dict(defaultdict(int))

    anahashes = anahash_df(df[['frequency']], alphabet_file)

    bulk_add_anahashes(session, anahashes, tqdm_factory=tqdm_factory, batch_size=batch_size)

    connect_anahashes_to_wordforms(session, anahashes, wf_mapping, batch_size)


def write_wf_links_data(session, wf_mapping, links_df, wf_from_name,
                        wf_to_name, lexicon_id, wf_from_correct, wf_to_correct,
                        links_file, sources_file, add_columns=None):
    """
    Write wordform links (obtained from lexica) to JSON files for later processing.

    Two JSON files will be written to: `links_file` and `sources_file`. The links
    file contains only the wordform links and corresponds to the wordform_links
    database table. The sources file contains the source lexicon of each link and
    also whether either wordform is considered a "correct" form or not, which is
    defined by the lexicon (whether it is a "dictionary" with only correct words
    or a correction list with correct words in one column and incorrect ones in the
    other).
    """
    if add_columns is None:
        add_columns = []
    num_wf_links = 0
    num_wf_link_sources = 0
    wf_links = defaultdict(bool)
    for _, row in tqdm(links_df.iterrows(), total=links_df.shape[0]):
        wf_from = wf_mapping[row[wf_from_name]]
        wf_to = wf_mapping[row[wf_to_name]]

        # Don't add links to self! and keep track of what was added,
        # because duplicates may occur
        if wf_from != wf_to and (wf_from, wf_to) not in wf_links:
            select_statement = select([WordformLink]). \
                where(and_(WordformLink.wordform_from == wf_from, WordformLink.wordform_to == wf_to))
            result = session.execute(select_statement).fetchone()
            if result is None:
                # Both directions of the relationship need to be added.
                links_file.write(json_line({'wordform_from': wf_from,
                                            'wordform_to': wf_to}))
                links_file.write(json_line({'wordform_from': wf_to,
                                            'wordform_to': wf_from}))

                num_wf_links += 2
            # The wordform link sources (in both directions) need to be
            # written regardless of the existence of the wordform links.
            link_source = {'wordform_from': wf_from,
                           'wordform_to': wf_to,
                           'lexicon_id': lexicon_id,
                           'wordform_from_correct': wf_from_correct,
                           'wordform_to_correct': wf_to_correct}
            for column in add_columns:
                link_source[column] = row[column]
            line = json_line(link_source)
            sources_file.write(line)
            link_source = {'wordform_from': wf_to,
                           'wordform_to': wf_from,
                           'lexicon_id': lexicon_id,
                           'wordform_from_correct': wf_to_correct,
                           'wordform_to_correct': wf_from_correct}
            for column in add_columns:
                link_source[column] = row[column]
            line = json_line(link_source)
            sources_file.write(line)
            num_wf_link_sources += 2

            wf_links[(wf_from, wf_to)] = True
            wf_links[(wf_to, wf_from)] = True

    return num_wf_links, num_wf_link_sources


def add_lexicon_with_links(session, lexicon_name, vocabulary, wfs, from_column,
                           to_column, from_correct, to_correct,
                           batch_size=50000, preprocess_wfs=True, to_add=None):
    """
    Add wordforms from a lexicon with links to the database.

    Lexica with links contain wordform pairs that are linked. The `wfs`
    dataframe must contain two columns: the `from_column` and the `to_column`,
    which contains the two words of each pair (per row). Using the arguments
    `from_correct` and `to_correct`, you can indicate whether the columns of
    this dataframe contain correct words or not (boolean). Typically, there
    are two types of linked lexica: True + True, meaning it links correct
    wordforms (e.g. morphological variants) or True + False, meaning it links
    correct wordforms to incorrect ones (e.g. a spelling correction list).
    """
    LOGGER.info('Adding lexicon with links between wordforms.')

    if to_add is None:
        to_add = []

    # Make a dataframe containing all wordforms in the lexicon
    wordforms = pd.DataFrame()
    wordforms['wordform'] = wfs[from_column].append(wfs[to_column],
                                                    ignore_index=True)
    wordforms = wordforms.drop_duplicates(subset='wordform')

    # Create the lexicon (with all the wordforms)
    lexicon = add_lexicon(session, lexicon_name, vocabulary, wordforms,
                          preprocess_wfs=preprocess_wfs)

    wf_mapping = get_wf_mapping(session, lexicon_id=lexicon.lexicon_id)

    if preprocess_wfs:
        wfs = preprocess_wordforms(wfs, columns=[from_column, to_column])

    with get_temp_file() as wfl_file:
        LOGGER.debug('Writing wordform links to add to (possibly unnamed) temporary file.')

        with get_temp_file() as wfls_file:
            LOGGER.debug('Writing wordform link sources to add to (possibly unnamed) temporary file.')

            num_l, num_s = write_wf_links_data(session, wf_mapping, wfs,
                                               from_column, to_column,
                                               lexicon.lexicon_id,
                                               from_correct, to_correct,
                                               wfl_file, wfls_file,
                                               add_columns=to_add)

            LOGGER.info('Inserting %s wordform links.', num_l)
            sql_insert_batches(session, WordformLink, read_json_lines(wfl_file),
                               batch_size=batch_size)

            LOGGER.info('Inserting %s wordform link sources.', num_s)
            sql_insert_batches(session, WordformLinkSource,
                               read_json_lines(wfls_file), batch_size=batch_size)

    return lexicon


def add_morphological_paradigms(session, in_file):
    """
    Add morphological paradigms to database from CSV file.
    """
    data = pd.read_csv(in_file, sep='\t', index_col=False,
                       names=['wordform', 'corpus_freq', 'component_codes',
                              'human_readable_c_code', 'first_year', 'last_year',
                              'dict_ids', 'pos_tags', 'int_ids'])
    # drop first row (contains empty wordform)
    data = data.drop([0])

    # store wordforms for in database
    wfs = data[['wordform']].copy()
    bulk_add_wordforms(session, wfs)

    # get the morphological variants from the pandas dataframe
    LOGGER.info('extracting morphological variants')
    morph_paradigms_per_wordform = defaultdict(list)
    with tqdm(total=data.shape[0]) as pbar:
        for row in data.iterrows():
            codes = row[1]['component_codes'].split('#')
            wordform = row[1]['wordform']
            for code in codes:
                morph_paradigms_per_wordform[wordform].append(split_component_code(code, wordform))
            pbar.update()

    LOGGER.info('Looking up wordform ids.')
    select_statement = select([Wordform]).where(Wordform.wordform.in_(wfs['wordform']))
    mapping = session.execute(select_statement).fetchall()

    LOGGER.info('Writing morphological variants to file.')
    with get_temp_file() as mp_file:
        total_lines_written = write_json_lines(mp_file, morph_iterator(morph_paradigms_per_wordform, mapping))
        LOGGER.info('Wrote %s morphological variants.', total_lines_written)
        LOGGER.info('Inserting morphological variants to the database.')
        sql_insert_batches(session, MorphologicalParadigm,
                           read_json_lines(mp_file), batch_size=50000)


def create_ticclat_database(delete_existing=False):
    """
    Create the TICCLAT database.

    Sets the proper encoding settings and uses the schema to create tables.
    """
    # db = MySQLdb.connect(user=user, passwd=passwd, host=host)
    # engine = create_engine(f"mysql://{user}:{passwd}@{host}/{dbname}?charset=utf8mb4")
    engine = get_engine(without_database=True)
    connection = engine.connect()
    db_name = get_db_name()
    try:
        connection.execute(f"CREATE DATABASE {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;")
    except MySQLdb.ProgrammingError as exception:
        if database_exists(engine.url):
            if not delete_existing:
                raise Exception(f"Database `{db_name}` already exists, delete it first before recreating.")
            drop_database(engine.url)
            connection.execute(f"CREATE DATABASE {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;")
        else:
            raise exception

    connection.close()

    engine = get_engine()

    # create tables
    Base.metadata.create_all(engine)


def empty_table(session, table_class):
    """
    Empty a database table.

    - table_class: the ticclat_schema class corresponding to the table
    """
    row = session.query(table_class).first()

    if row is not None:
        LOGGER.info('Table "%s" is not empty.', table_class.__table__.name)
        LOGGER.info('Deleting rows...')
        Base.metadata.drop_all(bind=session.get_bind(),
                               tables=[table_class.__table__])
        Base.metadata.create_all(bind=session.get_bind(),
                                 tables=[table_class.__table__])


def create_wf_frequencies_table(session):
    """
    Create wordform_frequencies table in the database.

    The text_attestations frequencies are summed and stored in this table.
    This can be used to save time when needing total-database frequencies.
    """
    LOGGER.info('Creating wordform_frequencies table.')
    # Make sure the wordform_frequency table exists (create it if it doesn't)
    Base.metadata.create_all(session.get_bind(),
                             tables=[WordformFrequencies.__table__])

    empty_table(session, WordformFrequencies)

    session.execute("""
INSERT INTO wordform_frequency
SELECT
       wordforms.wordform_id,
       wordforms.wordform,
       SUM(frequency) AS frequency
FROM
     wordforms LEFT JOIN text_attestations ta ON wordforms.wordform_id = ta.wordform_id
GROUP BY wordforms.wordform, wordforms.wordform_id
    """)


def add_ticcl_variants(session, name, df, **kwargs):
    """
    Add TICCL variants as a linked lexicon.
    """
    lexicon = add_lexicon_with_links(session,
                                     lexicon_name=name,
                                     vocabulary=False,
                                     wfs=df,
                                     from_column='ocr_variant',
                                     to_column='correction_candidate',
                                     from_correct=False,
                                     to_correct=True,
                                     preprocess_wfs=False,
                                     to_add=['ld'],
                                     **kwargs)
    return lexicon
