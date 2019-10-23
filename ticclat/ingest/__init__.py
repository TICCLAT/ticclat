"""
TICCLAT module for ingesting data into the database.

The module contains separate functions for each data set. The run function
can be used as a default entrypoint for either ingesting everything or only
a specified selection for testing or later additions.
"""

import logging
from ticclat.utils import set_logger
from ticclat.ingest import elex, gb, opentaal, sonar, inl, sgd, edbo, \
    twente_spelling_correction_list, dbnl, morph_par, wf_frequencies, \
    sgd_ticcl_variants, ticcl_variants
from ticclat.dbutils import get_db_name, update_anahashes_new


LOGGER = logging.getLogger(__name__)


ALL_SOURCES = {
    'twente': twente_spelling_correction_list,
    'inl': inl,
    'SoNaR500': sonar,
    'elex': elex,
    'groene boekje': gb,
    'OpenTaal': opentaal,
    'sgd': sgd,
    'edbo': edbo,
    'dbnl': dbnl,
    'morph_par': morph_par,
    'wf_freqs': wf_frequencies,
    # 'sgd_ticcl': sgd_ticcl_variants
    'ticcl_variants': ticcl_variants
}


def ingest_all(session, base_dir='/data',
               include=None, exclude=None, **kwargs):
    """
    Ingest all data sources into the database.

    Pass lists of source identifiers (the keys in ALL_SOURCES) to
    include or exclude kwargs (not both at the same time) to specify
    which sources to ingest. If include is used, only the sources in
    that list will be ingested. For exclude, all sources except those
    in the list will be ingested. The sources are expected to be in
    a certain format under one central base directory specfied using
    base_dir. See the specific ingestion functions for more on how
    the sources should be organized.
    """
    if include is None:
        include = []
    if exclude is None:
        exclude = []

    if len(include) > 0 and len(exclude) > 0:
        raise Exception("ingest_all: Don't use include and exclude at the same time!")
    elif len(include) > 0:
        sources = {k: ALL_SOURCES[k] for k in include}
    elif len(exclude) > 0:
        sources = {k: v for k, v in ALL_SOURCES.items() if k not in exclude}
    else:
        sources = ALL_SOURCES

    for name, source in sources.items():
        LOGGER.info('ingesting %s...', name)
        source.ingest(session, base_dir=base_dir, **kwargs)


def run(reset_db=False,
        alphabet_file="/data/ALPH/nld.aspell.dict.clip20.lc.LD3.charconfus.clip20.lc.chars",
        batch_size=5000, include=None, exclude=None, ingest=True, anahash=True,
        tmpdir="/data/tmp", loglevel="INFO", reset_anahashes=False, **kwargs):
    """
    Ingest data sources into the database.

    Arguments:
    - reset_db: if True, the entire database will be removed and an empty one
                recreated.
    - alphabet_file: the alphabet file used by TICCL.
    - batch_size: used by some functions to batch ingestion into the database
                  to avoid memory issues.
    - include/exclude: see documentation for `ingest_all`.
    - ingest: if True, `ingest_all` is called to ingest all data sources.
    - anahash: if True, anagram hashes are added for all (new) wordforms.
    - tmpdir: some ingestion functions use temporary files for bulk ingestion.
              The path of these files can be given here.
    - loglevel: set logging level, see `utils.set_logger`
    - reset_anahashes: if True, will remove all existing anahashes before
                       adding new ones.
    - **kwargs: are passed on to `ingest_all`, see there for more options.
    """
    if include is None:
        include = []
    if exclude is None:
        exclude = []

    from ticclat.dbutils import create_ticclat_database, get_session_maker, update_anahashes, session_scope
    from ticclat.ticclat_schema import Anahash

    import tempfile

    set_logger(loglevel)

    tempfile.tempdir = tmpdir

    if reset_db:
        LOGGER.info('Reseting database "%s".', get_db_name())
        create_ticclat_database(delete_existing=True)

    session_maker = get_session_maker()

    if ingest:
        ingest_all(session_maker, batch_size=batch_size, include=include,
                   exclude=exclude, **kwargs)

    if reset_anahashes:
        LOGGER.info("removing all existing anahashes...")
        with session_scope(session_maker) as session:
            num_rows_deleted = session.query(Anahash).delete()
        LOGGER.info("removed %s anahashes", num_rows_deleted)

    if anahash:
        LOGGER.info("adding anahashes...")
        with session_scope(session_maker) as session:
            update_anahashes_new(session, alphabet_file)
