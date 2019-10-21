from ticclat.utils import set_logger
from ticclat.ingest import elex, gb, opentaal, sonar, inl, sgd, edbo, \
    twente_spelling_correction_list, dbnl, morph_par, wf_frequencies, \
    sgd_ticcl_variants, ticcl_variants
from ticclat.dbutils import get_db_name, update_anahashes_new
import logging


logger = logging.getLogger(__name__)


all_sources = {
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
               include=[], exclude=[], **kwargs):
    if len(include) > 0 and len(exclude) > 0:
        raise Exception("ingest_all: Don't use include and exclude at the same time!")
    elif len(include) > 0:
        sources = {k: all_sources[k] for k in include}
    elif len(exclude) > 0:
        sources = {k: v for k, v in all_sources.items() if k not in exclude}
    else:
        sources = all_sources

    for name, source in sources.items():
        logger.info('ingesting ' + name + '...')
        source.ingest(session, base_dir=base_dir, **kwargs)


def run(reset_db=False,
        alphabet_file="/data/ALPH/nld.aspell.dict.clip20.lc.LD3.charconfus.clip20.lc.chars",
        batch_size=5000, include=[], exclude=[], ingest=True, anahash=True,
        tmpdir="/data/tmp", loglevel="INFO", reset_anahashes=False, **kwargs):

    # Read information to connect to the database and put it in environment variables
    import os
    from ticclat.dbutils import create_ticclat_database, get_session_maker, update_anahashes, session_scope
    from ticclat.ticclat_schema import Anahash

    from tqdm import tqdm
    import tempfile

    set_logger(loglevel)

    tempfile.tempdir = tmpdir

    if reset_db:
        logger.info(f'Reseting database "{get_db_name()}".')
        create_ticclat_database(delete_existing=True)

    session_maker = get_session_maker()

    if ingest:
        ingest_all(session_maker, batch_size=batch_size, include=include,
                   exclude=exclude, **kwargs)

    if reset_anahashes:
        logger.info("removing all existing anahashes...")
        with session_scope(session_maker) as session:
            num_rows_deleted = session.query(Anahash).delete()
        logger.info(f"removed {num_rows_deleted} anahashes")

    if anahash:
        logger.info("adding anahashes...")
        with session_scope(session_maker) as session:
            update_anahashes_new(session, alphabet_file)
