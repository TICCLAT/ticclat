from ticclat.ingest import elex, gb, opentaal, sonar, twente_spelling_correction_list, inl
import logging


logger = logging.getLogger(__name__)


all_sources = {
    'twente': twente_spelling_correction_list,
    'inl': inl,
    'SoNaR500': sonar,
    'elex': elex,
    'groene boekje': gb,
    'OpenTaal': opentaal,
}


def ingest_all(session, base_dir='/data',
               include=[], exclude=[], **kwargs):
    if len(include) > 0 and len(exclude) > 0:
        raise Exception("ingest_all: Don't use include and exclude at the same time!")
    elif len(include) > 0:
        sources = {k: all_sources[k] for k in include}
    elif len(exclude) > 0:
        sources = {k: v for k, v in all_sources if k not in exclude}
    else:
        sources = all_sources

    for name, source in sources.items():
        logger.info('ingesting ' + name + '...')
        source.ingest(session, base_dir=base_dir, **kwargs)


def run(envvars_path='ENVVARS.txt', db_name='ticclat', reset_db=False,
        alphabet_file='/data/ticcl/nld.aspell.dict.lc.chars', batch_size=5000,
        include=[], exclude=[], ingest=True, anahash=True, **kwargs):
    # Read information to connect to the database and put it in environment variables
    import os
    from ticclat.dbutils import create_ticclat_database, get_session, update_anahashes, session_scope
    from tqdm import tqdm

    with open(envvars_path) as f:
        for line in f:
            parts = line.split('=')
            if len(parts) == 2:
                os.environ[parts[0]] = parts[1].strip()
    
    os.environ['dbname'] = db_name
    if 'host' not in os.environ.keys():
        os.environ['host'] = 'localhost'

    if reset_db:
        create_ticclat_database(delete_existing=True, dbname=os.environ['dbname'],
                                user=os.environ['user'], passwd=os.environ['password'],
                                host=os.environ['host'])

    Session = get_session(os.environ['user'], os.environ['password'], os.environ['dbname'])

    if ingest:
        ingest_all(Session, batch_size=batch_size, include=include,
                exclude=exclude, **kwargs)

    if anahash:
        logger.info("adding anahashes...")
        with session_scope(Session) as session:
            update_anahashes(session, alphabet_file, tqdm, batch_size)
