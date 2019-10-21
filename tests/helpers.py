from pathlib import Path

import pandas


def load_test_db_data(dbsession):
    files = (Path(__file__).parent/'db_data').glob('./*.tsv')
    dbsession.execute('set foreign_key_checks=0;')
    for file in files:
        df = pandas.read_csv(file, sep='\t')
        df.to_sql(file.stem, if_exists='append', index=False, con=dbsession.bind)

    # note that the just inserted data is not checked for consistency, only new data/updates will be checked
    dbsession.execute('set foreign_key_checks=1;')
