from pathlib import Path

from tests.helpers import load_test_db_data


def test_load_db_data(dbsession):
    # load the data
    load_test_db_data(dbsession)

    # assert that numbers of rows correspond to number of rows in tsv files
    files = (Path(__file__).parent / 'db_data').glob('./*.tsv')
    for file in files:
        r = dbsession.execute(f"SELECT * FROM {file.stem}")
        with open(file) as f:
            num_lines = sum(1 for _ in f)
            # assert DB contains num of lines + 1 (head with column names)
            assert num_lines == r.rowcount + 1


def test_db_data_no_load(dbsession):
    # assert that DB is empty without load_test_db_data call
    files = (Path(__file__).parent / 'db_data').glob('./*.tsv')
    for file in files:
        r = dbsession.execute(f"SELECT * FROM {file.stem}")
        assert r.rowcount == 0
