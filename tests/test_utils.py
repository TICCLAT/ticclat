import pandas as pd

from ticclat.utils import chunk_df


def test_chunk_df_smaller_than_num():
    data = pd.DataFrame({'number': range(5)})

    i = 0
    for c in chunk_df(data):
        i += 1

    assert i == 1


def test_chunk_df_larger_than_num():
    data = pd.DataFrame({'number': range(5)})

    i = 0
    for c in chunk_df(data, num=1):
        i += 1

    assert i == 5
