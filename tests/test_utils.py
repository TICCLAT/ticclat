import os

import pandas as pd

from ticclat.utils import chunk_df, read_json_lines, write_json_lines


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


def test_read_and_write_json_lines(fs):
    objects = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}, {'a': 5, 'b': 6}]

    fname = 'objects'

    write_json_lines(fname, objects)

    assert os.path.exists(fname)

    results = [o for o in read_json_lines(fname)]

    assert objects == results
