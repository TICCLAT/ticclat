import os

import pandas as pd

from ticclat.utils import chunk_df, read_json_lines, write_json_lines, \
    json_line, iterate_wf, chunk_json_lines


def test_chunk_df_smaller_than_num():
    data = pd.DataFrame({'number': range(5)})

    i = 0
    for c in chunk_df(data):
        i += 1

    assert i == 1


def test_chunk_df_larger_than_num():
    data = pd.DataFrame({'number': range(5)})

    i = 0
    for c in chunk_df(data, 1):
        i += 1

    assert i == 5


def test_read_and_write_json_lines(fs):
    objects = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}, {'a': 5, 'b': 6}]

    fname = 'objects'

    f = open(fname, 'w')

    total = write_json_lines(f, objects)
    f.close()
    f = open(fname, 'r')

    assert os.path.exists(fname)
    assert total == len(objects)

    results = [o for o in read_json_lines(f)]
    f.close()

    assert objects == results


def test_read_and_write_json_lines_empty(fs):
    objects = []

    fname = 'objects'

    f = open(fname, 'w')

    write_json_lines(f, objects)

    f.close()
    f = open(fname, 'r')

    assert os.path.exists(fname)
    assert os.path.getsize(fname) == 0

    results = [o for o in read_json_lines(f)]
    f.close()

    assert objects == results


def test_json_line():
    obj = {'a': 1, 'b': 2}

    assert json_line(obj) == '{"a": 1, "b": 2}\n'


def test_iterate_wf():
    inp = ['wf1', 'wf2', 'wf3']
    outp = [{'wordform': 'wf1'}, {'wordform': 'wf2'}, {'wordform': 'wf3'}]

    res = list(iterate_wf(inp))

    assert outp == res


def test_chunk_json_lines_with_remainder(fs):
    out_file_path = "wordforms"
    out_file = open(out_file_path, 'w')
    write_json_lines(out_file, iterate_wf(["wf1", "wf2", "wf3", "wf4", "wf5"]))
    out_file.close()
    out_file = open(out_file_path, 'r')
    res = list(chunk_json_lines(out_file, 2))
    out_file.close()
    outp = [
        [{"wordform": "wf1"}, {"wordform": "wf2"}],
        [{"wordform": "wf3"}, {"wordform": "wf4"}],
        [{"wordform": "wf5"}],
    ]

    assert outp == res


def test_chunk_json_lines_without_remainder(fs):
    out_file_path = "wordforms"
    out_file = open(out_file_path, 'w')
    write_json_lines(out_file, iterate_wf(["wf1", "wf2", "wf3", "wf4"]))
    out_file.close()

    out_file = open(out_file_path, 'r')
    res = list(chunk_json_lines(out_file, 2))
    out_file.close()

    outp = [[{'wordform': 'wf1'}, {'wordform': 'wf2'}],
            [{'wordform': 'wf3'}, {'wordform': 'wf4'}]]

    assert outp == res
