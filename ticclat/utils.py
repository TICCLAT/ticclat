"""
Non-database related utility functions for TICCLAT.
"""

import logging
import os
import tempfile
import warnings
import json
import time
import copy
import re

import numpy as np
import pandas as pd

import sh

LOGGER = logging.getLogger(__name__)


def anahash_df(wfreq, alphabet_file):
    """Get anahash values for word frequency data.

    The result can be used to add anahash values to the database
    (ticclat.dbutils.bulk_add_anahashes) and connect wordforms to anahash
    values (ticclat.dbutils.connect_anahases_to_wordforms).

    Inputs:
        wfreq (pandas DataFrame): Dataframe containing word frequency data (the
                                  result of
                                  ticcl.dbutils.get_word_frequency_df)
        alphabet_file (str): path to the ticcl alphabet file to use

    Returns:
        pandas DataFrame containing the word forms as index and anahash values
        as column.
    """
    LOGGER.info('Running TICCL-anahash.')

    if wfreq.empty or wfreq is None:
        msg = 'Input "wfreq" is empty or None. Please input non-empty word ' \
              'frequency data.'
        warnings.warn(msg)

    # save word frequency data to temporary file
    (file_descriptor, tmpfile) = tempfile.mkstemp()
    os.close(file_descriptor)

    wfreq.to_csv(tmpfile, sep='\t', header=False)

    # run ticcl using sh
    try:
        sh.TICCL_anahash(['--list', '--alph', alphabet_file, tmpfile])
    except sh.ErrorReturnCode as exception:
        raise ValueError('Running TICCL-anahash failed: {}'.format(exception.stdout))

    # read anahashes and return dataframe
    anahashes = pd.read_csv('{}.list'.format(tmpfile), sep='\t', header=None,
                            names=['anahash'], index_col=0,
                            # Make sure 'null' is read as string and not NaN
                            keep_default_na=False)
    return anahashes


def chunk_df(df, batch_size=1000):
    """Generator that returns about equally size chunks from a pandas DataFrame

    Inputs:
        df (DataFrame): the DataFrame to be chunked
        batch_size (int, default 10000): the approximate number of records that will
            be in each chunk
    """
    if df.shape[0] > batch_size:
        num_sections = df.shape[0] // batch_size
    else:
        num_sections = 1

    for chunk in np.array_split(df, num_sections):
        yield chunk


def write_json_lines(file_handle, generator):
    """Write a sequence of dictionaries to file, one dictionary per line

    This can be used when doing mass inserts (i.e., inserts not using the ORM)
    into the database. The data that will be inserted is written to file, so
    it can be read (using ``read_json_lines``) without using a lot of memory.

    Inputs:
        file_handle: File handle of the file to save the data to
        generator (generator): Generator that produces objects to write to file

    Returns:
        int: the number of records written.
    """
    total = 0
    for obj in generator:
        file_handle.write(json_line(obj))

        total += 1
    return total


def json_line(obj):
    """Convert an object `obj` to a string containing a line of JSON."""
    return f'{json.dumps(obj)}\n'


def count_lines(file_handle):
    """From https://stackoverflow.com/q/845058/1199693"""
    file_handle.seek(0)
    i = 0
    for i, _ in enumerate(file_handle):
        pass
    return i + 1


def read_json_lines(file_handle):
    """Generator that reads a dictionary per line from a file

    This can be used when doing mass inserts (i.e., inserts not using the ORM)
    into the database. The data that will be inserted is written to file (using
    ``write_json_lines``), so it can be read and inserted into the database
    without using a lot of memory.

    Inputs:
        file_handle: File handle of the file containing the data, one dictionary
                     (JSON) object per line

    Returns:
        iterator over the lines in the input file
    """
    file_handle.seek(0)
    for line in file_handle:
        yield json.loads(line)


def chunk_json_lines(file_handle, batch_size=1000):
    """Read a JSON file and yield lines in batches."""
    res = []
    i = 0
    for obj in read_json_lines(file_handle):
        res.append(obj)
        i += 1
        if i == batch_size:
            yield res
            res = []
            i = 0
    if res != []:
        yield res


def get_temp_file():
    """Create a temporary file and its file handle.

    Returns:
        File handle of the temporary file.
    """
    file_handle = tempfile.TemporaryFile(mode='w+')
    return file_handle


def iterate_wf(lst):
    """Generator that yields `{'wordform': value}` for all values in `lst`."""
    for wordform in lst:
        yield {'wordform': wordform}


def split_component_code(code, wordform):
    """
    Split morphological paradigm code into its components.

    Morphological paradigm codes in Reynaert's encoding scheme consist of 8
    subcomponents. These are returned as separate entries of a dictionary from
    this function.
    """
    regex = r'Z(?P<Z>\d{4})Y(?P<Y>\d{4})X(?P<X>\d{4})W(?P<W>\d{8})V(?P<V>\d{4})_(?P<wt_code>\w{3})(?P<wt_num>\d{3})?'
    match = re.search(regex, code)
    if match:
        wt_num = None
        if match.group('wt_num'):
            wt_num = int(match.group('wt_num'))
        return {'Z': int(match.group('Z')),
                'Y': int(match.group('Y')),
                'X': int(match.group('X')),
                'W': int(match.group('W')),
                'V': int(match.group('V')),
                'word_type_code': match.group('wt_code'),
                'word_type_number': wt_num,
                'wordform': wordform}
    return None


def morph_iterator(morph_paradigms_per_wordform, mapping):
    """
    Generator that yields dicts of morphological paradigm code components plus wordform_id in the database.

    Inputs:
        morph_paradigms_per_wordform: dictionary with wordforms (keys) and
                                      lists (values) of dictionaries of code
                                      components (return values of
                                      `split_component_code`).
        mapping: iterable of named tuples / dictionaries that contain the
                 result of a query on the wordforms table, i.e. fields
                 'wordform' and 'wordform_id'.
    """
    for wordform in mapping:
        for code in morph_paradigms_per_wordform[wordform['wordform']]:
            if code is not None:  # ignore incomplete codes for now
                code_copy = copy.copy(code)

                code_copy['wordform_id'] = wordform['wordform_id']

                # we don't need the wordform
                del code_copy['wordform']
                yield code_copy


def set_logger(level='INFO'):
    """Configure logging format and level."""
    logging.basicConfig(format="%(asctime)s [%(process)d] %(levelname)-8s "
                        "%(name)s,%(lineno)s\t%(message)s")
    logging.getLogger().setLevel(level)


def preprocess_wordforms(wfs, columns=None):
    """
    Clean wordforms in dataframe `wfs`.

    Strips whitespace, replaces underscores with asterisks (misc character)
    and spaces with underscores.
    """
    if columns is None:
        columns = ['wordform']
    LOGGER.info("Preprocessing the wordforms...")
    for col in columns:
        # remove whitespace from wordforms
        wfs[col] = wfs[col].str.strip()

        # replace underscores with asterisk
        # underscore means space and asterisk means misc character
        wfs[col] = wfs[col].str.replace('_', '*')

        # replace spaces with underscores
        wfs[col] = wfs[col].str.replace(' ', '_')

    return wfs


def timeit(method):
    """Decorator for timing methods.

    Can be used for benchmarking queries.

    Source: https://medium.com/pythonhive/fa04cb6bb36d
    """
    def timed(*args, **kw):
        time_start = time.time()
        result = method(*args, **kw)
        time_end = time.time()

        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((time_end - time_start) * 1000)
        else:
            LOGGER.info('{} took {:.2f} ms'.format(method.__name__,
                                                   (time_end - time_start) * 1000))
        return result

    return timed


def read_ticcl_variants_file(fname):
    """Return dataframe containing data in TICCL variants file."""
    df = pd.read_csv(fname, sep='#', header=None, engine='python')
    df.columns = ['ocr_variant', 'corpus_frequency', 'correction_candidate',
                  '?1', 'ld', '?2', 'anahash']
    return df
