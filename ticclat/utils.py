import logging
import os
import tempfile
import warnings
import json

import numpy as np
import pandas as pd

import sh

logger = logging.getLogger(__name__)


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
    logger.info('Running TICCL-anahash.')

    if wfreq.empty or wfreq is None:
        msg = 'Input "wfreq" is empty or None. Please input non-empty word ' \
              'frequency data.'
        warnings.warn(msg)

    # save word frequency data to temporary file
    (fd, tmpfile) = tempfile.mkstemp()
    os.close(fd)

    wfreq.to_csv(tmpfile, sep='\t', header=False)

    # run ticcl using sh
    try:
        sh.TICCL_anahash(['--list', '--alph', alphabet_file, tmpfile])
    except sh.ErrorReturnCode as e:
        raise(ValueError('Running TICCL-anahash failed: {}'.format(e.stdout)))

    # read anahashes and return dataframe
    anahashes = pd.read_csv('{}.list'.format(tmpfile), sep='\t', header=None,
                            names=['anahash'], index_col=0,
                            # Make sure 'null' is read as string and not NaN
                            keep_default_na=False)
    return anahashes


def chunk_df(df, num=1000):
    """Generator that returns about equally size chunks from a pandas DataFrame

    Inputs:
        df (DataFrame): the DataFrame to be chunked
        num (int, default 10000): the approximate number of records that will
            be in each chunk
    """
    if df.shape[0] > num:
        n = df.shape[0] // num
    else:
        n = 1

    for chunk in np.array_split(df, n):
        yield chunk


def write_json_lines(fname, generator):
    """Write a sequence of dictionaries to file, one dictionary per line

    This can be used when doing mass inserts (i.e., inserts not using the ORM)
    into the database. The data that will be inserted is written to file, so
    it can be read (using ``read_json_lines``) without using a lot of memory.

    Inputs:
        fname (str): Path to the file to save the data to
        generator (generator): Generator that produces objects to write to file

    Returns:
        int: the number of records written.
    """
    total = 0
    with open(fname, 'w') as f:
        for obj in generator:
            f.write(json.dumps(obj))
            f.write('\n')

            total += 1
    return total


def read_json_lines(fname):
    """Generator that reads a dictionary per line from a file

    This can be used when doing mass inserts (i.e., inserts not using the ORM)
    into the database. The data that will be inserted is written to file (using
    ``write_json_lines``), so it can be read and inserted into the database
    without using a lot of memory.

    Inputs:
        fname (str): Path to the file containing the data, one dictionary
            (JSON) object per line

    Returns:
        iterator over the lines in the input file
    """
    with open(fname) as f:
        for line in f:
            yield json.loads(line)


def get_temp_file():
    """Create a temporary file and return the path.

    Returns:
        Path to the temporary file.
    """
    (fd, fname) = tempfile.mkstemp()
    os.close(fd)

    return fname
