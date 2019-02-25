import pytest
import os
import sqlite3

import numpy as np
import pandas as pd

from ticclat.ticclat_schema import Wordform, Lexicon, Anahash
from ticclat.dbutils import bulk_add_wordforms, add_lexicon, \
    get_word_frequency_df, bulk_add_anahashes, connect_anahases_to_wordforms, \
    update_anahashes

from . import data_dir


# Make sure np.int64 are inserted into the testing database as integers and
# not binary data.
# Source: https://stackoverflow.com/questions/38753737
sqlite3.register_adapter(np.int64, int)


def test_bulk_add_wordforms_all_new(dbsession):
    wfs = pd.DataFrame()
    wfs['wordform'] = ['wf1', 'wf2', 'wf3']

    print(dbsession)

    bulk_add_wordforms(dbsession, wfs, disable_pbar=True)

    wordforms = dbsession.query(Wordform).order_by(Wordform.wordform_id).all()

    assert len(wordforms) == len(wfs['wordform'])
    assert [wf.wordform for wf in wordforms] == list(wfs['wordform'])


def test_bulk_add_wordforms_some_new(dbsession):
    wfs = pd.DataFrame()
    wfs['wordform'] = ['wf1', 'wf2', 'wf3']

    print(dbsession)

    bulk_add_wordforms(dbsession, wfs, disable_pbar=True)

    wfs['wordform'] = ['wf3', 'wf4', 'wf5']
    wfs['wordform_lowercase'] = ['wf3', 'wf4', 'wf4']

    n = bulk_add_wordforms(dbsession, wfs, disable_pbar=True)

    assert n == 2

    wrdfrms = dbsession.query(Wordform).order_by(Wordform.wordform_id).all()

    assert len(wrdfrms) == 5
    assert [w.wordform for w in wrdfrms] == ['wf1', 'wf2', 'wf3', 'wf4', 'wf5']


def test_bulk_add_wordforms_not_unique(dbsession):
    wfs = pd.DataFrame()
    wfs['wordform'] = ['wf1', 'wf1', 'wf2']

    print(dbsession)

    with pytest.raises(ValueError):
        bulk_add_wordforms(dbsession, wfs, disable_pbar=True)

    wrdfrms = dbsession.query(Wordform).order_by(Wordform.wordform_id).all()

    assert len(wrdfrms) == 0


def test_add_lexicon(dbsession):
    name = 'test lexicon'

    wfs = pd.DataFrame()
    wfs['wordform'] = ['wf1', 'wf2', 'wf3']

    print(dbsession)

    add_lexicon(dbsession, lexicon_name=name, vocabulary=True, wfs=wfs)

    wrdfrms = dbsession.query(Wordform).order_by(Wordform.wordform_id).all()

    assert len(wrdfrms) == 3

    lexicons = dbsession.query(Lexicon).all()

    assert len(lexicons) == 1
    assert lexicons[0].lexicon_name == name
    assert len(lexicons[0].lexicon_wordforms) == 3

    wrdfrms = sorted([w.wordform for w in lexicons[0].lexicon_wordforms])
    assert wrdfrms == list(wfs['wordform'])


def test_get_word_frequency_df(dbsession):
    wfs = pd.DataFrame()
    wfs['wordform'] = ['wf1', 'wf2', 'wf3']

    bulk_add_wordforms(dbsession, wfs, disable_pbar=True)

    freq_df = get_word_frequency_df(dbsession)

    expected = pd.DataFrame({'wordform': ['wf1', 'wf2', 'wf3'],
                             'frequency': [1, 1, 1]}).set_index('wordform')

    assert freq_df.equals(expected)


def test_get_word_frequency_df_empty(dbsession):
    freq_df = get_word_frequency_df(dbsession)

    assert freq_df is None


def test_bulk_add_anahashes(dbsession):
    wfs = pd.DataFrame()
    wfs['wordform'] = ['wf1', 'wf2', 'wf3']

    bulk_add_wordforms(dbsession, wfs, disable_pbar=True)

    a = pd.DataFrame({'wordform': ['wf1', 'wf2', 'wf3'],
                      'anahash': [1, 2, 3]}).set_index('wordform')

    bulk_add_anahashes(dbsession, a)

    ahs = dbsession.query(Anahash).order_by(Anahash.anahash_id).all()

    print(ahs[0])

    assert [a.anahash for a in ahs] == list(a['anahash'])


def test_connect_anahases_to_wordforms(dbsession):
    wfs = pd.DataFrame()
    wfs['wordform'] = ['wf1', 'wf2', 'wf3']

    bulk_add_wordforms(dbsession, wfs, disable_pbar=True)

    a = pd.DataFrame({'wordform': ['wf1', 'wf2', 'wf3'],
                      'anahash': [1, 2, 3]}).set_index('wordform')

    bulk_add_anahashes(dbsession, a)

    connect_anahases_to_wordforms(dbsession, a)

    wrdfrms = dbsession.query(Wordform).order_by(Wordform.wordform_id).all()

    assert [wf.anahash.anahash for wf in wrdfrms] == list(a['anahash'])


def test_connect_anahases_to_wordforms_empty(dbsession):
    wfs = pd.DataFrame()
    wfs['wordform'] = ['wf1', 'wf2', 'wf3']

    bulk_add_wordforms(dbsession, wfs, disable_pbar=True)

    a = pd.DataFrame({'wordform': ['wf1', 'wf2', 'wf3'],
                      'anahash': [1, 2, 3]}).set_index('wordform')

    bulk_add_anahashes(dbsession, a)

    connect_anahases_to_wordforms(dbsession, a)

    # nothing was updated the second time around
    # (and there is no error when running this)
    t = connect_anahases_to_wordforms(dbsession, a)

    assert t == 0


@pytest.mark.skip(reason='Install TICCL before testing this.')
@pytest.mark.datafiles(os.path.join(data_dir(), 'alphabet'))
def test_update_anahashes(dbsession, datafiles):
    wfs = pd.DataFrame()
    wfs['wordform'] = ['wf-a', 'wf-b', 'wf-c']

    alphabet_file = datafiles.listdir()[0]

    bulk_add_wordforms(dbsession, wfs, disable_pbar=True)

    update_anahashes(dbsession, alphabet_file)

    wrdfrms = dbsession.query(Wordform).order_by(Wordform.wordform_id).all()
    anahashes = dbsession.query(Anahash).order_by(Anahash.anahash_id).all()

    # Three anahashes were added
    assert len(anahashes) == 3

    # The anahases are connected to the correct wordforms
    for wf, a in zip(wrdfrms, anahashes):
        assert wf.anahash_id == a.anahash_id


@pytest.mark.datafiles(os.path.join(data_dir(), 'alphabet'))
def test_update_anahashes_nothing_to_update(dbsession, datafiles):
    wfs = pd.DataFrame()
    wfs['wordform'] = ['wf1', 'wf2', 'wf3']

    bulk_add_wordforms(dbsession, wfs, disable_pbar=True)

    a = pd.DataFrame({'wordform': ['wf1', 'wf2', 'wf3'],
                      'anahash': [1, 2, 3]}).set_index('wordform')

    bulk_add_anahashes(dbsession, a)

    connect_anahases_to_wordforms(dbsession, a)

    alphabet_file = datafiles.listdir()[0]
    update_anahashes(dbsession, alphabet_file)

    wrdfrms = dbsession.query(Wordform).order_by(Wordform.wordform_id).all()

    assert [wf.anahash.anahash for wf in wrdfrms] == list(a['anahash'])
