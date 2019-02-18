import pytest

import pandas as pd

from ticclat.ticclat_schema import Wordform
from ticclat.dbutils import bulk_add_wordforms


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
