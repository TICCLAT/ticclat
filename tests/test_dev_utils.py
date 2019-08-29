import pytest

import pandas as pd

from sqlalchemy import and_
from sqlalchemy.sql import select

from ticclat.ticclat_schema import Wordform, Lexicon, WordformLinkSource, lexical_source_wordform
from ticclat.dbutils import add_lexicon, add_lexicon_with_links
from ticclat.dev_utils import delete_lexicon


def test_delete_lexicon(dbsession):
    name = 'test lexicon'

    wfs = pd.DataFrame()
    wfs['wordform'] = ['wf1', 'wf2', 'wf3']

    lex = add_lexicon(dbsession, lexicon_name=name, vocabulary=True, wfs=wfs)
    print('lexicon id:', lex.lexicon_id)

    delete_lexicon(dbsession, lex.lexicon_id)
    dbsession.commit()

    # Wordforms should not be deleted
    wrdfrms = dbsession.query(Wordform).all()
    assert len(wrdfrms) == 3

    r = dbsession.execute(select([Lexicon])).fetchall()
    result = [row for row in r]
    print(result)
    assert len(result) == 0

    # Lexicon should be deleted
    lexica = dbsession.query(Lexicon).all()
    assert len(lexica) == 0

    # Wordforms are not associated with a lexicon
    # (i.e., lexical_source_wordform is empty)
    for wf in wrdfrms:
        print('Wordform:', wf.wordform)
        print('Wordform lexica:', wf.wf_lexica)
        assert wf.wf_lexica == []
    r = dbsession.execute(select([lexical_source_wordform])).fetchall()
    result = [row for row in r]
    print(len(result))
    assert len(result) == 0


def test_delete_lexicon_with_links(dbsession):
    name = 'linked test lexicon'

    wfs = pd.DataFrame()
    wfs['lemma'] = ['wf1', 'wf2', 'wf3']
    wfs['variant'] = ['wf1s', 'wf2s', 'wf3s']

    lex = add_lexicon_with_links(dbsession,
                                 lexicon_name=name,
                                 vocabulary=True,
                                 wfs=wfs,
                                 from_column='lemma',
                                 to_column='variant',
                                 from_correct=True,
                                 to_correct=True)

    delete_lexicon(dbsession, lex.lexicon_id)

    # Wordforms should not be deleted
    wrdfrms = dbsession.query(Wordform).all()
    assert len(wrdfrms) == 6

    # Lexicon should be deleted
    lexica = dbsession.query(Lexicon).all()
    assert len(lexica) == 0

    # Wordforms are not associated with a lexicon
    # (i.e., source_x_wordform_link is empty)
    for wf in wrdfrms:
        assert wf.wf_lexica == []

    for w1, w2 in zip(wfs['lemma'], wfs['variant']):
        wf1 = dbsession.query(Wordform).filter(Wordform.wordform == w1).first()
        wf2 = dbsession.query(Wordform).filter(Wordform.wordform == w2).first()

        # Wordform links should not be deleted
        links = [w.linked_to for w in wf1.links]
        assert wf2 in links

        links = [w.linked_to for w in wf2.links]
        assert wf1 in links

        # Wordform link sources should be deleted
        wfls = dbsession.query(WordformLinkSource).all()
        assert len(wfls) == 0
