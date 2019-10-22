import json


def test_word_type_codes(flask_test_client):
    response = flask_test_client.get('/word_type_codes')
    assert response.status_code == 200
    response_list = json.loads(response.data)
    assert set(response_list) == {'HCL', 'HCT'}


def test_tables(flask_test_client):
    response = flask_test_client.get('/tables')
    assert response.status_code == 200
    response_list = json.loads(response.data)
    assert set(response_list) == {
        'anahashes', 'corpora', 'corpusId_x_documentId', 'documents', 'external_links', 'lexica',
        'lexical_source_wordform', 'morphological_paradigms', 'source_x_wordform_link', 'text_attestations',
        'ticcl_variants', 'wordform_frequency', 'wordform_links', 'wordforms'
    }


def test_corpora(flask_test_client):
    response = flask_test_client.get('/corpora')
    assert response.status_code == 200
    response_list = json.loads(response.data)
    assert set(response_list) == {'Dummy corpus 1', 'Dummy corpus 2'}
