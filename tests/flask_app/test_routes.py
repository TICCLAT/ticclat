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


def test_table_wordforms(flask_test_client):
    response = flask_test_client.get('/tables/wordforms')
    assert response.status_code == 200
    response_dict = json.loads(response.data)
    expected_dict = {
        'anahash_id': 'BIGINT(20)', 'wordform': 'VARCHAR(255)', 'wordform_id': 'BIGINT(20)',
        'wordform_lowercase': 'VARCHAR(255)'
    }
    assert response_dict == expected_dict


def test_corpora(flask_test_client):
    response = flask_test_client.get('/corpora')
    assert response.status_code == 200
    response_list = json.loads(response.data)
    assert len(response_list) == 2
    first_corpus = response_list[0]
    assert first_corpus['name'] == 'Dummy corpus 1'
    assert first_corpus['document_count'] > 0
    assert first_corpus['word_count'] > 0
