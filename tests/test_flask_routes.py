import pytest
from urllib.parse import urlencode


def test_root(flask_test_client):
    response = flask_test_client.get('/')
    assert response.status_code == 200
    expected_set = {
        "/", "/corpora", "/corrections", "/lemmas_for_wordform/<word_form>", "/lexica/<word_name>",
        "/morphological_variants_for_lemma/<paradigm_id>", "/network/<wordform>", "/paradigm_count",
        "/plots/corpus_size", "/plots/lexicon_size", "/plots/paradigm_size", "/plots/word_count_per_year",
        "/regexp_search/<regexp>", "/static/<path:filename>", "/suffixes/<suffix_1>", "/suffixes/<suffix_1>/<suffix_2>",
        "/tables", "/tables/<table_name>", "/ticcl_variants/<word_form>", "/variants/<word_name>",
        "/variants/<word_name>/0/<end_year>", "/variants/<word_name>/<start_year>",
        "/variants/<word_name>/<start_year>/<end_year>", "/variants_by_wxyz", "/word/<word_name>",
        "/word_frequency_per_corpus/<word_name>", "/word_frequency_per_corpus_per_year/<word_name>",
        "/word_frequency_per_corpus_per_year/<word_name>/0/<end_year>",
        "/word_frequency_per_corpus_per_year/<word_name>/<start_year>",
        "/word_frequency_per_corpus_per_year/<word_name>/<start_year>/<end_year>",
        "/word_frequency_per_year/<word_name>", "/word_type_codes", "/year_range"
    }

    assert set(response.json) == expected_set


def test_corpora(flask_test_client):
    response = flask_test_client.get('/corpora')
    assert response.status_code == 200
    response_list = response.json
    assert len(response_list) == 2
    first_corpus = response_list[0]
    assert first_corpus['name'] == 'Dummy corpus 1'
    assert first_corpus['document_count'] > 0
    assert first_corpus['word_count'] > 0


@pytest.mark.parametrize("wordform_input,expected", [
    ("aandacht", [{'W': 1, 'X': 1, 'Y': 1, 'Z': 1, 'paradigm_id': 1, 'wordform': 'aandacht'}]),
    ("drmoedaris", [{'W': 2, 'X': 1, 'Y': 1, 'Z': 1, 'paradigm_id': 3, 'wordform': 'dromedaris'}]),
    ("iederene", [{'W': 3, 'X': 1, 'Y': 2, 'Z': 1, 'paradigm_id': 9, 'wordform': 'iedereen'}]),
    ("blaa123", []),
])
def test_lemmas_for_wordform(flask_test_client, wordform_input, expected):
    response = flask_test_client.get(f'/lemmas_for_wordform/{wordform_input}')
    assert response.status_code == 200
    assert response.json == expected


# lexicon 1 = not vocabulary
# lexicon 2 = vocabulary
@pytest.mark.parametrize("wordform_input,expected", [
    ("idonotexist", [{'correct': None,  'has_wordform': False, 'lexicon_name': 'DUMMY.LEXICON.1'},
                     {'correct': None,  'has_wordform': False, 'lexicon_name': 'DUMMY.LEXICON.2'}
                     ]),
    ("aandacht",    [{'correct': None,  'has_wordform': False, 'lexicon_name': 'DUMMY.LEXICON.1'},
                     {'correct': None,  'has_wordform': False, 'lexicon_name': 'DUMMY.LEXICON.2'}
                     ]),
    ("foxtrot",     [{'correct': None,  'has_wordform': False, 'lexicon_name': 'DUMMY.LEXICON.1'},
                     {'correct': True,  'has_wordform': True,  'lexicon_name': 'DUMMY.LEXICON.2'}
                     ]),
    ("dromedaris",  [{'correct': True,  'has_wordform': True,  'lexicon_name': 'DUMMY.LEXICON.1'},
                     {'correct': True,  'has_wordform': True,  'lexicon_name': 'DUMMY.LEXICON.2'}
                     ]),
    ("drmoedaris",  [{'correct': False, 'has_wordform': True,  'lexicon_name': 'DUMMY.LEXICON.1'},
                     {'correct': None,  'has_wordform': False,  'lexicon_name': 'DUMMY.LEXICON.2'}
                     ]),
])
def test_lexica(flask_test_client, wordform_input, expected):
    response = flask_test_client.get(f'/lexica/{wordform_input}')
    assert response.status_code == 200
    assert response.json == expected


# lexicon 1 = not vocabulary
# lexicon 2 = vocabulary
@pytest.mark.parametrize("paradigm_id,expected", [
    (1, [{'V': 1, 'frequency': 5.0, 'max_year': 1980.0, 'min_year': 1510.0, 'num_corpora': 2, 'num_lexica': 0,
          'num_paradigms': 1, 'word_type_code': 'HCL', 'wordform': 'aandacht', 'wordform_id': 1
          }]),
    (3, [
        {'V': 1, 'frequency': 4.0, 'max_year': 1510.0, 'min_year': 1510.0, 'num_corpora': 1, 'num_lexica': 2,
         'num_paradigms': 1, 'word_type_code': 'HCL', 'wordform': 'dromedaris', 'wordform_id': 3},
        {'V': 2, 'frequency': 4.0, 'max_year': 1510.0, 'min_year': 1510.0, 'num_corpora': 1, 'num_lexica': 1,
         'num_paradigms': 1, 'word_type_code': 'HCT', 'wordform': 'drmoedaris', 'wordform_id': 4}
    ]),
])
def test_morphological_variants_for_lemma(flask_test_client, paradigm_id, expected):
    response = flask_test_client.get(f'/morphological_variants_for_lemma/{paradigm_id}')
    assert response.status_code == 200
    assert response.json == expected


def test_network(flask_test_client):
    response = flask_test_client.get('/network/dromedaris')
    assert response.status_code == 200
    response_dict = response.json
    assert len(response_dict['links']) == 5
    assert len(response_dict['nodes']) == 6


def test_paradigm_count(flask_test_client):
    response = flask_test_client.get('/paradigm_count')
    assert response.status_code == 200
    expected_response = [
        {'X': 1, 'Y': 1, 'Z': 1, 'num_paradigms': 5},
        {'X': 1, 'Y': 2, 'Z': 1, 'num_paradigms': 4},
        {'X': 2, 'Y': 1, 'Z': 1, 'num_paradigms': 1}
    ]
    assert response.json == expected_response


def test_tables(flask_test_client):
    response = flask_test_client.get('/tables')
    assert response.status_code == 200
    response_list = response.json
    assert set(response_list) == {
        'anahashes', 'corpora', 'corpusId_x_documentId', 'documents', 'external_links', 'lexica',
        'lexical_source_wordform', 'morphological_paradigms', 'source_x_wordform_link', 'text_attestations',
        'ticcl_variants', 'wordform_frequency', 'wordform_links', 'wordforms'
    }


def test_table_wordforms(flask_test_client):
    response = flask_test_client.get('/tables/wordforms')
    assert response.status_code == 200
    response_dict = response.json
    expected_dict = {
        'anahash_id': 'BIGINT(20)', 'wordform': 'VARCHAR(255)', 'wordform_id': 'BIGINT(20)',
        'wordform_lowercase': 'VARCHAR(255)'
    }
    assert response_dict == expected_dict


@pytest.mark.parametrize("plot_name", ["corpus_size", "lexicon_size", "paradigm_size", "word_count_per_year"])
def test_bokeh_plots(flask_test_client, plot_name):
    response = flask_test_client.get(f'/plots/{plot_name}')
    assert response.status_code == 200
    assert 'doc' in response.json


@pytest.mark.parametrize("wordform,start_year,end_year,expected", [
    ('asdasdasd', 0, 2000, {'metadata': {'max_freq': 0.0, 'max_year': 0, 'min_freq': 0.0, 'min_year': 0,
                                         'overall_max_year': 2000, 'overall_min_year': 1510
                                         }, 'paradigms': [], 'wordform': 'asdasdasd'}),
    ('aandacht', 0, 2000, {'metadata': {'max_freq': 1.5163640319967973e-06,
                                        'max_year': 1510, 'min_freq': 1.5163640319967973e-06, 'min_year': 1510,
                                        'overall_max_year': 2000, 'overall_min_year': 1510},
                           'paradigms': [{'lemma': 'aandacht', 'paradigm_code': 'Z0001Y0001X0001W00000001',
                                          'variants': [{'V': 1, 'anahash': 10000000000,
                                                        'corpora': [{'frequencies': [{'freq': 1.5163640319967973e-06,
                                                                                      'term_frequency': 4.0,
                                                                                      'total': 2637889.0,
                                                                                      'year': 1510}],
                                                                     'name': 'Dummy corpus 2'}],
                                                        'frequency': 3, 'word_type_code': 'HCL',
                                                        'word_type_number': None,
                                                        'wordform': 'aandacht'}]}],
                           'wordform': 'aandacht'}),
])
def test_variants(flask_test_client, wordform, start_year, end_year, expected):
    response = flask_test_client.get(f'/variants/{wordform}/{start_year}/{end_year}')
    assert response.status_code == 200
    assert response.json == expected


@pytest.mark.parametrize("wxyz,expected", [
    ({'w': 1, 'x': 1, 'y': 1, 'z': 1}, [{'frequency': 3, 'wordform': 'aandacht'}]),
    ({'w': 3, 'x': 1, 'y': 2, 'z': 1}, [{'frequency': 4, 'wordform': 'iedereen'}]),
])
def test_variants_by_wxyz(flask_test_client, wxyz, expected):
    response = flask_test_client.get(f'/variants_by_wxyz?{urlencode(wxyz)}')
    assert response.status_code == 200
    assert response.json == expected


@pytest.mark.parametrize("wordform,expected", [
    ('aandacht', {'anahash_variants': ['aandacht'], 'lexicon_variants': [], 'morph_variants': ['aandacht']}),
    ('drmoedaris', {'anahash_variants': ['dromedaris', 'drmoedaris'], 'lexicon_variants': [],
                    'morph_variants': ['dromedaris', 'drmoedaris']}),
    ('idonotexist', {'anahash_variants': [], 'lexicon_variants': [], 'morph_variants': []}),
])
def test_word(flask_test_client, wordform, expected):
    response = flask_test_client.get(f'/word/{wordform}')
    assert response.status_code == 200
    assert response.json == expected


@pytest.mark.parametrize("wordform,expected", [
    ('aandacht', [{'corpus_name': 'Dummy corpus 1', 'relative_frequency': 34658.44106332097},
                  {'corpus_name': 'Dummy corpus 2', 'relative_frequency': 1516.3640319967974}]),
    ('drmoedaris', [{'corpus_name': 'Dummy corpus 2', 'relative_frequency': 1516.3640319967974}]),
    ('idonotexist', []),
])
def test_word_frequency_per_corpus(flask_test_client, wordform, expected):
    response = flask_test_client.get(f'/word_frequency_per_corpus/{wordform}')
    assert response.status_code == 200
    assert response.json == expected


@pytest.mark.parametrize("wordform", ['aandacht', 'drmoedaris', 'idonotexist'])
def test_word_frequency_per_corpus_per_year(flask_test_client, wordform):
    response = flask_test_client.get(f'/word_frequency_per_corpus_per_year/{wordform}')
    assert response.status_code == 200
    response_dict = response.json
    assert 'corpora' in response_dict
    assert 'metadata' in response_dict
    assert 'wordform' in response_dict


@pytest.mark.parametrize("wordform,expected", [
    ('aandacht', [{'normalized_frequency': 1516.3640319967974, 'year': 1510.0},
                  {'normalized_frequency': 34658.44106332097, 'year': 1980.0}]),
    ('drmoedaris', [{'normalized_frequency': 1516.3640319967974, 'year': 1510.0}]),
    ('idonotexist', []),
])
def test_word_frequency_per_year(flask_test_client, wordform, expected):
    response = flask_test_client.get(f'/word_frequency_per_year/{wordform}')
    assert response.status_code == 200
    assert response.json == expected


def test_word_type_codes(flask_test_client):
    response = flask_test_client.get('/word_type_codes')
    assert response.status_code == 200
    assert set(response.json) == {'HCL', 'HCT'}


def test_corrections(flask_test_client):
    response = flask_test_client.get('/corrections/wanneer')
    assert response.status_code == 200
    expected_response = {'corrections': [{'frequency': 1, 'levenshtein_distance': 3, 'wordform': '-A-nee:r'},
                                         {'frequency': 1, 'levenshtein_distance': 3, 'wordform': '-Janneef'},
                                         {'frequency': 1, 'levenshtein_distance': 4, 'wordform': '-S.Vanneer'},
                                         {'frequency': 1, 'levenshtein_distance': 3, 'wordform': '-Wanfeen'},
                                         {'frequency': 85, 'levenshtein_distance': 1, 'wordform': '-Wanneer'},
                                         {'frequency': 1, 'levenshtein_distance': 2, 'wordform': '-Wanneet'},
                                         {'frequency': 1, 'levenshtein_distance': 2, 'wordform': '-Wan„eer'},
                                         {'frequency': 3, 'levenshtein_distance': 1, 'wordform': '-anneer'},
                                         {'frequency': 1, 'levenshtein_distance': 3, 'wordform': '-annexh'},
                                         {'frequency': 1, 'levenshtein_distance': 4, 'wordform': '-ar.neei'}],
                         'metadata': {'max_freq': 0.0, 'max_year': 0, 'min_freq': 0.0, 'min_year': 0,
                                      'overall_max_year': 1930, 'overall_min_year': 1510},
                         'paradigms': [], 'wordform': 'wanneer'}

    assert response.json == expected_response
