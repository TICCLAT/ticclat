def query_word_frequency_per_year(corpus_id: int):
    return f"""
SELECT (1e9 * SUM(frequency)/SUM(word_count)) AS normalized_frequency, ROUND(
CASE
    WHEN pub_year IS NOT NULL THEN pub_year
    ELSE (year_from + year_to) / 2
END) AS year
FROM text_attestations
    LEFT JOIN documents ON text_attestations.document_id = documents.document_id
    LEFT JOIN corpusId_x_documentId on documents.document_id = corpusId_x_documentId.document_id
WHERE wordform_id = (SELECT wordform_id FROM wordforms WHERE wordform = %(lookup_word)s)
{f"AND corpus_id={corpus_id}" if corpus_id else ""}
GROUP BY year
HAVING year IS NOT NULL
ORDER BY year ASC
"""


def query_word_frequency_per_corpus():
    return """
SELECT 1e9 * SUM(frequency) / SUM(word_count) AS relative_frequency, c.name as corpus_name
FROM text_attestations
    LEFT JOIN documents ON text_attestations.document_id = documents.document_id
    LEFT JOIN corpusId_x_documentId cIxdI on documents.document_id = cIxdI.document_id
    LEFT JOIN corpora c on cIxdI.corpus_id = c.corpus_id
WHERE wordform_id = (SELECT wordform_id FROM wordforms WHERE wordform = %(lookup_word)s)
GROUP BY c.corpus_id
    """


def query_word_links():
    return """
SELECT    wordforms.wordform,
          lexicon_name
FROM      wordform_links
LEFT JOIN wordforms
ON        wordform_links.wordform_to = wordforms.wordform_id
LEFT JOIN source_x_wordform_link
ON        wordform_links.wordform_from = source_x_wordform_link.wordform_from
AND       wordform_links.wordform_to = source_x_wordform_link.wordform_to
LEFT JOIN lexica
ON        source_x_wordform_link.lexicon_id = lexica.lexicon_id
WHERE     wordform_links.wordform_from =
          (
                 SELECT wordform_id
                 FROM   wordforms
                 WHERE  wordform = %(lookup_word)s
          )
"""


def query_anahash_links():
    return """
SELECT wf2.wordform
FROM wordforms
       LEFT JOIN wordforms AS wf2 ON wordforms.anahash_id = wf2.anahash_id
WHERE wordforms.wordform = %(lookup_word)s
"""


def query_morph_links():
    return """
SELECT wordform FROM morphological_paradigms AS m1 LEFT JOIN morphological_paradigms AS m2 ON m1.X = m2.X AND m1.Y = m2.Y AND m1.Z = m2.Z AND m1.W = m2.W
    LEFT JOIN wordforms w on m1.wordform_id = w.wordform_id
WHERE m2.wordform_id = (SELECT wordform_id FROM wordforms WHERE wordform = %(lookup_word)s);
"""


def find_lemmas_for_wordform():
    return """
SELECT paradigm_id, wordform, W, X, Y, Z FROM morphological_paradigms
    LEFT JOIN wordforms on morphological_paradigms.wordform_id = wordforms.wordform_id
    WHERE (Z,Y,X,W) IN (
        SELECT Z, Y, X, W
        FROM morphological_paradigms
                 LEFT JOIN wordforms w on morphological_paradigms.wordform_id = w.wordform_id
        WHERE wordform = %(lookup_word)s
    )
    AND word_type_code = 'HCL'
    """


def find_morphological_variants_for_lemma():
    return """
SELECT V,
       W,
       X,
       Y,
       Z,
       word_type_code,
       wordform,
       wordform_id,
       COUNT(DISTINCT corpus_id) AS num_corpora,
       COUNT(DISTINCT lexicon_id) AS num_lexica,
       MIN(year) AS min_year,
       MAX(year) AS max_year,
       COUNT(DISTINCT code) AS num_paradigms,
       SUM(frequency) AS frequency
FROM (SELECT mp2.V,
             mp2.W,
             mp2.X,
             mp2.Y,
             mp2.Z,
             mp2.word_type_code,
             wordform,
             wordforms.wordform_id,
             d.document_id,
             c.corpus_id,
             lsw.lexicon_id,
             ta.frequency,
             (mp3.W + mp3.X + mp3.Y + mp3.Z) AS code,
             CASE
                 WHEN d.pub_year IS NOT NULL THEN d.pub_year
                 ELSE ROUND((d.year_from + d.year_to) / 2)
             END
             AS year
      FROM morphological_paradigms mp1
               LEFT JOIN morphological_paradigms mp2 ON
                mp1.W = mp2.W AND
                mp1.X = mp2.X AND
                mp1.Y = mp2.Y AND
                mp1.Z = mp2.Z
               LEFT JOIN wordforms ON mp2.wordform_id = wordforms.wordform_id
               LEFT JOIN text_attestations ta on wordforms.wordform_id = ta.wordform_id
               LEFT JOIN documents d on ta.document_id = d.document_id
               LEFT JOIN corpusId_x_documentId cIxdI on d.document_id = cIxdI.document_id
               LEFT JOIN corpora c on cIxdI.corpus_id = c.corpus_id
               LEFT JOIN lexical_source_wordform lsw on wordforms.wordform_id = lsw.wordform_id
               LEFT JOIN morphological_paradigms mp3 ON wordforms.wordform_id = mp3.wordform_id
      WHERE mp1.paradigm_id = %(paradigm_id)s
     ) AS wordform_stats
GROUP BY V, W, X, Y, Z, word_type_code, wordform, wordform_id
"""


def fill_wordform_frequency_table():
    return """
INSERT INTO wordform_frequency(wordform_id, wordform, frequency)
SELECT wordforms.wordform_id       AS wordform_id,
       wordforms.wordform          AS wordform,
       COALESCE(SUM(frequency), 0) AS frequency
FROM wordforms
         LEFT JOIN text_attestations ON wordforms.wordform_id = text_attestations.wordform_id
GROUP BY wordforms.wordform_id
    """


def get_frequent_x_for_zy():
    return """
SELECT X, SUM(frequency) AS sum_freq
FROM morphological_paradigms
         LEFT JOIN wordforms w2 ON morphological_paradigms.wordform_id = w2.wordform_id
         LEFT JOIN wordform_frequency ON w2.wordform_id = wordform_frequency.wordform_id
WHERE Y = %(Y)s
AND Z = %(Z)s
AND word_type_code = 'HCL'
GROUP BY X
ORDER BY sum_freq DESC
LIMIT 50
    """


def get_min_dist_x_xyz():
    return """
SELECT X, SUM(frequency) AS sum_freq
FROM morphological_paradigms
         LEFT JOIN wordforms w2 ON morphological_paradigms.wordform_id = w2.wordform_id
         LEFT JOIN wordform_frequency ON w2.wordform_id = wordform_frequency.wordform_id
WHERE Y = %(Y)s
AND Z = %(Z)s
AND word_type_code = 'HCL'
GROUP BY X
ORDER BY ABS(X - %(X)s) ASC
LIMIT 50
    """


def get_wxyz():
    return """
SELECT W,X,Y,Z
FROM morphological_paradigms mp1
         LEFT JOIN wordforms w on mp1.wordform_id = w.wordform_id
WHERE w.wordform LIKE %(wordform)s
    """


# def get_most_frequent_lemmas_for_xlist():
#     return """
# SELECT mp.X, mp.wordform_id, wordform, frequency FROM morphological_paradigms mp
#
# LEFT JOIN wordform_frequency ON mp.wordform_id = wordform_frequency.wordform_id
# RIGHT JOIN
#   (
#       SELECT X, MAX(frequency) AS max_freq
#       FROM morphological_paradigms
#                LEFT JOIN wordform_frequency
#                          ON morphological_paradigms.wordform_id = wordform_frequency.wordform_id
#       WHERE Z = %(Z)s
#         AND Y = %(Y)s
#         AND X IN %(x_list)s
#         AND word_type_code = 'HCL'
#       GROUP BY X
#   ) t1 ON t1.X = mp.X AND t1.max_freq = frequency
# """


def get_most_frequent_lemmas_for_xyz():
    return """
SELECT X, W, morphological_paradigms.wordform_id, frequency, wordform FROM morphological_paradigms
LEFT JOIN wordform_frequency
ON morphological_paradigms.wordform_id = wordform_frequency.wordform_id
WHERE Z = %(Z)s
AND Y = %(Y)s
AND X = %(X)s
AND word_type_code = 'HCL'
ORDER BY frequency DESC
LIMIT %(limit)s
"""
