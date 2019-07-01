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

