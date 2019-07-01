SET @lookup_word = 'kopen';
SELECT *
FROM (SELECT wf2.wordform_id, wf2.wordform, 'anahash' AS source
      FROM wordforms
               LEFT JOIN wordforms AS wf2 ON wordforms.anahash_id = wf2.anahash_id
      WHERE wordforms.wordform = @lookup_word
     ) AS words_anahash
UNION
SELECT *
FROM (
         SELECT wordforms.wordform_id,
                wordforms.wordform,
                CONCAT('lexicon ', source_x_wordform_link.lexicon_id) AS source
         FROM wordform_links
                  LEFT JOIN wordforms ON wordform_links.wordform_to = wordforms.wordform_id
                  LEFT JOIN source_x_wordform_link
                            ON wordform_links.wordform_from = source_x_wordform_link.wordform_from and
                               wordform_links.wordform_to = source_x_wordform_link.wordform_to
         WHERE wordform_links.wordform_from = (SELECT wordform_id FROM wordforms WHERE wordform = @lookup_word)
     ) AS words_lexicon