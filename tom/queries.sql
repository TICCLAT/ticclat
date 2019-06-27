# SELECT anahash_id, COUNT(anahash_id) AS cnt FROM wordforms GROUP BY anahash_id ORDER BY cnt DESC




# cnt   wf_from wf
# 49	13568	zijn
# 41	14278	hebben
# 27	16884	kunnen
# 26	19063	staatssecretaris
SELECT COUNT(wordform_from) AS cnt,
       wordform_from,
       wordform
FROM wordform_links
         LEFT JOIN wordforms ON wordform_links.wordform_from = wordforms.wordform_id
GROUP BY wordform_from
ORDER BY cnt DESC;





# SELECT wordform
#     FROM wordform_links
#     LEFT JOIN wordforms
#     ON wordform_links.wordform_to = wordforms.wordform_id
#     WHERE wordform_from = 19063



SELECT * FROM morphological_paradigms AS m1 LEFT JOIN morphological_paradigms AS m2 ON m1.X = m2.X AND m1.Y = m2.Y AND m1.Z = m2.Z AND m1.W = m2.W
    LEFT JOIN wordforms w on m1.wordform_id = w.wordform_id
WHERE m2.wordform_id = (SELECT wordform_id FROM wordforms WHERE wordform = 'zes');




# SELECT *
#     FROM wordforms AS w1
#              LEFT JOIN anahashes AS a1 ON w1.anahash_id = a1.anahash_id
#              LEFT JOIN anahashes AS a2 ON (a1.anahash - 40427369568) = a2.anahash
#              LEFT JOIN wordforms AS w2 ON a2.anahash_id = w2.anahash_id
#     WHERE w1.wordform LIKE '%ess'

SELECT *
    FROM wordforms AS w1
         LEFT JOIN wordforms AS w2 ON w2.wordform = SUBSTRING(w1.wordform FROM 1 FOR CHAR_LENGTH(w1.wordform) - 3)
    WHERE w1.wordform LIKE '%ess'



SELECT *
    FROM wordforms AS w1
         LEFT JOIN morphological_paradigms ON w1.wordform_id = morphological_paradigms.wordform_id
    WHERE w1.wordform LIKE '%ess'
    AND morphological_paradigms.wordform_id IS NOT NULL