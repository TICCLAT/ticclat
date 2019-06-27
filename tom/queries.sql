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