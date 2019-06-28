from ticclat.flask_app import engine
import matplotlib.pyplot as plt
import pandas

connection = engine.connect()

query = """
SELECT SUM(1e9 * frequency/word_count) AS normalized_frequency, ROUND(
CASE
    WHEN pub_year IS NOT NULL THEN pub_year
    ELSE (year_from + year_to) / 2
END) AS year
FROM text_attestations
    LEFT JOIN documents ON text_attestations.document_id = documents.document_id
WHERE wordform_id = (SELECT wordform_id FROM wordforms WHERE wordform = %(lookup_word)s)
GROUP BY year
HAVING year IS NOT NULL
ORDER BY year ASC
    """
df = pandas.read_sql(query, connection, params={'lookup_word': 'scheurbuik'})

# df.sort_index(by='year').plot.bar(x='year', y='sum_word_count')
df.plot.scatter(x='year', y='normalized_frequency', )
plt.show()
