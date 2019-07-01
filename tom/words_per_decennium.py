from ticclat.flask_app import engine
import matplotlib.pyplot as plt
import pandas

connection = engine.connect()

df = pandas.read_sql("""
SELECT ROUND(pub_year, -1) AS year,
       SUM(word_count) AS sum_word_count
       FROM documents
       WHERE pub_year IS NOT NULL
       GROUP BY year
""", connection)

df.sort_index(by='year').plot.bar(x='year', y='sum_word_count')
plt.show()
