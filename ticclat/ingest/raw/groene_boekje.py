#!/usr/bin/env python
import pandas as pd
import numpy as np
import os
import sqlalchemy
import sqlalchemy_utils

import ticclat.dbutils
import ticclat.ticclat_schema as schema

import tqdm

"""
This script cleans and ingests raw Groene Boekje CSV files into the TICCLAT
database.

For reference only. This was developed before we switched to ingesting
frequency lists from TICCL-unk.
"""


######################
# Groene Boekje 1995 #
######################
def load_GB95(path):
    df = pd.read_csv(path, sep=';', escapechar='\\',
                     names=["word", "syllables", "see also", "disambiguation",
                            "grammatical tag", "article",
                            "plural/past/attrib", "plural/past/attrib syllables",
                            "diminu/compara/past plural", "diminu/compara/past plural syllables",
                            "past perfect/superla", "past perfect/superla syllables"])

    df = df.where(df != "Ili/as", other=None)

    return df


# note that these are regex formatted, i.e. with special characters escaped
diacritic_markers = {'@`': '\u0300',    # accent grave
                     "@\\'": '\u0301',  # accent aigu
                     '@\\"': '\u0308', # trema
                     '@\+': '\u0327',   # cedilla
                     '@\^': '\u0302',   # accent circumflex
                     '@=': '\u0303',    # tilde
                     '@@': "'",         # apostrophe (not actually a diacritic)
                     '@2': '\u2082',    # subscript 2
                     '@n': '\u0308n'    # trema followed by n
                    }


def nd_any(*args):
    result = (args[0] | args[0])
    for arg in args:
        result = (result | arg)
    return result


def contains_in_any_column(df, word):
    return df[nd_any(*tuple(df[col].str.contains(word) for col in df.columns))]


def clean_wordform_df(wordform_df):
    # remove colons
    wordform_df = wordform_df.str.replace(':', '')
    # strip whitespace
    wordform_df = wordform_df.str.strip()
    # remove duplicate word footnote numbers
    duplicates = wordform_df.str.contains('[0-9]$', regex=True)
    wordform_df = pd.concat((wordform_df[~duplicates], wordform_df[duplicates].str.replace('[0-9]$', '', regex=True)))
    # remove parentheses around some words
    wordform_df = wordform_df.sort_values().str.strip("()")
    # remove abbreviations
    abbreviation = wordform_df.str.contains('\.$')
    wordform_df = wordform_df[~abbreviation]
    # remove duplicates
    wordform_df = pd.Series(wordform_df.unique())
    return wordform_df


def clean_disambiguation_column(df):
    df['disambiguation'] = df['disambiguation'].str.strip('()')
    df['disambiguation'][df['disambiguation'] == 'andere bett.'] = None
    df['disambiguation'][df['disambiguation'] == 'andere bet.'] = None
    df['disambiguation'][df['disambiguation'].str.contains(" ", na=False)
                                & ~df['disambiguation'].str.contains(",", na=False)] = None
    df['disambiguation'][df['disambiguation'].str.contains("[^,] ", na=False)] = (
        df['disambiguation']
         [df['disambiguation'].str.contains("[^,] ", na=False)]
         .str.split(', ')
         .map(lambda x: [i for i in x if not ' ' in i])
         .map(lambda x: None if len(x) == 0 else ', '.join(x))
    )
    df['disambiguation'][df['disambiguation'].str.contains(", -", na=False)] = None

    return df


def create_GB95_wordform_df(df_GB1995):
    data = df_GB1995.drop(["syllables", "see also", "grammatical tag", "article",
                           "plural/past/attrib syllables", "diminu/compara/past plural syllables",
                           "past perfect/superla syllables"], axis=1)
    
    df = clean_disambiguation_column(data)

    wordform_df = pd.concat((df["word"],
                             df["disambiguation"],
                             df["plural/past/attrib"],
                             df["diminu/compara/past plural"],
                             df["past perfect/superla"]))\
                    .dropna()
    has_comma = wordform_df.str.contains(', ')
    wordform_df = pd.concat((wordform_df[~has_comma],) + tuple(pd.Series(row.split(', ')) for row in wordform_df[has_comma]))

    wordform_clean_df = clean_wordform_df(wordform_df)

    for marker, umarker in diacritic_markers.items():
        wordform_clean_df = wordform_clean_df.str.replace(marker, umarker)

    return wordform_clean_df


def clean_wordform_series(wordform_series, remove_duplicates=False):
    # remove colons
    wordform_series = wordform_series.str.replace(':', '')
    # strip whitespace
    wordform_series = wordform_series.str.strip()
    # remove duplicate word footnote numbers
    duplicates = wordform_series.str.contains('[0-9]$', regex=True)
    wordform_series = pd.concat((wordform_series[~duplicates], wordform_series[duplicates].str.replace('[0-9]$', '', regex=True)))
    # remove parentheses around some words
    wordform_series = wordform_series.sort_values().str.strip("()")
    # remove abbreviations
    abbreviation = wordform_series.str.contains('\.$')
    wordform_series = wordform_series[~abbreviation]
    
    if remove_duplicates:
        wordform_series = pd.Series(wordform_series.unique())
    return wordform_series


def create_GB95_link_df(df_GB1995):
    link_data = df_GB1995.drop(["syllables", "grammatical tag", "article",
                                "plural/past/attrib syllables", "diminu/compara/past plural syllables", "past perfect/superla syllables"], axis=1)

    # clean link_data
    link_data['see also'] = link_data['see also'].str.replace('zie ook ', '')
    link_data = clean_disambiguation_column(link_data)
    link_data = link_data.dropna(how='all', subset=["see also", "disambiguation", "plural/past/attrib", "diminu/compara/past plural", "past perfect/superla"])

    # convert to link_df
    link_df = (link_data.set_index('word').stack().reset_index().drop('level_1', axis=1)
                        .rename({'word': 'wordform_1', 0: 'wordform_2'}, axis=1))
    has_comma1 = link_df['wordform_1'].str.contains(',')
    link_df = pd.concat((link_df[~has_comma1],) + tuple(pd.DataFrame({'wordform_1': row['wordform_1'].split(', '),
                                                                      'wordform_2': (row['wordform_2'],) * len(row['wordform_1'].split(', '))})
                                                         for ix, row in link_df[has_comma1].iterrows()))

    has_comma2 = link_df['wordform_2'].str.contains(',')
    link_df = pd.concat((link_df[~has_comma2],) + tuple(pd.DataFrame({'wordform_1': (row['wordform_1'],) * len(row['wordform_2'].split(', ')),
                                                                      'wordform_2': row['wordform_2'].split(', ')})
                                                         for ix, row in link_df[has_comma2].iterrows()))

    link_df = link_df.reset_index(drop=True)

    link_clean_df = link_df.copy()
    link_clean_df['wordform_1'] = clean_wordform_series(link_clean_df['wordform_1'])
    link_clean_df['wordform_2'] = clean_wordform_series(link_clean_df['wordform_2'])
    # some links will be removed (abbreviations), so drop those rows
    link_clean_df = link_clean_df.dropna()

    for column in link_clean_df:
        for marker, umarker in diacritic_markers.items():
            link_clean_df[column] = link_clean_df[column].str.replace(marker, umarker)

    return link_clean_df


if __name__ == '__main__':
    # basic config
    db_name = 'ticclat'
    GB_basepath = "/Users/pbos/projects/ticclat/data/GB/"
    envvars_path = '/Users/pbos/projects/ticclat/ticclat/notebooks/ticclat_db_ingestion/ENVVARS.txt'

    # paths
    GB1914_path = GB_basepath + "1914/22722-8.txt"
    GB1995_path = GB_basepath + "1995-2005/1995/GB95_002.csv"
    GB2005_path = GB_basepath + "1995-2005/2005/GB05_002.csv"


    # Read information to connect to the database and put it in environment variables
    with open(envvars_path) as f:
        for line in f:
            parts = line.split('=')
            if len(parts) == 2:
                os.environ[parts[0]] = parts[1].strip()


    engine = sqlalchemy.create_engine("mysql://{}:{}@localhost/{}?charset=utf8mb4".format(os.environ['user'], 
                                                                        os.environ['password'], 
                                                                        os.environ['dbname']))
    if not sqlalchemy_utils.database_exists(engine.url):
        sqlalchemy_utils.create_database(engine.url)

    Session = sqlalchemy.orm.sessionmaker(bind=engine)

    # create tables
    schema.Base.metadata.create_all(engine)

    # load data from file
    df95 = load_GB95(GB1995_path)

    # wordforms
    wordform_clean_df = create_GB95_wordform_df(df95)

    # load wordforms into TICCLAT database
    with ticclat.dbutils.session_scope(Session) as session:
        ticclat.dbutils.add_lexicon(session, "Groene Boekje 1995", True, wordform_clean_df.to_frame(name='wordform'))

    # links
    link_df = create_GB95_link_df(df95)

    # load links into TICCLAT database
    with ticclat.dbutils.session_scope(Session) as session:
        lexicon = session.query(ticclat.ticclat_schema.Lexicon).filter(ticclat.ticclat_schema.Lexicon.lexicon_name=='Groene Boekje 1995').first()
        if lexicon is None:
            raise Exception("No lexicon found!")
        for idx, row in tqdm.tqdm(link_df.iterrows(), total=link_df.shape[0]):
            wf = session.query(ticclat.ticclat_schema.Wordform).filter(ticclat.ticclat_schema.Wordform.wordform == row['wordform_1']).first()
            corr = session.query(ticclat.ticclat_schema.Wordform).filter(ticclat.ticclat_schema.Wordform.wordform == row['wordform_2']).first()
            if corr is None:
                print("Wordform '", row['wordform_2'], "' not found in wordforms table, continuing...")
            wf.link_with_metadata(corr, True, True, lexicon)
