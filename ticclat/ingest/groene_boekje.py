#!/usr/bin/env python
import pandas as pd
import numpy as np
import os
import sqlalchemy
import sqlalchemy_utils

import ticclat.dbutils
import ticclat.ticclat_schema as schema


######################
# Groene Boekje 1995 #
######################
def load_GB95(path):
    df = pd.read_csv(path, sep=';', escapechar='\\',
                     names=["word", "syllables", "see also", "disambiguation",
                            "grammatical tag", "article",
                            "plural/past/attrib", "plural/past/attrib syllables",
                            "diminu/compara/past plural", "diminu/compara/past plural syllables",
                            "past perfect/superla", "past perfect/superla syllables"],
                     encoding='utf8') # encoding necessary for later loading into sqlalchemy!

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


def create_GB95_wordform_df(df_GB1995):
    wordform_df = pd.concat((df_GB1995["word"],
                             df_GB1995["plural/past/attrib"],
                             df_GB1995["diminu/compara/past plural"],
                             df_GB1995["past perfect/superla"]))\
                    .dropna()
    has_comma = wordform_df.str.contains(', ')
    wordform_df = pd.concat((wordform_df[~has_comma],) + tuple(pd.Series(row.split(', ')) for row in wordform_df[has_comma]))

    wordform_clean_df = clean_wordform_df(wordform_df)

    for marker, umarker in diacritic_markers.items():
        wordform_clean_df = wordform_clean_df.str.replace(marker, umarker)

    return wordform_df


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


    engine = sqlalchemy.create_engine("mysql://{}:{}@localhost/{}".format(os.environ['user'], 
                                                                        os.environ['password'], 
                                                                        os.environ['dbname']))
    if not sqlalchemy_utils.database_exists(engine.url):
        sqlalchemy_utils.create_database(engine.url)

    Session = sqlalchemy.orm.sessionmaker(bind=engine)

    # create tables
    schema.Base.metadata.create_all(engine)


    # wordforms
    wordform_clean_df = create_GB95_wordform_df(load_GB95(GB1995_path))

    # # Load Groene Boekje wordforms into TICCLAT database
    with ticclat.dbutils.session_scope(Session) as session:
        ticclat.dbutils.add_lexicon(session, "Groene Boekje 1995", True, wordform_clean_df.str.encode('utf8').to_frame(name='wordform'))

