"""Twente spelling correction list ingestion."""

import os.path
import logging
import re
import pandas as pd
from ..dbutils import add_lexicon_with_links, session_scope

LOGGER = logging.getLogger(__name__)


def parse_line(line):
    """Extract wordform and corrections from line of Twente spelling correction list file."""
    # the wordform starts with *
    # corrections start with # (there can be multiple) and end with whitspace or ~
    # example text start with ~
    # 'rules' start with <space>[

    # get the wordform
    wf_regex = r'^\*(?P<wf>.+?)[\t#]'
    match = re.match(wf_regex, line)
    wordform = match.group('wf')
    # Wordforms need to be stripped!
    # Whitespace before or after wordforms also leads to duplicate entries in the database.
    wordform = wordform.strip()

    # get example text (and remove it)
    ex_regex = r'~.+~?'
    line = re.sub(ex_regex, '', line)

    # remove 'rule'
    rule_regex = r'\[EA?XAMPL: .+\]'
    line = re.sub(rule_regex, '', line)

    # get the corrections
    corrections = []
    corr_regex = r'#(?P<corr>.+)'
    match = re.search(corr_regex, line)
    if match:
        # Wordforms need to be stripped!
        # Whitespace before or after wordforms also leads to duplicate entries in the database.
        corrections = [c.strip().replace('\t', '') for c in match.group('corr').split('#') if c != '' and len(c) < 100]

    return wordform, corrections


def load_data(in_file):
    """Load the Twente spelling correction data as a dataframe."""
    # read error data
    corrections = []

    # File is in windows-1252 encoding and needs to be converted to utf-8
    num_lines = 0
    with open(in_file) as file_handle:
        for line in file_handle:
            num_lines += 1
            wordform, corr = parse_line(line)
            if wordform is not None:
                for correction_i in corr:
                    corrections.append({'wf': wordform, 'corr': correction_i})
            else:
                LOGGER.debug("no wf in line: %s", line)

    data = pd.DataFrame(corrections)
    LOGGER.debug('%s lines processed from %s', num_lines, in_file)

    return data


def ingest(session_maker, base_dir='',
           twente_file='twente/TWENTE.noxml.2002.sq.clean.norm.tok.sortu.unifrq.LC.noapekrol.allasterisk.12.withcorrections.12186.txt', **kwargs):
    """Ingest Twente spelling correction list into TICCLAT database."""
    wfs_with_links = load_data(os.path.join(base_dir, twente_file))

    with session_scope(session_maker) as session:
        name = 'TWENTE.noxml.2002.sq.clean.norm.tok.sortu.unifrq.LC.noapekrol.allasterisk.12.withcorrections.12186'
        vocabulary = False
        from_column = 'wf'
        from_correct = False
        to_column = 'corr'
        to_correct = True

        add_lexicon_with_links(session, name, vocabulary, wfs_with_links,
                               from_column, to_column, from_correct, to_correct, **kwargs)
