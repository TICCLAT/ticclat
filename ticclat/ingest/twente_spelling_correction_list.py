import os.path
import re
import pandas as pd
from ..dbutils import add_lexicon_with_links, session_scope
import logging

logger = logging.getLogger(__name__)


def parse_line(line):
    # the wordform starts with *
    # corrections start with # (there can be multiple) and end with whitspace or ~
    # example text start with ~
    # 'rules' start with <space>[

    # get the wordform
    wf_regex = r"^\*(?P<wf>.+?)[\t#]"
    m = re.match(wf_regex, line)
    wf = m.group("wf")
    # Wordforms need to be stripped!
    # Whitespace before or after wordforms also leads to duplicate entries in the database.
    wf = wf.strip()

    # get example text (and remove it)
    ex_regex = r"~.+~?"
    line = re.sub(ex_regex, "", line)

    # remove 'rule'
    rule_regex = r"\[EA?XAMPL: .+\]"
    line = re.sub(rule_regex, "", line)

    # get the corrections
    corrections = []
    corr_regex = r"#(?P<corr>.+)"
    m = re.search(corr_regex, line)
    if m:
        # Wordforms need to be stripped!
        # Whitespace before or after wordforms also leads to duplicate entries in the database.
        corrections = [
            c.strip().replace("\t", "")
            for c in m.group("corr").split("#")
            if c != "" and len(c) < 100
        ]

    return wf, corrections


def load_data(in_file):
    # read error data
    corrections = []

    # File is in windows-1252 encoding and needs to be converted to utf-8
    num_lines = 0
    with open(in_file) as f:
        for line in f:
            num_lines += 1
            wf, corr = parse_line(line)
            if wf is not None:
                for c in corr:
                    corrections.append({"wf": wf, "corr": c})
            else:
                logger.debug(f"no wf in line: {line}")

    data = pd.DataFrame(corrections)
    logger.debug(f"{num_lines} lines processed from {in_file}")

    return data


def ingest(
    session,
    base_dir="",
    twente_file="twente/TWENTE.noxml.2002.sq.clean.norm.tok.sortu.unifrq.LC.noapekrol.allasterisk.12.withcorrections.12186.txt",
    **kwargs,
):
    wfs_with_links = load_data(os.path.join(base_dir, twente_file))

    with session_scope(session) as s:
        name = "TWENTE.noxml.2002.sq.clean.norm.tok.sortu.unifrq.LC.noapekrol.allasterisk.12.withcorrections.12186"
        vocabulary = False
        from_column = "wf"
        from_correct = False
        to_column = "corr"
        to_correct = True

        add_lexicon_with_links(
            s,
            name,
            vocabulary,
            wfs_with_links,
            from_column,
            to_column,
            from_correct,
            to_correct,
        )
