import logging
import shutil
import subprocess
import tempfile

from pathlib import Path

LOGGER = logging.getLogger(__name__)


def run(cmd):
    proc = subprocess.Popen(['sh', '-c', cmd],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            )
    stdout, stderr = proc.communicate()

    return proc.returncode, stdout, stderr


def ingest(session_maker, base_dir='/',
           ticcl_variants_file='ticcl_variants/TICCLATEDBO.INDEXERUnigramsOnly.clean.confuslist.indexNT.ldcalc.RANK.ranked.NEWCHAIN.manualfiltering.chained.gz',
           **kwargs):
    ingest_file_path = Path(base_dir) / ticcl_variants_file
    tmp_path = Path(tempfile.mkdtemp())
    csv_file_path = tmp_path / 'to_load.csv'

    session = session_maker()

    try:
        exit_code, stdout, stderr = run(
            f'gunzip -c "{ingest_file_path}" | cut -d# -f1,2,3,6 | tr \\# \\\t > "{csv_file_path}"'
        )
        assert exit_code == 0, 'Error running unix toolchain'
        query = f"""
LOAD DATA LOCAL INFILE :file_name INTO TABLE ticcl_variants
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
(wordform, frequency, wordform_source, levenshtein_distance)
"""
        LOGGER.info("Running CSV Import query")
        session.execute(query, {'file_name': str(csv_file_path)})
        LOGGER.info("Running ticcl_variants set wordform_id query")
        session.execute("""
UPDATE ticcl_variants
LEFT JOIN wordforms w ON ticcl_variants.wordform_source = w.wordform
SET ticcl_variants.wordform_source_id = w.wordform_id WHERE 1
        """)
        session.commit()

    except Exception as exception:
        LOGGER.error(exception)
    finally:
        shutil.rmtree(tmp_path)
        session.close()
