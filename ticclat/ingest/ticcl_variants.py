import logging
import shutil
import subprocess
import tempfile

from pathlib import Path

logger = logging.getLogger(__name__)


def run(cmd):
    proc = subprocess.Popen(['sh', '-c', cmd],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            )
    stdout, stderr = proc.communicate()

    return proc.returncode, stdout, stderr


def ingest(session, base_dir='/', data_dir='ticcl_variants', **kwargs):
    ingest_file_name = 'TICCLATEDBO.INDEXERUnigramsOnly.clean.confuslist.' +\
                       'indexNT.ldcalc.RANK.ranked.NEWCHAIN.manualfiltering.chained.gz'
    ingest_file_path = Path(base_dir) / data_dir / ingest_file_name
    tmp_path = Path(tempfile.mkdtemp())
    csv_file_path = tmp_path / 'to_load.csv'

    s = session()

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
        logger.info("Running CSV Import query")
        s.execute(query, {'file_name': str(csv_file_path)})
        logger.info("Running ticcl_variants set wordform_id query")
        s.execute("""
UPDATE ticcl_variants
LEFT JOIN wordforms w ON ticcl_variants.wordform_source = w.wordform
SET ticcl_variants.wordform_source_id = w.wordform_id WHERE 1
        """)
        s.commit()

    except Exception as e:
        logger.error(e)
    finally:
        shutil.rmtree(tmp_path)
        s.close()
