"""Add years to the Sonar corpus

Revision ID: a3428b9e36f5
Revises: 5d6f83e2780e
Create Date: 2019-07-02 13:27:01.635733

"""
from alembic import op
import sqlalchemy as sa

from ticclat.ticclat_schema import Document, Corpus, corpusId_x_documentId


# revision identifiers, used by Alembic.
revision = 'a3428b9e36f5'
down_revision = '5d6f83e2780e'
branch_labels = None
depends_on = None

corpus_name = 'SoNaR-500'


def upgrade():
    q = sa.update(Document).values(year_from=1950, year_to=2010) \
        .where(Document.document_id.in_(
            sa.select('*').select_from(sa.select([Document.document_id]) \
                .select_from(corpusId_x_documentId.join(Corpus) \
                             .join(Document)) \
                .where(Corpus.name == 'SoNaR-500').alias('subquery')) \
                    .as_scalar()))
    op.execute(q)


def downgrade():
    q = sa.update(Document).values(year_from=None, year_to=None) \
        .where(Document.document_id.in_(
            sa.select('*').select_from(sa.select([Document.document_id]) \
                .select_from(corpusId_x_documentId.join(Corpus) \
                             .join(Document)) \
                .where(Corpus.name == 'SoNaR-500').alias('subquery')) \
                    .as_scalar()))
    op.execute(q)
