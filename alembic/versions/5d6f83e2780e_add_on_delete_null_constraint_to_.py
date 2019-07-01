"""Add ON DELETE NULL constraint to wordform anahashes

Revision ID: 5d6f83e2780e
Revises: 01be7e7dff09
Create Date: 2019-07-01 16:35:31.710357

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5d6f83e2780e'
down_revision = '01be7e7dff09'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('wordforms_ibfk_1', 'wordforms', type_='foreignkey')
    op.create_foreign_key(None, 'wordforms', 'anahashes', ['anahash_id'], ['anahash_id'], ondelete='SET NULL')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'wordforms', type_='foreignkey')
    op.create_foreign_key('wordforms_ibfk_1', 'wordforms', 'anahashes', ['anahash_id'], ['anahash_id'])
    # ### end Alembic commands ###