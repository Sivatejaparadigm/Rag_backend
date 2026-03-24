"""add_lang_confidence_to_chunks

Revision ID: 115fe9b82237
Revises: e316f84d9b9e
Create Date: 2026-03-24 10:34:49.540076

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = '115fe9b82237'
down_revision = 'e316f84d9b9e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'chunks',
        sa.Column(
            'lang_confidence',
            sa.Float(),
            nullable=False,
            server_default='0.0',
            comment='Language detection confidence score (0.0-1.0) for chunk text',
        ),
    )


def downgrade() -> None:
    op.drop_column('chunks', 'lang_confidence')

