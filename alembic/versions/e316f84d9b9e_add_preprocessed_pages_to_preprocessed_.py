"""add_preprocessed_pages_to_preprocessed_data

Revision ID: e316f84d9b9e
Revises: add_language_to_preproc
Create Date: 2026-03-24 10:23:30.620751

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = 'e316f84d9b9e'
down_revision = 'add_language_to_preproc'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add preprocessed_pages column to preprocessed_data table
    op.add_column(
        'preprocessed_data',
        sa.Column(
            'preprocessed_pages',
            sa.dialects.postgresql.JSONB,
            nullable=True,
            comment="List of preprocessed page objects, each with page_number, text, word_count, etc."
        )
    )


def downgrade() -> None:
    # Remove preprocessed_pages column from preprocessed_data table
    op.drop_column('preprocessed_data', 'preprocessed_pages')

