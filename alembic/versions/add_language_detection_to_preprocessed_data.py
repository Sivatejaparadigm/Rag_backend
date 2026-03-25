"""Add language detection columns to preprocessed_data table

Revision ID: add_language_to_preproc
Revises: 354bc809a57d
Create Date: 2026-03-24 10:00:00.000000

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = 'add_language_to_preproc'
down_revision = '354bc809a57d'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add language detection columns to preprocessed_data table
    op.add_column(
        'preprocessed_data',
        sa.Column(
            'language',
            sa.String(length=20),
            nullable=False,
            server_default='UNKNOWN',
            comment="Detected language (e.g. 'ENGLISH', 'SPANISH', 'UNKNOWN')"
        )
    )
    op.add_column(
        'preprocessed_data',
        sa.Column(
            'lang_confidence',
            sa.Float(),
            nullable=False,
            server_default='0.0',
            comment="Language detection confidence score (0.0-1.0)"
        )
    )


def downgrade() -> None:
    # Remove language detection columns from preprocessed_data table
    op.drop_column('preprocessed_data', 'lang_confidence')
    op.drop_column('preprocessed_data', 'language')
