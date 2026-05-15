"""Initial schema

Revision ID: 20260411_0001
Revises: 
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "recommendations",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("direction", sa.String(length=8), nullable=False),
        sa.Column("entry_zone_low", sa.Float(), nullable=False),
        sa.Column("entry_zone_high", sa.Float(), nullable=False),
        sa.Column("stop_loss", sa.Float(), nullable=False),
        sa.Column("tp1", sa.Float(), nullable=False),
        sa.Column("tp2", sa.Float(), nullable=False),
        sa.Column("holding_period", sa.String(length=64), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("risk_grade", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("thesis", sa.JSON(), nullable=False),
        sa.Column("invalid_if", sa.JSON(), nullable=False),
        sa.Column("explanation", sa.Text(), nullable=False),
        sa.Column("score_vector", sa.JSON(), nullable=False),
        sa.Column("source_snapshot_id", sa.String(length=128), nullable=False),
        sa.Column("feature_snapshot_id", sa.String(length=64), nullable=False),
        sa.Column("signal_snapshot_id", sa.String(length=64), nullable=False),
        sa.Column("pattern_template", sa.String(length=32), nullable=False),
        sa.Column("model_version", sa.String(length=32), nullable=False),
        sa.Column("prompt_version", sa.String(length=32), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_recommendations_source_snapshot_id"), "recommendations", ["source_snapshot_id"], unique=False)
    op.create_index(op.f("ix_recommendations_ticker"), "recommendations", ["ticker"], unique=False)

    op.create_table(
        "signal_snapshots",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trend_score", sa.Float(), nullable=False),
        sa.Column("momentum_score", sa.Float(), nullable=False),
        sa.Column("volatility_score", sa.Float(), nullable=False),
        sa.Column("liquidity_score", sa.Float(), nullable=False),
        sa.Column("relative_strength_score", sa.Float(), nullable=False),
        sa.Column("event_score", sa.Float(), nullable=False),
        sa.Column("regime_label", sa.String(length=32), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_signal_snapshots_ticker"), "signal_snapshots", ["ticker"], unique=False)

    op.create_table(
        "feature_snapshots",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("atr", sa.Float(), nullable=False),
        sa.Column("ma_20", sa.Float(), nullable=False),
        sa.Column("ma_50", sa.Float(), nullable=False),
        sa.Column("ma_200", sa.Float(), nullable=False),
        sa.Column("volatility_20d", sa.Float(), nullable=False),
        sa.Column("momentum_20d", sa.Float(), nullable=False),
        sa.Column("relative_strength_63d", sa.Float(), nullable=False),
        sa.Column("avg_dollar_volume_20d", sa.Float(), nullable=False),
        sa.Column("breakout_level_20d", sa.Float(), nullable=False),
        sa.Column("support_level_20d", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_feature_snapshots_ticker"), "feature_snapshots", ["ticker"], unique=False)

    op.create_table(
        "event_records",
        sa.Column("source_id", sa.String(length=128), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("headline", sa.Text(), nullable=False),
        sa.Column("normalized_text", sa.Text(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("sentiment", sa.Float(), nullable=False),
        sa.Column("relevance", sa.Float(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("source_id"),
    )

    op.create_table(
        "paper_orders",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("recommendation_id", sa.String(length=64), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("qty", sa.Float(), nullable=False),
        sa.Column("limit_price", sa.Float(), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("simulated_fill_price", sa.Float(), nullable=True),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_reason", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_paper_orders_recommendation_id"), "paper_orders", ["recommendation_id"], unique=False)

    op.create_table(
        "positions",
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("open_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("avg_price", sa.Float(), nullable=False),
        sa.Column("qty", sa.Float(), nullable=False),
        sa.Column("realized_pnl", sa.Float(), nullable=False),
        sa.Column("unrealized_pnl", sa.Float(), nullable=False),
        sa.Column("stop_state", sa.String(length=16), nullable=False),
        sa.Column("target_state", sa.String(length=16), nullable=False),
        sa.Column("last_mark", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("ticker"),
    )

    op.create_table(
        "approval_decisions",
        sa.Column("decision_id", sa.String(length=64), nullable=False),
        sa.Column("recommendation_id", sa.String(length=64), nullable=False),
        sa.Column("decision", sa.String(length=16), nullable=False),
        sa.Column("approver", sa.String(length=128), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("decision_id"),
    )
    op.create_index(op.f("ix_approval_decisions_recommendation_id"), "approval_decisions", ["recommendation_id"], unique=False)

    op.create_table(
        "execution_controls",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("enabled", sa.Integer(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by", sa.String(length=128), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "market_bars",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Float(), nullable=False),
        sa.Column("high", sa.Float(), nullable=False),
        sa.Column("low", sa.Float(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("vendor_id", sa.String(length=64), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_market_bars_ticker"), "market_bars", ["ticker"], unique=False)
    op.create_index(op.f("ix_market_bars_timestamp"), "market_bars", ["timestamp"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_market_bars_timestamp"), table_name="market_bars")
    op.drop_index(op.f("ix_market_bars_ticker"), table_name="market_bars")
    op.drop_table("market_bars")
    op.drop_table("execution_controls")
    op.drop_index(op.f("ix_approval_decisions_recommendation_id"), table_name="approval_decisions")
    op.drop_table("approval_decisions")
    op.drop_table("positions")
    op.drop_index(op.f("ix_paper_orders_recommendation_id"), table_name="paper_orders")
    op.drop_table("paper_orders")
    op.drop_table("event_records")
    op.drop_index(op.f("ix_signal_snapshots_ticker"), table_name="signal_snapshots")
    op.drop_table("signal_snapshots")
    op.drop_index(op.f("ix_feature_snapshots_ticker"), table_name="feature_snapshots")
    op.drop_table("feature_snapshots")
    op.drop_index(op.f("ix_recommendations_ticker"), table_name="recommendations")
    op.drop_index(op.f("ix_recommendations_source_snapshot_id"), table_name="recommendations")
    op.drop_table("recommendations")
