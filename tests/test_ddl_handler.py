"""Tests for the DDL handler."""
from __future__ import annotations

from transform.ddl_handler import DdlHandler
from transform.envelope import DdlEvent


def _ddl(sql: str, table: str = "orders", schema: str = "public") -> DdlEvent:
    return DdlEvent(
        ddl=sql,
        database="ecommerce",
        schema=schema,
        table=table,
        kind="ALTER",
    )


def test_add_column_classified_as_evolve() -> None:
    handler = DdlHandler()
    decision = handler.handle(_ddl("ALTER TABLE public.orders ADD COLUMN discount_cents INTEGER"))
    assert decision.action == "evolve"
    assert "discount_cents" in decision.details
    assert handler.stats.add_columns == 1


def test_drop_column_classified_as_manual() -> None:
    handler = DdlHandler()
    decision = handler.handle(_ddl("ALTER TABLE public.orders DROP COLUMN internal_notes"))
    assert decision.action == "manual"
    assert handler.stats.drop_columns == 1


def test_create_table_classified_as_skip() -> None:
    handler = DdlHandler()
    decision = handler.handle(_ddl("CREATE TABLE public.new (id INTEGER)", table="new"))
    assert decision.action == "skip"
    assert handler.stats.other_ddl == 1


def test_drop_table_classified_as_skip() -> None:
    handler = DdlHandler()
    decision = handler.handle(_ddl("DROP TABLE public.old", table="old"))
    assert decision.action == "skip"


def test_other_alter_classified_as_skip() -> None:
    handler = DdlHandler()
    decision = handler.handle(_ddl("ALTER TABLE public.orders ALTER COLUMN status TYPE TEXT"))
    assert decision.action == "skip"
    assert handler.stats.other_ddl == 1


def test_non_table_ddl_skipped() -> None:
    handler = DdlHandler()
    decision = handler.handle(_ddl("CREATE INDEX idx_orders_status ON public.orders(status)"))
    assert decision.action == "skip"


def test_ddl_handler_invokes_callback() -> None:
    captured = []
    handler = DdlHandler(on_ddl=captured.append)
    handler.handle(_ddl("ALTER TABLE public.orders ADD COLUMN x INT"))
    assert len(captured) == 1
    assert captured[0].action == "evolve"


def test_ddl_handler_swallows_callback_exception() -> None:
    def bad_cb(_decision):  # type: ignore[no-untyped-def]
        raise RuntimeError("boom")

    handler = DdlHandler(on_ddl=bad_cb)
    # Should not raise.
    handler.handle(_ddl("ALTER TABLE public.orders ADD COLUMN x INT"))


def test_quoted_identifiers_handled() -> None:
    handler = DdlHandler()
    decision = handler.handle(
        _ddl('ALTER TABLE "public"."orders" ADD COLUMN "discount" INTEGER'),
    )
    assert decision.action == "evolve"
    assert "discount" in decision.details
