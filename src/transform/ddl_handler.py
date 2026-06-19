"""DDL event handler for schema evolution.

The transform service sees Debezium schema-change events alongside data events.
Phase A policy:
- ALTER TABLE ... ADD COLUMN: log and rely on JDBC sink's `auto.evolve=true`
- ALTER TABLE ... DROP COLUMN: log a warning (auto-drop is dangerous; manual intervention)
- Other DDL (CREATE, DROP, etc.): log and skip
- DDL events must be processed before subsequent data events on the same table
  to maintain schema consistency. The transform service processes events in
  Kafka order (per-partition), so this is implicit. We expose an explicit
  barrier hook (`on_ddl`) so the consumer can flush pending produces before
  acknowledging a DDL event.
"""
from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field

from .envelope import DdlEvent

log = logging.getLogger(__name__)


@dataclass
class DdlDecision:
    action: str  # "evolve" | "manual" | "skip"
    reason: str
    table: str
    details: str = ""


@dataclass
class DdlHandlerStats:
    add_columns: int = 0
    drop_columns: int = 0
    other_ddl: int = 0
    decisions: list[DdlDecision] = field(default_factory=list)


ADD_COLUMN_RE = re.compile(
    r"ALTER\s+TABLE\s+(?P<table>[^ \s]+)\s+ADD\s+COLUMN\s+(?P<column>[^ ]+)",
    re.IGNORECASE,
)
DROP_COLUMN_RE = re.compile(
    r"ALTER\s+TABLE\s+(?P<table>[^ \s]+)\s+DROP\s+COLUMN\s+(?P<column>[^ ]+)",
    re.IGNORECASE,
)


def _strip_quotes(ident: str) -> str:
    return ident.strip().strip('"').strip("`")


class DdlHandler:
    """Decide what to do with each DDL event and emit a decision log.

    The handler is intentionally stateless across events (apart from stats).
    The actual schema-evolution work is performed by the JDBC sink connector
    via `auto.evolve=true`; this handler's job is observability and gating.
    """

    def __init__(self, on_ddl: Callable[[DdlDecision], None] | None = None) -> None:
        self._on_ddl = on_ddl
        self.stats = DdlHandlerStats()

    def handle(self, event: DdlEvent) -> DdlDecision:
        decision = self._classify(event)
        self.stats.decisions.append(decision)
        if decision.action == "evolve":
            self.stats.add_columns += 1
        elif decision.action == "manual":
            self.stats.drop_columns += 1
        else:
            self.stats.other_ddl += 1
        log.info(
            "DDL %s: table=%s reason=%s details=%s",
            decision.action, decision.table, decision.reason, decision.details,
        )
        if self._on_ddl is not None:
            try:
                self._on_ddl(decision)
            except Exception:
                log.exception("on_ddl callback raised")
        return decision

    def _classify(self, event: DdlEvent) -> DdlDecision:
        ddl = event.ddl.strip()
        upper = ddl.upper()

        m = ADD_COLUMN_RE.search(ddl)
        if m:
            column = _strip_quotes(m.group("column").rstrip(","))
            return DdlDecision(
                action="evolve",
                reason="ALTER TABLE ADD COLUMN — auto.evolve handles this in JDBC sink",
                table=m.group("table"),
                details=f"add column: {column}",
            )

        m = DROP_COLUMN_RE.search(ddl)
        if m:
            column = _strip_quotes(m.group("column").rstrip(","))
            return DdlDecision(
                action="manual",
                reason=(
                    "ALTER TABLE DROP COLUMN is dangerous — requires manual "
                    "intervention on the target table to avoid data loss"
                ),
                table=m.group("table"),
                details=f"drop column: {column}",
            )

        if upper.startswith("CREATE"):
            return DdlDecision(
                action="skip",
                reason="CREATE statement — JDBC sink will auto-create the target table",
                table=event.table or "(unknown)",
            )
        if upper.startswith("DROP"):
            return DdlDecision(
                action="skip",
                reason="DROP statement — log only, manual intervention required",
                table=event.table or "(unknown)",
            )
        if upper.startswith("ALTER"):
            return DdlDecision(
                action="skip",
                reason="ALTER (non-add/drop column) — log only",
                table=event.table or "(unknown)",
            )
        return DdlDecision(
            action="skip",
            reason="Other DDL — log only",
            table=event.table or "(unknown)",
        )
