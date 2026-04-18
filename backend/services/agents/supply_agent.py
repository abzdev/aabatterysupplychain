from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, Protocol


@dataclass(frozen=True)
class SupplyEventInput:
    """Input needed to evaluate T6 logic for a stockout event candidate."""

    sku_id: str
    dest_dc: str
    stockout_date: date | None = None
    days_of_supply: float | None = None
    as_of_date: date | None = None

    def resolve_stockout_date(self, today: date) -> date | None:
        if self.stockout_date is not None:
            return self.stockout_date
        if self.days_of_supply is None:
            return None
        base_date = self.as_of_date or today
        # Convert projected DoS to a date without requiring persisted features.
        projected_days = max(0, int(self.days_of_supply))
        return base_date + timedelta(days=projected_days)


@dataclass(frozen=True)
class OpenPO:
    """Open PO row subset used by supply-relief logic."""

    po_number: int | None
    sku_id: str
    dc: str
    qty_shipped: int | None
    qty_invoiced: int | None
    required_date: date | None
    promised_ship_date: date | None
    receipt_date: date | None
    ship_to_address: str | None = None

    @property
    def open_qty(self) -> int:
        shipped = self.qty_shipped or 0
        invoiced = self.qty_invoiced or 0
        return max(0, shipped - invoiced)

    @property
    def eta(self) -> date | None:
        # For open POs, receipt_date can be null. Fall back to promised/required.
        return self.receipt_date or self.promised_ship_date or self.required_date

    @property
    def delayed(self) -> bool:
        if self.required_date is None:
            return False
        if self.receipt_date is None:
            return False
        return self.receipt_date > self.required_date


@dataclass(frozen=True)
class SupplyDecision:
    """Output from T6.1/T6.2/T6.3 to feed event creation/update."""

    sku_id: str
    dest_dc: str
    relief_arriving: bool
    relief_eta: date | None
    relief_qty: int | None
    is_delayed: bool
    suppress_event: bool
    po_at_risk: bool
    selected_po_number: int | None


class PORepository(Protocol):
    def fetch_open_pos(self, sku_id: str, dest_dc: str, today: date) -> list[OpenPO]:
        """
        Return open POs for SKU/DC matching:
        receipt_date IS NULL OR receipt_date > today
        """


class SupabasePORepository:
    """PORepository backed by a Supabase client."""

    def __init__(self, client) -> None:
        self.client = client

    def fetch_open_pos(self, sku_id: str, dest_dc: str, today: date) -> list[OpenPO]:
        # Use the persisted dc mapping derived from Primary Ship To Address.
        query = (
            self.client.table("po_history")
            .select(
                "po_number,sku_id,dc,qty_shipped,qty_invoiced,required_date,"
                "promised_ship_date,receipt_date,ship_to_address"
            )
            .eq("sku_id", sku_id)
            .eq("dc", dest_dc)
            .or_(f"receipt_date.is.null,receipt_date.gt.{today.isoformat()}")
        )
        rows = query.execute().data or []
        return [_row_to_open_po(row) for row in rows]


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        return date.fromisoformat(value)
    return None


def _row_to_open_po(row: dict) -> OpenPO:
    return OpenPO(
        po_number=row.get("po_number"),
        sku_id=row.get("sku_id"),
        dc=row.get("dc"),
        qty_shipped=row.get("qty_shipped"),
        qty_invoiced=row.get("qty_invoiced"),
        required_date=_parse_date(row.get("required_date")),
        promised_ship_date=_parse_date(row.get("promised_ship_date")),
        receipt_date=_parse_date(row.get("receipt_date")),
        ship_to_address=row.get("ship_to_address"),
    )


def evaluate_supply_for_event(
    event: SupplyEventInput,
    po_repository: PORepository,
    *,
    today: date | None = None,
) -> SupplyDecision:
    """
    Evaluate T6 logic for one event candidate.

    T6.1: query open POs for SKU/DC.
    T6.2: return relief fields and delayed flag.
    T6.3: suppress event when relief_eta < stockout_date unless delayed.
    """
    as_of = today or date.today()
    open_pos = po_repository.fetch_open_pos(event.sku_id, event.dest_dc, as_of)
    return _decide_supply(event, open_pos, as_of)


def evaluate_supply_for_events(
    events: Iterable[SupplyEventInput],
    po_repository: PORepository,
    *,
    today: date | None = None,
) -> list[SupplyDecision]:
    as_of = today or date.today()
    return [
        _decide_supply(event, po_repository.fetch_open_pos(event.sku_id, event.dest_dc, as_of), as_of)
        for event in events
    ]


def _decide_supply(event: SupplyEventInput, open_pos: list[OpenPO], today: date) -> SupplyDecision:
    is_delayed = any(po.delayed for po in open_pos)
    selected = _select_relief_po(open_pos)
    stockout_date = event.resolve_stockout_date(today)

    relief_arriving = selected is not None
    relief_eta = selected.eta if selected else None
    relief_qty = selected.open_qty if selected else None

    suppress_event = False
    po_at_risk = False

    if relief_eta is not None and stockout_date is not None and relief_eta < stockout_date:
        if is_delayed:
            suppress_event = False
            po_at_risk = True
        else:
            suppress_event = True

    return SupplyDecision(
        sku_id=event.sku_id,
        dest_dc=event.dest_dc,
        relief_arriving=relief_arriving,
        relief_eta=relief_eta,
        relief_qty=relief_qty,
        is_delayed=is_delayed,
        suppress_event=suppress_event,
        po_at_risk=po_at_risk,
        selected_po_number=selected.po_number if selected else None,
    )


def _select_relief_po(open_pos: list[OpenPO]) -> OpenPO | None:
    # T6.2: select the soonest non-delayed open PO if multiple exist.
    candidates = [po for po in open_pos if not po.delayed and po.eta is not None]
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda po: (
            po.eta,  # soonest ETA first
            -po.open_qty,  # larger relief first on tie
            po.po_number if po.po_number is not None else 10**12,
        ),
    )[0]
