from dataclasses import replace
import sqlite3

import pytest

from entropy_bot.grid_accounting import GridAccounting, GridAccountingError
from entropy_bot.models import Fill


def fill(key, buy, price, qty=1, fee=0, timestamp=2000, role="grid:1", **kwargs):
    return Fill(
        fill_id=key, order_role=role, cloid="test", oid=1, is_buy=buy,
        price=price, qty=qty, fee_usd=fee, liquidity="maker", time_ms=timestamp,
        **kwargs,
    )


def ledger(tmp_path, qty=0, entry=0, direction="long"):
    book = GridAccounting(tmp_path / "inventory.sqlite", "test-account:coin", direction)
    assert book.initialize(qty, entry, time_ms=1000)
    book.mark_funding_coverage(10000)
    return book


def test_high_cost_baseline_pair_profit_can_realize_loss_and_reduce_break_even(tmp_path):
    with ledger(tmp_path, 10, 100) as book:
        book.record_fill(fill("buy", True, 50, fee=0.10))
        book.record_fill(fill("sell", False, 55, fee=0.11, timestamp=3000))
        result = book.snapshot(55)
        assert result["inventory_qty"] == 10
        assert result["average_entry_price"] == pytest.approx(1050 / 11)
        assert result["grid_pair_gross_usd"] == 5
        assert result["grid_pair_net_usd"] == pytest.approx(4.79)
        assert result["realized_gross_usd"] == pytest.approx(55 - 1050 / 11)
        assert result["realized_net_usd"] == pytest.approx(55 - 1050 / 11 - 0.21)
        assert result["realized_net_usd"] < 0
        assert result["net_invested_usd"] == pytest.approx(995.21)
        assert result["break_even_price"] == pytest.approx(99.521)
        assert result["total_net_usd"] == pytest.approx(-445.21)
        assert result["unmatched_qty"] == 0


def test_short_inventory_uses_sell_then_buy_pairs_with_real_loss(tmp_path):
    with ledger(tmp_path, -10, 100, "short") as book:
        book.record_fill(fill("sell", False, 150, fee=0.10))
        book.record_fill(fill("buy", True, 145, fee=0.11, timestamp=3000))
        result = book.snapshot(145)
        assert result["inventory_qty"] == -10
        assert result["average_entry_price"] == pytest.approx(1150 / 11)
        assert result["grid_pair_net_usd"] == pytest.approx(4.79)
        assert result["realized_net_usd"] == pytest.approx(1150 / 11 - 145 - 0.21)
        assert result["realized_net_usd"] < 0
        assert result["net_invested_usd"] == pytest.approx(-1004.79)
        assert result["break_even_price"] == pytest.approx(100.479)
        assert result["total_net_usd"] == pytest.approx(-445.21)


def test_fifo_partial_matches_allocate_both_fees_without_duplication(tmp_path):
    with ledger(tmp_path) as book:
        book.record_fill(fill("a", True, 100, qty=2, fee=0.2))
        book.record_fill(fill("b", True, 90, qty=1, fee=0.3, timestamp=2100))
        book.record_fill(fill("c", False, 110, qty=2.5, fee=0.5, timestamp=3000))
        result = book.snapshot(110)
        assert result["grid_pair_gross_usd"] == 30
        assert result["grid_pair_fees_usd"] == pytest.approx(0.85)
        assert result["grid_pair_net_usd"] == pytest.approx(29.15)
        assert result["matched_qty"] == 2.5
        assert result["unmatched_qty"] == 0.5
        assert result["unmatched_open_fees_usd"] == pytest.approx(0.15)
        book.record_fill(fill("d", False, 95, qty=0.5, fee=0.1, timestamp=4000))
        closed = book.snapshot(95)
        assert closed["grid_pair_gross_usd"] == 32.5
        assert closed["grid_pair_fees_usd"] == pytest.approx(1.1)
        assert closed["grid_pair_net_usd"] == pytest.approx(31.4)
        assert closed["realized_net_usd"] == pytest.approx(31.4)
        assert closed["unmatched_qty"] == 0
        assert closed["inventory_qty"] == 0
        assert closed["break_even_price"] is None


def test_baseline_exit_and_later_repurchase_do_not_invent_closed_grid_cycle(tmp_path):
    with ledger(tmp_path, 1, 100) as book:
        book.record_fill(fill("sell", False, 90))
        book.record_fill(fill("buy", True, 80, timestamp=3000))
        result = book.snapshot(80)
        assert result["realized_gross_usd"] == -10
        assert result["grid_pair_net_usd"] == 0
        assert result["unmatched_qty"] == 1
        assert result["net_invested_usd"] == 90
        assert result["break_even_price"] == 90


def test_baseline_quantity_beyond_matched_grid_lot_never_generates_pair_profit(tmp_path):
    with ledger(tmp_path, 10, 100) as book:
        book.record_fill(fill("a", True, 80, qty=1, fee=0.1))
        book.record_fill(fill("b", False, 90, qty=3, fee=0.6, timestamp=3000))
        result = book.snapshot(90)
        assert result["matched_qty"] == 1
        assert result["grid_pair_net_usd"] == pytest.approx(9.7)
        assert result["inventory_qty"] == 8


def test_external_fills_change_true_cost_but_do_not_fabricate_grid_pairs(tmp_path):
    with ledger(tmp_path) as book:
        book.record_fill(fill("a", True, 100, qty=2, fee=0.2))
        book.record_fill(fill("b", False, 90, qty=1.5, fee=0.3, timestamp=3000, role="external"))
        result = book.snapshot(90)
        assert result["inventory_qty"] == 0.5
        assert result["unmatched_qty"] == 0.5
        assert result["discarded_pair_qty"] == 1.5
        assert result["grid_pair_net_usd"] == 0
        book.record_fill(fill("c", False, 110, qty=0.5, fee=0.1, timestamp=4000))
        result = book.snapshot(110)
        assert result["grid_pair_net_usd"] == pytest.approx(4.85)
        assert result["realized_net_usd"] == pytest.approx(-10.6)


def test_recovery_is_persistent_idempotent_and_never_reseeds_cost(tmp_path):
    path = tmp_path / "inventory.sqlite"
    initial = fill("one", True, 50, fee=0.1)
    with ledger(tmp_path, 2, 100) as book:
        assert book.record_fill(initial)
        assert not book.record_fill(initial)
        before = book.snapshot(55)
    with GridAccounting(path, "test-account:coin") as restored:
        assert restored.snapshot(55)["status"] == "reconciliation_required"
        assert not restored.initialize(3, 999, time_ms=5000)
        assert restored.reconcile(3, 250 / 3)
        assert not restored.record_fill(initial)
        after = restored.snapshot(55)
        assert after["average_entry_price"] == before["average_entry_price"]
        assert after["fill_count"] == 1
        assert after["unmatched_qty"] == 1
        assert after["inception_time_ms"] == 1000
        assert after["last_fill_time_ms"] == 2000
        assert after["fees_paid_usd"] == 0.1


def test_duplicate_discovery_role_difference_does_not_duplicate_profit(tmp_path):
    with ledger(tmp_path) as book:
        row = fill("one", True, 100)
        assert book.record_fill(row)
        assert not book.record_fill(replace(row, order_role="external"))
        assert book.snapshot(100)["unmatched_qty"] == 1


def test_has_fill_is_read_only_and_survives_reopening(tmp_path):
    with ledger(tmp_path) as book:
        assert not book.has_fill("one")
        assert book.snapshot(100)["fill_count"] == 0
        book.record_fill(fill("one", True, 100))
        assert book.has_fill("one")
    with GridAccounting(tmp_path / "inventory.sqlite", "test-account:coin") as restored:
        assert restored.has_fill("one")
        assert not restored.has_fill("other")
        assert restored.snapshot(100)["status"] == "reconciliation_required"


def test_conflicting_duplicate_is_sticky_error_without_second_economic_event(tmp_path):
    with ledger(tmp_path) as book:
        row = fill("one", True, 100)
        book.record_fill(row)
        with pytest.raises(GridAccountingError, match="矛盾"):
            book.record_fill(replace(row, qty=2))
        assert not book.reconcile(1, 100)
        result = book.snapshot(100)
        assert result["status"] == "error"
        assert result["fill_count"] == 1
        assert result["inventory_qty"] == 1
        assert result["realized_net_usd"] is None


def test_funding_unknown_is_not_reported_as_zero_and_signed_events_are_idempotent(tmp_path):
    with GridAccounting(tmp_path / "funding.sqlite", "account:coin") as book:
        book.initialize(1, 100, 1000)
        result = book.snapshot(110)
        assert result["realized_net_usd"] is None
        assert result["total_net_usd"] is None
        assert result["total_after_trade_fees_usd"] == 10
        assert result["funding_complete"] is False
        assert not book.record_funding("old", 999, 100)
        assert book.record_funding("f1", 2000, -2)
        assert book.record_funding("f2", 3000, 0.5)
        assert not book.record_funding("f1", 2000, -2.0)
        book.mark_funding_coverage(4000)
        result = book.snapshot(110)
        assert result["funding_net_usd"] == -1.5
        assert result["funding_event_count"] == 2
        assert result["realized_net_usd"] == -1.5
        assert result["total_net_usd"] == 8.5
        assert result["break_even_price"] == 101.5
        assert result["grid_pair_net_usd"] == 0
        assert result["funding_coverage_until_ms"] == 4000
        book.mark_funding_coverage(5000, complete=False)
        assert book.snapshot(110)["total_net_usd"] is None


def test_conflicting_funding_duplicate_does_not_double_count(tmp_path):
    with ledger(tmp_path) as book:
        book.record_funding("one", 2000, 1)
        with pytest.raises(GridAccountingError, match="矛盾"):
            book.record_funding("one", 2000, 2)
        result = book.snapshot(100)
        assert result["funding_net_usd"] == 1
        assert result["status"] == "error"


@pytest.mark.parametrize("changes", [
    {"fee_known": False}, {"fee_token": None}, {"fee_token": "HYPE"},
    {"realized_pnl_known": False}, {"fee_usd": float("nan")},
    {"price": float("inf")}, {"qty": -1}, {"qty": 0},
])
def test_unknown_or_invalid_fill_fields_block_quotes_without_inventing_cost(tmp_path, changes):
    with ledger(tmp_path) as book:
        with pytest.raises(GridAccountingError):
            book.record_fill(replace(fill("one", True, 100), **changes))
        result = book.snapshot(100)
        assert result["inventory_qty"] == 0
        assert result["fill_count"] == 0
        assert result["status"] == "error"
        assert result["total_net_usd"] is None


def test_actual_negative_maker_fee_is_booked_but_projected_rebate_is_excluded(tmp_path):
    with ledger(tmp_path) as book:
        book.record_fill(fill("buy", True, 100, fee=-0.1, projected_self_rebate=1000))
        book.record_fill(fill("sell", False, 101, fee=-0.2, timestamp=3000, projected_referred_benefit=1000))
        result = book.snapshot(101)
        assert result["realized_net_usd"] == pytest.approx(1.3)
        assert result["grid_pair_net_usd"] == pytest.approx(1.3)
        assert result["fees_paid_usd"] == pytest.approx(-0.3)
        assert result["projected_rebates_included"] is False


def test_reconcile_mismatch_can_recover_after_missing_history_is_applied(tmp_path):
    with ledger(tmp_path, 1, 100) as book:
        assert not book.reconcile(2, 90)
        assert book.snapshot(90)["status"] == "reconciliation_required"
        book.record_fill(fill("late", True, 80))
        assert book.reconcile(2, 90)
        assert book.snapshot(90)["status"] == "ready"
        assert not book.reconcile(2, 95)
        assert book.snapshot(95)["average_entry_price"] == 90
        assert book.reconcile(2, 90)


def test_official_start_position_evidence_detects_offsetting_missing_history(tmp_path):
    with ledger(tmp_path, 1, 100) as book:
        with pytest.raises(GridAccountingError, match="成交前仓位"):
            book.record_fill(fill("one", True, 100, start_position_qty=2))
        assert book.snapshot(100)["fill_count"] == 0


@pytest.mark.parametrize("direction,qty,is_buy", [("long", 1, False), ("short", -1, True)])
def test_close_cannot_reverse_or_sell_unbacked_inventory(tmp_path, direction, qty, is_buy):
    with ledger(tmp_path, qty, 100, direction) as book:
        with pytest.raises(GridAccountingError, match="超过真实库存"):
            book.record_fill(fill("bad", is_buy, 100, qty=1.1))
        assert book.snapshot(100)["inventory_qty"] == qty


def test_out_of_order_fill_is_blocked_but_replayed_duplicate_and_pre_inception_are_skipped(tmp_path):
    with ledger(tmp_path) as book:
        assert not book.record_fill(fill("before", True, 100, timestamp=999))
        row = fill("new", True, 100, timestamp=3000)
        book.record_fill(row)
        assert not book.record_fill(row)
        with pytest.raises(GridAccountingError, match="乱序"):
            book.record_fill(fill("late", True, 90, timestamp=2000))
        assert book.snapshot(100)["fill_count"] == 1


def test_identity_mismatch_and_nonflat_direction_change_are_rejected(tmp_path):
    path = tmp_path / "inventory.sqlite"
    with ledger(tmp_path, 1, 100):
        pass
    with pytest.raises(GridAccountingError, match="标识不符"):
        GridAccounting(path, "another-account:coin")
    with pytest.raises(GridAccountingError, match="切换"):
        GridAccounting(path, "test-account:coin", "short")


def test_flat_direction_change_preserves_actual_pnl_and_next_short_pairs(tmp_path):
    path = tmp_path / "inventory.sqlite"
    with ledger(tmp_path) as book:
        book.record_fill(fill("buy", True, 100))
        book.record_fill(fill("sell", False, 105, timestamp=3000))
    with GridAccounting(path, "test-account:coin", "short") as book:
        assert book.reconcile(0, 0)
        book.record_fill(fill("sell2", False, 110, timestamp=4000))
        book.record_fill(fill("buy2", True, 108, timestamp=5000))
        result = book.snapshot(108)
        assert result["direction"] == "short"
        assert result["realized_net_usd"] == 7
        assert result["grid_pair_net_usd"] == 7


def test_unknown_initial_cost_cannot_bootstrap_zero_cost_and_zero_inventory_needs_no_cost(tmp_path):
    with GridAccounting(tmp_path / "bad.sqlite", "account:coin") as book:
        with pytest.raises(GridAccountingError, match="成本未知"):
            book.initialize(1, 0, 1000)
        assert book.snapshot(100)["status"] == "uninitialized"
        assert book.initialize(1, 100, 1000)
        assert book.snapshot(100)["status"] == "ready"
    with ledger(tmp_path, 0, 0) as book:
        assert book.snapshot()["unrealized_usd"] == 0


@pytest.mark.parametrize("invalid", [
    {"qty": -1}, {"entry": 0}, {"time_ms": -1},
    {"time_ms": -0.5}, {"time_ms": float("nan")}, {"time_ms": True},
])
def test_invalid_bootstrap_inputs_roll_back_and_can_be_corrected(tmp_path, invalid):
    with GridAccounting(tmp_path / "new.sqlite", "account:coin") as book:
        args = {"qty": 1, "entry": 100, "time_ms": 1000, **invalid}
        with pytest.raises(GridAccountingError):
            book.initialize(**args)
        state = book.snapshot(100)
        assert state["status"] == "uninitialized"
        assert state["initialized"] is False
        assert state["fill_count"] == 0
        assert state["inventory_qty"] == 0
        assert state["inception_time_ms"] is None
        assert book.initialize(1, 100, 1000)
        assert book.snapshot(100)["status"] == "ready"


def test_wrong_first_direction_does_not_poison_book_when_reopened_correctly(tmp_path):
    path = tmp_path / "new.sqlite"
    with GridAccounting(path, "account:coin", "long") as book:
        with pytest.raises(GridAccountingError, match="方向"):
            book.initialize(-1, 100, 1000)
    with GridAccounting(path, "account:coin", "short") as book:
        assert book.snapshot(100)["status"] == "uninitialized"
        assert book.initialize(-1, 100, 1000)
        result = book.snapshot(100)
        assert result["status"] == "ready"
        assert result["inventory_qty"] == -1
        assert result["average_entry_price"] == 100


def test_initialize_cannot_clear_error_on_previously_booked_inventory(tmp_path):
    with ledger(tmp_path) as book:
        row = fill("booked", True, 100)
        book.record_fill(row)
        with pytest.raises(GridAccountingError, match="矛盾"):
            book.record_fill(replace(row, qty=2))
        reason = book.snapshot(100)["reason"]
        assert not book.initialize(0, 0, 5000)
        result = book.snapshot(100)
        assert result["status"] == "error"
        assert result["reason"] == reason
        assert result["inventory_qty"] == 1
        assert result["fill_count"] == 1
        assert result["inception_time_ms"] == 1000


def test_sqlite_event_and_projection_are_one_atomic_transaction(tmp_path):
    with ledger(tmp_path) as book:
        connection = sqlite3.connect(tmp_path / "inventory.sqlite")
        connection.execute("CREATE TRIGGER fail_projection BEFORE UPDATE ON accounting_state BEGIN SELECT RAISE(ABORT,'injected write failure'); END")
        connection.commit()
        with pytest.raises(sqlite3.IntegrityError, match="injected"):
            book.record_fill(fill("one", True, 100))
        assert connection.execute("SELECT count(*) FROM accounting_events").fetchone()[0] == 0
        assert book.snapshot(100)["inventory_qty"] == 0
        connection.execute("DROP TRIGGER fail_projection")
        connection.commit()
        assert book.record_fill(fill("one", True, 100))
        assert book.snapshot(100)["inventory_qty"] == 1
        connection.close()


def test_decimal_dust_does_not_create_phantom_inventory_and_fully_closed_books_agree(tmp_path):
    with ledger(tmp_path) as book:
        for index in range(10):
            book.record_fill(fill(f"b{index}", True, 0.1, qty=0.1, fee=0.0001, timestamp=2000 + index))
        book.record_fill(fill("sell", False, 0.2, qty=1, fee=0.001, timestamp=3000))
        result = book.snapshot(0.2)
        assert result["inventory_qty"] == 0
        assert result["inventory_cost_usd"] == 0
        assert result["unmatched_qty"] == 0
        assert result["realized_net_usd"] == pytest.approx(0.098)
        assert result["grid_pair_net_usd"] == pytest.approx(0.098)
        assert result["net_invested_usd"] == pytest.approx(-0.098)
