"""The reason a refund reply was WITHHELD has to reach BigQuery.

`main._refund_would_be_eval` has always written `result["refund_reply_suppressed"]`,
but `bq_logger.SCHEMA` never carried the column, so the value died in the dict.
Consequence, measured over 2026-08-15..09-03: 616 of 2121 refund escalations were
tickets where the engine had reached a clean verdict and a template existed, and
which of the three suppression rules fired could only be guessed at from the
absence of `refund_draft_reply`. The cross-sale guard's premise — that a
soft-routed ticket may really be disputing the cross-sale — could be argued only
from reading the code, never from data.

Two of the assertions here are deliberately about the SCHEMA and not about
behaviour: on 2026-08-11 three fields were added to the row builder while the
live table lacked them, and BigQuery's streaming insert rejects the WHOLE row on
an unknown field — 704 log rows were lost over ~1.5 days. `ensure_log_table`
now self-heals that, but a field present in the row and absent from SCHEMA would
still break it, so the pair is locked together by test.
"""
import os
from unittest.mock import MagicMock, patch

os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")

import bq_logger


_NEW_FIELDS = {
    "refund_reply_suppressed":   "STRING",
    "refund_ask_in_text":        "BOOLEAN",
    "refund_has_cross_or_first": "BOOLEAN",
    "refund_soft_routed":        "BOOLEAN",
}


def _row_for(result: dict) -> dict:
    """Run log_result and hand back the single row it tried to stream."""
    client = MagicMock()
    client.insert_rows_json.return_value = []
    with patch.object(bq_logger, "_client", return_value=client):
        bq_logger.log_result(result)
    client.insert_rows_json.assert_called_once()
    _table, rows = client.insert_rows_json.call_args.args
    assert len(rows) == 1
    return rows[0]


def test_schema_carries_the_suppression_columns():
    by_name = {f.name: f.field_type for f in bq_logger.SCHEMA}
    for name, ftype in _NEW_FIELDS.items():
        assert name in by_name, f"{name} missing from bq_logger.SCHEMA"
        assert by_name[name] == ftype


def test_every_row_field_exists_in_schema():
    # The 2026-08-11 failure mode in one assertion: a key in the row builder with
    # no SchemaField drops the entire row, not just that key.
    schema_names = {f.name for f in bq_logger.SCHEMA}
    row = _row_for({"ticket_id": "1", "intent": "REFUND_REQUEST"})
    assert set(row) - schema_names == set()


def test_suppression_reason_and_guard_inputs_are_logged():
    row = _row_for({
        "ticket_id": "9203",
        "intent": "REFUND_REQUEST",
        "refund_reason_code": "WOULD_BE_REFUNDED",
        "refund_reply_suppressed": "cross_sale_ambiguous_route",
        "refund_ask_in_text": True,
        "refund_has_cross_or_first": True,
        "refund_soft_routed": True,
    })
    assert row["refund_reply_suppressed"] == "cross_sale_ambiguous_route"
    assert row["refund_ask_in_text"] is True
    assert row["refund_has_cross_or_first"] is True
    assert row["refund_soft_routed"] is True


def test_not_evaluated_stays_null_not_false():
    # Three-state on purpose. "The customer's words carried no refund ask" and
    # "we never looked" are different facts, and only one of them is evidence
    # about the guard. bool()-wrapping these would silently merge them and make
    # the log read as if every non-refund ticket had been checked.
    row = _row_for({"ticket_id": "1", "intent": "SUB_CANCELLATION"})
    assert row["refund_ask_in_text"] is None
    assert row["refund_has_cross_or_first"] is None
    assert row["refund_soft_routed"] is None
    assert row["refund_reply_suppressed"] == ""


def test_suppression_reason_survives_the_early_return_paths():
    # opened_dispute / followup_existing_refund are set on branches that
    # `return decision` before the reply block. They reach BQ only because
    # `result` is mutated in place — worth pinning.
    for reason in ("opened_dispute", "followup_existing_refund"):
        row = _row_for({"ticket_id": "1", "refund_reply_suppressed": reason})
        assert row["refund_reply_suppressed"] == reason
