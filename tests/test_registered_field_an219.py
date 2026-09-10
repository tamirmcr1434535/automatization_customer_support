"""AN-219 — the "Registered" field on cancellation tickets.

Anna: every ticket the bot handles should record which product the customer
registered for, marked "+Cross" when they also bought the add-on. Her example:
"customer purchases IQ Test Certification and Add-on/Cross Sale (IQ Test Report
on the WWIQTEST) → WWIQTEST+Cross".

Looking at how refunds did it turned up a defect rather than a pattern to copy:
`_build_refund_fields` read `nexus_data["cross_sale"]`, a key the
search-subscription response does not have (docs/nexus_refund_api_spec.md lists
`charges[]` with a per-charge `type`; refund_engine picks the Report out of that
list). It was therefore always False. In the 14 days to 2026-09-10 the field was
written 386 times, always the base value, never once "+Cross", while 478
cross_sale charges passed through the engine. So the fix belongs on both paths.
"""
import os
import sys
from unittest.mock import MagicMock, patch

os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")
os.environ.setdefault("ZENDESK_SUBDOMAIN", "wwiqtest")
os.environ.setdefault("ZENDESK_EMAIL", "bot@test.com")
os.environ.setdefault("ZENDESK_API_TOKEN", "token")
os.environ.setdefault("WOO_SITE_URL", "https://iqbooster.org")
os.environ.setdefault("WOO_CONSUMER_KEY", "ck_test")
os.environ.setdefault("WOO_CONSUMER_SECRET", "cs_test")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test")
os.environ.setdefault("SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")

sys.modules.setdefault("classifier", MagicMock())
sys.modules.setdefault("reply_generator", MagicMock())
sys.modules.setdefault("bq_logger", MagicMock())

import main  # noqa: E402

main.reply_generator.REFUND_APPROVE_CODES = {
    "WOULD_BE_REFUNDED", "WOULD_BE_REFUNDED_LAST_ONLY"}

REGISTERED_FIELD = int(main._ZENDESK_REGISTERED_FIELD_ID)


# ── The signal itself ────────────────────────────────────────────────────── #

def test_cross_sale_is_read_from_the_charge_list():
    nexus_data = {"subscription_id": 1, "charges": [
        {"charge_id": "ch_1", "type": "subscription"},
        {"charge_id": "ch_2", "type": "cross_sale"},
    ]}
    assert main._has_cross_sale(main._charge_types(nexus_data)) is True


def test_account_without_the_add_on_is_not_cross():
    nexus_data = {"subscription_id": 1, "charges": [
        {"charge_id": "ch_1", "type": "first_sale"},
        {"charge_id": "ch_2", "type": "renewal"},
    ]}
    assert main._has_cross_sale(main._charge_types(nexus_data)) is False


def test_missing_or_empty_charges_never_raise():
    for nexus_data in ({}, None, {"charges": None}, {"charges": []}):
        assert main._has_cross_sale(main._charge_types(nexus_data)) is False
    assert main._has_cross_sale(None) is False
    assert main._has_cross_sale([None, ""]) is False


def test_the_dead_top_level_key_is_no_longer_trusted():
    """The regression that hid for two months: a payload that carries the
    charge but not the (non-existent) top-level flag must still read as Cross."""
    nexus_data = {"subscription_id": 1,
                  "charges": [{"charge_id": "ch_1", "type": "cross_sale"}]}
    assert nexus_data.get("cross_sale") is None       # the key never existed
    assert main._has_cross_sale(main._charge_types(nexus_data)) is True


# ── Refund path: the same defect, fixed ─────────────────────────────────── #

def test_refund_fields_mark_cross_when_the_account_has_the_add_on():
    fields = main._build_refund_fields(
        True, "5490", "jpy", host_brand="wwiqtest", cross_sale=True,
        provider="stripe", charge_type="renewal", country="jp",
    )
    # NB: "qt_сross" has a CYRILLIC 'с' — verbatim from the Zendesk options.
    assert fields[main._ZENDESK_REGISTERED_FIELD_ID] == "qt_сross"


def test_refund_fields_keep_the_base_value_without_an_add_on():
    fields = main._build_refund_fields(
        True, "5490", "jpy", host_brand="wwiqtest", cross_sale=False,
        provider="stripe", charge_type="renewal", country="jp",
    )
    assert fields[main._ZENDESK_REGISTERED_FIELD_ID] == "iq_test"


# ── Cancellation path: what AN-219 actually asks for ────────────────────── #

def _cancel_result(charge_types, host="wwiqtest.com", plan="WW IQ Test Trial"):
    return {
        "status": "trial_cancelled", "cancelled": True,
        "subscription_type": "trial", "subscription_id": 1,
        "plan": plan, "source": "woocommerce", "nexus_host": host,
        "order_count": 1, "nexus_charge_types": charge_types,
    }


def _registered_writes(mock_zd):
    return [c.args[2] for c in mock_zd.set_custom_field.call_args_list
            if c.args[1] == REGISTERED_FIELD]


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "zendesk")
def test_cancellation_records_the_product(
    mock_zd, mock_reply, mock_validate, mock_log
):
    main._finish_cancellation(
        "1", "T", "EN", "TRIAL_CANCELLATION",
        _cancel_result(["subscription", "renewal"]), {},
        zendesk_brand="wwiqtest")
    assert _registered_writes(mock_zd) == ["iq_test"]


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "zendesk")
def test_cancellation_marks_cross_when_the_add_on_was_bought(
    mock_zd, mock_reply, mock_validate, mock_log
):
    """Anna's own example: IQ Test Certification + the Report on WWIQTEST."""
    main._finish_cancellation(
        "1", "T", "EN", "TRIAL_CANCELLATION",
        _cancel_result(["first_sale", "cross_sale", "subscription"]), {},
        zendesk_brand="wwiqtest")
    assert _registered_writes(mock_zd) == ["qt_сross"]


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "zendesk")
def test_registered_follows_the_product_not_the_inbox(
    mock_zd, mock_reply, mock_validate, mock_log
):
    """The lookup is cross-brand: 3 of the first 5 cancellations after the
    2026-09-10 deploy were contacted-iqbooster / owns-wwiqtest. iqbooster has no
    Registered option at all, so reading the inbox would write nothing."""
    main._finish_cancellation(
        "193375", "T", "NL", "TRIAL_CANCELLATION",
        _cancel_result(["subscription"], host="jap.wwiqtest.com"), {},
        zendesk_brand="iqbooster")
    assert _registered_writes(mock_zd) == ["iq_test"]


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "zendesk")
def test_brand_without_a_field_option_is_left_to_the_agent(
    mock_zd, mock_reply, mock_validate, mock_log
):
    """iqbooster is the subscription funnel, not a front-end test — it has no
    Registered option. A blank field an agent fills beats a wrong one."""
    main._finish_cancellation(
        "1", "T", "EN", "TRIAL_CANCELLATION",
        _cancel_result(["subscription"], host="iqbooster.org",
                       plan="IQ Test Subscription"), {},
        zendesk_brand="iqbooster")
    assert _registered_writes(mock_zd) == []


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "zendesk")
def test_a_failed_registered_write_never_breaks_the_cancellation(
    mock_zd, mock_reply, mock_validate, mock_log
):
    mock_zd.set_custom_field.side_effect = RuntimeError("Zendesk 500")
    main._finish_cancellation(
        "1", "T", "EN", "TRIAL_CANCELLATION",
        _cancel_result(["cross_sale"]), {}, zendesk_brand="wwiqtest")
    mock_zd.solve_ticket.assert_called_once()


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "zendesk")
def test_unknown_cross_sale_writes_nothing(
    mock_zd, mock_reply, mock_validate, mock_log
):
    """The legacy WooCommerce lookup returns no charge list — and that path is
    what a USE_NEXUS_FOR_LOOKUP=false rollback switches to. Guessing "no
    add-on" would write a base value that is wrong for every cross-sale
    customer, and nobody re-checks a field that is already filled in."""
    cr = _cancel_result([])
    del cr["nexus_charge_types"]          # key absent = we never looked
    main._finish_cancellation("1", "T", "EN", "TRIAL_CANCELLATION", cr, {},
                              zendesk_brand="wwiqtest")
    assert _registered_writes(mock_zd) == []


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "zendesk")
def test_empty_charge_list_is_a_fact_and_does_write(
    mock_zd, mock_reply, mock_validate, mock_log
):
    """Present-but-empty means we looked and there is no add-on."""
    main._finish_cancellation("1", "T", "EN", "TRIAL_CANCELLATION",
                              _cancel_result([]), {}, zendesk_brand="wwiqtest")
    assert _registered_writes(mock_zd) == ["iq_test"]
