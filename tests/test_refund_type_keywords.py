"""Naming the charge type has to work in the languages the tickets are in.

`_route_by_type_keyword` is the deterministic route that reads "I mean the
report, not the subscription" and picks the group. It fires only when exactly
one group's words hit, so a language with no words for one side cannot route at
all — the ticket falls through to the heuristic/LLM route and is then suppressed
by Guard 2b for having been routed by heuristic.

The report list carried 16 terms against ~30 for subscription, with Turkish,
Vietnamese, Indonesian, Thai, Chinese, Arabic, Polish and Italian missing
entirely. Those languages are 34 of the 421 soft-routed tickets per 20 days.
The subscription list had the mirror-image hole for VI/TH/ZH/AR.

And one term was actively wrong. "bericht" is German for report and Dutch for
MESSAGE, matched as a plain substring over the whole ticket, so every NL
customer writing "dit bericht" was routed to the report group. NL is 16 of that
cohort. It now applies only when the classifier says the ticket is German — a
misdetected language loses the hit and falls through, which is the safe
direction to fail.
"""
import main
import refund_engine as re_


def _ctx(text, lang="EN"):
    return re_.RefundContext(
        intent="REFUND_REQUEST", confidence=1.0, language=lang, country="",
        as_of_date="2026-08-20", nexus_available=True,
        nexus_data={"charges": []}, nexus_lookup_failed=False, ticket_text=text,
    )


_GROUPS = (("subscription", [{"charge_id": "s"}]),
           ("report", [{"charge_id": "r"}]),
           ("first_sale", []))


def _route(text, lang="EN"):
    return re_._route_by_type_keyword(_ctx(text, lang), _GROUPS)


# ── The Dutch "bericht" false positive ──────────────────────────────────── #

def test_dutch_message_is_no_longer_read_as_the_report():
    # "bericht" = message in Dutch. Every one of these used to route to report.
    assert _route("Naar aanleiding van dit bericht wil ik mijn geld terug", "NL") is None
    assert _route("Ik heb geen bericht ontvangen over deze afschrijving", "NL") is None


def test_german_bericht_still_routes_to_the_report():
    assert _route("Ich möchte nur den Bericht zurückerstattet haben", "DE") == "report"
    assert _route("Die Auswertung habe ich nie gelesen", "DE") == "report"


def test_dutch_can_still_name_the_report_properly():
    assert _route("Ik wil het rapport terugbetaald hebben", "NL") == "report"
    assert _route("Het verslag heb ik nooit gekregen", "NL") == "report"


# ── The languages that could not name a report at all ───────────────────── #

def test_report_now_reachable_in_the_missing_languages():
    for text, lang in [
        ("Sadece raporu iade edin lütfen", "TR"),
        ("Tôi chỉ muốn hoàn tiền báo cáo", "VI"),
        ("Saya hanya minta pengembalian laporan", "ID"),
        ("ฉันต้องการเงินคืนสำหรับรายงาน", "TH"),
        ("我只要退报告的钱", "ZH"),
        ("أريد استرداد قيمة التقرير", "AR"),
        ("Proszę o zwrot za raport", "PL"),
        ("Voglio il rimborso del rapporto", "IT"),
    ]:
        assert _route(text, lang) == "report", (lang, text)


def test_subscription_now_reachable_in_the_missing_languages():
    for text, lang in [
        ("Tôi muốn hoàn tiền gói đăng ký hàng tháng", "VI"),
        ("ยกเลิกสมัครสมาชิกรายเดือน", "TH"),
        ("我要取消每月订阅", "ZH"),
        ("أريد إلغاء الاشتراك", "AR"),
    ]:
        assert _route(text, lang) == "subscription", (lang, text)


# ── The one-group rule and the EN threat phrasing ───────────────────────── #

def test_naming_both_types_still_refuses_to_route():
    assert _route("refund the report and cancel my subscription too") is None


def test_threatening_to_report_us_is_not_a_charge_type():
    # A bare "report" is refund-dispute wording, which is why EN stays
    # phrase-anchored. These must not route to the report group.
    assert _route("I will report this to my bank and the authorities") is None
    assert _route("Ich werde das melden", "DE") is None


def test_english_report_phrases_route():
    for t in ("refund the report please", "I never opened my report",
              "the IQ report was useless", "full report was not delivered"):
        assert _route(t) == "report", t


def test_language_gating_fails_safe_not_wrong():
    # A German ticket misdetected as English loses the "bericht" hit and returns
    # None — the ticket falls through to the next route rather than routing to a
    # group the customer may not have meant.
    assert _route("Ich möchte den Bericht zurück", "EN") is None
