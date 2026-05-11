# ====================================================================
# MERGER INTEGRATION TESTS — paste as a new cell in dryrun_monitor_v6.ipynb
# Same style as cell 4 (`_run_guard_test`). Requires `bot`, `WriteTracker`,
# `_PASS`, `_FAIL`, `_RESULTS`, `_make_ticket` to be already in scope from
# the earlier cells.
# ====================================================================
import copy
from unittest.mock import patch, MagicMock
from contextlib import ExitStack


def _run_merger_test(
    name, ticket_data, siblings, merger_status,
    refetch_overrides=None, expect_status=None,
    expect_no_reply=True, requester_id=42,
):
    """
    Test the merger integration inside main._process.

    siblings:           list of sibling ticket dicts (mocks
                        find_active_tickets_for_email). Empty list → no
                        merger invoked, normal flow.
    merger_status:      dict to return from ticket_merger.merge_user_tickets,
                        e.g. {"status": "merged", "target_id": 999, ...}.
                        None → merger raises RuntimeError (simulates crash).
    refetch_overrides:  dict merged into the ticket on the SECOND get_ticket
                        call (after merger ran), e.g. {"tags": [...,"merge"]}
                        or {"status": "closed"}.
    expect_status:      single string or list of acceptable statuses.
    """
    global _PASS, _FAIL
    tracker = WriteTracker()
    tid = ticket_data.get("id", "99999")
    if isinstance(expect_status, str):
        expect_status = [expect_status]

    # Inject requester_id (merger reads it from the ticket)
    ticket_data = copy.deepcopy(ticket_data)
    ticket_data.setdefault("requester_id", requester_id)

    refetched = copy.deepcopy(ticket_data)
    if refetch_overrides:
        for k, v in refetch_overrides.items():
            if k == "tags":
                refetched["tags"] = (refetched.get("tags") or []) + list(v)
            else:
                refetched[k] = v

    get_ticket_seq = [copy.deepcopy(ticket_data), refetched]

    def _get_ticket_side(*_a, **_kw):
        return get_ticket_seq.pop(0) if len(get_ticket_seq) > 1 else refetched

    original_test_mode = bot.TEST_MODE
    bot.TEST_MODE = False

    with ExitStack() as stack:
        stack.enter_context(patch.object(bot.zendesk, 'get_ticket',
            side_effect=_get_ticket_side))
        stack.enter_context(patch.object(bot.zendesk, 'find_active_tickets_for_email',
            return_value=copy.deepcopy(siblings)))

        # Mock merger entry point at the call site in main
        if merger_status is None:
            merger_mock = MagicMock(side_effect=RuntimeError("simulated merger crash"))
        else:
            merger_mock = MagicMock(return_value=merger_status)
        stack.enter_context(patch('main.ticket_merger.merge_user_tickets', merger_mock))

        # Standard tracker hooks (same as _run_guard_test)
        def _on_reply(t, b): tracker.replies.append(b)
        def _on_add_tag(t, tag): tracker.tags_added.append(tag)
        def _on_note(t, body): tracker.internal_notes.append(body)
        stack.enter_context(patch.object(bot.zendesk, 'post_reply', side_effect=_on_reply))
        stack.enter_context(patch.object(bot.zendesk, 'post_reply_and_set_pending',
            side_effect=lambda t, b: tracker.replies.append(b)))
        stack.enter_context(patch.object(bot.zendesk, 'add_tag', side_effect=_on_add_tag))
        stack.enter_context(patch.object(bot.zendesk, 'remove_tag', side_effect=lambda *a: None))
        stack.enter_context(patch.object(bot.zendesk, 'solve_ticket', side_effect=lambda t: None))
        stack.enter_context(patch.object(bot.zendesk, 'set_open', return_value=None))
        stack.enter_context(patch.object(bot.zendesk, 'add_internal_note', side_effect=_on_note))
        stack.enter_context(patch.object(bot.zendesk, 'count_bot_replies', return_value=0))
        stack.enter_context(patch.object(bot.zendesk, 'get_ticket_tags',
            return_value=ticket_data.get('tags', [])))
        stack.enter_context(patch.object(bot.zendesk, 'last_public_comment_is_from_agent',
            return_value=False))
        stack.enter_context(patch.object(bot.zendesk, 'get_first_customer_comment', return_value=None))
        stack.enter_context(patch.object(bot.zendesk, 'get_all_customer_comments_text',
            return_value=ticket_data.get('description', '')))
        stack.enter_context(patch.object(bot.zendesk, 'get_last_customer_comment', return_value=None))
        stack.enter_context(patch.object(bot.zendesk, 'was_recently_handled', return_value=False))
        for sm in ['notify_manual_review', 'notify_not_found', 'notify_refund_skip',
                   'notify_error', 'notify_spam_detected']:
            stack.enter_context(patch.object(bot.slack, sm,
                side_effect=lambda *a, _m=sm, **kw: tracker.slack_calls.append(_m) or True))
        # Default classify for "passes through" tests
        stack.enter_context(patch('main.classify_ticket', return_value={
            'intent': 'TRIAL_CANCELLATION', 'language': 'EN',
            'confidence': 0.95, 'chargeback_risk': '',
        }))
        stack.enter_context(patch.object(bot.woo, 'cancel_subscription', return_value={
            'status': 'dry_run', 'cancelled': True, 'subscription_type': 'trial',
            'subscription_id': 999, 'order_count': 1, 'plan': 'IQ Test',
        }))
        stack.enter_context(patch.object(bot.stripe_cli, 'cancel_subscription',
            return_value={'status': 'not_found'}))
        stack.enter_context(patch.object(bot.stripe_cli, 'find_email_by_last4', return_value=None))
        stack.enter_context(patch('main.log_result', return_value=None))

        try:
            result = bot._process(str(tid))
        except Exception as e:
            result = {'status': 'EXCEPTION', 'error': str(e)}

    bot.TEST_MODE = original_test_mode

    errors = []
    got_status = result.get('status', '?')
    if got_status not in expect_status:
        errors.append(f"status: expected {expect_status}, got '{got_status}'")
    if expect_no_reply and tracker.reply_count > 0:
        errors.append(f"expected NO reply but got {tracker.reply_count}")
    # Confirm merger called iff siblings exist AND requester_id present
    should_call_merger = bool(siblings) and bool(requester_id)
    if should_call_merger and merger_mock.call_count == 0:
        errors.append("expected merger to be invoked but it was not")
    if not should_call_merger and merger_mock.call_count > 0:
        errors.append(f"expected merger NOT invoked but it ran {merger_mock.call_count}x")

    passed = len(errors) == 0
    if passed: _PASS += 1
    else: _FAIL += 1
    _RESULTS.append({'name': name, 'passed': passed, 'errors': errors})
    return passed, errors


print('=' * 74)
print('MERGER INTEGRATION TESTS')
print('=' * 74)

print('--- 12. Merger Integration ---')

_run_merger_test(
    name='12.1 No siblings -> merger NOT called, normal flow proceeds',
    ticket_data=_make_ticket(),
    siblings=[],
    merger_status=None,  # irrelevant; merger won't be called
    expect_status=['success', 'skipped_not_handled', 'awaiting_card_digits',
                   'manual_review_required'],
    expect_no_reply=False)

_run_merger_test(
    name='12.2 Siblings exist, current merged INTO oldest -> skipped_merged',
    ticket_data=_make_ticket(tid='200'),
    siblings=[{'id': 199, 'subject': 'Older ticket', 'status': 'open'}],
    merger_status={'status': 'merged', 'target_id': 199,
                   'merged_ids': [200], 'current_was_target': False},
    refetch_overrides={'tags': ['merge']},
    expect_status='skipped_merged')

_run_merger_test(
    name='12.3 Siblings + current closed during merge -> skipped_closed',
    ticket_data=_make_ticket(tid='201'),
    siblings=[{'id': 198, 'subject': 'Older', 'status': 'open'}],
    merger_status={'status': 'merged', 'target_id': 198, 'merged_ids': [201]},
    refetch_overrides={'status': 'closed'},
    expect_status='skipped_closed')

_run_merger_test(
    name='12.4 Current IS oldest -> siblings folded INTO current, bot continues',
    ticket_data=_make_ticket(tid='150'),
    siblings=[{'id': 151, 'subject': 'Newer dup', 'status': 'new'}],
    merger_status={'status': 'merged', 'target_id': 150,
                   'merged_ids': [151], 'current_was_target': True},
    refetch_overrides=None,  # current ticket unchanged
    expect_status=['success', 'skipped_not_handled', 'awaiting_card_digits',
                   'manual_review_required'],
    expect_no_reply=False)

_run_merger_test(
    name='12.5 Merger raises -> falls through to skipped_merge_candidate',
    ticket_data=_make_ticket(tid='250'),
    siblings=[{'id': 249, 'subject': 'Older', 'status': 'open'}],
    merger_status=None,  # makes merger_mock raise
    refetch_overrides=None,
    expect_status='skipped_merge_candidate')

_run_merger_test(
    name='12.6 Siblings but no requester_id -> merger NOT called, fallback',
    ticket_data=_make_ticket(tid='260'),
    siblings=[{'id': 259, 'subject': 'Older', 'status': 'open'}],
    merger_status=None,
    refetch_overrides=None,
    expect_status='skipped_merge_candidate',
    requester_id=None)

_run_merger_test(
    name='12.7 Merger returns no_action -> falls through to skipped_merge_candidate',
    ticket_data=_make_ticket(tid='270'),
    siblings=[{'id': 269, 'subject': 'Older', 'status': 'open'}],
    merger_status={'status': 'no_action', 'active_count': 1},
    refetch_overrides=None,
    expect_status='skipped_merge_candidate')

print()
print('=' * 74)
print(f'MERGER RESULTS: {_PASS} passed, {_FAIL} failed total (across all sections)')
print('=' * 74)
for r in _RESULTS[-7:]:  # last 7 are merger tests
    icon = 'PASS' if r['passed'] else 'FAIL'
    print(f"  [{icon}] {r['name']}")
    if not r['passed']:
        for err in r['errors']:
            print(f"         {err}")
