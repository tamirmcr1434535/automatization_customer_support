import os
import logging
import functions_framework

# Твої існуючі імпорти з інших файлів
from classifier import classify_ticket
from reply_generator import generate_reply
from stripe_client import cancel_subscription
from zendesk_client import get_ticket, send_reply, add_tags, solve_ticket
from bq_logger import log_ticket

# Налаштовуємо логер, щоб все було видно
logging.basicConfig(level=logging.INFO, format='%(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

@functions_framework.http
def zendesk_webhook(request):
    logger.info("Webhook received")
    try:
        request_json = request.get_json(silent=True)
        if not request_json or 'ticket_id' not in request_json:
            logger.error("Missing ticket_id in request")
            return 'Missing ticket_id', 400
            
        ticket_id = request_json['ticket_id']
        logger.info(f"[{ticket_id}] Starting to process ticket")
        
        result = _process(ticket_id)
        return result, 200
        
    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}", exc_info=True)
        return 'Internal Server Error', 500

def _process(ticket_id):
    # Змінні ініціалізуємо заздалегідь, щоб у разі помилки логер мав хоч якісь дані
    intent, language, action_taken, reply_text = None, None, "unhandled", None
    confidence, chargeback_risk, reasoning = None, None, None

    try:
        # 1. ЧИТАЄМО ЗЕНДЕСК
        logger.info(f"[{ticket_id}] Fetching ticket data from Zendesk...")
        ticket = get_ticket(ticket_id)
        subject = ticket.get("subject", "")
        body = ticket.get("description", "")
        requester_email = ticket.get("requester", {}).get("email", "unknown@example.com")
        tags = ticket.get("tags",[])
        
        logger.info(f"[{ticket_id}] Subject: {subject} | Email: {requester_email}")

        if TEST_MODE and "automation_test" not in tags:
            logger.info(f"[{ticket_id}] Skip - test mode, missing tag 'automation_test'")
            return "Skipped (test mode)"

        # 2. КЛАСИФІКАЦІЯ
        logger.info(f"[{ticket_id}] Classifying with Claude...")
        classification = classify_ticket(subject, body)
        
        intent = classification.get("intent")
        language = classification.get("language")
        confidence = classification.get("confidence")
        chargeback_risk = classification.get("chargeback_risk")
        reasoning = classification.get("reasoning")
        
        logger.info(f"[{ticket_id}] Intent: {intent} ({confidence}) | Lang: {language}")
        logger.info(f"[{ticket_id}] Reasoning: {reasoning}")

        # 3. СКАСУВАННЯ В STRIPE (Якщо інтент відповідний)
        if intent in["SUB_CANCELLATION", "TRIAL_CANCELLATION", "SUB_RENEWAL_CANCELLATION"]:
            logger.info(f"[{ticket_id}] Trying to cancel Stripe sub for {requester_email}")
            if not DRY_RUN:
                cancel_subscription(requester_email)
                logger.info(f"[{ticket_id}] Stripe cancellation successful")
                action_taken = "cancelled_stripe"
            else:
                logger.info(f"[{ticket_id}] [DRY] Stripe: dry_run")
                action_taken = "dry_run_stripe"
        else:
            logger.info(f"[{ticket_id}] No Stripe action needed for intent {intent}")
            action_taken = "no_action"

        # 4. ГЕНЕРАЦІЯ ВІДПОВІДІ
        logger.info(f"[{ticket_id}] Generating reply text...")
        reply_text = generate_reply(intent, language, requester_email)
        
        # 5. ДІЇ В ZENDESK
        if not DRY_RUN:
            logger.info(f"[{ticket_id}] Sending reply and solving ticket in Zendesk...")
            send_reply(ticket_id, reply_text)
            add_tags(ticket_id,["bot_handled", "subscription_cancelled"])
            solve_ticket(ticket_id)
        else:
            logger.info(f"[{ticket_id}] [DRY] reply -> {reply_text[:100]}...")
            logger.info(f"[{ticket_id}] [DRY] tag 'bot_handled'")
            logger.info(f"[{ticket_id}] [DRY] solve")

        logger.info(f"[{ticket_id}] ✅ Done processing")
        status = "success"

    except Exception as e:
        logger.error(f"[{ticket_id}] ❌ Error during ticket processing: {str(e)}", exc_info=True)
        status = "error"
        # Якщо впало з помилкою, перекидаємо її далі, щоб Cloud Function повернув 500
        raise e

    finally:
        # 6. ЛОГУВАННЯ В BIGQUERY (виконається завжди, навіть якщо виникла помилка)
        logger.info(f"[{ticket_id}] Logging results to BigQuery...")
        log_ticket(
            ticket_id=ticket_id,
            intent=intent,
            language=language,
            action=action_taken,
            status=status,
            dry_run=DRY_RUN,
            reply_text=reply_text,
            confidence=confidence,
            chargeback_risk=chargeback_risk,
            reasoning=reasoning
        )
        
    return "Processed"
