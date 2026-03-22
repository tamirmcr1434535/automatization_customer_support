import os
import logging
from google.cloud import bigquery
from datetime import datetime

logger = logging.getLogger(__name__)

def log_ticket(ticket_id, intent, language, action, status, dry_run, 
               reply_text=None, confidence=None, chargeback_risk=None, reasoning=None):
    try:
        project_id = os.getenv("GCP_PROJECT", "powerful-vine-426615-r2")
        client = bigquery.Client(project=project_id)
        
        table_id = f"{project_id}.zendesk_bot.cancellation_logs"
        
        row_to_insert =[{
            "ticket_id": str(ticket_id),
            "intent": intent,
            "language": language,
            "action": action,
            "status": status,
            "dry_run": dry_run,
            "logged_at": datetime.utcnow().isoformat(),
            
            # Нові поля
            "reply_text": reply_text,
            "confidence": confidence,
            "chargeback_risk": chargeback_risk,
            "reasoning": reasoning
        }]
        
        logger.info(f"[{ticket_id}] Sening data to BigQuery: {table_id}...")
        
        errors = client.insert_rows_json(table_id, row_to_insert)
        
        if errors:
            logger.error(f"[{ticket_id}] BQ log failed: {errors}")
        else:
            logger.info(f"[{ticket_id}] ✅ Successfully logged to BigQuery!")
            
    except Exception as e:
        logger.error(f"[{ticket_id}] BQ log exception: {str(e)}", exc_info=True)
