import json
import time
from config import logger, LOGGING_CONFIG
from utils import create_response, LambdaError
from thread_logic import get_attributes_for_thread

def lambda_handler(event, context):
    start_time = time.time()
    conversation_id = None
    try:
        if LOGGING_CONFIG.get('ENABLE_REQUEST_LOGGING'):
            logger.info(f"Incoming event: {event}")

        if not event.get('body'):
            raise LambdaError(400, "Missing request body.")

        try:
            body = json.loads(event['body'])
            conversation_id = body.get('conversationId')
        except json.JSONDecodeError:
            raise LambdaError(400, "Invalid JSON in request body.")
        
        if not conversation_id:
            raise LambdaError(400, "Missing conversationId in request body.")

        attributes, account_id, email_count = get_attributes_for_thread(conversation_id)
        
        processing_time = time.time() - start_time
        
        response_body = {
            'attributes': attributes,
            'metadata': {
                'conversationId': conversation_id,
                'accountId': account_id,
                'emailCount': email_count,
                'processingTime': f"{processing_time:.2f}s"
            }
        }
        
        if LOGGING_CONFIG.get('ENABLE_PERFORMANCE_LOGGING'):
            logger.info(f"Lambda execution for {conversation_id} completed in {processing_time:.2f} seconds.")

        return create_response(200, response_body)

    except LambdaError as e:
        logger.error(f"Error processing get-thread-attrs for {conversation_id}: {e.message}")
        return create_response(e.status_code, {"error": e.message, "errorType": type(e).__name__})
    except Exception as e:
        logger.error(f"An unexpected error occurred in lambda_handler for {conversation_id}: {e}", exc_info=True)
        return create_response(500, {"error": "An internal server error occurred.", "errorType": "InternalServerError"})
