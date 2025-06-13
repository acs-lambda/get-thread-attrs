import json
import logging
import time
from typing import Dict, Any
from db import get_email_chain, get_thread_account_id
from llm_interface import get_thread_attributes
from utils import format_conversation_for_llm
from config import LOGGING_CONFIG

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOGGING_CONFIG['LEVEL']))

def create_error_response(status_code: int, message: str, error_type: str = None) -> Dict[str, Any]:
    """
    Create a standardized error response.
    """
    response = {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',  # Enable CORS
            'Access-Control-Allow-Credentials': True
        },
        'body': json.dumps({
            'error': message,
            'errorType': error_type or 'UnknownError'
        })
    }
    return response

def create_success_response(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a standardized success response.
    """
    response = {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',  # Enable CORS
            'Access-Control-Allow-Credentials': True
        },
        'body': json.dumps(data)
    }
    return response

def lambda_handler(event, context):
    """
    Lambda handler that retrieves thread attributes for a given conversation.
    """
    start_time = time.time()
    request_id = context.aws_request_id if context else 'unknown'
    logger.info(f"Lambda invocation started - Request ID: {request_id}")
    
    if LOGGING_CONFIG['ENABLE_REQUEST_LOGGING']:
        logger.info("Incoming event details:")
        logger.info(f"  Event type: {type(event).__name__}")
        logger.info(f"  Event keys: {list(event.keys())}")
        if 'requestContext' in event:
            logger.info(f"  Request context: {json.dumps(event['requestContext'], indent=2)}")

    try:
        # Parse the event body to get conversation ID
        if not event.get('body'):
            logger.warning("Missing request body in event")
            return create_error_response(400, 'Missing request body', 'MissingRequestBody')

        try:
            body = json.loads(event['body'])
            conversation_id = body.get('conversationId')
            logger.info(f"Parsed request body - Conversation ID: {conversation_id}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse request body: {str(e)}")
            logger.error(f"Raw body: {event['body']}")
            return create_error_response(400, 'Invalid JSON in request body', 'InvalidJSON')
        
        if not conversation_id:
            logger.warning("Missing conversationId in request body")
            return create_error_response(400, 'Missing conversationId in request body', 'MissingConversationId')

        # Get the account_id from the Threads table
        logger.info(f"Fetching account_id for conversation: {conversation_id}")
        account_id = get_thread_account_id(conversation_id)
        if account_id:
            logger.info(f"Found account_id {account_id} for conversation {conversation_id}")
        else:
            logger.warning(f"No account_id found for conversation {conversation_id}")

        # Get the email chain
        logger.info(f"Fetching email chain for conversation: {conversation_id}")
        email_chain = get_email_chain(conversation_id)
        if not email_chain:
            logger.warning(f"No email chain found for conversation {conversation_id}")
            return create_error_response(404, 'No conversation found with the given ID', 'ConversationNotFound')
        
        logger.info(f"Retrieved {len(email_chain)} emails in the chain")
            
        # Format conversation for LLM
        logger.info("Formatting conversation for LLM processing")
        conversation_text = format_conversation_for_llm(email_chain)
        logger.info(f"Formatted conversation length: {len(conversation_text)} characters")
        
        # Get thread attributes
        logger.info("Initiating thread attributes analysis")
        try:
            attributes = get_thread_attributes(
                conversation_text=conversation_text,
                account_id=account_id,
                conversation_id=conversation_id
            )
            
            if LOGGING_CONFIG['ENABLE_RESPONSE_LOGGING']:
                logger.info("Thread attributes analysis completed successfully:")
                for key, value in attributes.items():
                    logger.info(f"  {key}: {value}")

            total_duration = time.time() - start_time
            if LOGGING_CONFIG['ENABLE_PERFORMANCE_LOGGING']:
                logger.info(f"Lambda execution completed in {total_duration:.2f} seconds")
            
            return create_success_response({
                'attributes': attributes,
                'metadata': {
                    'conversationId': conversation_id,
                    'accountId': account_id,
                    'emailCount': len(email_chain),
                    'processingTime': f"{total_duration:.2f}s"
                }
            })
            
        except ValueError as e:
            logger.error(f"LLM validation error: {str(e)}")
            return create_error_response(422, str(e), 'LLMValidationError')
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        logger.error("Error context:")
        logger.error(f"  Request ID: {request_id}")
        logger.error(f"  Conversation ID: {conversation_id if 'conversation_id' in locals() else 'unknown'}")
        logger.error(f"  Account ID: {account_id if 'account_id' in locals() else 'unknown'}")
        logger.error(f"  Execution time: {time.time() - start_time:.2f} seconds")
        return create_error_response(500, f'Internal server error: {str(e)}', 'InternalServerError')
