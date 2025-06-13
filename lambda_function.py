import json
import logging
from typing import Dict, Any
from db import get_email_chain, get_thread_account_id
from llm_interface import get_thread_attributes
from utils import format_conversation_for_llm
from config import LOGGING_CONFIG

# Set up logging
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOGGING_CONFIG['LEVEL']))

def lambda_handler(event, context):
    """
    Lambda handler that retrieves thread attributes for a given conversation.
    """
    try:
        # Parse the event body to get conversation ID
        if not event.get('body'):
            return {
                'statusCode': 400,
                'body': json.dumps('Missing request body')
            }

        try:
            body = json.loads(event['body'])
            conversation_id = body.get('conversationId')
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'body': json.dumps('Invalid JSON in request body')
            }
        
        if not conversation_id:
            return {
                'statusCode': 400,
                'body': json.dumps('Missing conversationId in request body')
            }

        # Get the account_id from the Threads table
        account_id = get_thread_account_id(conversation_id)
        if account_id:
            logger.info(f"Found account_id {account_id} for conversation {conversation_id}")
        else:
            logger.warning(f"No account_id found for conversation {conversation_id}")

        # Get the email chain
        email_chain = get_email_chain(conversation_id)
        if not email_chain:
            return {
                'statusCode': 404,
                'body': json.dumps('No conversation found with the given ID')
            }
            
        # Format conversation for LLM
        conversation_text = format_conversation_for_llm(email_chain)
        
        # Get thread attributes
        attributes = get_thread_attributes(
            conversation_text=conversation_text,
            account_id=account_id,
            conversation_id=conversation_id
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(attributes)
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps(f'Internal server error: {str(e)}')
        }
