import json
import logging
from db import get_email_chain
from llm_interface import get_thread_attributes
from utils import format_conversation_for_llm

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda handler that retrieves thread attributes for a given conversation.
    """
    try:
        # Get conversation ID from the event
        conversation_id = event.get('conversation_id')
        if not conversation_id:
            return {
                'statusCode': 400,
                'body': json.dumps('Missing conversation_id in event')
            }

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
        attributes = get_thread_attributes(conversation_text)

        return {
            'statusCode': 200,
            'body': json.dumps(attributes)
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Internal server error: {str(e)}')
        }
