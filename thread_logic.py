from config import logger
from utils import LambdaError, format_conversation_for_llm, invoke_lambda
from db import get_email_chain, get_thread_account_id
from llm_interface import get_thread_attributes
from typing import Dict, Any, List

def format_conversation_for_llm(email_chain: List[Dict[str, Any]]) -> str:
    """
    Formats the email chain into a single string for LLM processing.
    """
    formatted_text = ""
    for email in email_chain:
        formatted_text += f"Subject: {email.get('subject', '')}\n"
        formatted_text += f"Body: {email.get('body', '')}\n\n"
    return formatted_text

def get_attributes_for_thread(conversation_id):
    """
    Retrieves and processes thread attributes for a given conversation ID.
    """
    account_id = get_thread_account_id(conversation_id)
    if not account_id:
        raise LambdaError(404, "Account not found for this conversation.")

    session_id = "dummy_session_id" # This should be replaced with a real session ID

    # Check AWS and AI rate limits by invoking the respective lambdas
    invoke_lambda('RateLimitAWS', {'client_id': account_id, 'session': session_id})
    invoke_lambda('RateLimitAI', {'client_id': account_id, 'session': session_id})

    email_chain = get_email_chain(conversation_id)
    if not email_chain:
        raise LambdaError(404, "No conversation found with the given ID.")
    
    conversation_text = format_conversation_for_llm(email_chain)
    
    try:
        attributes = get_thread_attributes(
            conversation_text=conversation_text,
            account_id=account_id,
            conversation_id=conversation_id
        )
        return attributes, account_id, len(email_chain)
        
    except ValueError as e:
        logger.error(f"LLM validation error for {conversation_id}: {e}")
        raise LambdaError(422, str(e))
    except Exception as e:
        logger.error(f"Error getting thread attributes for {conversation_id}: {e}")
        raise LambdaError(500, "Failed to get thread attributes from LLM.")
