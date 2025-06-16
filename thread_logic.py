from config import logger
from utils import LambdaError, format_conversation_for_llm
from db import get_email_chain, get_thread_account_id, check_rate_limit
from llm_interface import get_thread_attributes

def get_attributes_for_thread(conversation_id):
    """
    Retrieves and processes thread attributes for a given conversation ID.
    """
    account_id = get_thread_account_id(conversation_id)
    if not account_id:
        raise LambdaError(404, "Account not found for this conversation.")

    # Check AWS and AI rate limits
    check_rate_limit(account_id, 'AWS')
    check_rate_limit(account_id, 'AI')

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
