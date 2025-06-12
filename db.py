import boto3
import logging
import time
import uuid
from typing import Dict, Any, List, Optional

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')

def get_email_chain(conversation_id: str) -> List[Dict[str, Any]]:
    """
    Retrieves and formats the email chain for a conversation.
    Returns a list of dictionaries with consistent 'subject' and 'body' keys.
    """
    table = dynamodb.Table('Conversations')
    try:
        res = table.query(
            KeyConditionExpression='conversation_id = :cid',
            ExpressionAttributeValues={':cid': conversation_id}
        )
        items = res.get('Items', [])
        
        # Sort by timestamp
        sorted_items = sorted(items, key=lambda x: x.get('timestamp', ''))
        
        # Format items to have consistent keys
        formatted_chain = []
        for item in sorted_items:
            formatted_chain.append({
                'subject': item.get('subject', ''),
                'body': item.get('body', ''),
                'sender': item.get('sender', ''),
                'timestamp': item.get('timestamp', ''),
                'type': item.get('type', '')
            })
        
        return formatted_chain
    except Exception as e:
        logger.error(f"Error fetching email chain: {str(e)}")
        return [] 

def store_llm_invocation(
    associated_account: str,
    input_tokens: int,
    output_tokens: int,
    llm_email_type: str,
    model_name: str,
    conversation_id: Optional[str] = None,
    invocation_id: Optional[str] = None
) -> bool:
    """
    Store an LLM invocation record in DynamoDB.
    Returns True if successful, False otherwise.
    """
    try:
        # Log detailed information about the invocation
        logger.info(f"Storing LLM invocation:")
        logger.info(f"  - Account: {associated_account}")
        logger.info(f"  - Type: {llm_email_type}")
        logger.info(f"  - Model: {model_name}")
        logger.info(f"  - Input tokens: {input_tokens}")
        logger.info(f"  - Output tokens: {output_tokens}")
        logger.info(f"  - Total tokens: {input_tokens + output_tokens}")
        if conversation_id:
            logger.info(f"  - Conversation ID: {conversation_id}")
        if invocation_id:
            logger.info(f"  - Invocation ID: {invocation_id}")
        
        invocations_table = dynamodb.Table('Invocations')
        
        # Create timestamp for sorting
        timestamp = int(time.time() * 1000)
        
        item = {
            'id': str(uuid.uuid4()),  # Unique identifier for the invocation
            'associated_account': associated_account,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'llm_email_type': llm_email_type,
            'model_name': model_name,
            'timestamp': timestamp,
            'total_tokens': input_tokens + output_tokens  # Convenience field for analytics
        }
        
        # Add optional fields if provided
        if conversation_id:
            item['conversation_id'] = conversation_id
        if invocation_id:
            item['invocation_id'] = invocation_id
            
        invocations_table.put_item(Item=item)
        
        # Success logging
        logger.info(f"✅ Successfully stored LLM invocation record:")
        logger.info(f"   - Account: {associated_account}")  
        logger.info(f"   - Type: {llm_email_type}")
        logger.info(f"   - Tokens: {input_tokens + output_tokens} total")
        logger.info(f"   - Record ID: {item['id']}")
        if invocation_id:
            logger.info(f"   - Invocation ID: {invocation_id}")
        if conversation_id:
            logger.info(f"   - Conversation ID: {conversation_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error storing LLM invocation record: {str(e)}", exc_info=True)
        logger.error(f"Failed invocation details:")
        logger.error(f"   - Account: {associated_account}")
        logger.error(f"   - Type: {llm_email_type}")  
        logger.error(f"   - Model: {model_name}")
        logger.error(f"   - Tokens: {input_tokens}/{output_tokens}")
        if invocation_id:
            logger.error(f"   - Invocation ID: {invocation_id}")
        if conversation_id:
            logger.error(f"   - Conversation ID: {conversation_id}")
        return False 

def get_thread_account_id(conversation_id: str) -> Optional[str]:
    """
    Get the associated account ID for a conversation from the Threads table.
    Returns None if the thread doesn't exist or there's an error.
    """
    try:
        table = dynamodb.Table('Threads')
        response = table.get_item(
            Key={'conversation_id': conversation_id}
        )
        
        if 'Item' not in response:
            logger.warning(f"Thread not found for conversation {conversation_id}")
            return None
            
        account_id = response['Item'].get('associated_account')
        if not account_id:
            logger.warning(f"No associated_account found for conversation {conversation_id}")
            return None
            
        logger.info(f"Found account_id {account_id} for conversation {conversation_id}")
        return account_id
        
    except Exception as e:
        logger.error(f"Error getting thread account_id: {str(e)}")
        return None 