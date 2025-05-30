import boto3
import logging
from typing import Dict, Any, List

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