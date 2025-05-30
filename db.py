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

def update_thread_attributes(conversation_id: str, attributes: Dict[str, str]) -> bool:
    """
    Updates the thread attributes in the Threads table.
    
    Args:
        conversation_id (str): The ID of the conversation
        attributes (Dict[str, str]): Dictionary containing the thread attributes
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    table = dynamodb.Table('Threads')
    try:
        # Build the update expression and attribute values
        update_expr = "SET "
        expr_attr_values = {}
        expr_attr_names = {}
        
        # Add each attribute to the update expression
        for i, (key, value) in enumerate(attributes.items()):
            placeholder = f":val{i}"
            name_placeholder = f"#attr{i}"
            update_expr += f"{name_placeholder} = {placeholder}, "
            expr_attr_values[placeholder] = value
            expr_attr_names[name_placeholder] = key.lower().replace(' ', '_')
        
        # Remove trailing comma and space
        update_expr = update_expr[:-2]
        
        # Perform the update
        table.update_item(
            Key={'conversation_id': conversation_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_attr_values,
            ExpressionAttributeNames=expr_attr_names
        )
        
        logger.info(f"Successfully updated thread attributes for conversation {conversation_id}")
        return True
    except Exception as e:
        logger.error(f"Error updating thread attributes: {str(e)}")
        return False 