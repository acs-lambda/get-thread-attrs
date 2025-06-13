import boto3
import logging
import time
import uuid
from typing import Dict, Any, List, Optional
from config import get_table_name, LOGGING_CONFIG

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOGGING_CONFIG['LEVEL']))

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')

def get_email_chain(conversation_id: str) -> List[Dict[str, Any]]:
    """
    Retrieves and formats the email chain for a conversation.
    Returns a list of dictionaries with consistent 'subject' and 'body' keys.
    """
    start_time = time.time()
    logger.info(f"Fetching email chain for conversation: {conversation_id}")
    
    table = dynamodb.Table(get_table_name('CONVERSATIONS'))
    try:
        if LOGGING_CONFIG['ENABLE_REQUEST_LOGGING']:
            logger.info(f"Querying DynamoDB table: {table.name}")
            logger.info(f"Query parameters: conversation_id = {conversation_id}")
        
        query_start = time.time()
        res = table.query(
            KeyConditionExpression='conversation_id = :cid',
            ExpressionAttributeValues={':cid': conversation_id}
        )
        query_duration = time.time() - query_start
        
        if LOGGING_CONFIG['ENABLE_PERFORMANCE_LOGGING']:
            logger.info(f"DynamoDB query completed in {query_duration:.2f} seconds")
        
        items = res.get('Items', [])
        logger.info(f"Retrieved {len(items)} items from DynamoDB")
        
        # Sort by timestamp
        sorted_items = sorted(items, key=lambda x: x.get('timestamp', ''))
        
        # Format items to have consistent keys
        formatted_chain = []
        for idx, item in enumerate(sorted_items, 1):
            formatted_item = {
                'subject': item.get('subject', ''),
                'body': item.get('body', ''),
                'sender': item.get('sender', ''),
                'timestamp': item.get('timestamp', ''),
                'type': item.get('type', '')
            }
            formatted_chain.append(formatted_item)
            if LOGGING_CONFIG['ENABLE_RESPONSE_LOGGING']:
                logger.debug(f"Email {idx}:")
                logger.debug(f"  Subject: {formatted_item['subject']}")
                logger.debug(f"  Sender: {formatted_item['sender']}")
                logger.debug(f"  Timestamp: {formatted_item['timestamp']}")
                logger.debug(f"  Type: {formatted_item['type']}")
                logger.debug(f"  Body length: {len(formatted_item['body'])} characters")
        
        total_duration = time.time() - start_time
        if LOGGING_CONFIG['ENABLE_PERFORMANCE_LOGGING']:
            logger.info(f"Total email chain retrieval completed in {total_duration:.2f} seconds")
        
        return formatted_chain
        
    except Exception as e:
        logger.error(f"Error fetching email chain: {str(e)}", exc_info=True)
        logger.error("Error context:")
        logger.error(f"  Conversation ID: {conversation_id}")
        logger.error(f"  Table: {table.name}")
        logger.error(f"  Execution time: {time.time() - start_time:.2f} seconds")
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
    start_time = time.time()
    logger.info(f"Storing LLM invocation record for account: {associated_account}")
    
    if LOGGING_CONFIG['ENABLE_REQUEST_LOGGING']:
        logger.info("Invocation details:")
        logger.info(f"  Account: {associated_account}")
        logger.info(f"  Type: {llm_email_type}")
        logger.info(f"  Model: {model_name}")
        logger.info(f"  Input tokens: {input_tokens}")
        logger.info(f"  Output tokens: {output_tokens}")
        logger.info(f"  Total tokens: {input_tokens + output_tokens}")
        if conversation_id:
            logger.info(f"  Conversation ID: {conversation_id}")
        if invocation_id:
            logger.info(f"  Invocation ID: {invocation_id}")
    
    try:
        invocations_table = dynamodb.Table(get_table_name('INVOCATIONS'))
        
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
            
        if LOGGING_CONFIG['ENABLE_REQUEST_LOGGING']:
            logger.info(f"Writing to DynamoDB table: {invocations_table.name}")
            logger.info(f"Item ID: {item['id']}")
        
        write_start = time.time()
        invocations_table.put_item(Item=item)
        write_duration = time.time() - write_start
        
        if LOGGING_CONFIG['ENABLE_PERFORMANCE_LOGGING']:
            logger.info(f"DynamoDB write completed in {write_duration:.2f} seconds")
        
        # Success logging
        logger.info(f"✅ Successfully stored LLM invocation record:")
        logger.info(f"   - Record ID: {item['id']}")
        logger.info(f"   - Account: {associated_account}")
        logger.info(f"   - Type: {llm_email_type}")
        logger.info(f"   - Tokens: {input_tokens + output_tokens} total")
        if invocation_id:
            logger.info(f"   - Invocation ID: {invocation_id}")
        if conversation_id:
            logger.info(f"   - Conversation ID: {conversation_id}")
        
        total_duration = time.time() - start_time
        if LOGGING_CONFIG['ENABLE_PERFORMANCE_LOGGING']:
            logger.info(f"Total invocation storage completed in {total_duration:.2f} seconds")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error storing LLM invocation record: {str(e)}", exc_info=True)
        logger.error("Error context:")
        logger.error(f"   - Account: {associated_account}")
        logger.error(f"   - Type: {llm_email_type}")
        logger.error(f"   - Model: {model_name}")
        logger.error(f"   - Tokens: {input_tokens}/{output_tokens}")
        logger.error(f"   - Table: {invocations_table.name}")
        if invocation_id:
            logger.error(f"   - Invocation ID: {invocation_id}")
        if conversation_id:
            logger.error(f"   - Conversation ID: {conversation_id}")
        logger.error(f"   - Execution time: {time.time() - start_time:.2f} seconds")
        return False

def get_thread_account_id(conversation_id: str) -> Optional[str]:
    """
    Get the associated account ID for a conversation from the Threads table.
    Returns None if the thread doesn't exist or there's an error.
    """
    start_time = time.time()
    logger.info(f"Fetching account ID for conversation: {conversation_id}")
    
    try:
        table = dynamodb.Table(get_table_name('THREADS'))
        
        if LOGGING_CONFIG['ENABLE_REQUEST_LOGGING']:
            logger.info(f"Querying DynamoDB table: {table.name}")
            logger.info(f"Query parameters: conversation_id = {conversation_id}")
        
        query_start = time.time()
        response = table.get_item(
            Key={'conversation_id': conversation_id}
        )
        query_duration = time.time() - query_start
        
        if LOGGING_CONFIG['ENABLE_PERFORMANCE_LOGGING']:
            logger.info(f"DynamoDB query completed in {query_duration:.2f} seconds")
        
        if 'Item' not in response:
            logger.warning(f"Thread not found for conversation {conversation_id}")
            return None
            
        account_id = response['Item'].get('associated_account')
        if not account_id:
            logger.warning(f"No associated_account found for conversation {conversation_id}")
            return None
            
        logger.info(f"Found account_id {account_id} for conversation {conversation_id}")
        
        total_duration = time.time() - start_time
        if LOGGING_CONFIG['ENABLE_PERFORMANCE_LOGGING']:
            logger.info(f"Total account ID retrieval completed in {total_duration:.2f} seconds")
            
        return account_id
        
    except Exception as e:
        logger.error(f"Error getting thread account_id: {str(e)}", exc_info=True)
        logger.error("Error context:")
        logger.error(f"  Conversation ID: {conversation_id}")
        logger.error(f"  Table: {table.name}")
        logger.error(f"  Execution time: {time.time() - start_time:.2f} seconds")
        return None 

def get_user_rate_limits(account_id: str) -> Dict[str, int]:
    """
    Get the rate limits for a user from the Users table.
    Returns a dictionary with 'rl_aws' and 'rl_ai' limits.
    """
    start_time = time.time()
    logger.info(f"Fetching rate limits for account: {account_id}")
    
    try:
        table = dynamodb.Table(get_table_name('USERS'))
        
        if LOGGING_CONFIG['ENABLE_REQUEST_LOGGING']:
            logger.info(f"Querying DynamoDB table: {table.name}")
            logger.info(f"Query parameters: account_id = {account_id}")
        
        query_start = time.time()
        response = table.get_item(
            Key={'account_id': account_id}
        )
        query_duration = time.time() - query_start
        
        if LOGGING_CONFIG['ENABLE_PERFORMANCE_LOGGING']:
            logger.info(f"DynamoDB query completed in {query_duration:.2f} seconds")
        
        if 'Item' not in response:
            logger.warning(f"User not found for account {account_id}")
            return {'rl_aws': 0, 'rl_ai': 0}
            
        item = response['Item']
        rate_limits = {
            'rl_aws': item.get('rl_aws', 0),
            'rl_ai': item.get('rl_ai', 0)
        }
        
        logger.info(f"Found rate limits for account {account_id}: AWS={rate_limits['rl_aws']}, AI={rate_limits['rl_ai']}")
        return rate_limits
        
    except Exception as e:
        logger.error(f"Error fetching user rate limits: {str(e)}", exc_info=True)
        logger.error("Error context:")
        logger.error(f"  Account ID: {account_id}")
        logger.error(f"  Table: {table.name}")
        logger.error(f"  Execution time: {time.time() - start_time:.2f} seconds")
        return {'rl_aws': 0, 'rl_ai': 0}

def get_and_increment_rate_limit(account_id: str, table_name: str) -> int:
    """
    Get the current invocation count and increment it for a given account.
    Uses DynamoDB atomic increment operation.
    Returns the new invocation count.
    """
    start_time = time.time()
    logger.info(f"Getting and incrementing rate limit for account: {account_id} in table: {table_name}")
    
    try:
        table = dynamodb.Table(get_table_name(table_name))
        
        # Try to get existing record
        try:
            response = table.get_item(
                Key={'associated_account': account_id}
            )
            current_count = response.get('Item', {}).get('invocations', 0)
            item_exists = 'Item' in response
        except Exception:
            current_count = 0
            item_exists = False
        
        # Calculate TTL (1 minute from now in seconds)
        ttl = int(time.time()) + 60
        
        # Update or create record with atomic increment
        if item_exists:
            update_expression = "SET invocations = if_not_exists(invocations, :zero) + :inc"
            expression_values = {
                ':zero': 0,
                ':inc': 1
            }
            expression_names = {}
        else:
            update_expression = "SET invocations = if_not_exists(invocations, :zero) + :inc, #ttl = :ttl"
            expression_values = {
                ':zero': 0,
                ':inc': 1,
                ':ttl': ttl
            }
            expression_names = {
                '#ttl': 'ttl'
            }
        
        response = table.update_item(
            Key={'associated_account': account_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ExpressionAttributeNames=expression_names,
            ReturnValues='UPDATED_NEW'
        )
        
        new_count = response['Attributes']['invocations']
        logger.info(f"Updated rate limit for account {account_id} in {table_name}: {new_count}")
        if not item_exists:
            logger.info(f"Added TTL of {ttl} (1 minute from now) for new record")
        return new_count
        
    except Exception as e:
        logger.error(f"Error updating rate limit: {str(e)}", exc_info=True)
        logger.error("Error context:")
        logger.error(f"  Account ID: {account_id}")
        logger.error(f"  Table: {table_name}")
        logger.error(f"  Execution time: {time.time() - start_time:.2f} seconds")
        return 0

def check_rate_limit(account_id: str, limit_type: str) -> tuple[bool, int, int]:
    """
    Check if a rate limit has been exceeded for a given account.
    Returns (is_exceeded, current_count, limit) tuple.
    """
    start_time = time.time()
    logger.info(f"Checking {limit_type} rate limit for account: {account_id}")
    
    try:
        # Get user's rate limits
        rate_limits = get_user_rate_limits(account_id)
        limit = rate_limits[f'rl_{limit_type.lower()}']
        
        # Get and increment current count
        table_name = f'RL_{limit_type.upper()}'
        current_count = get_and_increment_rate_limit(account_id, table_name)
        
        is_exceeded = current_count > limit
        
        logger.info(f"Rate limit check for {limit_type}:")
        logger.info(f"  Account: {account_id}")
        logger.info(f"  Current count: {current_count}")
        logger.info(f"  Limit: {limit}")
        logger.info(f"  Exceeded: {is_exceeded}")
        
        return is_exceeded, current_count, limit
        
    except Exception as e:
        logger.error(f"Error checking rate limit: {str(e)}", exc_info=True)
        logger.error("Error context:")
        logger.error(f"  Account ID: {account_id}")
        logger.error(f"  Limit type: {limit_type}")
        logger.error(f"  Execution time: {time.time() - start_time:.2f} seconds")
        return False, 0, 0 