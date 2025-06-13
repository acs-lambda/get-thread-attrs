import json
import urllib3
import logging
import time
from typing import Dict, Any, Optional
from db import store_llm_invocation
from config import get_together_ai_config, get_system_prompt, LOGGING_CONFIG

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOGGING_CONFIG['LEVEL']))

# Initialize urllib3 pool manager
http = urllib3.PoolManager()

def get_thread_attributes(conversation_text: str, account_id: Optional[str] = None, conversation_id: Optional[str] = None) -> Dict[str, str]:
    """
    Get thread attributes by analyzing conversation text using LLM.
    Returns a dictionary of attributes.
    """
    start_time = time.time()
    logger.info(f"Starting thread attributes analysis for conversation_id: {conversation_id}")
    
    # Get Together AI configuration
    tai_config = get_together_ai_config()
    
    headers = {
        "Authorization": f"Bearer {tai_config['API_KEY']}",
        "Content-Type": "application/json"
    }

    messages = [
        {
            "role": "system",
            "content": get_system_prompt('THREAD_ATTRIBUTES')
        },
        {
            "role": "user",
            "content": f"Please analyze this email conversation and provide the attributes:\n\n{conversation_text}"
        }
    ]

    payload = {
        "model": tai_config['MODEL'],
        "messages": messages,
        "temperature": tai_config['TEMPERATURE'],
        "max_tokens": tai_config['MAX_TOKENS'],
        "stop": tai_config['STOP_SEQUENCES'],
        "stream": False
    }

    if LOGGING_CONFIG['ENABLE_REQUEST_LOGGING']:
        logger.info("Preparing Together AI API request:")
        logger.info(f"  Model: {payload['model']}")
        logger.info(f"  Temperature: {payload['temperature']}")
        logger.info(f"  Max Tokens: {payload['max_tokens']}")
        logger.info(f"  System Prompt: {messages[0]['content'][:100]}...")  # Log first 100 chars of system prompt
        logger.info(f"  User Message Length: {len(messages[1]['content'])} characters")

    try:
        encoded_data = json.dumps(payload).encode('utf-8')
        api_start_time = time.time()
        
        logger.info("Sending request to Together AI API...")
        response = http.request(
            'POST',
            tai_config['API_URL'],
            body=encoded_data,
            headers=headers
        )
        api_duration = time.time() - api_start_time
        
        if LOGGING_CONFIG['ENABLE_PERFORMANCE_LOGGING']:
            logger.info(f"API request completed in {api_duration:.2f} seconds")

        if response.status != 200:
            logger.error(f"API call failed with status {response.status}")
            logger.error(f"Response data: {response.data.decode('utf-8')}")
            raise Exception("Failed to fetch response from Together AI API")

        response_data = json.loads(response.data.decode('utf-8'))
        if "choices" not in response_data:
            logger.error(f"Invalid API response structure: {json.dumps(response_data, indent=2)}")
            raise Exception("Invalid response from Together AI API")

        # Extract token usage
        usage = response_data.get("usage", {})
        input_tokens = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)
        total_tokens = input_tokens + output_tokens

        if LOGGING_CONFIG['ENABLE_RESPONSE_LOGGING']:
            logger.info("Together AI API Response Details:")
            logger.info(f"  Status Code: {response.status}")
            logger.info(f"  Input Tokens: {input_tokens}")
            logger.info(f"  Output Tokens: {output_tokens}")
            logger.info(f"  Total Tokens: {total_tokens}")
            logger.info(f"  Response Time: {api_duration:.2f} seconds")
            logger.info(f"  Tokens/Second: {total_tokens/api_duration:.2f}")

        # Store invocation record if we have an account_id
        if account_id:
            logger.info(f"Storing LLM invocation record for account: {account_id}")
            invocation_success = store_llm_invocation(
                associated_account=account_id,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                llm_email_type="thread_attributes",
                model_name=payload["model"],
                conversation_id=conversation_id
            )
            logger.info(f"LLM invocation record storage: {'Success' if invocation_success else 'Failed'}")

        # Parse the response into a dictionary
        content = response_data["choices"][0]["message"]["content"]
        attributes = {}
        for line in content.split('\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                attributes[key.strip()] = value.strip()

        if LOGGING_CONFIG['ENABLE_RESPONSE_LOGGING']:
            logger.info("Extracted Thread Attributes:")
            for key, value in attributes.items():
                logger.info(f"  {key}: {value}")

        total_duration = time.time() - start_time
        if LOGGING_CONFIG['ENABLE_PERFORMANCE_LOGGING']:
            logger.info(f"Total thread attributes analysis completed in {total_duration:.2f} seconds")

        return attributes

    except Exception as e:
        logger.error(f"Error in get_thread_attributes: {str(e)}", exc_info=True)
        logger.error(f"Failed request details:")
        logger.error(f"  Conversation ID: {conversation_id}")
        logger.error(f"  Account ID: {account_id}")
        logger.error(f"  Model: {payload['model']}")
        if 'response' in locals():
            logger.error(f"  Response Status: {response.status}")
            logger.error(f"  Response Data: {response.data.decode('utf-8')}")
        raise 