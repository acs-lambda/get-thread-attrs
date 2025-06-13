import json
import urllib3
import logging
from typing import Dict, Any, Optional
from db import store_llm_invocation
from config import get_together_ai_config, get_system_prompt, LOGGING_CONFIG

# Set up logging
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOGGING_CONFIG['LEVEL']))

# Initialize urllib3 pool manager
http = urllib3.PoolManager()

def get_thread_attributes(conversation_text: str, account_id: Optional[str] = None, conversation_id: Optional[str] = None) -> Dict[str, str]:
    """
    Get thread attributes by analyzing conversation text using LLM.
    Returns a dictionary of attributes.
    """
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

    try:
        encoded_data = json.dumps(payload).encode('utf-8')
        response = http.request(
            'POST',
            tai_config['API_URL'],
            body=encoded_data,
            headers=headers
        )

        if response.status != 200:
            logger.error(f"API call failed with status {response.status}: {response.data}")
            raise Exception("Failed to fetch response from Together AI API")

        response_data = json.loads(response.data.decode('utf-8'))
        if "choices" not in response_data:
            logger.error(f"Invalid API response: {response_data}")
            raise Exception("Invalid response from Together AI API")

        # Extract token usage
        usage = response_data.get("usage", {})
        input_tokens = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)

        # Store invocation record if we have an account_id
        if account_id:
            logger.info("Storing LLM invocation record")
            invocation_success = store_llm_invocation(
                associated_account=account_id,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                llm_email_type="thread_attributes",
                model_name=payload["model"],
                conversation_id=conversation_id
            )
            logger.info(f"Stored invocation record: {'Success' if invocation_success else 'Failed'}")

        # Parse the response into a dictionary
        content = response_data["choices"][0]["message"]["content"]
        attributes = {}
        for line in content.split('\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                attributes[key.strip()] = value.strip()

        return attributes
    except Exception as e:
        logger.error(f"Error in get_thread_attributes: {str(e)}")
        raise 