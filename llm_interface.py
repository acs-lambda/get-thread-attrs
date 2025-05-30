import json
import urllib3
import logging
from typing import Dict, Any

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Together AI configuration
TAI_KEY = "2e1a1e910693ae18c09ad0585a7645e0f4595e90ec35bb366b6f5520221b6ca7"
TAI_URL = "https://api.together.xyz/v1/chat/completions"

# Initialize HTTP client
http = urllib3.PoolManager()

def get_thread_attributes(conversation_text: str) -> Dict[str, str]:
    """
    Uses Together AI to analyze the conversation and extract thread attributes.
    """
    headers = {
        "Authorization": f"Bearer {TAI_KEY}",
        "Content-Type": "application/json"
    }

    system_prompt = """You are an AI assistant that analyzes real estate conversations. Extract the following attributes from the conversation:

1. AI Summary: A concise phrase describing the current state of the conversation
2. Budget Range: A 2-4 word description of the lead's budget (use "UNKNOWN" if not mentioned)
3. Preferred Property Types: A maximum 5 word description of preferred property types (use "UNKNOWN" if not mentioned)
4. Timeline: A 2-5 word description of the lead's timeline to buy

Format your response exactly as:
AI Summary: [summary]
Budget Range: [budget]
Preferred Property Types: [types]
Timeline: [timeline]"""

    payload = {
        "model": "meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": conversation_text}
        ],
        "max_tokens": 512,
        "temperature": 0.7,
        "top_p": 0.7,
        "top_k": 50,
        "repetition_penalty": 1,
        "stop": ["<|im_end|>", "<|endoftext|>"],
        "stream": False
    }

    try:
        encoded_data = json.dumps(payload).encode('utf-8')
        response = http.request(
            'POST',
            TAI_URL,
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