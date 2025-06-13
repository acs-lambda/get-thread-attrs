import os
from typing import Dict, Any

# DynamoDB Table Names
DYNAMODB_TABLES = {
    'CONVERSATIONS': 'Conversations',
    'THREADS': 'Threads',
    'INVOCATIONS': 'Invocations'
}

# Together AI Configuration
TOGETHER_AI = {
    'API_URL': 'https://api.together.xyz/v1/chat/completions',
    'API_KEY': os.environ.get('TAI_KEY', 'NULL'),  # Get from environment variable
    'MODEL': 'meta-llama/Llama-3.3-70B-Instruct-Turbo-Free',
    'TEMPERATURE': 0.1,
    'MAX_TOKENS': 500,
    'STOP_SEQUENCES': ['<|im_end|>', '<|endoftext|>']
}

# LLM System Prompts
SYSTEM_PROMPTS = {
    'THREAD_ATTRIBUTES': """You are a helpful assistant that analyzes email conversations and extracts key attributes. For each conversation, provide the following attributes in a clear format:

1. sentiment: The overall sentiment of the conversation (positive, negative, neutral)
2. urgency: How urgent the conversation is (high, medium, low)
3. complexity: How complex the conversation is (high, medium, low)
4. topic: The main topic of the conversation
5. action_required: Whether any action is required (yes, no)

Provide each attribute on a new line in the format 'attribute: value'"""
}

# Logging Configuration
LOGGING_CONFIG = {
    'LEVEL': 'INFO',
    'FORMAT': '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    'DATE_FORMAT': '%Y-%m-%d %H:%M:%S',
    'LOG_LEVELS': {
        'DEBUG': 10,
        'INFO': 20,
        'WARNING': 30,
        'ERROR': 40,
        'CRITICAL': 50
    },
    'ENABLE_REQUEST_LOGGING': True,
    'ENABLE_RESPONSE_LOGGING': True,
    'ENABLE_PERFORMANCE_LOGGING': True
}

# Lambda Configuration
LAMBDA_CONFIG = {
    'TIMEOUT': 30,  # seconds
    'MEMORY_SIZE': 256  # MB
}

def get_table_name(table_key: str) -> str:
    """Helper function to get DynamoDB table name"""
    return DYNAMODB_TABLES.get(table_key, '')

def get_together_ai_config() -> Dict[str, Any]:
    """Helper function to get Together AI configuration"""
    return TOGETHER_AI.copy()

def get_system_prompt(prompt_key: str) -> str:
    """Helper function to get system prompt"""
    return SYSTEM_PROMPTS.get(prompt_key, '') 