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