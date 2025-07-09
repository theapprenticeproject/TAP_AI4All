# File: ~/frappe-bench/apps/tap_educational_assistant/tap_educational_assistant/utils/response_formatter.py

import re
from typing import Any

def clean_response_text(text: str, response_type: str = "general") -> str:
    """
    Unified response text cleaning for all components
    
    Args:
        text: Raw response text
        response_type: Type of response ("general", "sql", "educational")
    
    Returns:
        Cleaned and formatted text
    """
    if not text or not isinstance(text, str):
        return ""
    
    # Basic cleaning (common to all types)
    cleaned = text.replace('\\n', '\n')
    cleaned = cleaned.replace('[BLANK LINE]', '\n')
    
    # Remove markdown formatting
    cleaned = re.sub(r'\*\*(.*?)\*\*', r'\1', cleaned)  # Remove bold
    cleaned = re.sub(r'\*(.*?)\*', r'\1', cleaned)      # Remove italic  
    cleaned = re.sub(r'#{1,6}\s*(.+)', r'\1\n', cleaned)  # Remove headers
    
    # Clean up spacing
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)       # Max 2 newlines
    cleaned = re.sub(r'[ \t]+', ' ', cleaned)          # Clean spaces
    cleaned = cleaned.strip()
    
    # Type-specific formatting
    if response_type == "sql":
        cleaned = _format_sql_response(cleaned)
    elif response_type == "educational":
        cleaned = _format_educational_response(cleaned)
    
    return cleaned

def _format_sql_response(text: str) -> str:
    """Format SQL-specific responses with educational context"""
    
    # If response is just a number, add context
    if text.strip().isdigit():
        return f"📊 Result: {text.strip()}"
    
    # Add educational context for short responses
    if len(text) < 50:
        if any(word in text.lower() for word in ['student', 'school', 'activity']):
            return f"📊 {text}\n\nThis data reflects the current state of your educational system."
    
    return text

def _format_educational_response(text: str) -> str:
    """Format educational responses with proper structure"""
    
    # Ensure proper paragraph breaks
    text = re.sub(r'([.!?])\s*([A-Z])', r'\1\n\n\2', text)
    
    # Clean up any remaining formatting issues
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    return text

def extract_answer_from_output(output: str) -> str:
    """
    Extract clean answer from subprocess output
    Used by Streamlit and other UIs
    """
    if not output:
        return "No output received"
    
    lines = output.strip().split('\n')
    answer_lines = []
    answer_started = False
    
    for line in lines:
        # Look for dash separator that starts answer section
        if re.match(r'^-{40,}', line):
            answer_started = True
            continue
        
        # If we're in answer section, collect lines until equals separator
        if answer_started:
            if re.match(r'^={40,}', line):
                break
            elif line.startswith(('🔍 ', '🔄 ', '⏱️ ', '🎯 ', '📊 ')):
                break
            else:
                answer_lines.append(line)
    
    if answer_lines:
        raw_answer = '\n'.join(answer_lines).strip()
        return clean_response_text(raw_answer, "educational")
    
    return "Could not extract answer from response."

def format_cache_metrics(cache_hit: bool, response_time: float) -> str:
    """Format cache performance indicators"""
    if cache_hit:
        if response_time < 3:
            return "⚡ Lightning Performance: Redis cache delivered instant results!"
        else:
            return f"📊 Good Performance: Cached response with {response_time:.1f}s system overhead"
    else:
        if response_time < 30:
            return f"⚡ Efficient Processing: First-time query completed in {response_time:.1f}s - next time will be instant!"
        else:
            return f"⏳ Complex Processing: Advanced query took {response_time:.1f}s - cached for 10-25x faster future responses"

def format_strategy_display(strategy: str) -> str:
    """Format strategy names for display"""
    if not strategy or strategy == 'unknown':
        return 'Unknown'
    
    return strategy.replace('_', ' ').title()

def format_error_response(question: str, error_msg: str) -> dict:
    """Generate standardized error response"""
    return {
        "question": question,
        "answer": f"I apologize, but I'm currently unable to process your question due to a system issue: {error_msg}. Please try again later or contact support.",
        "success": False,
        "error": error_msg,
        "strategy_used": "error",
        "cache_hit": False,
        "fallback_used": True
    }