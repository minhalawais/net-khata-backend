"""
Spintax Engine
Processes spintax patterns in message templates to generate unique,
human-like message variations for anti-ban protection.

Spintax syntax: [option1|option2|option3]
Nested spintax is supported: [Hi [dear|] |Hello ]{{name}}

Examples:
    Input:  "[Hi|Hello|Dear] {{name}}, your [bill|invoice] is ready."
    Output: "Hello Ahmed, your invoice is ready."  (randomized)
"""

import random
import re
import logging

logger = logging.getLogger(__name__)


def process_spintax(text: str) -> str:
    """
    Resolve all spintax patterns [option1|option2|option3] in the text.
    Handles nested brackets by processing from innermost outward.
    
    Args:
        text: Message template with spintax patterns
        
    Returns:
        str: Message with all spintax resolved to random choices
    """
    if not text:
        return text
    
    # Pattern matches [option1|option2|option3] (innermost brackets first)
    pattern = r'\[([^\[\]]+)\]'
    
    # Keep resolving until no more spintax patterns remain (handles nesting)
    max_iterations = 10  # Safety limit
    iteration = 0
    
    while re.search(pattern, text) and iteration < max_iterations:
        text = re.sub(pattern, _replace_spintax, text)
        iteration += 1
    
    # Clean up any double spaces from empty spintax options
    text = re.sub(r'  +', ' ', text).strip()
    
    return text


def _replace_spintax(match: re.Match) -> str:
    """Replace a single spintax match with a random option."""
    options = match.group(1).split('|')
    return random.choice(options).strip()


def validate_spintax(text: str) -> dict:
    """
    Validate spintax syntax and count possible combinations.
    
    Args:
        text: Template text with spintax patterns
        
    Returns:
        dict with is_valid, combination_count, and any errors
    """
    if not text:
        return {'is_valid': True, 'combination_count': 1, 'errors': []}
    
    errors = []
    
    # Check for unbalanced brackets
    open_count = text.count('[')
    close_count = text.count(']')
    if open_count != close_count:
        errors.append(f"Unbalanced brackets: {open_count} opening, {close_count} closing")
    
    # Count combinations
    pattern = r'\[([^\[\]]+)\]'
    matches = re.findall(pattern, text)
    
    combination_count = 1
    for match in matches:
        options = match.split('|')
        combination_count *= len(options)
    
    return {
        'is_valid': len(errors) == 0,
        'combination_count': combination_count,
        'spintax_groups': len(matches),
        'errors': errors
    }


# ------------------------------------------------------------------
# Default Spintax Templates
# ------------------------------------------------------------------

SPINTAX_INVOICE_TEMPLATE = """[🧾|📄|💰] *[Invoice Generated|Your Bill is Ready|New Invoice]*

[Hi|Hello|Dear|Assalam o Alaikum] {{customer_name}},

Your invoice *{{invoice_number}}* [has been generated|is now ready|is available].

*Amount:* Rs. {{amount}}
*Due Date:* {{due_date}}
*Billing Period:* {{billing_start_date}} to {{billing_end_date}}

📄 [View your invoice|Check your bill|See details]: {{invoice_link}}

[Please make payment before the due date|Kindly pay before the deadline|Payment is due by {{due_date}}].

[Thank you!|Thanks for your business!|We appreciate your trust!|Regards]""".strip()


SPINTAX_REMINDER_TEMPLATE = """[⏰|🔔|⚠️] *[Payment Reminder|Friendly Reminder|Due Date Alert]*

[Hi|Hello|Dear] {{customer_name}},

[This is a reminder|Just a reminder|We'd like to remind you] that your invoice *{{invoice_number}}* [is due on|has a due date of|should be paid by] *{{due_date}}*.

*Amount Due:* Rs. {{amount}}

📄 [View invoice|Pay now|See details]: {{invoice_link}}

[Please ensure timely payment|Kindly clear your dues|We request prompt payment] to avoid [service interruption|disconnection|any disruption].

[Thank you!|Thanks!|Regards]""".strip()


SPINTAX_DEADLINE_TEMPLATE = """[🚨|❗|⚠️] *[Urgent: Payment Overdue|Final Notice|Deadline Passed]*

[Dear|Hi] {{customer_name}},

Your invoice *{{invoice_number}}* [was due on|had a deadline of] *{{due_date}}* and [remains unpaid|is still pending|has not been cleared].

*Overdue Amount:* Rs. {{amount}}

[Your service may be|Service is subject to|Internet access may face] [suspended|interrupted|disconnection] [if not paid immediately|unless payment is received|without immediate payment].

📄 [Pay now|Clear dues|View invoice]: {{invoice_link}}

[Please pay immediately|Urgent action required|Contact us if you need help].

[Regards|Thank you]""".strip()


def get_default_template(category: str) -> str:
    """
    Get the default spintax template for a category.
    
    Args:
        category: Template category ('invoice', 'reminder', 'deadline_alert')
        
    Returns:
        str: Spintax template text
    """
    templates = {
        'invoice': SPINTAX_INVOICE_TEMPLATE,
        'reminder': SPINTAX_REMINDER_TEMPLATE,
        'deadline_alert': SPINTAX_DEADLINE_TEMPLATE,
        'payment_reminder': SPINTAX_REMINDER_TEMPLATE,
    }
    
    return templates.get(category, SPINTAX_INVOICE_TEMPLATE)
