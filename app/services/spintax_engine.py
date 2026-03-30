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

# ------------------------------------------------------------------
# Professional Spintax Templates (WhatsApp Markdown Optimized)
# ------------------------------------------------------------------

SPINTAX_INVOICE_TEMPLATE = """*[🧾 Monthly Invoice | 📄 New Invoice Generated | 🧾 Billing Update]*

[Dear | Respected | Assalam-o-Alaikum] *{{customer_name}}*,

[We hope you are enjoying our internet service. | Thank you for choosing our network. | We appreciate your continued trust in our services.] 

[Your invoice for the current billing cycle has been generated. | This is to inform you that your monthly internet bill is ready. | Your latest subscription invoice has been successfully generated.]

*Billing Details:*
▪ *Invoice No:* {{invoice_number}}
▪ *Plan:* {{plan_name}}
▪ *Period:* {{billing_start_date}} to {{billing_end_date}}
▪ *Total Amount:* Rs. {{amount}}
▪ *Due Date:* {{due_date}}

[Kindly arrange the payment before the due date to avoid any service interruption. | Please clear your dues before the deadline for uninterrupted services. | We request you to make the payment before the due date.]

🔗 *View / Download Invoice:* {{invoice_link}}

[Thank you for your business! | Regards, | Best Regards,]
*{{company_name}}*
*Support Team*""".strip()


SPINTAX_REMINDER_TEMPLATE = """*[⏳ Payment Reminder | 🔔 Friendly Reminder | 📅 Invoice Due Soon]*

[Dear | Respected | Assalam-o-Alaikum] *{{customer_name}}*,

[This is a gentle reminder that your invoice | Just a quick reminder regarding your internet bill | Please note that your monthly invoice] *{{invoice_number}}* [is approaching its due date | is due soon | requires your attention].

*Outstanding Details:*
▪ *Amount Payable:* Rs. {{amount}}
▪ *Due Date:* {{due_date}}

[Please process your payment at your earliest convenience. | Kindly clear your dues soon to enjoy uninterrupted internet. | We request you to pay the pending amount before the deadline.]

🔗 *View Invoice & Pay here:* {{invoice_link}}

_[If you have already paid, please ignore this message. | In case payment has been made, kindly disregard this alert. | Ignore this message if dues are already cleared.]_

[Regards, | Best Regards, | Thank you,]
*{{company_name}}*
*Billing Department*""".strip()


SPINTAX_DEADLINE_TEMPLATE = """*[⚠️ Action Required: Overdue Invoice | 🚨 Service Suspension Notice | ⚠️ Payment Overdue]*

[Dear | Respected | Assalam-o-Alaikum] *{{customer_name}}*,

[We noticed that your payment for invoice | Our records indicate that your invoice | This is an urgent reminder that your bill] *{{invoice_number}}* [is now overdue | has crossed the due date of {{due_date}} | is currently pending].

*Pending Amount:* Rs. {{amount}}

[To prevent automatic suspension of your internet service, please pay immediately. | Your service may be temporarily restricted if payment is not received promptly. | Please clear the dues immediately to avoid automated disconnection.]

🔗 *View Invoice & Pay Now:* {{invoice_link}}

[For any queries, please contact our support team. | If you face any billing issues, reply to this message. | Please reach out if you need assistance.]

[Regards, | Thank you,]
*{{company_name}}*
*Billing Department*""".strip()


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
