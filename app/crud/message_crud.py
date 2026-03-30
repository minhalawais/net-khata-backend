from app import db
from app.models import Message, User, Customer, Company
from app.services.whatsapp_queue_service import WhatsAppQueueService
from app.utils.logging_utils import log_action
import uuid
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)

def get_all_messages(company_id, user_role):
    try:
        if user_role == 'super_admin':
            messages = Message.query.order_by(Message.created_at.desc()).all()
        elif user_role == 'auditor':
            messages = Message.query.filter_by(is_active=True, company_id=company_id).order_by(Message.created_at.desc()).all()
        else:  # company_owner
            messages = Message.query.filter_by(company_id=company_id).order_by(Message.created_at.desc()).all()

        return [
            {
                'id': str(message.id),
                'sender': f"{message.sender.first_name} {message.sender.last_name}",
                'recipient': get_recipient_name(message.recipient_id),
                'subject': message.subject,
                'content': message.content,
                'is_read': message.is_read,
                'created_at': message.created_at.isoformat()
            } for message in messages
        ]
    except Exception as e:
        logger.error(f"Error retrieving messages: {str(e)}")
        raise

def get_recipient_name(recipient_id):
    user = User.query.get(recipient_id)
    if user:
        return f"{user.first_name} {user.last_name}"
    customer = Customer.query.get(recipient_id)
    if customer:
        return f"{customer.first_name} {customer.last_name}"
    return "Unknown Recipient"

def add_message(data, current_user_id, ip_address, user_agent):
    try:
        # Support single recipient (recipient_id) or multiple recipients (recipient_ids)
        company_uuid = uuid.UUID(data['company_id'])
        sender_uuid = uuid.UUID(data['sender_id'])

        recipient_field = data.get('recipient_ids') or data.get('recipient_id')
        if not recipient_field:
            raise KeyError('recipient_id')

        # Normalize to list of uuid strings
        if isinstance(recipient_field, str):
            # allow comma-separated strings from the frontend
            recipients = [r.strip() for r in recipient_field.split(',') if r.strip()]
        elif isinstance(recipient_field, list):
            recipients = recipient_field
        else:
            recipients = [str(recipient_field)]

        created_messages = []
        for rid in recipients:
            r_uuid = uuid.UUID(rid)
            msg = Message(
                company_id=company_uuid,
                sender_id=sender_uuid,
                recipient_id=r_uuid,
                subject=data.get('subject'),
                content=data.get('content'),
                is_active=True
            )
            db.session.add(msg)
            created_messages.append(msg)

        db.session.commit()

        # Log creation for each message
        for msg in created_messages:
            log_action(
                current_user_id,
                'CREATE',
                'messages',
                msg.id,
                None,
                {
                    'company_id': str(company_uuid),
                    'sender_id': str(sender_uuid),
                    'recipient_id': str(msg.recipient_id),
                    'subject': msg.subject,
                    'content': msg.content,
                },
                ip_address,
                user_agent,
                str(company_uuid)
            )

        # Enqueue messages to WhatsApp queue if recipient is a customer
        for msg in created_messages:
            try:
                customer = Customer.query.get(msg.recipient_id)
                if customer and customer.phone_1:
                    try:
                        # Fetch company and replace {{company_name}} if present
                        company = Company.query.get(company_uuid)
                        message_content = msg.content
                        
                        if company and '{{company_name}}' in message_content:
                            message_content = message_content.replace('{{company_name}}', company.name)
                        
                        WhatsAppQueueService.enqueue_message(
                            company_id=str(company_uuid),
                            customer_id=str(customer.id),
                            mobile=customer.phone_1,
                            message_content=message_content,
                            message_type='custom',
                            priority=60
                        )
                        logger.info(f"Enqueued WhatsApp message for customer {customer.id} (message {msg.id})")
                    except Exception as e:
                        logger.warning(f"Failed to enqueue WhatsApp message for customer {customer.id}: {e}")
                else:
                    logger.info(f"Recipient {msg.recipient_id} is not a customer or has no phone number; skipping enqueue")
            except Exception as e:
                logger.error(f"Error while attempting to enqueue message {msg.id}: {e}")

        # Return single Message object if only one created, else list
        if len(created_messages) == 1:
            return created_messages[0]
        return created_messages
    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}")
        db.session.rollback()
        raise
    except Exception as e:
        logger.error(f"Error adding message: {str(e)}")
        raise

def update_message(id, data, company_id, user_role, current_user_id, ip_address, user_agent):
    try:
        if user_role == 'super_admin':
            message = db.session.get(Message, id)
        elif user_role == 'auditor':
            message = Message.query.filter_by(id=id, is_active=True, company_id=company_id).first()
        else:  # company_owner
            message = Message.query.filter_by(id=id, company_id=company_id).first()

        if not message:
            return None

        old_values = {
            'subject': message.subject,
            'content': message.content,
            'is_read': message.is_read,
            'is_active': message.is_active
        }

        message.subject = data.get('subject', message.subject)
        message.content = data.get('content', message.content)
        message.is_read = data.get('is_read', message.is_read)
        message.is_active = data.get('is_active', message.is_active)
        db.session.commit()

        log_action(
            current_user_id,
            'UPDATE',
            'messages',
            message.id,
            old_values,
            data,
                        ip_address,
            user_agent,
            company_id
)

        return message
    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}")
        db.session.rollback()
        raise
    except Exception as e:
        logger.error(f"Error updating message: {str(e)}")
        raise

def delete_message(id, company_id, user_role, current_user_id, ip_address, user_agent):
    try:
        if user_role == 'super_admin':
            message = Message.query.get(id)
        elif user_role == 'auditor':
            message = Message.query.filter_by(id=id, is_active=True, company_id=company_id).first()
        else:  # company_owner
            message = Message.query.filter_by(id=id, company_id=company_id).first()

        if not message:
            return False

        old_values = {
            'subject': message.subject,
            'content': message.content,
            'is_read': message.is_read,
            'is_active': message.is_active
        }

        db.session.delete(message)
        db.session.commit()

        log_action(
            current_user_id,
            'DELETE',
            'messages',
            message.id,
            old_values,
            None,
                        ip_address,
            user_agent,
            company_id
)

        return True
    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}")
        db.session.rollback()
        raise
    except Exception as e:
        logger.error(f"Error deleting message: {str(e)}")
        raise

