"""
WhatsApp Invoice Sender Service
Automatically sends invoice notifications via WhatsApp when invoices are generated.
"""

from app.models import WhatsAppConfig, WhatsAppTemplate, Invoice, Customer, Company
from app.services.whatsapp_queue_service import WhatsAppQueueService
from app.services.spintax_engine import get_default_template
from app.utils.phone_formatter import format_phone_number
from app import db
import logging
import os

logger = logging.getLogger(__name__)


class WhatsAppInvoiceSender:
    """Service for auto-sending invoice notifications via WhatsApp"""
    
    @staticmethod
    def is_auto_send_enabled(company_id: str) -> bool:
        """
        Check if auto-send invoices is enabled for company
        
        Args:
            company_id: Company UUID string
            
        Returns:
            bool: True if enabled and configured, False otherwise
        """
        try:
            config = WhatsAppConfig.query.filter_by(company_id=company_id).first()
            if not config:
                return False
            
            # Check configuration based on provider type
            if config.provider_type == 'evolution':
                # Evolution mode: check if phone is connected
                if not config.phone_connected or not config.instance_name:
                    logger.warning(f"Evolution API not connected for company {company_id}")
                    return False
            else:
                # Legacy gateway mode: check api_key and server_address
                if not config.api_key or not config.server_address:
                    logger.warning(f"WhatsApp not properly configured for company {company_id}")
                    return False
            
            return config.auto_send_invoices
        except Exception as e:
            logger.error(f"Error checking auto-send status: {str(e)}")
            return False
    
    @staticmethod
    def get_invoice_template(company_id: str) -> str:
        """
        Get invoice template for company or return default spintax template
        
        Args:
            company_id: Company UUID string
            
        Returns:
            str: Template text
        """
        try:
            template = WhatsAppTemplate.query.filter_by(
                company_id=company_id,
                category='invoice',
                is_active=True
            ).first()
            
            if template:
                return template.template_text
        except Exception as e:
            logger.warning(f"Error fetching invoice template: {str(e)}")
        
        # Use spintax professional template as default (includes {{company_name}})
        return get_default_template('invoice')
    
    @staticmethod
    def generate_invoice_url(invoice_id: str) -> str:
        """
        Generate public invoice URL
        
        Args:
            invoice_id: Invoice UUID string
            
        Returns:
            str: Complete invoice URL
        """
        # Get base URL from environment or use default
        base_url = os.getenv('FRONTEND_URL', 'http://localhost:3000')
        return f"{base_url}/public/invoice/{invoice_id}"
    
    @staticmethod
    def send_invoice_notification(invoice: Invoice, company_id: str) -> bool:
        """
        Send WhatsApp notification for an invoice
        
        Args:
            invoice: Invoice object
            company_id: Company UUID string
            
        Returns:
            bool: True if enqueued successfully, False otherwise
        """
        try:
            # Check if auto-send is enabled
            if not WhatsAppInvoiceSender.is_auto_send_enabled(company_id):
                logger.info(f"Auto-send invoices disabled for company {company_id}, skipping")
                return False
            
            # Get customer
            customer = Customer.query.get(invoice.customer_id)
            if not customer:
                logger.error(f"Customer not found for invoice {invoice.id}")
                return False
            
            # Check if customer has mobile number
            if not customer.phone_1:
                logger.warning(f"Customer {customer.id} has no phone number, skipping invoice notification")
                return False
            
            # Format phone number to international format
            try:
                formatted_mobile = format_phone_number(customer.phone_1)
            except ValueError as e:
                logger.error(f"Invalid phone number for customer {customer.id}: {str(e)}")
                return False
            
            # Get company for company_name
            company = Company.query.get(company_id)
            if not company:
                logger.error(f"Company not found for company_id {company_id}")
                return False
            
            # Get template
            template = WhatsAppInvoiceSender.get_invoice_template(company_id)
            
            # Generate invoice URL
            invoice_url = WhatsAppInvoiceSender.generate_invoice_url(str(invoice.id))
            
            # Replace placeholders
            message = template
            message = message.replace('{{company_name}}', company.name)
            message = message.replace('{{customer_name}}', f"{customer.first_name} {customer.last_name}")
            message = message.replace('{{first_name}}', customer.first_name)
            message = message.replace('{{invoice_number}}', invoice.invoice_number)
            message = message.replace('{{amount}}', str(int(invoice.total_amount)))
            message = message.replace('{{due_date}}', invoice.due_date.strftime('%d/%m/%Y'))
            message = message.replace('{{billing_start_date}}', invoice.billing_start_date.strftime('%d/%m/%Y'))
            message = message.replace('{{billing_end_date}}', invoice.billing_end_date.strftime('%d/%m/%Y'))
            message = message.replace('{{invoice_link}}', invoice_url)
            
            # Add plan name if available (from first active CustomerPackage)
            # If no plan found, use placeholder text
            plan_name = ''
            try:
                first_pkg = None
                if hasattr(customer, 'packages'):
                    first_pkg = customer.packages.filter_by(is_active=True).first()
                if first_pkg and getattr(first_pkg, 'service_plan', None):
                    plan_name = first_pkg.service_plan.name
            except Exception as e:
                logger.warning(f"Error fetching plan name for customer {customer.id}: {str(e)}")
            
            # Always replace plan_name, even if empty (to prevent {{plan_name}} showing in message)
            message = message.replace('{{plan_name}}', plan_name if plan_name else 'Internet Service')
            
            # Enqueue message using company's configured default invoice priority
            cfg = WhatsAppConfig.query.filter_by(company_id=company_id).first()
            invoice_priority = cfg.default_invoice_priority if cfg else 10

            WhatsAppQueueService.enqueue_message(
                company_id=company_id,
                customer_id=str(customer.id),
                mobile=formatted_mobile,
                message_content=message,
                message_type='invoice',
                media_type='text',
                priority=invoice_priority,
                related_invoice_id=str(invoice.id)
            )
            
            logger.info(f"Enqueued invoice notification for invoice {invoice.invoice_number} to customer {customer.id}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending invoice notification: {str(e)}")
            return False
