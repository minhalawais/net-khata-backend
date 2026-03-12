"""
Backfill Invoice Line Items Script
===================================
This script creates or updates InvoiceLineItem records for legacy invoices
that may have missing or empty descriptions.

Run this script once to fix historical invoice data.

Usage:
    cd api
    python -c "from scripts.backfill_invoice_line_items import backfill_all; backfill_all()"
"""
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def backfill_all():
    """Main entry point - backfill all invoice line items."""
    from app import create_app, db
    from app.models import Invoice, InvoiceLineItem, CustomerPackage, ServicePlan
    from sqlalchemy import func, or_
    
    app = create_app()
    
    with app.app_context():
        logger.info("Starting invoice line items backfill...")
        
        # Find subscription invoices with missing or incomplete line items
        invoices_to_fix = db.session.query(Invoice).outerjoin(
            InvoiceLineItem
        ).filter(
            Invoice.invoice_type == 'subscription',
            Invoice.is_active == True
        ).group_by(Invoice.id).having(
            # Either no line items at all, or line items with empty descriptions
            or_(
                func.count(InvoiceLineItem.id) == 0,
                func.count(InvoiceLineItem.id).filter(
                    InvoiceLineItem.description.isnot(None),
                    InvoiceLineItem.description != ''
                ) == 0
            )
        ).all()
        
        logger.info(f"Found {len(invoices_to_fix)} invoices to check")
        
        fixed_count = 0
        skipped_count = 0
        
        for invoice in invoices_to_fix:
            try:
                result = backfill_invoice_line_items(invoice, db, CustomerPackage, ServicePlan, InvoiceLineItem)
                if result:
                    fixed_count += 1
                else:
                    skipped_count += 1
            except Exception as e:
                logger.error(f"Error fixing invoice {invoice.invoice_number}: {e}")
        
        db.session.commit()
        logger.info(f"Backfill complete: Fixed={fixed_count}, Skipped={skipped_count}")


def backfill_invoice_line_items(invoice, db, CustomerPackage, ServicePlan, InvoiceLineItem):
    """
    Backfill line items for a single invoice.
    
    Strategy:
    1. Check if line items exist but have empty descriptions -> Update them
    2. If no line items exist -> Create them based on CustomerPackage history
    """
    from sqlalchemy import or_
    
    # Get existing line items
    existing_items = InvoiceLineItem.query.filter_by(invoice_id=invoice.id).all()
    
    if existing_items:
        # Line items exist - check if descriptions are empty
        items_fixed = 0
        for item in existing_items:
            if not item.description or item.description.strip() == '':
                # Try to get description from customer_package_id
                if item.customer_package_id:
                    cp = CustomerPackage.query.get(item.customer_package_id)
                    if cp and cp.service_plan:
                        plan = cp.service_plan
                        desc = f"{plan.name} - {plan.speed_mbps}Mbps" if plan.speed_mbps else plan.name
                        item.description = desc
                        items_fixed += 1
                        logger.info(f"  Updated line item description for invoice {invoice.invoice_number}: {desc}")
        return items_fixed > 0
    
    else:
        # No line items - create them from CustomerPackage history
        # Find packages that were likely active during the invoice period
        packages = CustomerPackage.query.filter(
            CustomerPackage.customer_id == invoice.customer_id,
            CustomerPackage.start_date <= invoice.billing_end_date,
            or_(
                CustomerPackage.end_date.is_(None),
                CustomerPackage.end_date >= invoice.billing_start_date
            )
        ).all()
        
        if not packages:
            logger.warning(f"  No packages found for invoice {invoice.invoice_number}")
            return False
        
        created_count = 0
        for cp in packages:
            if cp.service_plan:
                plan = cp.service_plan
                desc = f"{plan.name} - {plan.speed_mbps}Mbps" if plan.speed_mbps else plan.name
                
                line_item = InvoiceLineItem(
                    invoice_id=invoice.id,
                    customer_package_id=cp.id,
                    item_type='package',
                    description=desc,
                    quantity=1,
                    unit_price=float(plan.price) if plan.price else 0,
                    discount_amount=0,
                    line_total=float(plan.price) if plan.price else 0
                )
                db.session.add(line_item)
                created_count += 1
                logger.info(f"  Created line item for invoice {invoice.invoice_number}: {desc}")
        
        return created_count > 0


if __name__ == "__main__":
    backfill_all()
