from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta, date
import pytz
import logging
import os
import atexit

from app import db
from app.models import Customer, Invoice, User, EmployeeLedger
from app.models import WhatsAppConfig, WhatsAppMessageQueue

# Auto Invoice Service (multi-package, runs on 25th)
from app.services.auto_invoice_service import generate_next_month_invoices

# WhatsApp helpers used by scheduler jobs.
# NOTE: Queue *sending* is NOT done here — that is owned entirely by
# WhatsAppDispatcher (whatsapp_dispatcher.py) which runs as a separate
# PM2 process / daemon thread with anti-ban controls, warm-up quotas,
# send-window enforcement, and Evolution API integration.
from app.services.whatsapp_queue_service import WhatsAppQueueService
from app.services.whatsapp_rate_limiter import WhatsAppRateLimiter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PAK_TZ = pytz.timezone('Asia/Karachi')

# Global scheduler instance
scheduler = None


# ─── WhatsApp Deadline Alerts ─────────────────────────────────────────────────

def check_deadline_alerts(app=None):
    """
    Check for invoices with upcoming due dates and enqueue alert messages.
    Runs daily at 9:00 AM PKT.

    Only enqueues to the DB queue — WhatsAppDispatcher handles actual sending.
    """
    logger.info(f"Running deadline alerts check: {datetime.now(PAK_TZ)}")

    if not app:
        logger.error("No Flask app provided to check_deadline_alerts")
        return

    with app.app_context():
        try:
            # Get all companies with deadline alerts enabled
            configs = WhatsAppConfig.query.filter_by(auto_send_deadline_alerts=True).all()

            for config in configs:
                company_id = str(config.company_id)
                days_before = config.deadline_alert_days_before

                # Calculate target due date (today + days_before)
                target_date = datetime.now(PAK_TZ).date() + timedelta(days=days_before)

                # Find invoices due on target date that are still pending/overdue
                invoices = Invoice.query.filter(
                    Invoice.company_id == company_id,
                    Invoice.due_date == target_date,
                    Invoice.status.in_(['pending', 'partially_paid', 'overdue']),
                    Invoice.is_active == True
                ).all()

                logger.info(
                    f"Found {len(invoices)} invoices due in {days_before} days "
                    f"for company {company_id}"
                )

                for invoice in invoices:
                    try:
                        # Skip if alert already queued for this invoice
                        existing_alert = db.session.query(WhatsAppMessageQueue).filter(
                            WhatsAppMessageQueue.related_invoice_id == invoice.id,
                            WhatsAppMessageQueue.message_type == 'deadline_alert'
                        ).first()

                        if existing_alert:
                            logger.info(
                                f"Alert already queued for invoice {invoice.invoice_number}"
                            )
                            continue

                        customer = invoice.customer
                        if not customer:
                            logger.warning(
                                f"No customer for invoice {invoice.invoice_number}, skipping"
                            )
                            continue

                        if not customer.phone_1:
                            logger.warning(
                                f"Customer {customer.id} has no phone number, skipping alert"
                            )
                            continue

                        # Format phone number before enqueuing
                        try:
                            from app.utils.phone_formatter import format_phone_number
                            formatted_mobile = format_phone_number(customer.phone_1)
                        except (ValueError, Exception) as fmt_err:
                            logger.warning(
                                f"Invalid phone for customer {customer.id}: {fmt_err}, skipping alert"
                            )
                            continue

                        message = (
                            f"Dear {customer.first_name}, your invoice "
                            f"#{invoice.invoice_number} for Rs.{int(invoice.total_amount)} "
                            f"is due on {invoice.due_date.strftime('%d/%m/%Y')}. "
                            f"Please make payment before the due date."
                        )

                        WhatsAppQueueService.enqueue_message(
                            company_id=company_id,
                            customer_id=str(customer.id),
                            mobile=formatted_mobile,
                            message_content=message,
                            message_type='deadline_alert',
                            priority=config.default_alert_priority,
                            related_invoice_id=str(invoice.id)
                        )

                        logger.info(
                            f"Enqueued deadline alert for invoice {invoice.invoice_number}"
                        )

                    except Exception as e:
                        logger.error(
                            f"Error creating deadline alert for invoice {invoice.id}: {str(e)}"
                        )

        except Exception as e:
            logger.error(f"Error in deadline alerts check: {str(e)}")


# ─── WhatsApp Quota Reset ─────────────────────────────────────────────────────

def reset_whatsapp_quota(app=None):
    """
    Reset daily WhatsApp quota counters at midnight PKT.
    WhatsAppRateLimiter.get_or_create_today_quota() auto-creates a fresh record
    each day, but this explicit reset clears any same-day accumulation edge cases.
    """
    logger.info(f"Resetting WhatsApp daily quota: {datetime.now(PAK_TZ)}")

    if not app:
        logger.error("No Flask app provided to reset_whatsapp_quota")
        return

    with app.app_context():
        try:
            WhatsAppRateLimiter.reset_daily_quota()
            logger.info("WhatsApp quota reset completed")
        except Exception as e:
            logger.error(f"Error resetting WhatsApp quota: {str(e)}")


# ─── Monthly Salary Accrual ───────────────────────────────────────────────────

def accrue_monthly_salaries(app=None):
    """
    Accrue monthly salaries for all active employees.
    Adds the employee's base salary to their current_balance (pending amount).
    Runs on the 1st of every month at 12:01 AM PKT.

    Creates a positive ledger entry — the user then creates an Expense payment
    to pay off the balance.
    """
    logger.info(f"Running monthly salary accrual: {datetime.now(PAK_TZ)}")

    if not app:
        logger.error("No Flask app provided to accrue_monthly_salaries")
        return

    with app.app_context():
        try:
            from app.crud import employee_ledger_crud

            current_month = datetime.now(PAK_TZ).strftime('%B %Y')  # e.g. "April 2026"

            employees = User.query.filter(
                User.is_active == True,
                User.role.in_(['employee', 'manager', 'technician', 'recovery_agent']),
                User.salary != None,
                User.salary > 0
            ).all()

            logger.info(f"Found {len(employees)} employees eligible for salary accrual")

            accrued_count = 0
            skipped_count = 0

            for employee in employees:
                try:
                    salary_amount = float(employee.salary)

                    # Guard against duplicate accrual within the same month
                    month_start = datetime.now(PAK_TZ).replace(
                        day=1, hour=0, minute=0, second=0, microsecond=0
                    )
                    existing_accrual = EmployeeLedger.query.filter(
                        EmployeeLedger.employee_id == employee.id,
                        EmployeeLedger.transaction_type == 'salary_accrual',
                        EmployeeLedger.created_at >= month_start
                    ).first()

                    if existing_accrual:
                        logger.info(
                            f"Salary already accrued for "
                            f"{employee.first_name} {employee.last_name} this month"
                        )
                        skipped_count += 1
                        continue

                    employee_ledger_crud.add_ledger_entry(
                        employee_id=str(employee.id),
                        transaction_type='salary_accrual',
                        amount=salary_amount,
                        description=f"Monthly Salary for {current_month}",
                        company_id=str(employee.company_id),
                        reference_id=None
                    )

                    logger.info(
                        f"Accrued salary of PKR {salary_amount:,.2f} "
                        f"for {employee.first_name} {employee.last_name}"
                    )
                    accrued_count += 1

                except Exception as e:
                    logger.error(f"Error accruing salary for employee {employee.id}: {str(e)}")

            logger.info(
                f"Monthly salary accrual completed: "
                f"{accrued_count} accrued, {skipped_count} skipped"
            )

        except Exception as e:
            logger.error(f"Error in monthly salary accrual: {str(e)}")


# ─── Scheduler Init ───────────────────────────────────────────────────────────

def init_scheduler(app):
    """
    Initialize the APScheduler background scheduler and register all cron jobs.

    Called once from run.py at startup alongside create_app().

    Jobs registered:
      whatsapp_deadline_alerts_job   — daily 09:00 PKT  — enqueue deadline alerts
      whatsapp_quota_reset_job       — daily 00:00 PKT  — reset daily send counters
      monthly_invoice_generation_job — 25th  01:00 PKT  — generate next-month invoices
      monthly_salary_accrual_job     — 1st   00:01 PKT  — accrue employee salaries

    NOT registered here (owned by WhatsAppDispatcher):
      Queue processing — WhatsAppDispatcher runs as a continuous daemon thread
      started via init_dispatcher(app) in run.py or as a separate PM2 process.
      It uses evolution_client directly with anti-ban delays and warm-up quotas.
    """
    if not app:
        logger.error("No Flask app provided to init_scheduler")
        return

    global scheduler
    scheduler = BackgroundScheduler(timezone=PAK_TZ)

    # ── WhatsApp: Deadline Alerts (09:00 PKT daily) ───────────────────────
    # Enqueues DB records only — WhatsAppDispatcher handles actual sending
    scheduler.add_job(
        func=check_deadline_alerts,
        args=[app],
        trigger=CronTrigger(hour=9, minute=0, timezone=PAK_TZ),
        id='whatsapp_deadline_alerts_job',
        name='Enqueue WhatsApp deadline alerts (09:00 PKT)',
        replace_existing=True
    )

    # ── WhatsApp: Daily Quota Reset (00:00 PKT daily) ─────────────────────
    scheduler.add_job(
        func=reset_whatsapp_quota,
        args=[app],
        trigger=CronTrigger(hour=0, minute=0, timezone=PAK_TZ),
        id='whatsapp_quota_reset_job',
        name='Reset WhatsApp daily quota (00:00 PKT)',
        replace_existing=True
    )

    # ── Monthly Invoice Generation (25th at 01:00 PKT) ────────────────────
    # generate_next_month_invoices handles multi-package customers, duplicate
    # prevention, and auto-enqueues WhatsApp notifications via
    # WhatsAppInvoiceSender — which enqueues to DB for the dispatcher to send.
    scheduler.add_job(
        func=generate_next_month_invoices,
        args=[app],
        trigger=CronTrigger(day=25, hour=1, minute=0, timezone=PAK_TZ),
        id='monthly_invoice_generation_job',
        name='Generate next-month invoices (25th 01:00 PKT)',
        replace_existing=True
    )

    # ── Monthly Salary Accrual (1st at 00:01 PKT) ─────────────────────────
    scheduler.add_job(
        func=accrue_monthly_salaries,
        args=[app],
        trigger=CronTrigger(day=1, hour=0, minute=1, timezone=PAK_TZ),
        id='monthly_salary_accrual_job',
        name='Accrue monthly salaries (1st 00:01 PKT)',
        replace_existing=True
    )

    scheduler.start()
    logger.info("Background scheduler started. Registered jobs:")
    for job in scheduler.get_jobs():
        logger.info(f"  + {job.name}  |  next run: {job.next_run_time}")

    def shutdown_scheduler():
        if scheduler and scheduler.running:
            logger.info("Shutting down background scheduler...")
            scheduler.shutdown(wait=False)

    atexit.register(shutdown_scheduler)