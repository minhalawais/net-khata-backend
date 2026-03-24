"""
Executive Dashboard CRUD - Optimized for Owner's Morning View

8 KPIs, 6 Charts, 2 Action Tables.
All monthly aggregations use GROUP BY — no N+1 loops.
Filters: date range (start_date, end_date), area_id, isp_id.
"""

from app import db
from app.models import (
    Customer, Invoice, Payment, Complaint, ServicePlan, CustomerPackage,
    Area, ISP, ISPPayment, BankAccount, User, Expense, ExpenseType,
    ExtraIncome, EmployeeLedger
)
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from sqlalchemy import func, case, and_, or_, desc, extract, cast, Date
from sqlalchemy.exc import SQLAlchemyError
from pytz import timezone
import logging

logger = logging.getLogger(__name__)

# Pakistan timezone
PKT = timezone('Asia/Karachi')


# ─── Helpers ────────────────────────────────────────────────────────────────

def get_date_range(start_date_str, end_date_str):
    """
    Parse date strings to PKT-aware datetime objects.

    Fix: was datetime.strptime(...).replace(tzinfo=PKT).
    pytz timezone objects MUST be applied via PKT.localize(dt), not .replace(tzinfo=PKT).
    .replace() attaches the timezone in pytz's internal 'LMT initial state' where
    utcoffset() can return None — Python's datetime.__sub__ then raises
    'can't subtract offset-naive and offset-aware datetimes' even though tzinfo is set.
    PKT.localize() looks up the DST transition table and sets the correct +05:00 offset.
    """
    try:
        today = datetime.now(PKT)
        if start_date_str:
            start_date = PKT.localize(
                datetime.strptime(start_date_str, '%Y-%m-%d')
                .replace(hour=0, minute=0, second=0, microsecond=0)
            )
        else:
            start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        if end_date_str:
            end_date = PKT.localize(
                datetime.strptime(end_date_str, '%Y-%m-%d')
                .replace(hour=23, minute=59, second=59, microsecond=0)
            )
        else:
            end_date = today

        return start_date, end_date
    except Exception as e:
        logger.error(f"Date parsing error: {e}")
        today = datetime.now(PKT)
        return today.replace(day=1, hour=0, minute=0, second=0, microsecond=0), today


def get_previous_period(start_date, end_date):
    """Previous period of the same length, ending the day before start_date."""
    period_days = (end_date - start_date).days + 1
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=period_days - 1)
    return prev_start.replace(hour=0, minute=0, second=0, microsecond=0), \
           prev_end.replace(hour=23, minute=59, second=59, microsecond=0)


def calculate_trend(current, previous):
    """Percentage change between two values."""
    if previous == 0:
        return 100.0 if current > 0 else 0.0
    return round(((current - previous) / abs(previous)) * 100, 1)


def _apply_customer_filters(query, area_id, isp_id, customer_alias=Customer):
    """Apply area_id / isp_id filters to a query that already has a Customer join."""
    if area_id:
        query = query.filter(customer_alias.area_id == area_id)
    if isp_id:
        query = query.filter(customer_alias.isp_id == isp_id)
    return query


def _twelve_months_ago():
    """Start of month, 11 months back from current month (gives 12 data points)."""
    today = datetime.now(PKT)
    return today.replace(day=1, hour=0, minute=0, second=0, microsecond=0) - relativedelta(months=11)


def _build_month_series():
    """Generate list of 12 month keys ['2025-04', '2025-05', ... , '2026-03']."""
    today = datetime.now(PKT)
    months = []
    for i in range(11, -1, -1):
        d = today - relativedelta(months=i)
        months.append(d.strftime('%Y-%m'))
    return months


def _resolve_chart_window(start_date=None, end_date=None):
    """
    Resolve chart window and bucket granularity.

    - If start/end provided: use selected range.
    - If not provided: fallback to rolling 12 months.
    - For very short windows (<= 45 days): daily buckets.
    - Otherwise: monthly buckets.
    """
    if start_date and end_date:
        window_start = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        window_end = end_date.replace(hour=23, minute=59, second=59, microsecond=0)
        total_days = max((window_end.date() - window_start.date()).days + 1, 1)
        granularity = 'day' if total_days <= 45 else 'month'
        return window_start, window_end, granularity

    now = datetime.now(PKT)
    window_start = _twelve_months_ago()
    window_end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return window_start, window_end, 'month'


def _build_series_keys(window_start, window_end, granularity):
    """Build ordered bucket keys for day or month granularity."""
    keys = []

    if granularity == 'day':
        cursor = window_start.date()
        end_day = window_end.date()
        while cursor <= end_day:
            keys.append(cursor.strftime('%Y-%m-%d'))
            cursor += timedelta(days=1)
        return keys

    cursor = date(window_start.year, window_start.month, 1)
    end_month = date(window_end.year, window_end.month, 1)
    while cursor <= end_month:
        keys.append(cursor.strftime('%Y-%m'))
        cursor = cursor + relativedelta(months=1)

    return keys


def _to_pkt(dt):
    """
    Normalize datetime to PKT-aware datetime.
    Uses PKT.localize() for naive datetimes — not .replace(tzinfo=PKT).
    See get_date_range docstring for explanation of the pytz LMT state bug.
    """
    if not dt:
        return None
    if dt.tzinfo is None:
        return PKT.localize(dt)
    return dt.astimezone(PKT)


def _pkt_day_bounds():
    now = datetime.now(PKT)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return start, end


# ─── Main Orchestrator ──────────────────────────────────────────────────────

def get_executive_dashboard_advanced(company_id, filters=None):
    """
    Build the full executive dashboard payload.

    Args:
        company_id: UUID of the company
        filters: Dict with start_date, end_date, area_id, isp_id

    Returns:
        Dict with kpis, charts, tables, filters, period
    """
    if not company_id:
        return {'error': 'Company ID is required'}

    filters = filters or {}

    try:
        start_date, end_date = get_date_range(
            filters.get('start_date'),
            filters.get('end_date')
        )
        prev_start, prev_end = get_previous_period(start_date, end_date)

        area_id = filters.get('area_id') if filters.get('area_id') != 'all' else None
        isp_id = filters.get('isp_id') if filters.get('isp_id') != 'all' else None
        open_complaints_data = get_open_complaints(company_id, area_id, isp_id)

        return {
            'today_pulse': get_today_pulse(company_id, area_id, isp_id),
            'kpis': get_kpis(company_id, start_date, end_date, prev_start, prev_end, area_id, isp_id),
            'charts': {
                'revenue_vs_isp_cost': get_revenue_vs_isp_cost_trend(company_id, start_date, end_date, area_id, isp_id),
                'customer_growth': get_customer_growth_chart(company_id, start_date, end_date, area_id, isp_id),
                'collection_rate': get_collection_rate_by_month(company_id, start_date, end_date, area_id, isp_id),
                'revenue_by_isp': get_revenue_by_isp(company_id, start_date, end_date, area_id),
                'isp_cost_per_subscriber': get_isp_cost_per_subscriber(company_id, start_date, end_date, area_id, isp_id),
                'top_areas': get_top_areas_by_revenue(company_id, start_date, end_date, isp_id),
                'top_plans': get_top_plans_by_mrr(company_id, start_date, end_date, area_id, isp_id),
            },
            'tables': {
                'overdue_invoices': get_critical_overdue_invoices(company_id, area_id, isp_id),
                'open_complaints': open_complaints_data['items'],
            },
            'alerts': {
                'sla': open_complaints_data['alerts'],
            },
            'filters': get_filter_options(company_id),
            'period': {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'chart_granularity': 'day' if (end_date.date() - start_date.date()).days + 1 <= 45 else 'month',
            }
        }

    except SQLAlchemyError as e:
        logger.error(f"Database error in executive dashboard: {e}")
        return {'error': 'Database error occurred'}
    except Exception as e:
        logger.error(f"Error in executive dashboard: {e}")
        return {'error': str(e)}


# ─── KPIs ───────────────────────────────────────────────────────────────────

def get_kpis(company_id, start_date, end_date, prev_start, prev_end, area_id=None, isp_id=None):
    """Calculate 8 KPIs with period-over-period comparison."""

    # ── KPI 1: Total Collections ──
    def _collections_query(sd, ed):
        q = db.session.query(
            func.coalesce(func.sum(Payment.amount), 0)
        ).join(
            Invoice, Payment.invoice_id == Invoice.id
        ).join(
            Customer, Invoice.customer_id == Customer.id
        ).filter(
            Invoice.company_id == company_id,
            Payment.status == 'paid',
            Payment.is_active == True,
            Payment.payment_date >= sd,
            Payment.payment_date <= ed,
        )
        return _apply_customer_filters(q, area_id, isp_id)

    total_collections = float(_collections_query(start_date, end_date).scalar() or 0)
    prev_collections = float(_collections_query(prev_start, prev_end).scalar() or 0)

    # ── KPI 2: Outstanding Receivables (snapshot vs previous snapshot) ──
    def _outstanding_snapshot(as_of_dt):
        invoiced_q = db.session.query(
            func.coalesce(func.sum(Invoice.total_amount), 0)
        ).join(
            Customer, Invoice.customer_id == Customer.id
        ).filter(
            Invoice.company_id == company_id,
            Invoice.is_active == True,
            Invoice.due_date <= as_of_dt.date(),
        )
        invoiced_q = _apply_customer_filters(invoiced_q, area_id, isp_id)
        total_invoiced = float(invoiced_q.scalar() or 0)

        paid_q = db.session.query(
            func.coalesce(func.sum(Payment.amount), 0)
        ).join(
            Invoice, Payment.invoice_id == Invoice.id
        ).join(
            Customer, Invoice.customer_id == Customer.id
        ).filter(
            Invoice.company_id == company_id,
            Payment.status == 'paid',
            Payment.is_active == True,
            Payment.payment_date <= as_of_dt,
        )
        paid_q = _apply_customer_filters(paid_q, area_id, isp_id)
        total_paid = float(paid_q.scalar() or 0)

        return max(total_invoiced - total_paid, 0.0)

    outstanding_amount = _outstanding_snapshot(end_date)
    prev_outstanding_amount = _outstanding_snapshot(prev_end)

    # ── KPI 3: Net Cash Position (historical snapshot) ──
    def _net_cash_snapshot(as_of_dt):
        base_balance = float(db.session.query(
            func.coalesce(func.sum(BankAccount.initial_balance), 0)
        ).filter(
            BankAccount.company_id == company_id,
            BankAccount.is_active == True,
            BankAccount.created_at <= as_of_dt,
        ).scalar() or 0)

        collections_in = float(db.session.query(
            func.coalesce(func.sum(Payment.amount), 0)
        ).filter(
            Payment.company_id == company_id,
            Payment.status == 'paid',
            Payment.is_active == True,
            Payment.payment_date <= as_of_dt,
        ).scalar() or 0)

        extra_income_in = float(db.session.query(
            func.coalesce(func.sum(ExtraIncome.amount), 0)
        ).filter(
            ExtraIncome.company_id == company_id,
            ExtraIncome.is_active == True,
            ExtraIncome.income_date <= as_of_dt,
        ).scalar() or 0)

        expense_out = float(db.session.query(
            func.coalesce(func.sum(Expense.amount), 0)
        ).filter(
            Expense.company_id == company_id,
            Expense.is_active == True,
            Expense.expense_date <= as_of_dt,
        ).scalar() or 0)

        isp_out = float(db.session.query(
            func.coalesce(func.sum(ISPPayment.amount), 0)
        ).filter(
            ISPPayment.company_id == company_id,
            ISPPayment.is_active == True,
            ISPPayment.payment_date <= as_of_dt,
        ).scalar() or 0)

        return base_balance + collections_in + extra_income_in - expense_out - isp_out

    net_cash_position = _net_cash_snapshot(end_date)
    prev_net_cash_position = _net_cash_snapshot(prev_end)

    # ── KPI 4: Operating Margin % (true operating costs) ──
    def _isp_payments_query(sd, ed):
        q = db.session.query(
            func.coalesce(func.sum(ISPPayment.amount), 0)
        ).filter(
            ISPPayment.company_id == company_id,
            ISPPayment.is_active == True,
            ISPPayment.payment_date >= sd,
            ISPPayment.payment_date <= ed,
        )
        # Fix: when isp_id filter is active, the collections numerator already
        # reflects only that ISP's customers. Apply the same isp_id to the cost
        # denominator so the margin is internally consistent.
        # expenses and employee_cost remain company-wide (can't be attributed to ISP).
        if isp_id:
            q = q.filter(ISPPayment.isp_id == isp_id)
        return q

    def _non_employee_expenses_query(sd, ed):
        return db.session.query(
            func.coalesce(func.sum(Expense.amount), 0)
        ).join(
            ExpenseType, Expense.expense_type_id == ExpenseType.id
        ).filter(
            Expense.company_id == company_id,
            Expense.is_active == True,
            Expense.expense_date >= sd,
            Expense.expense_date <= ed,
            ExpenseType.is_active == True,
            or_(ExpenseType.is_employee_payment == False, ExpenseType.is_employee_payment.is_(None)),
        )

    def _employee_cost_combined(sd, ed):
        # Source A: Expense entries explicitly marked as employee payments.
        employee_expense_rows = db.session.query(
            Expense.employee_id,
            cast(Expense.expense_date, Date).label('txn_date'),
            Expense.amount,
        ).join(
            ExpenseType, Expense.expense_type_id == ExpenseType.id
        ).filter(
            Expense.company_id == company_id,
            Expense.is_active == True,
            Expense.expense_date >= sd,
            Expense.expense_date <= ed,
            ExpenseType.is_active == True,
            ExpenseType.is_employee_payment == True,
        ).all()

        expense_sum = 0.0
        expense_keys = set()
        for row in employee_expense_rows:
            amt = abs(float(row.amount or 0))
            expense_sum += amt
            expense_keys.add((str(row.employee_id or ''), str(row.txn_date), round(amt, 2)))

        # Source B: Salary accrual entries in employee ledger.
        ledger_rows = db.session.query(
            EmployeeLedger.employee_id,
            cast(EmployeeLedger.created_at, Date).label('txn_date'),
            EmployeeLedger.amount,
        ).filter(
            EmployeeLedger.company_id == company_id,
            EmployeeLedger.transaction_type == 'salary_accrual',
            EmployeeLedger.created_at >= sd,
            EmployeeLedger.created_at <= ed,
        ).all()

        ledger_unique_sum = 0.0
        for row in ledger_rows:
            amt = abs(float(row.amount or 0))
            dedupe_key = (str(row.employee_id or ''), str(row.txn_date), round(amt, 2))
            if dedupe_key not in expense_keys:
                ledger_unique_sum += amt

        return expense_sum + ledger_unique_sum

    isp_payments = float(_isp_payments_query(start_date, end_date).scalar() or 0)
    prev_isp_payments = float(_isp_payments_query(prev_start, prev_end).scalar() or 0)
    non_employee_expenses = float(_non_employee_expenses_query(start_date, end_date).scalar() or 0)
    prev_non_employee_expenses = float(_non_employee_expenses_query(prev_start, prev_end).scalar() or 0)
    employee_cost = _employee_cost_combined(start_date, end_date)
    prev_employee_cost = _employee_cost_combined(prev_start, prev_end)

    total_operating_cost = isp_payments + non_employee_expenses + employee_cost
    prev_total_operating_cost = prev_isp_payments + prev_non_employee_expenses + prev_employee_cost

    gross_margin = ((total_collections - total_operating_cost) / total_collections * 100) if total_collections > 0 else 0
    prev_gross_margin = ((prev_collections - prev_total_operating_cost) / prev_collections * 100) if prev_collections > 0 else 0

    # ── KPI 5: Active Customers ──
    active_q = Customer.query.filter(
        Customer.company_id == company_id,
        Customer.is_active == True,
    )
    active_q = _apply_customer_filters(active_q, area_id, isp_id)
    active_customers = active_q.count()

    prev_active_q = Customer.query.filter(
        Customer.company_id == company_id,
        Customer.is_active == True,
        Customer.created_at <= prev_end,
    )
    prev_active_q = _apply_customer_filters(prev_active_q, area_id, isp_id)
    prev_active = prev_active_q.count()

    # ── KPI 6: New Connections MTD (uses installation_date) ──
    new_q = Customer.query.filter(
        Customer.company_id == company_id,
        Customer.installation_date >= start_date.date() if isinstance(start_date, datetime) else start_date,
        Customer.installation_date <= end_date.date() if isinstance(end_date, datetime) else end_date,
    )
    new_q = _apply_customer_filters(new_q, area_id, isp_id)
    new_connections = new_q.count()

    prev_new_q = Customer.query.filter(
        Customer.company_id == company_id,
        Customer.installation_date >= prev_start.date() if isinstance(prev_start, datetime) else prev_start,
        Customer.installation_date <= prev_end.date() if isinstance(prev_end, datetime) else prev_end,
    )
    prev_new_q = _apply_customer_filters(prev_new_q, area_id, isp_id)
    prev_new = prev_new_q.count()

    # ── KPI 7: Churned MTD ──
    # Uses deactivated_at (fallback via migrated historical backfill) instead of
    # generic updated_at to avoid false positives from profile edits.
    churned_q = Customer.query.filter(
        Customer.company_id == company_id,
        Customer.is_active == False,
        Customer.deactivated_at.isnot(None),
        Customer.deactivated_at >= start_date,
        Customer.deactivated_at <= end_date,
    )
    churned_q = _apply_customer_filters(churned_q, area_id, isp_id)
    churned = churned_q.count()

    prev_churned_q = Customer.query.filter(
        Customer.company_id == company_id,
        Customer.is_active == False,
        Customer.deactivated_at.isnot(None),
        Customer.deactivated_at >= prev_start,
        Customer.deactivated_at <= prev_end,
    )
    prev_churned_q = _apply_customer_filters(prev_churned_q, area_id, isp_id)
    prev_churned = prev_churned_q.count()

    # ── KPI 9: ARPU ──
    arpu = (total_collections / active_customers) if active_customers > 0 else 0
    prev_arpu = (prev_collections / prev_active) if prev_active > 0 else 0

    # ── KPI 8: Open Complaints ──
    complaints_q = Complaint.query.join(
        Customer, Complaint.customer_id == Customer.id
    ).filter(
        Customer.company_id == company_id,
        Complaint.status.in_(['open', 'in_progress']),
        Complaint.is_active == True,
    )
    complaints_q = _apply_customer_filters(complaints_q, area_id, isp_id)
    open_complaints = complaints_q.count()

    # Previous period open complaints (snapshot a week ago as proxy)
    week_ago = datetime.now(PKT) - timedelta(days=7)
    prev_complaints_q = Complaint.query.join(
        Customer, Complaint.customer_id == Customer.id
    ).filter(
        Customer.company_id == company_id,
        Complaint.status.in_(['open', 'in_progress']),
        Complaint.is_active == True,
        Complaint.created_at <= week_ago,
    )
    prev_complaints_q = _apply_customer_filters(prev_complaints_q, area_id, isp_id)
    prev_open_complaints = prev_complaints_q.count()

    # ── Build response ──
    return {
        'collections': {
            'value': round(total_collections, 2),
            'previous': round(prev_collections, 2),
            'trend': calculate_trend(total_collections, prev_collections),
            'is_positive': total_collections >= prev_collections,
        },
        'outstanding': {
            'value': round(outstanding_amount, 2),
            'previous': round(prev_outstanding_amount, 2),
            'trend': calculate_trend(outstanding_amount, prev_outstanding_amount),
            'is_positive': outstanding_amount <= prev_outstanding_amount,
        },
        'net_cash_position': {
            'value': round(net_cash_position, 2),
            'previous': round(prev_net_cash_position, 2),
            'trend': calculate_trend(net_cash_position, prev_net_cash_position),
            'is_positive': net_cash_position >= prev_net_cash_position,
        },
        'gross_margin': {
            'value': round(gross_margin, 1),
            'previous': round(prev_gross_margin, 1),
            'trend': round(gross_margin - prev_gross_margin, 1),
            'is_positive': gross_margin >= prev_gross_margin,
        },
        'arpu': {
            'value': round(arpu, 2),
            'previous': round(prev_arpu, 2),
            'trend': calculate_trend(arpu, prev_arpu),
            'is_positive': arpu >= prev_arpu,
        },
        'active_customers': {
            'value': active_customers,
            'previous': prev_active,
            'trend': calculate_trend(active_customers, prev_active),
            'is_positive': active_customers >= prev_active,
        },
        'new_connections': {
            'value': new_connections,
            'previous': prev_new,
            'trend': calculate_trend(new_connections, prev_new),
            'is_positive': new_connections >= prev_new,
        },
        'churned': {
            'value': churned,
            'previous': prev_churned,
            'trend': calculate_trend(churned, prev_churned),
            'is_positive': churned <= prev_churned,
        },
        'open_complaints': {
            'value': open_complaints,
            'previous': prev_open_complaints,
            'trend': calculate_trend(open_complaints, prev_open_complaints),
            'is_positive': open_complaints <= prev_open_complaints,
        },
    }


def get_today_pulse(company_id, area_id=None, isp_id=None):
    """Owner's first-glance metrics for the current PKT day."""
    day_start, day_end = _pkt_day_bounds()

    collections_q = db.session.query(
        func.coalesce(func.sum(Payment.amount), 0)
    ).join(
        Invoice, Payment.invoice_id == Invoice.id
    ).join(
        Customer, Invoice.customer_id == Customer.id
    ).filter(
        Invoice.company_id == company_id,
        Payment.status == 'paid',
        Payment.is_active == True,
        Payment.payment_date >= day_start,
        Payment.payment_date <= day_end,
    )
    collections_q = _apply_customer_filters(collections_q, area_id, isp_id)

    invoices_due_q = db.session.query(func.count(Invoice.id)).join(
        Customer, Invoice.customer_id == Customer.id
    ).filter(
        Invoice.company_id == company_id,
        Invoice.is_active == True,
        Invoice.status.in_(['pending', 'partially_paid', 'overdue']),
        Invoice.due_date == day_start.date(),
    )
    invoices_due_q = _apply_customer_filters(invoices_due_q, area_id, isp_id)

    new_connections_q = Customer.query.filter(
        Customer.company_id == company_id,
        Customer.installation_date == day_start.date(),
    )
    new_connections_q = _apply_customer_filters(new_connections_q, area_id, isp_id)

    complaints_opened_q = db.session.query(func.count(Complaint.id)).join(
        Customer, Complaint.customer_id == Customer.id
    ).filter(
        Customer.company_id == company_id,
        Complaint.is_active == True,
        Complaint.created_at >= day_start,
        Complaint.created_at <= day_end,
    )
    complaints_opened_q = _apply_customer_filters(complaints_opened_q, area_id, isp_id)

    return {
        'collections_today': round(float(collections_q.scalar() or 0), 2),
        'invoices_due_today': int(invoices_due_q.scalar() or 0),
        'new_connections_today': int(new_connections_q.count() or 0),
        'complaints_opened_today': int(complaints_opened_q.scalar() or 0),
    }


# ─── Charts ─────────────────────────────────────────────────────────────────

def get_revenue_vs_isp_cost_trend(company_id, start_date=None, end_date=None, area_id=None, isp_id=None):
    """Chart 1 — Collections vs ISP Payments for selected window."""
    window_start, window_end, granularity = _resolve_chart_window(start_date, end_date)
    series_keys = _build_series_keys(window_start, window_end, granularity)
    # Fix: was hardcoded 'YYYY-MM' in both queries below.
    # For short date ranges (<=45 days), _resolve_chart_window returns granularity='day'
    # and series_keys are daily ('2026-03-01'). With 'YYYY-MM' the queries return monthly
    # buckets ('2026-03') that never match any daily key → all chart points were 0.
    group_fmt = 'YYYY-MM-DD' if granularity == 'day' else 'YYYY-MM'

    # Collections — filtered by area/isp via _apply_customer_filters
    collections_q = db.session.query(
        func.to_char(Payment.payment_date, group_fmt).label('month'),
        func.coalesce(func.sum(Payment.amount), 0).label('amount'),
    ).join(
        Invoice, Payment.invoice_id == Invoice.id
    ).join(
        Customer, Invoice.customer_id == Customer.id
    ).filter(
        Invoice.company_id == company_id,
        Payment.status == 'paid',
        Payment.is_active == True,
        Payment.payment_date >= window_start,
        Payment.payment_date <= window_end,
    )
    collections_q = _apply_customer_filters(collections_q, area_id, isp_id)
    collections_q = collections_q.group_by('month').order_by('month')
    collections_map = {r.month: float(r.amount) for r in collections_q.all()}

    # ISP payments — apply group_fmt fix AND isp_id filter.
    # Fix: was 'YYYY-MM' (wrong for daily granularity) and had no isp_id filter.
    # Without isp_id filter: when ISP X is selected, collections show X's revenue
    # but ISP cost shows ALL ISPs' costs — chart was misleading/incorrect.
    isp_q = db.session.query(
        func.to_char(ISPPayment.payment_date, group_fmt).label('month'),
        func.coalesce(func.sum(ISPPayment.amount), 0).label('amount'),
    ).filter(
        ISPPayment.company_id == company_id,
        ISPPayment.is_active == True,
        ISPPayment.payment_date >= window_start,
        ISPPayment.payment_date <= window_end,
    )
    if isp_id:
        isp_q = isp_q.filter(ISPPayment.isp_id == isp_id)
    isp_q = isp_q.group_by('month').order_by('month')
    isp_map = {r.month: float(r.amount) for r in isp_q.all()}

    # Merge into series
    result = []
    for mk in series_keys:
        dt = datetime.strptime(mk, '%Y-%m-%d') if granularity == 'day' else datetime.strptime(mk, '%Y-%m')
        collections = collections_map.get(mk, 0)
        isp_cost = isp_map.get(mk, 0)
        result.append({
            'month': dt.strftime('%d %b %Y') if granularity == 'day' else dt.strftime('%b %Y'),
            'month_short': dt.strftime('%d %b') if granularity == 'day' else dt.strftime('%b'),
            'collections': round(collections, 2),
            'isp_cost': round(isp_cost, 2),
            'margin': round(collections - isp_cost, 2),
        })
    return result


def get_customer_growth_chart(company_id, start_date=None, end_date=None, area_id=None, isp_id=None):
    """Chart 2 — New vs Churned + Total Active for selected window."""
    window_start, window_end, granularity = _resolve_chart_window(start_date, end_date)
    series_keys = _build_series_keys(window_start, window_end, granularity)
    group_fmt = 'YYYY-MM-DD' if granularity == 'day' else 'YYYY-MM'

    # New customers by month (using installation_date)
    new_q = db.session.query(
        func.to_char(Customer.installation_date, group_fmt).label('month'),
        func.count(Customer.id).label('count'),
    ).filter(
        Customer.company_id == company_id,
        Customer.installation_date >= window_start.date(),
        Customer.installation_date <= window_end.date(),
    )
    new_q = _apply_customer_filters(new_q, area_id, isp_id)
    new_q = new_q.group_by('month').order_by('month')
    new_map = {r.month: r.count for r in new_q.all()}

    # Churned by month
    churned_q = db.session.query(
        func.to_char(func.coalesce(Customer.deactivated_at, Customer.updated_at), group_fmt).label('month'),
        func.count(Customer.id).label('count'),
    ).filter(
        Customer.company_id == company_id,
        Customer.is_active == False,
        func.coalesce(Customer.deactivated_at, Customer.updated_at) >= window_start,
        func.coalesce(Customer.deactivated_at, Customer.updated_at) <= window_end,
    )
    churned_q = _apply_customer_filters(churned_q, area_id, isp_id)
    churned_q = churned_q.group_by('month').order_by('month')
    churned_map = {r.month: r.count for r in churned_q.all()}

    # Total active as of today (we'll back-calculate for each month)
    base_q = Customer.query.filter(
        Customer.company_id == company_id,
        Customer.is_active == True,
    )
    base_q = _apply_customer_filters(base_q, area_id, isp_id)
    current_active = base_q.count()

    # Build series — work backward from current_active
    result = []
    for mk in series_keys:
        dt = datetime.strptime(mk, '%Y-%m-%d') if granularity == 'day' else datetime.strptime(mk, '%Y-%m')
        result.append({
            'month': dt.strftime('%d %b %Y') if granularity == 'day' else dt.strftime('%b %Y'),
            'month_short': dt.strftime('%d %b') if granularity == 'day' else dt.strftime('%b'),
            'new': new_map.get(mk, 0),
            'churned': churned_map.get(mk, 0),
            'total': 0,  # Will be filled below
        })

    # Calculate total active for each month by working backward from latest
    # current_active = base at end of last month + net of last month
    # We reverse, then walk forward
    running_total = current_active
    for i in range(len(result) - 1, -1, -1):
        result[i]['total'] = running_total
        # Undo this month: subtract new, add churned
        running_total = running_total - result[i]['new'] + result[i]['churned']

    return result


def get_collection_rate_by_month(company_id, start_date=None, end_date=None, area_id=None, isp_id=None):
    """Chart 3 — (Payments / Invoices Billed) * 100 per bucket for selected window."""
    window_start, window_end, granularity = _resolve_chart_window(start_date, end_date)
    series_keys = _build_series_keys(window_start, window_end, granularity)
    group_fmt = 'YYYY-MM-DD' if granularity == 'day' else 'YYYY-MM'

    # Invoices billed by month (using due_date)
    billed_q = db.session.query(
        func.to_char(Invoice.due_date, group_fmt).label('month'),
        func.coalesce(func.sum(Invoice.total_amount), 0).label('amount'),
    ).join(
        Customer, Invoice.customer_id == Customer.id
    ).filter(
        Invoice.company_id == company_id,
        Invoice.is_active == True,
        Invoice.due_date >= window_start.date(),
        Invoice.due_date <= window_end.date(),
    )
    billed_q = _apply_customer_filters(billed_q, area_id, isp_id)
    billed_q = billed_q.group_by('month').order_by('month')
    billed_map = {r.month: float(r.amount) for r in billed_q.all()}

    # Collections by month (reusable — same pattern as Chart 1)
    collected_q = db.session.query(
        func.to_char(Payment.payment_date, group_fmt).label('month'),
        func.coalesce(func.sum(Payment.amount), 0).label('amount'),
    ).join(
        Invoice, Payment.invoice_id == Invoice.id
    ).join(
        Customer, Invoice.customer_id == Customer.id
    ).filter(
        Invoice.company_id == company_id,
        Payment.status == 'paid',
        Payment.is_active == True,
        Payment.payment_date >= window_start,
        Payment.payment_date <= window_end,
    )
    collected_q = _apply_customer_filters(collected_q, area_id, isp_id)
    collected_q = collected_q.group_by('month').order_by('month')
    collected_map = {r.month: float(r.amount) for r in collected_q.all()}

    result = []
    for mk in series_keys:
        dt = datetime.strptime(mk, '%Y-%m-%d') if granularity == 'day' else datetime.strptime(mk, '%Y-%m')
        billed = billed_map.get(mk, 0)
        collected = collected_map.get(mk, 0)
        rate = round((collected / billed * 100), 1) if billed > 0 else 0
        result.append({
            'month': dt.strftime('%d %b %Y') if granularity == 'day' else dt.strftime('%b %Y'),
            'month_short': dt.strftime('%d %b') if granularity == 'day' else dt.strftime('%b'),
            'billed': round(billed, 2),
            'collected': round(collected, 2),
            'rate': rate,
        })
    return result


def get_revenue_by_isp(company_id, start_date, end_date, area_id=None):
    """Chart 4 — Donut: revenue per ISP. 1 query."""
    q = db.session.query(
        ISP.name.label('isp'),
        func.coalesce(func.sum(Payment.amount), 0).label('revenue'),
    ).select_from(Payment).join(
        Invoice, Payment.invoice_id == Invoice.id
    ).join(
        Customer, Invoice.customer_id == Customer.id
    ).join(
        ISP, Customer.isp_id == ISP.id
    ).filter(
        Invoice.company_id == company_id,
        Payment.status == 'paid',
        Payment.is_active == True,
        Payment.payment_date >= start_date,
        Payment.payment_date <= end_date,
    )
    if area_id:
        q = q.filter(Customer.area_id == area_id)

    results = q.group_by(ISP.name).order_by(desc('revenue')).all()
    total = sum(float(r.revenue) for r in results)

    return [{
        'isp': r.isp,
        'revenue': round(float(r.revenue), 2),
        'percentage': round(float(r.revenue) / total * 100, 1) if total > 0 else 0,
    } for r in results]


def get_isp_cost_per_subscriber(company_id, start_date, end_date, area_id=None, isp_id=None, limit=8):
    """Chart — ISP procurement efficiency: cost per active subscriber by ISP."""
    cost_q = db.session.query(
        ISP.id.label('isp_id'),
        ISP.name.label('isp_name'),
        func.coalesce(func.sum(ISPPayment.amount), 0).label('total_cost'),
    ).select_from(ISPPayment).join(
        ISP, ISPPayment.isp_id == ISP.id
    ).filter(
        ISPPayment.company_id == company_id,
        ISPPayment.is_active == True,
        ISPPayment.payment_date >= start_date,
        ISPPayment.payment_date <= end_date,
    )
    if isp_id:
        cost_q = cost_q.filter(ISPPayment.isp_id == isp_id)
    cost_rows = cost_q.group_by(ISP.id, ISP.name).all()
    cost_map = {
        str(r.isp_id): {
            'isp': r.isp_name,
            'total_cost': float(r.total_cost or 0),
        }
        for r in cost_rows
    }

    subs_q = db.session.query(
        Customer.isp_id,
        func.count(Customer.id).label('subscribers'),
    ).filter(
        Customer.company_id == company_id,
        Customer.is_active == True,
    )
    if area_id:
        subs_q = subs_q.filter(Customer.area_id == area_id)
    if isp_id:
        subs_q = subs_q.filter(Customer.isp_id == isp_id)
    subs_rows = subs_q.group_by(Customer.isp_id).all()
    subs_map = {str(r.isp_id): int(r.subscribers or 0) for r in subs_rows}

    all_isp_ids = set(cost_map.keys()) | set(subs_map.keys())
    result = []
    for i_id in all_isp_ids:
        total_cost = float(cost_map.get(i_id, {}).get('total_cost', 0))
        subscribers = int(subs_map.get(i_id, 0))
        cps = round(total_cost / subscribers, 2) if subscribers > 0 else round(total_cost, 2)
        result.append({
            'isp': cost_map.get(i_id, {}).get('isp', 'Unknown ISP'),
            'subscribers': subscribers,
            'total_cost': round(total_cost, 2),
            'cost_per_subscriber': cps,
        })

    result.sort(key=lambda x: x['cost_per_subscriber'], reverse=True)
    return result[:limit]


def get_top_areas_by_revenue(company_id, start_date, end_date, isp_id=None, limit=6):
    """Chart 5 — Horizontal bar: top areas by actual collections. 1 query."""
    q = db.session.query(
        Area.name.label('area'),
        func.coalesce(func.sum(Payment.amount), 0).label('revenue'),
    ).select_from(Payment).join(
        Invoice, Payment.invoice_id == Invoice.id
    ).join(
        Customer, Invoice.customer_id == Customer.id
    ).join(
        Area, Customer.area_id == Area.id
    ).filter(
        Invoice.company_id == company_id,
        Payment.status == 'paid',
        Payment.is_active == True,
        Payment.payment_date >= start_date,
        Payment.payment_date <= end_date,
    )
    if isp_id:
        q = q.filter(Customer.isp_id == isp_id)

    results = q.group_by(Area.name).order_by(desc('revenue')).limit(limit).all()

    return [{
        'area': r.area,
        'revenue': round(float(r.revenue), 2),
    } for r in results]


def get_top_plans_by_mrr(company_id, start_date=None, end_date=None, area_id=None, isp_id=None, limit=6):
    """Chart 6 — Horizontal bar: top plans by MRR. 1 query."""
    window_start, window_end, _ = _resolve_chart_window(start_date, end_date)

    q = db.session.query(
        ServicePlan.name.label('plan'),
        ServicePlan.price,
        func.count(CustomerPackage.id).label('subscribers'),
        (ServicePlan.price * func.count(CustomerPackage.id)).label('mrr'),
    ).join(
        CustomerPackage, CustomerPackage.service_plan_id == ServicePlan.id
    ).join(
        Customer, Customer.id == CustomerPackage.customer_id
    ).filter(
        ServicePlan.company_id == company_id,
        Customer.is_active == True,
        CustomerPackage.is_active == True,
        CustomerPackage.start_date <= window_end.date(),
        or_(CustomerPackage.end_date.is_(None), CustomerPackage.end_date >= window_start.date()),
    )
    q = _apply_customer_filters(q, area_id, isp_id)
    results = q.group_by(
        ServicePlan.id, ServicePlan.name, ServicePlan.price
    ).order_by(desc('mrr')).limit(limit).all()

    return [{
        'plan': r.plan,
        'price': round(float(r.price or 0), 2),
        'subscribers': r.subscribers,
        'mrr': round(float(r.mrr or 0), 2),
    } for r in results]


# ─── Action Tables ──────────────────────────────────────────────────────────

def get_critical_overdue_invoices(company_id, area_id=None, isp_id=None, limit=10):
    """Table 1 — Top overdue invoices by amount. 1 query."""
    today = datetime.now(PKT).date()

    q = db.session.query(
        Invoice.id,
        Invoice.invoice_number,
        Invoice.total_amount,
        Invoice.due_date,
        Customer.first_name,
        Customer.last_name,
        Customer.internet_id,
        Area.name.label('area_name'),
        User.first_name.label('tech_first_name'),
        User.last_name.label('tech_last_name'),
    ).join(
        Customer, Invoice.customer_id == Customer.id
    ).join(
        Area, Customer.area_id == Area.id
    ).outerjoin(
        User, Customer.technician_id == User.id
    ).filter(
        Invoice.company_id == company_id,
        Invoice.status.in_(['pending', 'partially_paid', 'overdue']),
        Invoice.due_date < today,
        Invoice.is_active == True,
    )
    q = _apply_customer_filters(q, area_id, isp_id)
    results = q.order_by(Invoice.total_amount.desc()).limit(limit).all()

    return [{
        'id': str(r.id),
        'invoice_number': r.invoice_number,
        'customer_name': f"{r.first_name} {r.last_name}",
        'internet_id': r.internet_id,
        'amount': round(float(r.total_amount or 0), 2),
        'due_date': r.due_date.strftime('%Y-%m-%d') if r.due_date else None,
        'days_overdue': (today - r.due_date).days if r.due_date else 0,
        'area': r.area_name,
        'technician': f"{r.tech_first_name} {r.tech_last_name}" if r.tech_first_name else 'Unassigned',
    } for r in results]


def get_open_complaints(company_id, area_id=None, isp_id=None):
    """Table 2 — Open complaints with SLA breach flag. 1 query."""
    now = datetime.now(PKT)

    q = db.session.query(
        Complaint.id,
        Complaint.ticket_number,
        Complaint.status,
        Complaint.created_at,
        Complaint.response_due_date,
        Customer.first_name,
        Customer.last_name,
        Area.name.label('area_name'),
        User.first_name.label('assigned_first'),
        User.last_name.label('assigned_last'),
    ).join(
        Customer, Complaint.customer_id == Customer.id
    ).join(
        Area, Customer.area_id == Area.id
    ).outerjoin(
        User, Complaint.assigned_to == User.id
    ).filter(
        Customer.company_id == company_id,
        Complaint.status.in_(['open', 'in_progress']),
        Complaint.is_active == True,
    )
    q = _apply_customer_filters(q, area_id, isp_id)
    results = q.order_by(Complaint.created_at.asc()).all()

    alerts = {
        'breach_in_3h': 0,
        'breach_in_6h': 0,
        'already_breached': 0,
    }

    items = []
    for r in results:
        due_dt = _to_pkt(r.response_due_date) if r.response_due_date else None
        seconds_remaining = int((due_dt - now).total_seconds()) if due_dt else None

        if seconds_remaining is None:
            sla_state = 'unknown'
        elif seconds_remaining < 0:
            sla_state = 'breached'
            alerts['already_breached'] += 1
        elif seconds_remaining <= 3 * 3600:
            sla_state = 'due_soon'
            alerts['breach_in_3h'] += 1
            alerts['breach_in_6h'] += 1
        elif seconds_remaining <= 6 * 3600:
            sla_state = 'on_track'
            alerts['breach_in_6h'] += 1
        else:
            sla_state = 'on_track'

        items.append({
            'id': str(r.id),
            'ticket_number': r.ticket_number,
            'customer_name': f"{r.first_name} {r.last_name}",
            'area': r.area_name,
            'assigned_to': f"{r.assigned_first} {r.assigned_last}" if r.assigned_first else 'Unassigned',
            'created_at': r.created_at.strftime('%Y-%m-%d %H:%M') if r.created_at else None,
            'status': r.status,
            'response_due_date': due_dt.strftime('%Y-%m-%d %H:%M') if due_dt else None,
            'sla_seconds_remaining': seconds_remaining,
            'sla_state': sla_state,
            'sla_breach': bool(seconds_remaining is not None and seconds_remaining < 0),
        })

    return {
        'items': items,
        'alerts': alerts,
    }


# ─── Filter Options ─────────────────────────────────────────────────────────

def get_filter_options(company_id):
    """Return dropdown options for Area and ISP filters only."""
    areas = Area.query.filter(
        Area.company_id == company_id,
        Area.is_active == True,
    ).order_by(Area.name).all()

    isps = ISP.query.filter(
        ISP.company_id == company_id,
        ISP.is_active == True,
    ).order_by(ISP.name).all()

    return {
        'areas': [{'id': str(a.id), 'name': a.name} for a in areas],
        'isps': [{'id': str(i.id), 'name': i.name} for i in isps],
    }