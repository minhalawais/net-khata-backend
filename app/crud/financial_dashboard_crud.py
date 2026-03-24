"""
Financial Intelligence Dashboard CRUD

6 Sections: P&L Summary, Revenue Breakdown, Cost Intelligence,
Employee Financial, Collections Aging, Bank Positions.

All monthly aggregations use GROUP BY — no N+1 loops.
Filters: date range (start_date, end_date).
"""

from app import db
from app.models import (
    Invoice, Payment, ISPPayment, Expense, ExpenseType,
    ExtraIncome, ExtraIncomeType, BankAccount, Customer, Area,
    User, RecoveryTask, EmployeeLedger, ISP, InternalTransfer
)
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import func, case, and_, desc
from sqlalchemy.exc import SQLAlchemyError
from pytz import timezone
from uuid import UUID
import logging

logger = logging.getLogger(__name__)
PKT = timezone('Asia/Karachi')

FINANCIAL_INTELLIGENCE_V2_SCHEMA = 'financial_intelligence.v2'
FINANCIAL_INTELLIGENCE_V2_SECTIONS = [
    'pl_summary',
    'revenue_breakdown',
    'cost_intelligence',
    'employee_financial',
    'collections_aging',
    'bank_positions',
]
FINANCIAL_INTELLIGENCE_V2_FILTER_KEYS = [
    'start_date',
    'end_date',
    'bank_account_id',
    'payment_method',
    'invoice_status',
    'isp_payment_type',
    'time_range',
]


# ─── Helpers ────────────────────────────────────────────────────────────────

def _parse_dates(start_str, end_str):
    """Parse date strings; default to MTD."""
    today = datetime.now(PKT)
    try:
        start = datetime.strptime(start_str, '%Y-%m-%d') if start_str else today.replace(day=1)
        end = datetime.strptime(end_str, '%Y-%m-%d') if end_str else today
    except Exception:
        start = today.replace(day=1)
        end = today
    return start, end


def _prev_period(start, end):
    days = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days - 1)
    return prev_start, prev_end


def _signed_payment_amount():
    """Refund invoices negate the amount."""
    return case((Invoice.invoice_type == 'refund', -Payment.amount), else_=Payment.amount)


def _net_payment_amount_by_status():
    """Signed payment amount using payment status so refunds reduce net collected."""
    return case((Payment.status == 'refunded', -Payment.amount), else_=Payment.amount)


def _trend(cur, prev):
    if prev == 0:
        return 100.0 if cur > 0 else 0.0
    return round(((cur - prev) / abs(prev)) * 100, 1)


def _twelve_months_ago():
    today = datetime.now(PKT)
    return today.replace(day=1) - relativedelta(months=11)


def _month_keys():
    today = datetime.now(PKT)
    return [(today - relativedelta(months=i)).strftime('%Y-%m') for i in range(11, -1, -1)]


def _month_window_bounds(month_key):
    """Return inclusive month window for a YYYY-MM key."""
    start = datetime.strptime(f"{month_key}-01", '%Y-%m-%d')
    end = (start + relativedelta(months=1)) - timedelta(days=1)
    return start, end


def _chart_buckets(start, end, daily_threshold=45):
    """Build date buckets from selected range with adaptive day/month granularity."""
    start_date = start.date() if isinstance(start, datetime) else start
    end_date = end.date() if isinstance(end, datetime) else end
    if end_date < start_date:
        start_date, end_date = end_date, start_date

    span_days = (end_date - start_date).days + 1
    granularity = 'day' if span_days <= daily_threshold else 'month'

    if granularity == 'day':
        keys = [
            (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
            for i in range(span_days)
        ]
    else:
        cursor = datetime(start_date.year, start_date.month, 1).date()
        last = datetime(end_date.year, end_date.month, 1).date()
        keys = []
        while cursor <= last:
            keys.append(cursor.strftime('%Y-%m'))
            cursor = (datetime(cursor.year, cursor.month, 1) + relativedelta(months=1)).date()

    return granularity, keys


def _format_bucket_labels(bucket_key, granularity):
    if granularity == 'day':
        dt = datetime.strptime(bucket_key, '%Y-%m-%d')
        return dt.strftime('%d %b %Y'), dt.strftime('%d %b')

    dt = datetime.strptime(bucket_key, '%Y-%m')
    return dt.strftime('%b %Y'), dt.strftime('%b')


def _normalize_invoice_status(raw_status):
    """Normalize invoice status filter; returns None when filter is not applied."""
    if not raw_status:
        return None

    status = str(raw_status).strip().lower()
    if status in ('', 'all'):
        return None

    allowed = {
        'pending',
        'partially_paid',
        'paid',
        'overdue',
        'cancelled',
        'refunded',
    }
    return status if status in allowed else None


def _normalize_bank_account_id(raw_bank_account_id):
    """Normalize bank account UUID filter; returns None when filter is not applied."""
    if not raw_bank_account_id:
        return None

    value = str(raw_bank_account_id).strip()
    if value.lower() in ('', 'all', 'none', 'null'):
        return None

    try:
        return UUID(value)
    except Exception:
        return None


def _normalize_payment_method(raw_payment_method):
    """Normalize payment method filter; returns None when filter is not applied."""
    if not raw_payment_method:
        return None

    value = str(raw_payment_method).strip().lower()
    if value in ('', 'all'):
        return None

    allowed = {'cash', 'online', 'bank_transfer', 'credit_card'}
    return value if value in allowed else None


def _normalize_isp_payment_type(raw_isp_payment_type):
    """Normalize ISP payment type filter; returns None when filter is not applied."""
    if not raw_isp_payment_type:
        return None

    value = str(raw_isp_payment_type).strip().lower()
    if value in ('', 'all'):
        return None

    allowed = {'monthly_subscription', 'bandwidth_usage', 'infrastructure', 'other'}
    return value if value in allowed else None


def _net_margin_trend_12m(company_id, invoice_status=None, bank_account_id=None, payment_method=None, isp_payment_type=None):
    """12-month monthly net margin trend using realized collections + income - costs."""
    month_keys = _month_keys()
    start_12m, _ = _month_window_bounds(month_keys[0])
    now = datetime.now(PKT)

    payment_bucket = func.to_char(Payment.payment_date, 'YYYY-MM').label('bucket')
    extra_bucket = func.to_char(ExtraIncome.income_date, 'YYYY-MM').label('bucket')
    isp_bucket = func.to_char(ISPPayment.payment_date, 'YYYY-MM').label('bucket')
    exp_bucket = func.to_char(Expense.expense_date, 'YYYY-MM').label('bucket')

    collections_q = db.session.query(
        payment_bucket,
        func.coalesce(func.sum(_signed_payment_amount()), 0).label('amount'),
    ).join(Invoice, Payment.invoice_id == Invoice.id).filter(
        Payment.company_id == company_id,
        Payment.is_active == True,
        Payment.status == 'paid',
        Payment.payment_date >= start_12m,
        Payment.payment_date <= now,
    ).group_by('bucket').order_by('bucket')
    if invoice_status:
        collections_q = collections_q.filter(Invoice.status == invoice_status)
    if bank_account_id:
        collections_q = collections_q.filter(Payment.bank_account_id == bank_account_id)
    if payment_method:
        collections_q = collections_q.filter(Payment.payment_method == payment_method)
    collections_map = {r.bucket: float(r.amount or 0) for r in collections_q.all()}

    extra_q = db.session.query(
        extra_bucket,
        func.coalesce(func.sum(ExtraIncome.amount), 0).label('amount'),
    ).filter(
        ExtraIncome.company_id == company_id,
        ExtraIncome.is_active == True,
        ExtraIncome.income_date >= start_12m,
        ExtraIncome.income_date <= now,
    )
    if bank_account_id:
        extra_q = extra_q.filter(ExtraIncome.bank_account_id == bank_account_id)
    if payment_method:
        extra_q = extra_q.filter(ExtraIncome.payment_method == payment_method)
    extra_map = {r.bucket: float(r.amount or 0) for r in extra_q.group_by('bucket').order_by('bucket').all()}

    isp_q = db.session.query(
        isp_bucket,
        func.coalesce(func.sum(ISPPayment.amount), 0).label('amount'),
    ).filter(
        ISPPayment.company_id == company_id,
        ISPPayment.is_active == True,
        ISPPayment.payment_date >= start_12m,
        ISPPayment.payment_date <= now,
    )
    if bank_account_id:
        isp_q = isp_q.filter(ISPPayment.bank_account_id == bank_account_id)
    if payment_method:
        isp_q = isp_q.filter(ISPPayment.payment_method == payment_method)
    if isp_payment_type:
        isp_q = isp_q.filter(ISPPayment.payment_type == isp_payment_type)
    isp_map = {r.bucket: float(r.amount or 0) for r in isp_q.group_by('bucket').order_by('bucket').all()}

    exp_q = db.session.query(
        exp_bucket,
        func.coalesce(func.sum(Expense.amount), 0).label('amount'),
    ).filter(
        Expense.company_id == company_id,
        Expense.is_active == True,
        Expense.expense_date >= start_12m,
        Expense.expense_date <= now,
    )
    if bank_account_id:
        exp_q = exp_q.filter(Expense.bank_account_id == bank_account_id)
    if payment_method:
        exp_q = exp_q.filter(Expense.payment_method == payment_method)
    exp_map = {r.bucket: float(r.amount or 0) for r in exp_q.group_by('bucket').order_by('bucket').all()}

    trend = []
    for key in month_keys:
        label, short_label = _format_bucket_labels(key, 'month')
        collections = collections_map.get(key, 0)
        extra_income = extra_map.get(key, 0)
        isp_cost = isp_map.get(key, 0)
        expenses = exp_map.get(key, 0)
        revenue = collections + extra_income
        net_profit = revenue - isp_cost - expenses
        margin_pct = round((net_profit / revenue) * 100, 1) if revenue > 0 else 0
        trend.append({
            'month': label,
            'month_short': short_label,
            'revenue': round(revenue, 2),
            'net_profit': round(net_profit, 2),
            'margin_pct': margin_pct,
        })

    return trend


def _isp_cost_per_subscriber_trend_12m(company_id, bank_account_id=None, payment_method=None, isp_payment_type=None):
    """12-month ISP procurement efficiency trend at company level."""
    month_keys = _month_keys()
    start_12m, _ = _month_window_bounds(month_keys[0])
    now = datetime.now(PKT)

    isp_bucket = func.to_char(ISPPayment.payment_date, 'YYYY-MM').label('bucket')
    invoice_bucket = func.to_char(Invoice.billing_start_date, 'YYYY-MM').label('bucket')

    isp_q = db.session.query(
        isp_bucket,
        func.coalesce(func.sum(ISPPayment.amount), 0).label('amount'),
    ).filter(
        ISPPayment.company_id == company_id,
        ISPPayment.is_active == True,
        ISPPayment.payment_date >= start_12m,
        ISPPayment.payment_date <= now,
    )
    if bank_account_id:
        isp_q = isp_q.filter(ISPPayment.bank_account_id == bank_account_id)
    if payment_method:
        isp_q = isp_q.filter(ISPPayment.payment_method == payment_method)
    if isp_payment_type:
        isp_q = isp_q.filter(ISPPayment.payment_type == isp_payment_type)
    isp_map = {r.bucket: float(r.amount or 0) for r in isp_q.group_by('bucket').order_by('bucket').all()}

    subscribers_map = {r.bucket: int(r.subscribers or 0) for r in db.session.query(
        invoice_bucket,
        func.count(func.distinct(Invoice.customer_id)).label('subscribers'),
    ).filter(
        Invoice.company_id == company_id,
        Invoice.is_active == True,
        Invoice.invoice_type != 'refund',
        Invoice.billing_start_date >= start_12m,
        Invoice.billing_start_date <= now,
    ).group_by('bucket').order_by('bucket').all()}

    trend = []
    for key in month_keys:
        label, short_label = _format_bucket_labels(key, 'month')
        total_cost = isp_map.get(key, 0)
        subscribers = subscribers_map.get(key, 0)
        trend.append({
            'month': label,
            'month_short': short_label,
            'isp_total_cost': round(total_cost, 2),
            'active_subscribers': subscribers,
            'cost_per_subscriber': round(total_cost / subscribers, 2) if subscribers > 0 else 0,
        })

    return trend


# ─── Main Orchestrator ──────────────────────────────────────────────────────

def get_financial_dashboard_data(company_id, filters=None):
    """Build the full Financial Intelligence payload."""
    if not company_id:
        return {'error': 'Company ID required'}

    filters = filters or {}
    start_date, end_date = _parse_dates(filters.get('start_date'), filters.get('end_date'))
    prev_start, prev_end = _prev_period(start_date, end_date)
    invoice_status = _normalize_invoice_status(filters.get('invoice_status'))
    bank_account_id = _normalize_bank_account_id(filters.get('bank_account_id'))
    payment_method = _normalize_payment_method(filters.get('payment_method'))
    isp_payment_type = _normalize_isp_payment_type(filters.get('isp_payment_type'))

    try:
        return {
            'pl_summary': get_pl_summary(
                company_id,
                start_date,
                end_date,
                prev_start,
                prev_end,
                invoice_status=invoice_status,
                bank_account_id=bank_account_id,
                payment_method=payment_method,
                isp_payment_type=isp_payment_type,
            ),
            'revenue_breakdown': get_revenue_breakdown(
                company_id,
                start_date,
                end_date,
                invoice_status=invoice_status,
                bank_account_id=bank_account_id,
                payment_method=payment_method,
                isp_payment_type=isp_payment_type,
            ),
            'cost_intelligence': get_cost_intelligence(
                company_id,
                start_date,
                end_date,
                bank_account_id=bank_account_id,
                payment_method=payment_method,
                isp_payment_type=isp_payment_type,
            ),
            'collections_aging': get_collections_aging(company_id, start_date, end_date, invoice_status),
            'bank_positions': get_bank_positions(
                company_id,
                start_date,
                end_date,
                invoice_status=invoice_status,
                bank_account_id=bank_account_id,
                payment_method=payment_method,
                isp_payment_type=isp_payment_type,
            ),
            'employee_financial': get_employee_financial_summary(company_id, start_date, end_date),
            'period': {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
            },
        }
    except SQLAlchemyError as e:
        logger.error(f"DB error in financial dashboard: {e}")
        return {'error': 'Database error'}
    except Exception as e:
        logger.error(f"Error in financial dashboard: {e}")
        return {'error': str(e)}


def get_financial_intelligence_v2(company_id, filters=None):
    """Versioned financial dashboard contract for stable frontend integration."""
    normalized_filters = filters or {}
    base_payload = get_financial_dashboard_data(company_id, normalized_filters)

    if not isinstance(base_payload, dict) or 'error' in base_payload:
        return base_payload

    return {
        'schema_version': FINANCIAL_INTELLIGENCE_V2_SCHEMA,
        'generated_at': datetime.now(PKT).isoformat(),
        'sections_order': FINANCIAL_INTELLIGENCE_V2_SECTIONS,
        'filters_applied': {
            key: normalized_filters.get(key, 'all')
            for key in FINANCIAL_INTELLIGENCE_V2_FILTER_KEYS
        },
        'data': base_payload,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Section A — P&L Summary (4 KPIs)
# ══════════════════════════════════════════════════════════════════════════════

def get_pl_summary(
    company_id,
    start,
    end,
    prev_start,
    prev_end,
    invoice_status=None,
    bank_account_id=None,
    payment_method=None,
    isp_payment_type=None,
):
    """4 KPIs: Gross Revenue, Total Costs, Net Profit, Cash in Bank."""

    def _collections(sd, ed):
        q = db.session.query(
            func.coalesce(func.sum(_signed_payment_amount()), 0)
        ).join(Invoice, Payment.invoice_id == Invoice.id).filter(
            Payment.company_id == company_id,
            Payment.status == 'paid', Payment.is_active == True,
            Payment.payment_date >= sd, Payment.payment_date <= ed,
        )
        if invoice_status:
            q = q.filter(Invoice.status == invoice_status)
        if bank_account_id:
            q = q.filter(Payment.bank_account_id == bank_account_id)
        if payment_method:
            q = q.filter(Payment.payment_method == payment_method)
        return float(q.scalar() or 0)

    def _extra_income(sd, ed):
        q = db.session.query(
            func.coalesce(func.sum(ExtraIncome.amount), 0)
        ).filter(
            ExtraIncome.company_id == company_id,
            ExtraIncome.is_active == True,
            ExtraIncome.income_date >= sd, ExtraIncome.income_date <= ed,
        )
        if bank_account_id:
            q = q.filter(ExtraIncome.bank_account_id == bank_account_id)
        if payment_method:
            q = q.filter(ExtraIncome.payment_method == payment_method)
        return float(q.scalar() or 0)

    def _isp_payments(sd, ed):
        q = db.session.query(
            func.coalesce(func.sum(ISPPayment.amount), 0)
        ).filter(
            ISPPayment.company_id == company_id,
            ISPPayment.is_active == True,
            ISPPayment.payment_date >= sd, ISPPayment.payment_date <= ed,
        )
        if bank_account_id:
            q = q.filter(ISPPayment.bank_account_id == bank_account_id)
        if payment_method:
            q = q.filter(ISPPayment.payment_method == payment_method)
        if isp_payment_type:
            q = q.filter(ISPPayment.payment_type == isp_payment_type)
        return float(q.scalar() or 0)

    def _expenses(sd, ed):
        q = db.session.query(
            func.coalesce(func.sum(Expense.amount), 0)
        ).filter(
            Expense.company_id == company_id,
            Expense.is_active == True,
            Expense.expense_date >= sd, Expense.expense_date <= ed,
        )
        if bank_account_id:
            q = q.filter(Expense.bank_account_id == bank_account_id)
        if payment_method:
            q = q.filter(Expense.payment_method == payment_method)
        return float(q.scalar() or 0)

    # Current period
    collections = _collections(start, end)
    extra = _extra_income(start, end)
    isp = _isp_payments(start, end)
    expenses = _expenses(start, end)

    gross_revenue = collections + extra
    total_costs = isp + expenses
    net_profit = gross_revenue - total_costs
    margin_pct = round((net_profit / gross_revenue * 100), 1) if gross_revenue > 0 else 0

    invoiced_q = db.session.query(
        func.coalesce(func.sum(Invoice.total_amount), 0)
    ).filter(
        Invoice.company_id == company_id,
        Invoice.is_active == True,
        Invoice.invoice_type != 'refund',
        Invoice.billing_start_date >= start,
        Invoice.billing_start_date <= end,
    )
    if invoice_status:
        invoiced_q = invoiced_q.filter(Invoice.status == invoice_status)
    invoiced_amount = float(invoiced_q.scalar() or 0)
    conversion_pct = round((collections / invoiced_amount) * 100, 1) if invoiced_amount > 0 else 0

    salary_accrued = float(db.session.query(
        func.coalesce(func.sum(EmployeeLedger.amount), 0)
    ).filter(
        EmployeeLedger.company_id == company_id,
        EmployeeLedger.transaction_type == 'salary_accrual',
        EmployeeLedger.created_at >= start,
        EmployeeLedger.created_at <= end,
    ).scalar() or 0)
    salary_pct_of_revenue = round((salary_accrued / gross_revenue) * 100, 1) if gross_revenue > 0 else 0

    if conversion_pct >= 95:
        conversion_band = 'good'
    elif conversion_pct >= 85:
        conversion_band = 'watch'
    else:
        conversion_band = 'risk'

    if salary_pct_of_revenue <= 25:
        salary_band = 'healthy'
    elif salary_pct_of_revenue <= 40:
        salary_band = 'watch'
    else:
        salary_band = 'risk'

    # Previous period
    p_collections = _collections(prev_start, prev_end)
    p_extra = _extra_income(prev_start, prev_end)
    p_isp = _isp_payments(prev_start, prev_end)
    p_expenses = _expenses(prev_start, prev_end)
    p_gross = p_collections + p_extra
    p_costs = p_isp + p_expenses
    p_net = p_gross - p_costs

    # Cash in Bank — snapshot
    bank_accounts_q = BankAccount.query.filter_by(
        company_id=company_id, is_active=True
    )
    if bank_account_id:
        bank_accounts_q = bank_accounts_q.filter(BankAccount.id == bank_account_id)
    bank_accounts = bank_accounts_q.all()
    cash_in_bank = sum(float(a.current_balance or 0) for a in bank_accounts)
    per_account = [{'name': f"{a.bank_name} - {a.account_number}",
                    'balance': round(float(a.current_balance or 0), 2)}
                   for a in bank_accounts]

    return {
        # ── Two new first-class KPI fields ────────────────────────────────────
        # Previously these lived only inside gross_revenue.breakdown (nested,
        # no trend data). Promoted so the frontend can render them as KPI cards
        # with proper vs-prev-period trend arrows.
        # All four variables (collections, extra, p_collections, p_extra) are
        # already computed above — zero new queries needed.
        'total_collections': {
            'value':       round(collections, 2),
            'previous':    round(p_collections, 2),
            'trend':       _trend(collections, p_collections),
            'is_positive': collections >= p_collections,
        },
        'extra_income': {
            'value':       round(extra, 2),
            'previous':    round(p_extra, 2),
            'trend':       _trend(extra, p_extra),
            'is_positive': extra >= p_extra,
        },
        'gross_revenue': {
            'value': round(gross_revenue, 2),
            'previous': round(p_gross, 2),
            'trend': _trend(gross_revenue, p_gross),
            'is_positive': gross_revenue >= p_gross,
            'breakdown': {
                'collections': round(collections, 2),
                'extra_income': round(extra, 2),
            },
        },
        'total_costs': {
            'value': round(total_costs, 2),
            'previous': round(p_costs, 2),
            'trend': _trend(total_costs, p_costs),
            'is_positive': total_costs <= p_costs,
            'breakdown': {
                'isp_payments': round(isp, 2),
                'expenses': round(expenses, 2),
            },
        },
        'net_profit': {
            'value': round(net_profit, 2),
            'previous': round(p_net, 2),
            'trend': _trend(net_profit, p_net),
            'is_positive': net_profit >= p_net,
            'margin_pct': margin_pct,
        },
        'invoice_to_cash_conversion': {
            'value': conversion_pct,
            'invoiced': round(invoiced_amount, 2),
            'collected': round(collections, 2),
            'status': conversion_band,
            'target_pct': 85,
        },
        'salary_pct_of_revenue': {
            'value': salary_pct_of_revenue,
            'salary_cost': round(salary_accrued, 2),
            'revenue': round(gross_revenue, 2),
            'status': salary_band,
            'benchmark_range': {'min': 15, 'max': 25},
        },
        'cash_in_bank': {
            'value': round(cash_in_bank, 2),
            'per_account': per_account,
            'as_of': 'current',
            'as_of_datetime': datetime.now(PKT).isoformat(),
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# Section B — Revenue Breakdown (2 charts)
# ══════════════════════════════════════════════════════════════════════════════

def get_revenue_breakdown(company_id, start, end, invoice_status=None, bank_account_id=None, payment_method=None, isp_payment_type=None):
    """B1: Revenue Sources stacked bar. B2: Invoice vs Collection."""
    granularity, buckets = _chart_buckets(start, end)
    bucket_fmt = 'YYYY-MM-DD' if granularity == 'day' else 'YYYY-MM'
    payment_bucket = func.to_char(Payment.payment_date, bucket_fmt).label('bucket')
    extra_bucket = func.to_char(ExtraIncome.income_date, bucket_fmt).label('bucket')
    invoice_bucket = func.to_char(Invoice.billing_start_date, bucket_fmt).label('bucket')

    # B1 — Customer payments by method by month
    payments_q = db.session.query(
        payment_bucket,
        Payment.payment_method,
        func.coalesce(func.sum(_signed_payment_amount()), 0).label('amount'),
    ).join(Invoice, Payment.invoice_id == Invoice.id).filter(
        Payment.company_id == company_id,
        Payment.status == 'paid', Payment.is_active == True,
        Payment.payment_date >= start,
        Payment.payment_date <= end,
    ).group_by('bucket', Payment.payment_method).order_by('bucket')
    if invoice_status:
        payments_q = payments_q.filter(Invoice.status == invoice_status)
    if bank_account_id:
        payments_q = payments_q.filter(Payment.bank_account_id == bank_account_id)
    if payment_method:
        payments_q = payments_q.filter(Payment.payment_method == payment_method)
    payments_data = payments_q.all()

    # Extra income by type by month
    extra_q = db.session.query(
        extra_bucket,
        ExtraIncomeType.name.label('income_type'),
        func.coalesce(func.sum(ExtraIncome.amount), 0).label('amount'),
    ).join(ExtraIncomeType, ExtraIncome.income_type_id == ExtraIncomeType.id).filter(
        ExtraIncome.company_id == company_id,
        ExtraIncome.is_active == True,
        ExtraIncome.income_date >= start,
        ExtraIncome.income_date <= end,
    )
    if bank_account_id:
        extra_q = extra_q.filter(ExtraIncome.bank_account_id == bank_account_id)
    if payment_method:
        extra_q = extra_q.filter(ExtraIncome.payment_method == payment_method)
    extra_q = extra_q.group_by('bucket', ExtraIncomeType.name).order_by('bucket')
    extra_data = extra_q.all()

    # Build stacked bar data
    # Collect all unique methods/types
    all_methods = set()
    all_income_types = set()
    method_map = {}  # {month: {method: amount}}
    income_type_map = {}  # {month: {type: amount}}

    for r in payments_data:
        m = r.payment_method or 'Unknown'
        all_methods.add(m)
        method_map.setdefault(r.bucket, {})[m] = float(r.amount)
    for r in extra_data:
        all_income_types.add(r.income_type)
        income_type_map.setdefault(r.bucket, {})[r.income_type] = float(r.amount)

    revenue_sources = []
    for bk in buckets:
        label, short_label = _format_bucket_labels(bk, granularity)
        entry = {
            'month': label,
            'month_short': short_label,
        }
        # Add payment methods
        for m in sorted(all_methods):
            entry[f"payment_{m}"] = round(method_map.get(bk, {}).get(m, 0), 2)
        # Add income types
        for t in sorted(all_income_types):
            entry[f"income_{t}"] = round(income_type_map.get(bk, {}).get(t, 0), 2)
        revenue_sources.append(entry)

    # B2 — Invoice generation vs Collection
    invoiced_q = db.session.query(
        invoice_bucket,
        func.coalesce(func.sum(Invoice.total_amount), 0).label('invoiced'),
    ).filter(
        Invoice.company_id == company_id,
        Invoice.is_active == True,
        Invoice.invoice_type != 'refund',
        Invoice.billing_start_date >= start,
        Invoice.billing_start_date <= end,
    ).group_by('bucket').order_by('bucket')
    if invoice_status:
        invoiced_q = invoiced_q.filter(Invoice.status == invoice_status)
    invoiced_map = {r.bucket: float(r.invoiced) for r in invoiced_q.all()}

    collected_q = db.session.query(
        payment_bucket,
        func.coalesce(func.sum(_signed_payment_amount()), 0).label('collected'),
    ).join(Invoice, Payment.invoice_id == Invoice.id).filter(
        Payment.company_id == company_id,
        Payment.status == 'paid', Payment.is_active == True,
        Payment.payment_date >= start,
        Payment.payment_date <= end,
    ).group_by('bucket').order_by('bucket')
    if invoice_status:
        collected_q = collected_q.filter(Invoice.status == invoice_status)
    if bank_account_id:
        collected_q = collected_q.filter(Payment.bank_account_id == bank_account_id)
    if payment_method:
        collected_q = collected_q.filter(Payment.payment_method == payment_method)
    collected_map = {r.bucket: float(r.collected) for r in collected_q.all()}

    invoice_vs_collection = []
    for bk in buckets:
        label, short_label = _format_bucket_labels(bk, granularity)
        inv = invoiced_map.get(bk, 0)
        col = collected_map.get(bk, 0)
        invoice_vs_collection.append({
            'month': label,
            'month_short': short_label,
            'invoiced': round(inv, 2),
            'collected': round(col, 2),
            'gap': round(inv - col, 2),
        })

    return {
        'revenue_sources': revenue_sources,
        'stacks': {
            'payment_methods': sorted(all_methods),
            'income_types': sorted(all_income_types),
        },
        'invoice_vs_collection': invoice_vs_collection,
        'net_profit_margin_trend_12m': _net_margin_trend_12m(
            company_id,
            invoice_status=invoice_status,
            bank_account_id=bank_account_id,
            payment_method=payment_method,
            isp_payment_type=isp_payment_type,
        ),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Section C — Cost Intelligence (3 charts)
# ══════════════════════════════════════════════════════════════════════════════

def get_cost_intelligence(company_id, start, end, bank_account_id=None, payment_method=None, isp_payment_type=None):
    """C1: ISP Cost donut. C2: Expense by type bar. C3: Monthly cost trend."""
    granularity, buckets = _chart_buckets(start, end)
    bucket_fmt = 'YYYY-MM-DD' if granularity == 'day' else 'YYYY-MM'
    isp_bucket = func.to_char(ISPPayment.payment_date, bucket_fmt).label('bucket')
    exp_bucket = func.to_char(Expense.expense_date, bucket_fmt).label('bucket')

    # C1 — ISP Cost by ISP + payment_type (donut)
    isp_cost_q = db.session.query(
        ISP.name.label('isp_name'),
        ISPPayment.payment_type,
        func.coalesce(func.sum(ISPPayment.amount), 0).label('amount'),
    ).join(ISP, ISPPayment.isp_id == ISP.id).filter(
        ISPPayment.company_id == company_id,
        ISPPayment.is_active == True,
        ISPPayment.payment_date >= start,
        ISPPayment.payment_date <= end,
    )
    if bank_account_id:
        isp_cost_q = isp_cost_q.filter(ISPPayment.bank_account_id == bank_account_id)
    if payment_method:
        isp_cost_q = isp_cost_q.filter(ISPPayment.payment_method == payment_method)
    if isp_payment_type:
        isp_cost_q = isp_cost_q.filter(ISPPayment.payment_type == isp_payment_type)
    isp_cost_q = isp_cost_q.group_by(ISP.name, ISPPayment.payment_type)
    isp_cost_raw = isp_cost_q.all()

    # Group by ISP for donut, with payment_type breakdown
    isp_totals = {}
    for r in isp_cost_raw:
        isp_totals.setdefault(r.isp_name, {'total': 0, 'by_type': {}})
        amt = float(r.amount)
        isp_totals[r.isp_name]['total'] += amt
        isp_totals[r.isp_name]['by_type'][r.payment_type or 'other'] = round(amt, 2)

    grand_isp = sum(v['total'] for v in isp_totals.values())
    isp_cost_donut = [{
        'isp': name,
        'amount': round(data['total'], 2),
        'percentage': round(data['total'] / grand_isp * 100, 1) if grand_isp > 0 else 0,
        'by_type': data['by_type'],
    } for name, data in sorted(isp_totals.items(), key=lambda x: -x[1]['total'])]

    # C2 — Expense by type (horizontal bar)
    expense_q = db.session.query(
        ExpenseType.name.label('expense_type'),
        func.coalesce(func.sum(Expense.amount), 0).label('amount'),
    ).join(ExpenseType, Expense.expense_type_id == ExpenseType.id).filter(
        Expense.company_id == company_id,
        Expense.is_active == True,
        Expense.expense_date >= start,
        Expense.expense_date <= end,
    )
    if bank_account_id:
        expense_q = expense_q.filter(Expense.bank_account_id == bank_account_id)
    if payment_method:
        expense_q = expense_q.filter(Expense.payment_method == payment_method)
    expense_q = expense_q.group_by(ExpenseType.name).order_by(desc('amount'))
    expense_by_type = [{'type': r.expense_type, 'amount': round(float(r.amount), 2)}
                       for r in expense_q.all()]

    # C3 — Monthly cost trend (ISP + Expenses stacked area, 12 months)
    isp_monthly_q = db.session.query(
        isp_bucket,
        func.coalesce(func.sum(ISPPayment.amount), 0).label('amount'),
    ).filter(
        ISPPayment.company_id == company_id,
        ISPPayment.is_active == True,
        ISPPayment.payment_date >= start,
        ISPPayment.payment_date <= end,
    )
    if bank_account_id:
        isp_monthly_q = isp_monthly_q.filter(ISPPayment.bank_account_id == bank_account_id)
    if payment_method:
        isp_monthly_q = isp_monthly_q.filter(ISPPayment.payment_method == payment_method)
    if isp_payment_type:
        isp_monthly_q = isp_monthly_q.filter(ISPPayment.payment_type == isp_payment_type)
    isp_monthly_q = isp_monthly_q.group_by('bucket').order_by('bucket')
    isp_map = {r.bucket: float(r.amount) for r in isp_monthly_q.all()}

    exp_monthly_q = db.session.query(
        exp_bucket,
        func.coalesce(func.sum(Expense.amount), 0).label('amount'),
    ).filter(
        Expense.company_id == company_id,
        Expense.is_active == True,
        Expense.expense_date >= start,
        Expense.expense_date <= end,
    )
    if bank_account_id:
        exp_monthly_q = exp_monthly_q.filter(Expense.bank_account_id == bank_account_id)
    if payment_method:
        exp_monthly_q = exp_monthly_q.filter(Expense.payment_method == payment_method)
    exp_monthly_q = exp_monthly_q.group_by('bucket').order_by('bucket')
    exp_map = {r.bucket: float(r.amount) for r in exp_monthly_q.all()}

    monthly_cost_trend = []
    for bk in buckets:
        label, short_label = _format_bucket_labels(bk, granularity)
        isp_amt = isp_map.get(bk, 0)
        exp_amt = exp_map.get(bk, 0)
        monthly_cost_trend.append({
            'month': label,
            'month_short': short_label,
            'isp_payments': round(isp_amt, 2),
            'expenses': round(exp_amt, 2),
            'total': round(isp_amt + exp_amt, 2),
        })

    return {
        'isp_cost_donut': isp_cost_donut,
        'expense_by_type': expense_by_type,
        'monthly_cost_trend': monthly_cost_trend,
        'isp_cost_per_subscriber_trend_12m': _isp_cost_per_subscriber_trend_12m(
            company_id,
            bank_account_id=bank_account_id,
            payment_method=payment_method,
            isp_payment_type=isp_payment_type,
        ),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Section D — Collections Aging
# ══════════════════════════════════════════════════════════════════════════════

def get_collections_aging(company_id, start, end, invoice_status=None):
    """D1: Aging table. D2: Top overdue customers. D3: Recovery performance."""
    today = datetime.now(PKT).date()

    paid_sq = db.session.query(
        Payment.invoice_id.label('invoice_id'),
        func.coalesce(func.sum(_net_payment_amount_by_status()), 0).label('paid_amount'),
    ).filter(
        Payment.company_id == company_id,
        Payment.is_active == True,
        Payment.status.in_(['paid', 'refunded']),
    ).group_by(Payment.invoice_id).subquery()

    outstanding_expr = func.greatest(
        Invoice.total_amount - func.coalesce(paid_sq.c.paid_amount, 0),
        0,
    )

    default_aging_statuses = ['pending', 'overdue', 'partially_paid']

    # D1 — Aging Analysis (5 buckets)
    outstanding_q = db.session.query(
        Invoice.id,
        outstanding_expr.label('outstanding_amount'),
        Invoice.due_date,
    ).outerjoin(
        paid_sq,
        Invoice.id == paid_sq.c.invoice_id,
    ).filter(
        Invoice.company_id == company_id,
        Invoice.is_active == True,
        outstanding_expr > 0,
    )
    if invoice_status:
        outstanding_q = outstanding_q.filter(Invoice.status == invoice_status)
    else:
        outstanding_q = outstanding_q.filter(Invoice.status.in_(default_aging_statuses))
    outstanding_q = outstanding_q.all()

    buckets = {
        '0-30': {'count': 0, 'amount': 0},
        '31-60': {'count': 0, 'amount': 0},
        '61-90': {'count': 0, 'amount': 0},
        '91-180': {'count': 0, 'amount': 0},
        '180+': {'count': 0, 'amount': 0},
    }
    total_outstanding = 0

    for inv in outstanding_q:
        amt = float(inv.outstanding_amount or 0)
        total_outstanding += amt
        if inv.due_date:
            # Not-yet-due invoices are treated as current for aging visibility.
            days = max((today - inv.due_date).days, 0)
        else:
            days = 0

        if days <= 30:
            buckets['0-30']['count'] += 1
            buckets['0-30']['amount'] += amt
        elif days <= 60:
            buckets['31-60']['count'] += 1
            buckets['31-60']['amount'] += amt
        elif days <= 90:
            buckets['61-90']['count'] += 1
            buckets['61-90']['amount'] += amt
        elif days <= 180:
            buckets['91-180']['count'] += 1
            buckets['91-180']['amount'] += amt
        else:
            buckets['180+']['count'] += 1
            buckets['180+']['amount'] += amt

    aging_table = []
    for label, data in buckets.items():
        pct = round(data['amount'] / total_outstanding * 100, 1) if total_outstanding > 0 else 0
        aging_table.append({
            'bucket': label,
            'count': data['count'],
            'amount': round(data['amount'], 2),
            'percentage': pct,
        })

    # D2 — Top Overdue Customers
    overdue_outstanding_sq = db.session.query(
        Invoice.id.label('invoice_id'),
        Invoice.customer_id.label('customer_id'),
        Invoice.due_date.label('due_date'),
        outstanding_expr.label('outstanding_amount'),
    ).outerjoin(
        paid_sq,
        Invoice.id == paid_sq.c.invoice_id,
    ).filter(
        Invoice.company_id == company_id,
        Invoice.due_date < today,
        Invoice.is_active == True,
        outstanding_expr > 0,
    )
    if invoice_status:
        overdue_outstanding_sq = overdue_outstanding_sq.filter(Invoice.status == invoice_status)
    else:
        overdue_outstanding_sq = overdue_outstanding_sq.filter(Invoice.status.in_(default_aging_statuses))
    overdue_outstanding_sq = overdue_outstanding_sq.subquery()

    overdue_q = db.session.query(
        Customer.id,
        Customer.first_name,
        Customer.last_name,
        Customer.internet_id,
        Area.name.label('area'),
        User.first_name.label('tech_first'),
        User.last_name.label('tech_last'),
        func.count(overdue_outstanding_sq.c.invoice_id).label('invoice_count'),
        func.coalesce(func.sum(overdue_outstanding_sq.c.outstanding_amount), 0).label('total_outstanding'),
        func.min(overdue_outstanding_sq.c.due_date).label('oldest_due'),
    ).join(Customer, overdue_outstanding_sq.c.customer_id == Customer.id
    ).join(Area, Customer.area_id == Area.id
    ).outerjoin(User, Customer.technician_id == User.id
    ).group_by(
        Customer.id, Customer.first_name, Customer.last_name, Customer.internet_id,
        Area.name, User.first_name, User.last_name,
    ).order_by(desc('total_outstanding')).limit(20)

    top_overdue = [{
        'customer_name': f"{r.first_name} {r.last_name}",
        'internet_id': r.internet_id,
        'area': r.area,
        'technician': f"{r.tech_first} {r.tech_last}" if r.tech_first else 'Unassigned',
        'invoice_count': r.invoice_count,
        'total_outstanding': round(float(r.total_outstanding), 2),
        'oldest_due_date': r.oldest_due.strftime('%Y-%m-%d') if r.oldest_due else None,
        'days_overdue': (today - r.oldest_due).days if r.oldest_due else 0,
    } for r in overdue_q.all()]

    # D3 — Recovery Performance
    # Status counts
    recovery_status_q = db.session.query(
        RecoveryTask.status,
        func.count(RecoveryTask.id),
    ).filter(
        RecoveryTask.company_id == company_id,
        RecoveryTask.created_at >= start,
        RecoveryTask.created_at <= end,
    ).group_by(RecoveryTask.status)
    status_map = {r[0]: r[1] for r in recovery_status_q.all()}

    # Completed this month
    completed_mtd = db.session.query(func.count(RecoveryTask.id)).filter(
        RecoveryTask.company_id == company_id,
        RecoveryTask.status == 'completed',
        RecoveryTask.completed_at >= start,
        RecoveryTask.completed_at <= end,
    ).scalar() or 0

    # Per-agent table
    agent_q = db.session.query(
        User.id,
        User.first_name,
        User.last_name,
        func.count(RecoveryTask.id).label('total'),
        func.count(case((RecoveryTask.status == 'completed', 1))).label('completed'),
        func.count(case((RecoveryTask.status == 'pending', 1))).label('pending'),
        func.count(case((RecoveryTask.status == 'in_progress', 1))).label('in_progress'),
    ).join(User, RecoveryTask.assigned_to == User.id).filter(
        RecoveryTask.company_id == company_id,
        RecoveryTask.created_at >= start,
        RecoveryTask.created_at <= end,
    ).group_by(User.id, User.first_name, User.last_name).order_by(desc('completed'))

    recovery_agents = [{
        'name': f"{r.first_name} {r.last_name}",
        'total': r.total,
        'completed': r.completed,
        'pending': r.pending,
        'in_progress': r.in_progress,
        'completion_rate': round(r.completed / r.total * 100, 1) if r.total > 0 else 0,
    } for r in agent_q.all()]

    return {
        'aging_table': aging_table,
        'total_outstanding': round(total_outstanding, 2),
        'top_overdue_customers': top_overdue,
        'recovery': {
            'pending': status_map.get('pending', 0),
            'in_progress': status_map.get('in_progress', 0),
            'completed_mtd': completed_mtd,
            'agents': recovery_agents,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# Section E — Bank Positions
# ══════════════════════════════════════════════════════════════════════════════

def get_bank_positions(company_id, start, end, invoice_status=None, bank_account_id=None, payment_method=None, isp_payment_type=None):
    """E1: Per-account cards. E2: Full table data. E3: Payment method donut."""

    bank_accounts_q = BankAccount.query.filter_by(company_id=company_id, is_active=True)
    if bank_account_id:
        bank_accounts_q = bank_accounts_q.filter(BankAccount.id == bank_account_id)
    bank_accounts = bank_accounts_q.order_by(BankAccount.bank_name).all()

    bank_ids = [a.id for a in bank_accounts]
    if not bank_ids:
        return {
            'accounts': [],
            'bank_table': [],
            'cash_payments': {
                'collections': 0,
                'extra_income': 0,
                'isp_payments': 0,
                'expenses': 0,
                'net_flow': 0,
            },
            'payment_method_donut': [],
            'cash_flow_bridge': {
                'scope': 'bank_accounts_only',
                'opening_balance': 0,
                'collections_in': 0,
                'extra_income_in': 0,
                'isp_payments_out': 0,
                'operating_expenses_out': 0,
                'salary_commission_out': 0,
                'closing_balance': 0,
                'calculated_closing_balance': 0,
                'reconciliation_delta': 0,
            },
        }

    # Bulk queries — collections per account for period
    collections_q = db.session.query(
        Payment.bank_account_id,
        func.coalesce(func.sum(_signed_payment_amount()), 0),
    ).join(Invoice, Payment.invoice_id == Invoice.id).filter(
        Payment.bank_account_id.in_(bank_ids),
        Payment.is_active == True, Payment.status == 'paid',
        Payment.payment_date >= start, Payment.payment_date <= end,
    ).group_by(Payment.bank_account_id)
    if invoice_status:
        collections_q = collections_q.filter(Invoice.status == invoice_status)
    if payment_method:
        collections_q = collections_q.filter(Payment.payment_method == payment_method)
    collections_map = dict(collections_q.all())

    # Extra income per account
    extra_q = db.session.query(
        ExtraIncome.bank_account_id,
        func.coalesce(func.sum(ExtraIncome.amount), 0),
    ).filter(
        ExtraIncome.bank_account_id.in_(bank_ids),
        ExtraIncome.is_active == True,
        ExtraIncome.income_date >= start, ExtraIncome.income_date <= end,
    )
    if payment_method:
        extra_q = extra_q.filter(ExtraIncome.payment_method == payment_method)
    extra_q = extra_q.group_by(ExtraIncome.bank_account_id)
    extra_map = dict(extra_q.all())

    # ISP payments per account
    isp_q = db.session.query(
        ISPPayment.bank_account_id,
        func.coalesce(func.sum(ISPPayment.amount), 0),
    ).filter(
        ISPPayment.bank_account_id.in_(bank_ids),
        ISPPayment.is_active == True,
        ISPPayment.payment_date >= start, ISPPayment.payment_date <= end,
    )
    if payment_method:
        isp_q = isp_q.filter(ISPPayment.payment_method == payment_method)
    if isp_payment_type:
        isp_q = isp_q.filter(ISPPayment.payment_type == isp_payment_type)
    isp_q = isp_q.group_by(ISPPayment.bank_account_id)
    isp_map = dict(isp_q.all())

    # Expenses per account
    exp_q = db.session.query(
        Expense.bank_account_id,
        func.coalesce(func.sum(Expense.amount), 0),
    ).filter(
        Expense.bank_account_id.in_(bank_ids),
        Expense.is_active == True,
        Expense.expense_date >= start, Expense.expense_date <= end,
    )
    if payment_method:
        exp_q = exp_q.filter(Expense.payment_method == payment_method)
    exp_q = exp_q.group_by(Expense.bank_account_id)
    exp_map = dict(exp_q.all())

    # Expense split for bridge (to avoid salary double-counting in outflows)
    operating_expenses_q = db.session.query(
        func.coalesce(func.sum(Expense.amount), 0)
    ).join(ExpenseType, Expense.expense_type_id == ExpenseType.id).filter(
        Expense.company_id == company_id,
        Expense.bank_account_id.in_(bank_ids),
        Expense.is_active == True,
        Expense.expense_date >= start,
        Expense.expense_date <= end,
        ExpenseType.is_employee_payment == False,
    )
    if payment_method:
        operating_expenses_q = operating_expenses_q.filter(Expense.payment_method == payment_method)
    operating_expenses_total = float(operating_expenses_q.scalar() or 0)

    salary_commission_q = db.session.query(
        func.coalesce(func.sum(Expense.amount), 0)
    ).join(ExpenseType, Expense.expense_type_id == ExpenseType.id).filter(
        Expense.company_id == company_id,
        Expense.bank_account_id.in_(bank_ids),
        Expense.is_active == True,
        Expense.expense_date >= start,
        Expense.expense_date <= end,
        ExpenseType.is_employee_payment == True,
    )
    if payment_method:
        salary_commission_q = salary_commission_q.filter(Expense.payment_method == payment_method)
    salary_commission_total = float(salary_commission_q.scalar() or 0)

    # Build per-account data
    accounts = []
    bank_table = []
    for acc in bank_accounts:
        coll = float(collections_map.get(acc.id, 0) or 0)
        extra = float(extra_map.get(acc.id, 0) or 0)
        isp_out = float(isp_map.get(acc.id, 0) or 0)
        exp_out = float(exp_map.get(acc.id, 0) or 0)
        total_in = coll + extra
        total_out = isp_out + exp_out
        net = total_in - total_out

        card = {
            'id': str(acc.id),
            'name': acc.bank_name,
            'account_number': acc.account_number,
            'balance': round(float(acc.current_balance or 0), 2),
            'total_in': round(total_in, 2),
            'total_out': round(total_out, 2),
            'net_movement': round(net, 2),
        }
        accounts.append(card)

        # Bank table row (matches BankPerformance.tsx structure)
        bank_table.append({
            'bank_name': acc.bank_name,
            'account_number': acc.account_number,
            'collections': round(coll, 2),
            'extra_income': round(extra, 2),
            'isp_payments': round(isp_out, 2),
            'expenses': round(exp_out, 2),
            'net_flow': round(net, 2),
            'initial_balance': round(float(acc.initial_balance or 0), 2),
            'current_balance': round(float(acc.current_balance or 0), 2),
        })

    # Cash payments (no bank_account_id)
    cash_coll_q = db.session.query(
        func.coalesce(func.sum(_signed_payment_amount()), 0)
    ).join(Invoice, Payment.invoice_id == Invoice.id).filter(
        Payment.company_id == company_id,
        Payment.bank_account_id == None,
        Payment.is_active == True, Payment.status == 'paid',
        Payment.payment_date >= start, Payment.payment_date <= end,
    )
    if invoice_status:
        cash_coll_q = cash_coll_q.filter(Invoice.status == invoice_status)
    if payment_method:
        cash_coll_q = cash_coll_q.filter(Payment.payment_method == payment_method)
    cash_coll = float(cash_coll_q.scalar() or 0)

    cash_extra_q = db.session.query(
        func.coalesce(func.sum(ExtraIncome.amount), 0)
    ).filter(
        ExtraIncome.company_id == company_id,
        ExtraIncome.bank_account_id == None,
        ExtraIncome.is_active == True,
        ExtraIncome.income_date >= start, ExtraIncome.income_date <= end,
    )
    if payment_method:
        cash_extra_q = cash_extra_q.filter(ExtraIncome.payment_method == payment_method)
    cash_extra = float(cash_extra_q.scalar() or 0)

    cash_isp_q = db.session.query(
        func.coalesce(func.sum(ISPPayment.amount), 0)
    ).filter(
        ISPPayment.company_id == company_id,
        ISPPayment.bank_account_id == None,
        ISPPayment.is_active == True,
        ISPPayment.payment_date >= start, ISPPayment.payment_date <= end,
    )
    if payment_method:
        cash_isp_q = cash_isp_q.filter(ISPPayment.payment_method == payment_method)
    if isp_payment_type:
        cash_isp_q = cash_isp_q.filter(ISPPayment.payment_type == isp_payment_type)
    cash_isp = float(cash_isp_q.scalar() or 0)

    cash_exp_q = db.session.query(
        func.coalesce(func.sum(Expense.amount), 0)
    ).filter(
        Expense.company_id == company_id,
        Expense.bank_account_id == None,
        Expense.is_active == True,
        Expense.expense_date >= start, Expense.expense_date <= end,
    )
    if payment_method:
        cash_exp_q = cash_exp_q.filter(Expense.payment_method == payment_method)
    cash_exp = float(cash_exp_q.scalar() or 0)

    cash_payments = {
        'collections': round(cash_coll, 2),
        'extra_income': round(cash_extra, 2),
        'isp_payments': round(cash_isp, 2),
        'expenses': round(cash_exp, 2),
        'net_flow': round((cash_coll + cash_extra) - (cash_isp + cash_exp), 2),
    }

    # E3 — Payment Method Distribution (donut)
    method_q = db.session.query(
        Payment.payment_method,
        func.coalesce(func.sum(_signed_payment_amount()), 0).label('amount'),
    ).join(Invoice, Payment.invoice_id == Invoice.id).filter(
        Payment.company_id == company_id,
        Payment.is_active == True, Payment.status == 'paid',
        Payment.payment_date >= start, Payment.payment_date <= end,
    ).group_by(Payment.payment_method).order_by(desc('amount'))
    if invoice_status:
        method_q = method_q.filter(Invoice.status == invoice_status)
    if bank_account_id:
        method_q = method_q.filter(Payment.bank_account_id == bank_account_id)
    if payment_method:
        method_q = method_q.filter(Payment.payment_method == payment_method)

    method_results = method_q.all()
    total_methods = sum(float(r.amount) for r in method_results)

    payment_method_donut = [{
        'method': r.payment_method or 'Unknown',
        'amount': round(float(r.amount), 2),
        'percentage': round(float(r.amount) / total_methods * 100, 1) if total_methods > 0 else 0,
    } for r in method_results]

    collections_in = float(sum(float(v or 0) for v in collections_map.values()))
    extra_income_in = float(sum(float(v or 0) for v in extra_map.values()))
    isp_out = float(sum(float(v or 0) for v in isp_map.values()))
    closing_balance = float(sum(float(a.current_balance or 0) for a in bank_accounts))

    calculated_closing = (
        (closing_balance - ((collections_in + extra_income_in) - (isp_out + operating_expenses_total + salary_commission_total)))
        + collections_in
        + extra_income_in
        - isp_out
        - operating_expenses_total
        - salary_commission_total
    )
    opening_balance = closing_balance - (
        (collections_in + extra_income_in) - (isp_out + operating_expenses_total + salary_commission_total)
    )
    reconciliation_delta = round(closing_balance - calculated_closing, 2)

    return {
        'accounts': accounts,
        'bank_table': bank_table,
        'cash_payments': cash_payments,
        'payment_method_donut': payment_method_donut,
        'cash_flow_bridge': {
            'scope': 'bank_accounts_only',
            'opening_balance': round(opening_balance, 2),
            'collections_in': round(collections_in, 2),
            'extra_income_in': round(extra_income_in, 2),
            'isp_payments_out': round(isp_out, 2),
            'operating_expenses_out': round(operating_expenses_total, 2),
            'salary_commission_out': round(salary_commission_total, 2),
            'closing_balance': round(closing_balance, 2),
            'calculated_closing_balance': round(calculated_closing, 2),
            'reconciliation_delta': reconciliation_delta,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# Section F — Employee Financial Summary
# ══════════════════════════════════════════════════════════════════════════════

def get_employee_financial_summary(company_id, start, end):
    """Salary/commission stats + per-employee breakdown."""

    # Salary accrued MTD
    salary_accrued = float(db.session.query(
        func.coalesce(func.sum(EmployeeLedger.amount), 0)
    ).join(User, EmployeeLedger.employee_id == User.id).filter(
        EmployeeLedger.company_id == company_id,
        EmployeeLedger.transaction_type == 'salary_accrual',
        EmployeeLedger.created_at >= start,
        EmployeeLedger.created_at <= end,
    ).scalar() or 0)

    # Payout splits — strict typed categories only.
    payout_split = db.session.query(
        func.coalesce(func.sum(func.abs(case(
            (EmployeeLedger.transaction_type == 'salary_payout', EmployeeLedger.amount),
            else_=0,
        ))), 0).label('salary_paid'),
        func.coalesce(func.sum(func.abs(case(
            (EmployeeLedger.transaction_type == 'commission_payout', EmployeeLedger.amount),
            else_=0,
        ))), 0).label('commission_paid'),
        func.coalesce(func.sum(func.abs(case(
            (EmployeeLedger.transaction_type == 'payout', EmployeeLedger.amount),
            (EmployeeLedger.transaction_type == 'salary_payout', EmployeeLedger.amount),
            (EmployeeLedger.transaction_type == 'commission_payout', EmployeeLedger.amount),
            else_=0,
        ))), 0).label('total_payout'),
    ).filter(
        EmployeeLedger.company_id == company_id,
        EmployeeLedger.created_at >= start,
        EmployeeLedger.created_at <= end,
    ).one()

    salary_paid = float(payout_split.salary_paid or 0)
    commission_paid = float(payout_split.commission_paid or 0)

    # Commission earned MTD
    commission_earned = float(db.session.query(
        func.coalesce(func.sum(EmployeeLedger.amount), 0)
    ).join(User, EmployeeLedger.employee_id == User.id).filter(
        EmployeeLedger.company_id == company_id,
        EmployeeLedger.transaction_type.in_(['connection_commission', 'complaint_commission']),
        EmployeeLedger.created_at >= start,
        EmployeeLedger.created_at <= end,
    ).scalar() or 0)

    # Outstanding balance = total owed to all employees
    outstanding = float(db.session.query(
        func.coalesce(func.sum(User.current_balance), 0)
    ).filter(
        User.company_id == company_id,
        User.is_active == True,
        User.role != 'company_owner',
    ).scalar() or 0)

    # Per-employee table
    # Get commission earned in period via subquery
    emp_commission_sq = db.session.query(
        EmployeeLedger.employee_id,
        func.coalesce(func.sum(EmployeeLedger.amount), 0).label('commission_earned'),
    ).filter(
        EmployeeLedger.company_id == company_id,
        EmployeeLedger.transaction_type.in_(['connection_commission', 'complaint_commission']),
        EmployeeLedger.created_at >= start,
        EmployeeLedger.created_at <= end,
    ).group_by(EmployeeLedger.employee_id).subquery()

    # Payout by employee in period
    emp_paid_sq = db.session.query(
        EmployeeLedger.employee_id,
        func.coalesce(func.sum(func.abs(EmployeeLedger.amount)), 0).label('paid_period'),
        func.coalesce(func.sum(func.abs(case(
            (EmployeeLedger.transaction_type == 'salary_payout', EmployeeLedger.amount),
            else_=0,
        ))), 0).label('salary_paid_period'),
        func.coalesce(func.sum(func.abs(case(
            (EmployeeLedger.transaction_type == 'commission_payout', EmployeeLedger.amount),
            else_=0,
        ))), 0).label('commission_paid_period'),
    ).filter(
        EmployeeLedger.company_id == company_id,
        EmployeeLedger.transaction_type.in_(['payout', 'salary_payout', 'commission_payout']),
        EmployeeLedger.created_at >= start,
        EmployeeLedger.created_at <= end,
    ).group_by(EmployeeLedger.employee_id).subquery()

    employees_q = db.session.query(
        User.id,
        User.first_name,
        User.last_name,
        User.role,
        User.salary,
        User.current_balance,
        User.paid_amount,
        emp_commission_sq.c.commission_earned,
        emp_paid_sq.c.paid_period,
        emp_paid_sq.c.salary_paid_period,
        emp_paid_sq.c.commission_paid_period,
    ).outerjoin(emp_commission_sq, User.id == emp_commission_sq.c.employee_id
    ).outerjoin(emp_paid_sq, User.id == emp_paid_sq.c.employee_id
    ).filter(
        User.company_id == company_id,
        User.is_active == True,
        User.role != 'company_owner',
    ).order_by(User.first_name)

    employees = [{
        'name': f"{r.first_name} {r.last_name}",
        'role': r.role,
        'salary': round(float(r.salary or 0), 2),
        'commission_earned': round(float(r.commission_earned or 0), 2),
        'paid_this_period': round(float(r.paid_period or 0), 2),
        'salary_paid_this_period': round(float(r.salary_paid_period or 0), 2),
        'commission_paid_this_period': round(float(r.commission_paid_period or 0), 2),
        'total_paid': round(float(r.paid_amount or 0), 2),
        'balance_owed': round(float(r.current_balance or 0), 2),
    } for r in employees_q.all()]

    return {
        'stats': {
            'salary_accrued': round(salary_accrued, 2),
            'salary_paid': round(salary_paid, 2),
            'commission_earned': round(commission_earned, 2),
            'commission_paid': round(commission_paid, 2),
            'outstanding_balance': round(outstanding, 2),
        },
        'employees': employees,
    }