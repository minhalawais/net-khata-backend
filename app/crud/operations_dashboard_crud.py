"""
Operations & Network Dashboard CRUD

5 Sections: Customer & Service Health, Complaint & SLA Performance,
Field Tasks & Operations, Inventory Status, Technician Performance.

All fixes applied:
  — datetime.now(PKT) everywhere — no naive datetimes (was TypeError at runtime)
  — SQL-native avg(extract epoch) — no Python loops for timing arithmetic
  — N+1 eliminated in get_complaint_sla_performance:
      * technician resolution time merged into GROUP BY aggregation
      * open_complaints assigned-user names via single bulk query
  — MRR = subscriber_count × unit_price (was incorrectly sum(price) = 1 row)
  — Trends added for open_today and sla_breaches KPIs (were hardcoded 0)
  — SLA countdown (sla_seconds_remaining) added per open complaint
  — Inventory recent_assignments scoped to period (was all-time)
  — Complaint rate normalized by area customer count added
"""

from app import db
from app.models import (
    Customer, Area, SubZone, ISP, ServicePlan, CustomerPackage,
    Complaint, Task, TaskAssignee, User,
    InventoryItem, InventoryAssignment, InventoryTransaction, Supplier,
    EmployeeLedger, Invoice
)
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import func, case, and_, desc
from sqlalchemy.exc import SQLAlchemyError
from pytz import timezone
import logging
import traceback

logger = logging.getLogger(__name__)
PKT = timezone('Asia/Karachi')
LOW_STOCK_THRESHOLD = 10


# ─── Helpers ────────────────────────────────────────────────────────────────

def _parse_dates(start_str, end_str):
    """
    Parse date strings to PKT-aware datetime objects.

    CRITICAL: Never use datetime.replace(tzinfo=pytz_tz).
    pytz timezone objects must ONLY be used via PKT.localize(dt).
    Using .replace(tzinfo=PKT) attaches the timezone in its internal
    "LMT initial state" where utcoffset() can return None — Python's
    datetime.__sub__ then raises "can't subtract offset-naive and
    offset-aware datetimes" even though tzinfo is technically set.
    PKT.localize() correctly looks up the transition table and sets
    the proper +05:00 offset.
    """
    today = datetime.now(PKT)
    try:
        if start_str:
            start = PKT.localize(
                datetime.strptime(start_str, '%Y-%m-%d')
                .replace(hour=0, minute=0, second=0, microsecond=0)
            )
        else:
            start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        if end_str:
            end = PKT.localize(
                datetime.strptime(end_str, '%Y-%m-%d')
                .replace(hour=23, minute=59, second=59, microsecond=0)
            )
        else:
            end = today
    except Exception:
        start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = today
    return start, end


def _prev_period(start, end):
    days = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days - 1)
    return prev_start, prev_end


def _trend(cur, prev):
    if prev == 0:
        return 100.0 if cur > 0 else 0.0
    return round(((cur - prev) / abs(prev)) * 100, 1)


def _twelve_months_ago():
    today = datetime.now(PKT)
    return today.replace(day=1, hour=0, minute=0, second=0, microsecond=0) - relativedelta(months=11)


def _month_keys():
    today = datetime.now(PKT)
    return [(today - relativedelta(months=i)).strftime('%Y-%m') for i in range(11, -1, -1)]


def _aware(dt):
    """
    Make a datetime timezone-aware (PKT) if it isn't already.

    Why this is needed:
    SQLAlchemy can return naive datetimes when the Postgres column is
    TIMESTAMP WITHOUT TIME ZONE (no tz info stored). If the column was
    declared with timezone=False or was created without tz, ORM rows come
    back as naive. Subtracting a naive DB datetime from a PKT-aware `now`
    raises: "can't subtract offset-naive and offset-aware datetimes".

    This helper localizes any naive datetime to PKT before arithmetic.
    If the datetime is already aware it is returned unchanged.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return PKT.localize(dt)
    return dt


# ─── Main Orchestrator ──────────────────────────────────────────────────────

def get_operations_dashboard_data(company_id, filters=None):
    """Build the full Operations & Network dashboard payload."""
    if not company_id:
        return {'error': 'Company ID required'}

    filters = filters or {}
    start_date, end_date = _parse_dates(filters.get('start_date'), filters.get('end_date'))
    prev_start, prev_end = _prev_period(start_date, end_date)

    # Each section is wrapped individually so a crash in one section
    # is logged with a full traceback and returns an empty fallback,
    # instead of crashing the entire dashboard silently.
    def _run(name, fn, *args):
        try:
            return fn(*args)
        except Exception as e:
            logger.error(
                f"[operations_dashboard] section '{name}' failed: {e}\n"
                + traceback.format_exc()
            )
            return {}

    try:
        return {
            'customer_health':        _run('customer_health',        get_customer_service_health,   company_id, start_date, end_date, prev_start, prev_end),
            'complaint_sla':          _run('complaint_sla',          get_complaint_sla_performance, company_id, start_date, end_date, prev_start, prev_end),
            'field_tasks':            _run('field_tasks',            get_field_tasks,               company_id, start_date, end_date),
            'inventory':              _run('inventory',              get_inventory_status,          company_id, start_date, end_date),
            'technician_performance': _run('technician_performance', get_technician_performance,    company_id, start_date, end_date),
            'period': {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date':   end_date.strftime('%Y-%m-%d'),
            },
        }
    except SQLAlchemyError as e:
        logger.error(f"DB error in operations dashboard:\n{traceback.format_exc()}")
        return {'error': 'Database error'}
    except Exception as e:
        logger.error(f"Error in operations dashboard:\n{traceback.format_exc()}")
        return {'error': str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# Section A — Customer & Service Health
# ══════════════════════════════════════════════════════════════════════════════

def get_customer_service_health(company_id, start, end, prev_start, prev_end):
    """4 KPIs + 3 charts: area distribution, plan adoption, churn by area."""

    # KPI 1: Total Active Customers
    active_count = Customer.query.filter_by(company_id=company_id, is_active=True).count()
    prev_active = Customer.query.filter(
        Customer.company_id == company_id,
        Customer.is_active == True,
        Customer.created_at <= prev_end,
    ).count()

    # KPI 2: Customers Added This Period
    added_mtd = Customer.query.filter(
        Customer.company_id == company_id,
        Customer.installation_date >= start.date(),
        Customer.installation_date <= end.date(),
    ).count()
    prev_added = Customer.query.filter(
        Customer.company_id == company_id,
        Customer.installation_date >= prev_start.date(),
        Customer.installation_date <= prev_end.date(),
    ).count()

    # KPI 3: Connection Type Distribution
    conn_type_q = db.session.query(
        Customer.connection_type,
        func.count(Customer.id),
    ).filter(
        Customer.company_id == company_id,
        Customer.is_active == True,
    ).group_by(Customer.connection_type)
    connection_types = {r[0] or 'Unknown': r[1] for r in conn_type_q.all()}

    # KPI 4: Customers Per ISP
    isp_q = db.session.query(
        ISP.name,
        func.count(Customer.id),
    ).join(Customer, Customer.isp_id == ISP.id).filter(
        Customer.company_id == company_id,
        Customer.is_active == True,
    ).group_by(ISP.name)
    customers_per_isp = [{'isp': r[0], 'count': r[1]} for r in isp_q.all()]

    # Chart A1: Customer Distribution by Area
    area_q = db.session.query(
        Area.id,
        Area.name,
        func.count(Customer.id).label('count'),
    ).join(Customer, Customer.area_id == Area.id).filter(
        Customer.company_id == company_id,
        Customer.is_active == True,
    ).group_by(Area.id, Area.name).order_by(desc('count')).limit(8)
    area_distribution = [{'area': r.name, 'area_id': str(r.id), 'count': r.count} for r in area_q.all()]

    # Chart A2: Service Plan Adoption
    # Fix: MRR = subscriber_count × unit_price
    # Was: func.sum(ServicePlan.price) which returns 1 price row, not count×price
    plan_q = db.session.query(
        ServicePlan.name,
        func.count(CustomerPackage.id).label('subscribers'),
        func.max(ServicePlan.price).label('unit_price'),
    ).join(CustomerPackage, CustomerPackage.service_plan_id == ServicePlan.id).filter(
        ServicePlan.company_id == company_id,
        CustomerPackage.is_active == True,
    ).group_by(ServicePlan.name).order_by(desc('subscribers')).limit(5)
    plan_adoption = [{
        'plan':            r.name,
        'subscribers':     r.subscribers,
        'monthly_revenue': round(float(r.unit_price or 0) * r.subscribers, 2),
    } for r in plan_q.all()]

    # Chart A3: Churn by Area
    # Note: uses updated_at as proxy — add deactivated_at to Customer model for accuracy
    churn_q = db.session.query(
        Area.name,
        func.count(Customer.id).label('total'),
        func.count(case((Customer.is_active == True, 1))).label('active'),
        func.count(case((Customer.is_active == False, 1))).label('inactive'),
        func.count(case((
            and_(Customer.is_active == False, Customer.updated_at >= start, Customer.updated_at <= end),
            1
        ))).label('churned_mtd'),
    ).join(Customer, Customer.area_id == Area.id).filter(
        Area.company_id == company_id,
    ).group_by(Area.name).order_by(desc('churned_mtd'))

    churn_by_area = []
    for r in churn_q.all():
        churn_pct = round(r.churned_mtd / r.total * 100, 1) if r.total > 0 else 0
        churn_by_area.append({
            'area':        r.name,
            'total':       r.total,
            'active':      r.active,
            'inactive':    r.inactive,
            'churned_mtd': r.churned_mtd,
            'churn_pct':   churn_pct,
        })

    return {
        'kpis': {
            'active_customers': {
                'value':       active_count,
                'previous':    prev_active,
                'trend':       _trend(active_count, prev_active),
                'is_positive': active_count >= prev_active,
            },
            'added_mtd': {
                'value':       added_mtd,
                'previous':    prev_added,
                'trend':       _trend(added_mtd, prev_added),
                'is_positive': added_mtd >= prev_added,
            },
            'connection_types':  connection_types,
            'customers_per_isp': customers_per_isp,
        },
        'area_distribution': area_distribution,
        'plan_adoption':     plan_adoption,
        'churn_by_area':     churn_by_area,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Section B — Complaint & SLA Performance
# ══════════════════════════════════════════════════════════════════════════════

def get_complaint_sla_performance(company_id, start, end, prev_start, prev_end):
    """
    4 KPIs + complaint volume chart + by-area chart (with normalized rate)
    + technician table + open complaints queue with SLA countdown.

    Performance: all timing via SQL extract('epoch'), no Python loops.
    N+1 fixes:
      * Technician avg resolution time merged into GROUP BY aggregation.
      * Open complaints assigned-user names via single bulk IN query.
    """
    now = datetime.now(PKT)

    # ── KPI 1: Open complaints — live count + previous period trend ───────────
    open_today = Complaint.query.join(Customer).filter(
        Customer.company_id == company_id,
        Complaint.status.in_(['open', 'in_progress']),
        Complaint.is_active == True,
    ).count()

    prev_open = Complaint.query.join(Customer).filter(
        Customer.company_id == company_id,
        Complaint.status.in_(['open', 'in_progress']),
        Complaint.is_active == True,
        Complaint.created_at <= prev_end,
    ).count()

    # ── KPI 2: Avg resolution time — SQL avg(epoch diff), zero Python ─────────
    def _avg_res_hours(sd, ed):
        secs = db.session.query(
            func.avg(
                func.extract('epoch', Complaint.resolved_at - Complaint.created_at)
            )
        ).join(Customer).filter(
            Customer.company_id == company_id,
            Complaint.status == 'resolved',
            Complaint.resolved_at.isnot(None),
            Complaint.created_at >= sd,
            Complaint.created_at <= ed,
            Complaint.is_active == True,
        ).scalar()
        return round(float(secs or 0) / 3600, 1)

    avg_resolution = _avg_res_hours(start, end)
    prev_avg_res   = _avg_res_hours(prev_start, prev_end)

    # ── KPI 3: SLA breaches — live + prev period trend ────────────────────────
    sla_breach = Complaint.query.join(Customer).filter(
        Customer.company_id == company_id,
        Complaint.response_due_date < now,
        Complaint.status.in_(['open', 'in_progress']),
        Complaint.is_active == True,
    ).count()

    prev_sla_breach = Complaint.query.join(Customer).filter(
        Customer.company_id == company_id,
        Complaint.response_due_date < prev_end,
        Complaint.status.in_(['open', 'in_progress']),
        Complaint.is_active == True,
        Complaint.created_at <= prev_end,
    ).count()

    # ── KPI 4: Avg satisfaction rating ────────────────────────────────────────
    avg_satisfaction = round(float(db.session.query(
        func.avg(Complaint.satisfaction_rating)
    ).join(Customer).filter(
        Customer.company_id == company_id,
        Complaint.satisfaction_rating.isnot(None),
        Complaint.created_at >= start,
        Complaint.created_at <= end,
        Complaint.is_active == True,
    ).scalar() or 0), 1)

    prev_sat = round(float(db.session.query(
        func.avg(Complaint.satisfaction_rating)
    ).join(Customer).filter(
        Customer.company_id == company_id,
        Complaint.satisfaction_rating.isnot(None),
        Complaint.created_at >= prev_start,
        Complaint.created_at <= prev_end,
        Complaint.is_active == True,
    ).scalar() or 0), 1)

    # ── Chart B1: Complaint Volume by Month (12-month stacked bar) ────────────
    months     = _month_keys()
    twelve_ago = _twelve_months_ago()

    monthly_q = db.session.query(
        func.to_char(Complaint.created_at, 'YYYY-MM').label('month'),
        Complaint.status,
        func.count(Complaint.id).label('count'),
    ).join(Customer).filter(
        Customer.company_id == company_id,
        Complaint.is_active == True,
        Complaint.created_at >= twelve_ago,
    ).group_by('month', Complaint.status).order_by('month')

    month_status_map = {}
    all_statuses = set()
    for r in monthly_q.all():
        all_statuses.add(r.status)
        month_status_map.setdefault(r.month, {})[r.status] = r.count

    complaint_volume = []
    for mk in months:
        dt = datetime.strptime(mk, '%Y-%m')
        entry = {'month': dt.strftime('%b %Y'), 'month_short': dt.strftime('%b')}
        for s in sorted(all_statuses):
            entry[s] = month_status_map.get(mk, {}).get(s, 0)
        complaint_volume.append(entry)

    # ── Chart B2: Complaints by Area — raw count + normalized rate ────────────
    # Normalize by active customer count so dense areas don't dominate unfairly
    area_customer_q = db.session.query(
        Area.name,
        func.count(Customer.id).label('customer_count'),
    ).join(Customer, Customer.area_id == Area.id).filter(
        Area.company_id == company_id,
        Customer.is_active == True,
    ).group_by(Area.name)
    cust_count_map = {r.name: r.customer_count for r in area_customer_q.all()}

    area_complaint_q = db.session.query(
        Area.name,
        func.count(Complaint.id).label('count'),
    ).join(Customer, Customer.area_id == Area.id
    ).join(Complaint, Complaint.customer_id == Customer.id
    ).filter(
        Area.company_id == company_id,
        Complaint.created_at >= start,
        Complaint.created_at <= end,
        Complaint.is_active == True,
    ).group_by(Area.name).order_by(desc('count')).limit(15)

    complaints_by_area = []
    for r in area_complaint_q.all():
        cust = cust_count_map.get(r.name, 0)
        rate = round(r.count / cust * 100, 1) if cust > 0 else 0
        complaints_by_area.append({'area': r.name, 'count': r.count, 'rate': rate})

    # ── Table B3: Technician complaint performance — SQL avg, zero N+1 ────────
    # Fix: was a loop of per-technician queries for resolution time.
    # Now: avg resolution seconds computed inline in the GROUP BY aggregation.
    tech_q = db.session.query(
        User.id,
        User.first_name,
        User.last_name,
        func.count(Complaint.id).label('assigned'),
        func.count(case((Complaint.status == 'resolved', 1))).label('resolved'),
        func.avg(Complaint.satisfaction_rating).label('avg_csat'),
        func.avg(
            case(
                (
                    and_(
                        Complaint.status == 'resolved',
                        Complaint.resolved_at.isnot(None),
                    ),
                    func.extract('epoch', Complaint.resolved_at - Complaint.created_at),
                ),
                else_=None,
            )
        ).label('avg_res_seconds'),
    ).join(Complaint, Complaint.assigned_to == User.id
    ).join(Customer, Complaint.customer_id == Customer.id
    ).filter(
        Customer.company_id == company_id,
        Complaint.created_at >= start,
        Complaint.created_at <= end,
        Complaint.is_active == True,
    ).group_by(User.id, User.first_name, User.last_name).order_by(desc('resolved'))

    technician_complaints = [{
        'name':                 f"{t.first_name or ''} {t.last_name or ''}".strip() or 'Unknown',
        'assigned':             t.assigned,
        'resolved':             t.resolved,
        'avg_resolution_hours': round(float(t.avg_res_seconds or 0) / 3600, 1),
        'avg_csat':             round(float(t.avg_csat or 0), 1),
    } for t in tech_q.all()]

    # ── Open complaints queue — SLA countdown + bulk user name lookup ─────────
    # Fix: was User.query.get(c.assigned_to) inside a loop = N+1 (1 query/row).
    # Now: fetch all assigned user names in a single IN query, merge in Python.
    open_q = Complaint.query.join(Customer).filter(
        Customer.company_id == company_id,
        Complaint.status.in_(['open', 'in_progress']),
        Complaint.is_active == True,
    ).order_by(
        # Sort: breached first (nullslast pushes no-SLA rows to bottom),
        # then soonest-to-breach, then oldest created
        Complaint.response_due_date.asc().nullslast(),
        Complaint.created_at.asc(),
    ).limit(30).all()

    # Single bulk query for all assigned user names
    assigned_ids = list({c.assigned_to for c in open_q if c.assigned_to})
    user_name_map = {}
    if assigned_ids:
        user_rows = db.session.query(
            User.id, User.first_name, User.last_name
        ).filter(User.id.in_(assigned_ids)).all()
        user_name_map = {
            str(u.id): f"{u.first_name or ''} {u.last_name or ''}".strip()
            for u in user_rows
        }

    open_complaints_list = []
    for c in open_q:
        sla_seconds = None
        if c.response_due_date:
            # _aware() guards: response_due_date may be naive if stored as
            # TIMESTAMP WITHOUT TIME ZONE. Subtraction from PKT-aware `now` crashes.
            sla_seconds = int((_aware(c.response_due_date) - now).total_seconds())

        open_complaints_list.append({
            'id':                    str(c.id),
            'ticket_number':         c.ticket_number,
            'customer_name':         f"{c.customer.first_name or ''} {c.customer.last_name or ''}".strip() if c.customer else 'Unknown',
            'area':                  c.customer.area.name if c.customer and c.customer.area else None,
            'status':                c.status,
            'created_at':            c.created_at.isoformat() if c.created_at else None,
            'sla_due':               c.response_due_date.isoformat() if c.response_due_date else None,
            'sla_seconds_remaining': sla_seconds,  # negative = already breached
            'assigned_to':           user_name_map.get(str(c.assigned_to)) if c.assigned_to else None,
        })

    return {
        'kpis': {
            'open_today':             open_today,
            'open_today_previous':    prev_open,
            'open_today_trend':       _trend(open_today, prev_open),
            'open_today_is_positive': open_today <= prev_open,
            'avg_resolution_hours': {
                'value':       avg_resolution,
                'previous':    prev_avg_res,
                'trend':       _trend(avg_resolution, prev_avg_res),
                'is_positive': avg_resolution <= prev_avg_res,
            },
            'sla_breaches':             sla_breach,
            'sla_breaches_previous':    prev_sla_breach,
            'sla_breaches_trend':       _trend(sla_breach, prev_sla_breach),
            'sla_breaches_is_positive': sla_breach <= prev_sla_breach,
            'avg_satisfaction': {
                'value':       avg_satisfaction,
                'previous':    prev_sat,
                'trend':       round(avg_satisfaction - prev_sat, 1),
                'is_positive': avg_satisfaction >= prev_sat,
            },
        },
        'complaint_volume':      complaint_volume,
        'complaint_statuses':    sorted(all_statuses),
        'complaints_by_area':    complaints_by_area,
        'technician_complaints': technician_complaints,
        'open_complaints':       open_complaints_list,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Section C — Field Tasks & Operations
# ══════════════════════════════════════════════════════════════════════════════

def get_field_tasks(company_id, start, end):
    """4 stat cards + 2 charts + overdue table."""
    now = datetime.now(PKT)

    # 4 Stat Cards (pending/in_progress are live snapshots; completed/overdue are period-scoped)
    pending = Task.query.filter(
        Task.company_id == company_id,
        Task.status == 'pending',
        Task.is_active == True,
    ).count()

    in_progress = Task.query.filter(
        Task.company_id == company_id,
        Task.status == 'in_progress',
        Task.is_active == True,
    ).count()

    completed_mtd = Task.query.filter(
        Task.company_id == company_id,
        Task.status == 'completed',
        Task.completed_at >= start,
        Task.completed_at <= end,
        Task.is_active == True,
    ).count()

    overdue = Task.query.filter(
        Task.company_id == company_id,
        Task.status.in_(['pending', 'in_progress']),
        Task.due_date < now,
        Task.is_active == True,
    ).count()

    # Chart C1: Tasks by Type (donut)
    type_q = db.session.query(
        Task.task_type,
        func.count(Task.id).label('count'),
    ).filter(
        Task.company_id == company_id,
        Task.created_at >= start,
        Task.created_at <= end,
        Task.is_active == True,
    ).group_by(Task.task_type)
    tasks_by_type = [{'type': r.task_type or 'Unknown', 'count': r.count} for r in type_q.all()]

    # Chart C2: Tasks by Technician (bar — completed in period)
    tech_task_q = db.session.query(
        User.id,
        User.first_name,
        User.last_name,
        func.count(Task.id).label('completed'),
    ).join(TaskAssignee, TaskAssignee.employee_id == User.id
    ).join(Task, Task.id == TaskAssignee.task_id
    ).filter(
        Task.company_id == company_id,
        Task.status == 'completed',
        Task.completed_at >= start,
        Task.completed_at <= end,
        Task.is_active == True,
    ).group_by(User.id, User.first_name, User.last_name).order_by(desc('completed')).limit(15)
    tasks_by_technician = [{
        'name':      f"{r.first_name or ''} {r.last_name or ''}".strip() or 'Unknown',
        'completed': r.completed,
    } for r in tech_task_q.all()]

    # Table C3: Overdue Tasks — priority sort (critical first), then oldest due date
    overdue_q = Task.query.filter(
        Task.company_id == company_id,
        Task.status.in_(['pending', 'in_progress']),
        Task.due_date < now,
        Task.is_active == True,
    ).order_by(
        case(
            (Task.priority == 'critical', 0),
            (Task.priority == 'high', 1),
            (Task.priority == 'medium', 2),
            (Task.priority == 'low', 3),
        ),
        Task.due_date.asc(),
    ).limit(30).all()

    overdue_tasks = []
    for t in overdue_q:
        assignee_names = [
            f"{a.employee.first_name or ''} {a.employee.last_name or ''}".strip()
            for a in t.assignees if a.employee
        ]
        customer_name = f"{t.customer.first_name} {t.customer.last_name}" if t.customer else None
        # _aware() guards against TIMESTAMP WITHOUT TIME ZONE columns
        # returning naive datetimes from the ORM — subtract would otherwise crash.
        days_overdue  = (now - _aware(t.due_date)).days if t.due_date else 0

        overdue_tasks.append({
            'id':           str(t.id),
            'task_type':    t.task_type,
            'priority':     t.priority,
            'due_date':     t.due_date.strftime('%Y-%m-%d %H:%M') if t.due_date else None,
            'days_overdue': days_overdue,
            'status':       t.status,
            'assigned_to':  ', '.join(assignee_names) or 'Unassigned',
            'customer':     customer_name,
        })

    return {
        'stats': {
            'pending':       pending,
            'in_progress':   in_progress,
            'completed_mtd': completed_mtd,
            'overdue':       overdue,
        },
        'tasks_by_type':       tasks_by_type,
        'tasks_by_technician': tasks_by_technician,
        'overdue_tasks':       overdue_tasks,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Section D — Inventory Status
# ══════════════════════════════════════════════════════════════════════════════

def get_inventory_status(company_id, start=None, end=None):
    """
    4 stat cards (always live snapshots) + inventory-by-type chart
    + recent assignments (scoped to period).

    Stock counts don't have meaningful historical snapshots without an
    audit log — they're always the current state. recent_assignments
    is filtered to start/end so it reflects the selected period.
    """
    if start is None:
        start = datetime.now(PKT).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if end is None:
        end = datetime.now(PKT)

    # Live stock counts
    total_stock = db.session.query(
        func.coalesce(func.sum(InventoryItem.quantity), 0)
    ).join(Supplier, InventoryItem.vendor == Supplier.id).filter(
        Supplier.company_id == company_id,
        InventoryItem.is_active == True,
    ).scalar() or 0

    customer_deployed = db.session.query(
        func.count(InventoryAssignment.id)
    ).join(InventoryItem
    ).join(Supplier, InventoryItem.vendor == Supplier.id).filter(
        Supplier.company_id == company_id,
        InventoryAssignment.assigned_to_customer_id.isnot(None),
        InventoryAssignment.status == 'assigned',
        InventoryAssignment.returned_at.is_(None),
    ).scalar() or 0

    employee_assigned = db.session.query(
        func.count(InventoryAssignment.id)
    ).join(InventoryItem
    ).join(Supplier, InventoryItem.vendor == Supplier.id).filter(
        Supplier.company_id == company_id,
        InventoryAssignment.assigned_to_employee_id.isnot(None),
        InventoryAssignment.status == 'assigned',
        InventoryAssignment.returned_at.is_(None),
    ).scalar() or 0

    low_stock = InventoryItem.query.join(
        Supplier, InventoryItem.vendor == Supplier.id
    ).filter(
        Supplier.company_id == company_id,
        InventoryItem.is_active == True,
        InventoryItem.quantity < LOW_STOCK_THRESHOLD,
    ).count()

    # Chart D1: Inventory by Type (grouped bar: in-stock vs deployed)
    stock_map = {r.item_type: int(r.in_stock) for r in db.session.query(
        InventoryItem.item_type,
        func.sum(InventoryItem.quantity).label('in_stock'),
    ).join(Supplier, InventoryItem.vendor == Supplier.id).filter(
        Supplier.company_id == company_id,
        InventoryItem.is_active == True,
    ).group_by(InventoryItem.item_type).order_by(desc('in_stock')).all()}

    deployed_map = {r.item_type: r.deployed for r in db.session.query(
        InventoryItem.item_type,
        func.count(InventoryAssignment.id).label('deployed'),
    ).join(InventoryAssignment
    ).join(Supplier, InventoryItem.vendor == Supplier.id).filter(
        Supplier.company_id == company_id,
        InventoryAssignment.status == 'assigned',
        InventoryAssignment.returned_at.is_(None),
    ).group_by(InventoryItem.item_type).all()}

    all_types = set(list(stock_map.keys()) + list(deployed_map.keys()))
    inventory_by_type = [{
        'type':     t or 'Unknown',
        'in_stock': stock_map.get(t, 0),
        'deployed': deployed_map.get(t, 0),
    } for t in sorted(all_types)]

    # Table D2: Recently Assigned Equipment — scoped to period
    # Fix: was unscoped (all-time), now filtered to start/end
    recent_q = InventoryAssignment.query.join(
        InventoryItem
    ).join(
        Supplier, InventoryItem.vendor == Supplier.id
    ).filter(
        Supplier.company_id == company_id,
        InventoryAssignment.status == 'assigned',
        InventoryAssignment.assigned_at >= start,
        InventoryAssignment.assigned_at <= end,
    ).order_by(InventoryAssignment.assigned_at.desc()).limit(20).all()

    recent_assignments = []
    for a in recent_q:
        if a.assigned_to_customer_id and a.customer:
            assigned_name = f"{a.customer.first_name} {a.customer.last_name}"
            assigned_type = 'customer'
            area_name     = a.customer.area.name if a.customer.area else None
        elif a.assigned_to_employee_id and a.employee:
            assigned_name = f"{a.employee.first_name or ''} {a.employee.last_name or ''}".strip()
            assigned_type = 'employee'
            area_name     = None
        else:
            assigned_name = 'Unknown'
            assigned_type = None
            area_name     = None

        recent_assignments.append({
            'item_type':        a.inventory_item.item_type if a.inventory_item else 'Unknown',
            'assigned_to':      assigned_name,
            'assigned_to_type': assigned_type,
            'area':             area_name,
            'assigned_date':    a.assigned_at.strftime('%Y-%m-%d') if a.assigned_at else None,
        })

    return {
        'stats': {
            'total_stock':       int(total_stock),
            'customer_deployed': customer_deployed,
            'employee_assigned': employee_assigned,
            'low_stock':         low_stock,
        },
        'inventory_by_type':  inventory_by_type,
        'recent_assignments': recent_assignments,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Section E — Technician Performance
# ══════════════════════════════════════════════════════════════════════════════

def get_technician_performance(company_id, start, end):
    """
    Full performance table — all active technicians.
    5 bulk GROUP BY queries, Python merge. Zero N+1.
    """
    technicians = User.query.filter(
        User.company_id == company_id,
        User.is_active == True,
        User.role == 'technician',
    ).order_by(User.first_name).all()

    if not technicians:
        return []

    tech_ids = [t.id for t in technicians]

    managed_map = dict(db.session.query(
        Customer.technician_id,
        func.count(Customer.id),
    ).filter(
        Customer.company_id == company_id,
        Customer.is_active == True,
        Customer.technician_id.in_(tech_ids),
    ).group_by(Customer.technician_id).all())

    connections_map = dict(db.session.query(
        Customer.technician_id,
        func.count(Customer.id),
    ).filter(
        Customer.company_id == company_id,
        Customer.technician_id.in_(tech_ids),
        Customer.installation_date >= start.date(),
        Customer.installation_date <= end.date(),
    ).group_by(Customer.technician_id).all())

    complaint_map = {
        r[0]: {'assigned': r.assigned, 'resolved': r.resolved, 'avg_csat': round(float(r.avg_csat or 0), 1)}
        for r in db.session.query(
            Complaint.assigned_to,
            func.count(Complaint.id).label('assigned'),
            func.count(case((Complaint.status == 'resolved', 1))).label('resolved'),
            func.avg(Complaint.satisfaction_rating).label('avg_csat'),
        ).join(Customer).filter(
            Customer.company_id == company_id,
            Complaint.assigned_to.in_(tech_ids),
            Complaint.created_at >= start,
            Complaint.created_at <= end,
            Complaint.is_active == True,
        ).group_by(Complaint.assigned_to).all()
    }

    commission_map = dict(db.session.query(
        EmployeeLedger.employee_id,
        func.coalesce(func.sum(EmployeeLedger.amount), 0).label('commission'),
    ).filter(
        EmployeeLedger.company_id == company_id,
        EmployeeLedger.employee_id.in_(tech_ids),
        EmployeeLedger.transaction_type.in_(['connection_commission', 'complaint_commission']),
        EmployeeLedger.created_at >= start,
        EmployeeLedger.created_at <= end,
    ).group_by(EmployeeLedger.employee_id).all())

    result = []
    for t in technicians:
        c = complaint_map.get(t.id, {'assigned': 0, 'resolved': 0, 'avg_csat': 0})
        result.append({
            'name':                f"{t.first_name or ''} {t.last_name or ''}".strip() or 'Unknown',
            'active_customers':    managed_map.get(t.id, 0),
            'connections_mtd':     connections_map.get(t.id, 0),
            'complaints_assigned': c['assigned'],
            'complaints_resolved': c['resolved'],
            'avg_satisfaction':    c['avg_csat'],
            'commission_earned':   round(float(commission_map.get(t.id, 0)), 2),
            'pending_balance':     round(float(t.current_balance or 0), 2),
        })

    return result