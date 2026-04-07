from decimal import Decimal
from uuid import UUID

from sqlalchemy import func

from app import db
from app.models import (
    Area,
    BankAccount,
    Company,
    Complaint,
    Customer,
    Expense,
    ExtraIncome,
    InventoryItem,
    Invoice,
    ISP,
    Payment,
    ServicePlan,
    Supplier,
    Task,
    User,
    Vendor,
)
from app.utils.logging_utils import log_action


def _to_uuid(value: str) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


def _to_float(value):
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _serialize_company(company: Company):
    users_count = User.query.filter_by(company_id=company.id).count()
    active_users_count = User.query.filter_by(company_id=company.id, is_active=True).count()
    customers_count = Customer.query.filter_by(company_id=company.id).count()
    active_customers_count = Customer.query.filter_by(company_id=company.id, is_active=True).count()

    return {
        "id": str(company.id),
        "name": company.name,
        "address": company.address,
        "contact_number": company.contact_number,
        "email": company.email,
        "is_active": company.is_active,
        "created_at": company.created_at.isoformat() if company.created_at else None,
        "updated_at": company.updated_at.isoformat() if company.updated_at else None,
        "users_count": users_count,
        "active_users_count": active_users_count,
        "customers_count": customers_count,
        "active_customers_count": active_customers_count,
    }


def list_companies(search: str | None = None):
    query = Company.query
    if search:
        search_term = f"%{search.strip()}%"
        query = query.filter(
            Company.name.ilike(search_term)
            | Company.email.ilike(search_term)
            | Company.contact_number.ilike(search_term)
        )

    companies = query.order_by(Company.created_at.desc()).all()
    return [_serialize_company(c) for c in companies]


def add_company(data, current_user_id, ip_address, user_agent):
    name = (data.get("name") or "").strip()
    if not name:
        raise ValueError("Company name is required")

    company = Company(
        name=name,
        address=data.get("address"),
        contact_number=data.get("contact_number"),
        email=data.get("email"),
        is_active=True,
    )

    db.session.add(company)
    db.session.commit()

    log_action(
        current_user_id,
        "CREATE",
        "companies",
        company.id,
        None,
        {
            "name": company.name,
            "address": company.address,
            "contact_number": company.contact_number,
            "email": company.email,
            "is_active": company.is_active,
        },
        ip_address,
        user_agent,
        company.id,
    )

    return company


def get_company(company_id: str):
    company_uuid = _to_uuid(company_id)
    company = Company.query.get(company_uuid)
    if not company:
        return None
    return _serialize_company(company)


def update_company(company_id: str, data, current_user_id, ip_address, user_agent):
    company_uuid = _to_uuid(company_id)
    company = Company.query.get(company_uuid)
    if not company:
        return None

    old_values = {
        "name": company.name,
        "address": company.address,
        "contact_number": company.contact_number,
        "email": company.email,
        "is_active": company.is_active,
    }

    if "name" in data and data.get("name"):
        company.name = data["name"].strip()
    if "address" in data:
        company.address = data.get("address")
    if "contact_number" in data:
        company.contact_number = data.get("contact_number")
    if "email" in data:
        company.email = data.get("email")
    if "is_active" in data:
        company.is_active = bool(data.get("is_active"))

    db.session.commit()

    log_action(
        current_user_id,
        "UPDATE",
        "companies",
        company.id,
        old_values,
        {
            "name": company.name,
            "address": company.address,
            "contact_number": company.contact_number,
            "email": company.email,
            "is_active": company.is_active,
        },
        ip_address,
        user_agent,
        company.id,
    )

    return company


def deactivate_company(company_id: str, current_user_id, ip_address, user_agent):
    company_uuid = _to_uuid(company_id)
    company = Company.query.get(company_uuid)
    if not company:
        return None

    old_values = {"is_active": company.is_active}
    company.is_active = False
    db.session.commit()

    log_action(
        current_user_id,
        "UPDATE",
        "companies",
        company.id,
        old_values,
        {"is_active": company.is_active},
        ip_address,
        user_agent,
        company.id,
    )

    return company


def get_company_users(company_id: str):
    company_uuid = _to_uuid(company_id)
    users = User.query.filter_by(company_id=company_uuid).order_by(User.created_at.desc()).all()
    return [
        {
            "id": str(user.id),
            "company_id": str(user.company_id) if user.company_id else None,
            "username": user.username,
            "email": user.email,
            "role": user.role,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "contact_number": user.contact_number,
            "is_active": user.is_active,
            "created_at": user.created_at.isoformat() if user.created_at else None,
        }
        for user in users
    ]


def add_company_user(company_id: str, data, current_user_id, ip_address, user_agent):
    company_uuid = _to_uuid(company_id)

    required_fields = ["username", "email", "password", "role", "first_name", "last_name"]
    for field in required_fields:
        if not data.get(field):
            raise ValueError(f"{field} is required")

    if data.get("role") == "super_admin":
        raise ValueError("Super admin users cannot be created under a company")

    user = User(
        company_id=company_uuid,
        username=data.get("username").strip(),
        email=data.get("email").strip(),
        role=data.get("role"),
        first_name=data.get("first_name").strip(),
        last_name=data.get("last_name").strip(),
        contact_number=data.get("contact_number"),
        cnic=data.get("cnic"),
        is_active=True,
    )
    user.set_password(data.get("password"))

    db.session.add(user)
    db.session.commit()

    log_action(
        current_user_id,
        "CREATE",
        "users",
        user.id,
        None,
        {
            "company_id": str(user.company_id),
            "username": user.username,
            "email": user.email,
            "role": user.role,
            "first_name": user.first_name,
            "last_name": user.last_name,
        },
        ip_address,
        user_agent,
        company_uuid,
    )

    return user


def update_company_user(company_id: str, user_id: str, data, current_user_id, ip_address, user_agent):
    company_uuid = _to_uuid(company_id)
    user_uuid = _to_uuid(user_id)

    user = User.query.filter_by(id=user_uuid, company_id=company_uuid).first()
    if not user:
        return None

    old_values = {
        "username": user.username,
        "email": user.email,
        "role": user.role,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "contact_number": user.contact_number,
        "is_active": user.is_active,
    }

    if "username" in data and data.get("username"):
        user.username = data.get("username").strip()
    if "email" in data and data.get("email"):
        user.email = data.get("email").strip()
    if "first_name" in data and data.get("first_name"):
        user.first_name = data.get("first_name").strip()
    if "last_name" in data and data.get("last_name"):
        user.last_name = data.get("last_name").strip()
    if "contact_number" in data:
        user.contact_number = data.get("contact_number")
    if "cnic" in data:
        user.cnic = data.get("cnic")
    if "role" in data and data.get("role"):
        if data.get("role") == "super_admin":
            raise ValueError("Super admin role cannot be assigned inside a company")
        user.role = data.get("role")
    if "password" in data and data.get("password"):
        user.set_password(data.get("password"))
    if "is_active" in data:
        user.is_active = bool(data.get("is_active"))

    db.session.commit()

    log_action(
        current_user_id,
        "UPDATE",
        "users",
        user.id,
        old_values,
        {
            "username": user.username,
            "email": user.email,
            "role": user.role,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "contact_number": user.contact_number,
            "is_active": user.is_active,
        },
        ip_address,
        user_agent,
        company_uuid,
    )

    return user


def set_company_user_status(company_id: str, user_id: str, is_active: bool, current_user_id, ip_address, user_agent):
    company_uuid = _to_uuid(company_id)
    user_uuid = _to_uuid(user_id)

    user = User.query.filter_by(id=user_uuid, company_id=company_uuid).first()
    if not user:
        return None

    old_values = {"is_active": user.is_active}
    user.is_active = bool(is_active)
    db.session.commit()

    log_action(
        current_user_id,
        "UPDATE",
        "users",
        user.id,
        old_values,
        {"is_active": user.is_active},
        ip_address,
        user_agent,
        company_uuid,
    )

    return user


def get_company_profile(company_id: str):
    company_uuid = _to_uuid(company_id)
    company = Company.query.get(company_uuid)
    if not company:
        return None

    users_count = User.query.filter_by(company_id=company_uuid).count()
    active_users_count = User.query.filter_by(company_id=company_uuid, is_active=True).count()
    customers_count = Customer.query.filter_by(company_id=company_uuid).count()
    active_customers_count = Customer.query.filter_by(company_id=company_uuid, is_active=True).count()
    invoices_count = Invoice.query.filter_by(company_id=company_uuid).count()
    unpaid_invoices = Invoice.query.filter(
        Invoice.company_id == company_uuid,
        Invoice.status.in_(["pending", "partially_paid", "overdue"]),
    ).count()

    payments_total = db.session.query(func.coalesce(func.sum(Payment.amount), 0)).filter(
        Payment.company_id == company_uuid
    ).scalar()
    expenses_total = db.session.query(func.coalesce(func.sum(Expense.amount), 0)).filter(
        Expense.company_id == company_uuid
    ).scalar()
    extra_income_total = db.session.query(func.coalesce(func.sum(ExtraIncome.amount), 0)).filter(
        ExtraIncome.company_id == company_uuid
    ).scalar()

    complaint_customer_ids = db.session.query(Customer.id).filter(Customer.company_id == company_uuid)
    complaints_open = Complaint.query.filter(
        Complaint.customer_id.in_(complaint_customer_ids),
        Complaint.status.in_(["open", "in_progress"]),
    ).count()

    tasks_open = Task.query.filter(
        Task.company_id == company_uuid,
        Task.status.in_(["pending", "in_progress"]),
    ).count()

    return {
        "company": _serialize_company(company),
        "metrics": {
            "users_count": users_count,
            "active_users_count": active_users_count,
            "customers_count": customers_count,
            "active_customers_count": active_customers_count,
            "invoices_count": invoices_count,
            "unpaid_invoices_count": unpaid_invoices,
            "payments_total": _to_float(payments_total),
            "expenses_total": _to_float(expenses_total),
            "extra_income_total": _to_float(extra_income_total),
            "complaints_open": complaints_open,
            "tasks_open": tasks_open,
            "inventory_items_count": InventoryItem.query.filter_by(company_id=company_uuid).count(),
            "suppliers_count": Supplier.query.filter_by(company_id=company_uuid).count(),
            "vendors_count": Vendor.query.filter_by(company_id=company_uuid).count(),
            "areas_count": Area.query.filter_by(company_id=company_uuid).count(),
            "service_plans_count": ServicePlan.query.filter_by(company_id=company_uuid).count(),
            "isps_count": ISP.query.filter_by(company_id=company_uuid).count(),
            "bank_accounts_count": BankAccount.query.filter_by(company_id=company_uuid).count(),
        },
        "recent": {
            "users": [
                {
                    "id": str(u.id),
                    "name": f"{u.first_name or ''} {u.last_name or ''}".strip(),
                    "email": u.email,
                    "role": u.role,
                    "is_active": u.is_active,
                    "created_at": u.created_at.isoformat() if u.created_at else None,
                }
                for u in User.query.filter_by(company_id=company_uuid).order_by(User.created_at.desc()).limit(5).all()
            ],
            "customers": [
                {
                    "id": str(c.id),
                    "name": f"{c.first_name} {c.last_name}",
                    "internet_id": c.internet_id,
                    "is_active": c.is_active,
                    "created_at": c.created_at.isoformat() if c.created_at else None,
                }
                for c in Customer.query.filter_by(company_id=company_uuid).order_by(Customer.created_at.desc()).limit(5).all()
            ],
            "invoices": [
                {
                    "id": str(i.id),
                    "invoice_number": i.invoice_number,
                    "total_amount": _to_float(i.total_amount),
                    "status": i.status,
                    "created_at": i.created_at.isoformat() if i.created_at else None,
                }
                for i in Invoice.query.filter_by(company_id=company_uuid).order_by(Invoice.created_at.desc()).limit(5).all()
            ],
        },
    }