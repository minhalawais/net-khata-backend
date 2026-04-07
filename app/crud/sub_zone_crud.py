from app import db
from app.models import SubZone, Area
from app.utils.logging_utils import log_action
import uuid
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import logging

logger = logging.getLogger(__name__)


def _safe_uuid(value):
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _get_area_for_user(area_id, company_id, user_role):
    area_uuid = _safe_uuid(area_id)
    if user_role == 'super_admin':
        return Area.query.filter_by(id=area_uuid).first()
    if not company_id:
        return None
    company_uuid = _safe_uuid(company_id)
    return Area.query.filter_by(id=area_uuid, company_id=company_uuid).first()

def get_all_sub_zones(company_id, user_role):
    """Get all sub-zones for a company"""
    try:
        if user_role == 'super_admin':
            sub_zones = SubZone.query.order_by(SubZone.created_at.desc()).all()
        elif user_role == 'auditor':
            sub_zones = SubZone.query.filter_by(is_active=True, company_id=company_id).order_by(SubZone.created_at.desc()).all()
        else:
            sub_zones = SubZone.query.filter_by(company_id=company_id).order_by(SubZone.created_at.desc()).all()
        
        return [{
            'id': str(sz.id),
            'area_id': str(sz.area_id),
            'area_name': sz.area.name if sz.area else 'Unknown',
            'name': sz.name,
            'description': sz.description,
            'is_active': sz.is_active
        } for sz in sub_zones]
    except Exception as e:
        logger.error(f"Error getting sub-zones: {str(e)}")
        return []

def get_sub_zones_by_area(area_id, company_id, user_role, include_inactive=False):
    """Get sub-zones for a specific area with role-aware scoping."""
    try:
        area_uuid = _safe_uuid(area_id)

        if user_role == 'super_admin':
            query = SubZone.query.filter_by(area_id=area_uuid)
        else:
            company_uuid = _safe_uuid(company_id)
            query = SubZone.query.filter_by(area_id=area_uuid, company_id=company_uuid)

        if not include_inactive:
            query = query.filter_by(is_active=True)

        sub_zones = query.order_by(SubZone.created_at.desc()).all()
        
        return [{
            'id': str(sz.id),
            'area_id': str(sz.area_id),
            'name': sz.name,
            'description': sz.description,
            'is_active': sz.is_active
        } for sz in sub_zones]
    except Exception as e:
        logger.error(f"Error getting sub-zones by area: {str(e)}")
        return []

def add_sub_zone(data, user_role, current_user_id, ip_address, user_agent):
    """Add a new sub-zone"""
    try:
        if not data.get('area_id'):
            raise ValueError('area_id is required')
        if not data.get('name') or not data.get('name').strip():
            raise ValueError('name is required')

        area = _get_area_for_user(data['area_id'], data.get('company_id'), user_role)
        if not area:
            raise ValueError('Invalid area selection for this company')

        sub_zone_name = data['name'].strip()
        duplicate = SubZone.query.filter(
            SubZone.area_id == area.id,
            func.lower(SubZone.name) == sub_zone_name.lower(),
        ).first()
        if duplicate:
            raise ValueError('Sub-zone with this name already exists in selected area')

        company_uuid = area.company_id if user_role == 'super_admin' else _safe_uuid(data['company_id'])

        new_sub_zone = SubZone(
            company_id=company_uuid,
            area_id=area.id,
            name=sub_zone_name,
            description=data.get('description')
        )
        db.session.add(new_sub_zone)
        db.session.commit()

        log_action(
            current_user_id,
            'CREATE',
            'sub_zones',
            new_sub_zone.id,
            None,
            data,
            ip_address,
            user_agent,
            str(company_uuid)
        )

        return new_sub_zone
    except IntegrityError as e:
        db.session.rollback()
        logger.error(f"Integrity error adding sub-zone: {str(e)}")
        raise ValueError("Sub-zone with this name may already exist in this area")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error adding sub-zone: {str(e)}")
        raise

def update_sub_zone(id, data, company_id, user_role, current_user_id, ip_address, user_agent):
    """Update an existing sub-zone"""
    try:
        if user_role == 'super_admin':
            sub_zone = SubZone.query.filter_by(id=id).first()
        elif user_role == 'auditor':
            sub_zone = SubZone.query.filter_by(id=id, is_active=True, company_id=company_id).first()
        else:
            sub_zone = SubZone.query.filter_by(id=id, company_id=company_id).first()
        
        if not sub_zone:
            return None

        target_area_id = data.get('area_id', str(sub_zone.area_id))
        target_area = _get_area_for_user(target_area_id, company_id, user_role)
        if not target_area:
            raise ValueError('Invalid area selection for this company')

        target_name = data.get('name', sub_zone.name)
        if target_name and target_name.strip():
            duplicate = SubZone.query.filter(
                SubZone.id != sub_zone.id,
                SubZone.area_id == target_area.id,
                func.lower(SubZone.name) == target_name.strip().lower(),
            ).first()
            if duplicate:
                raise ValueError('Sub-zone with this name already exists in selected area')

        old_values = {
            'name': sub_zone.name,
            'description': sub_zone.description,
            'area_id': str(sub_zone.area_id),
            'is_active': sub_zone.is_active
        }

        sub_zone.name = target_name.strip() if target_name else sub_zone.name
        sub_zone.description = data.get('description', sub_zone.description)
        sub_zone.area_id = target_area.id
        sub_zone.company_id = target_area.company_id
        if 'is_active' in data:
            sub_zone.is_active = data['is_active']
        
        db.session.commit()

        log_action(
            current_user_id,
            'UPDATE',
            'sub_zones',
            sub_zone.id,
            old_values,
            data,
            ip_address,
            user_agent,
            company_id
        )

        return sub_zone
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating sub-zone: {str(e)}")
        raise

def delete_sub_zone(id, company_id, user_role, current_user_id, ip_address, user_agent):
    """Delete a sub-zone"""
    try:
        if user_role == 'super_admin':
            sub_zone = SubZone.query.filter_by(id=id).first()
        elif user_role == 'auditor':
            sub_zone = SubZone.query.filter_by(id=id, is_active=True, company_id=company_id).first()
        else:
            sub_zone = SubZone.query.filter_by(id=id, company_id=company_id).first()
        
        if not sub_zone:
            return False

        old_values = {
            'name': sub_zone.name,
            'description': sub_zone.description,
            'area_id': str(sub_zone.area_id),
            'is_active': sub_zone.is_active
        }

        db.session.delete(sub_zone)
        db.session.commit()

        log_action(
            current_user_id,
            'DELETE',
            'sub_zones',
            sub_zone.id,
            old_values,
            None,
            ip_address,
            user_agent,
            company_id
        )

        return True
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error deleting sub-zone: {str(e)}")
        return False
