from flask import Blueprint, jsonify, request, send_file
from flask_jwt_extended import jwt_required, get_jwt_identity, get_jwt
from app.crud import dashboard_crud
from app.crud import executive_dashboard_crud
from app.crud import service_support_crud
from app.crud import financial_dashboard_crud
from app.crud import operations_dashboard_crud
from . import main
import logging
dashboard = Blueprint('dashboard', __name__)

logger = logging.getLogger(__name__)



@main.route('/dashboard/executive-summary', methods=['GET'])
@jwt_required()
def get_executive_summary():
    current_user = get_jwt_identity()
    claims = get_jwt()
    company_id = claims['company_id']
    
    data = dashboard_crud.get_executive_summary_data(company_id)
    return jsonify(data)


@main.route('/dashboard/executive-advanced', methods=['GET'])
@jwt_required()
def get_executive_advanced():
    """
    Executive Dashboard — Owner's morning overview.

    Query Parameters:
        - start_date, end_date: Date range (YYYY-MM-DD)
        - area_id: Area filter (UUID or 'all')
        - isp_id: ISP filter (UUID or 'all')
    """
    claims = get_jwt()
    company_id = claims['company_id']
    
    filters = {
        'start_date': request.args.get('start_date'),
        'end_date': request.args.get('end_date'),
        'area_id': request.args.get('area_id', 'all'),
        'isp_id': request.args.get('isp_id', 'all'),
    }
    
    try:
        data = executive_dashboard_crud.get_executive_dashboard_advanced(company_id, filters)
        return jsonify(data), 200
    except Exception as e:
        logger.error(f"Error in executive-advanced: {str(e)}")
        return jsonify({'error': 'Failed to fetch executive dashboard data'}), 500



@main.route('/dashboard/financial-analytics', methods=['GET'])
@jwt_required()
def get_financial_analytics():
    current_user = get_jwt_identity()
    claims = get_jwt()
    company_id = claims['company_id']
    
    data = dashboard_crud.get_financial_analytics_data(company_id)
    return jsonify(data)

@main.route('/dashboard/service-support', methods=['GET'])
@jwt_required()
def get_service_support_metrics():
    current_user = get_jwt_identity()
    claims = get_jwt()
    company_id = claims['company_id']
    
    data = dashboard_crud.get_service_support_metrics(company_id)
    return jsonify(data)


@main.route('/dashboard/service-support-advanced', methods=['GET'])
@jwt_required()
def get_service_support_advanced():
    """
    Operations & Network Dashboard — consolidates Service & Support,
    Inventory, Area Analytics, Service Plans, and Employee Performance.

    Query Parameters:
        - start_date, end_date: Date range (YYYY-MM-DD)
    """
    claims = get_jwt()
    company_id = claims['company_id']

    filters = {
        'start_date': request.args.get('start_date'),
        'end_date': request.args.get('end_date'),
    }

    try:
        data = operations_dashboard_crud.get_operations_dashboard_data(company_id, filters)
        return jsonify(data), 200
    except Exception as e:
        logger.error(f"Error in operations dashboard: {str(e)}")
        return jsonify({'error': 'Failed to fetch operations dashboard data'}), 500


@main.route('/dashboard/bank-account-analytics', methods=['GET'])
@jwt_required()
def get_bank_account_analytics():
    current_user = get_jwt_identity()
    claims = get_jwt()
    company_id = claims['company_id']
    
    # Get filters from query parameters
    filters = {
        'start_date': request.args.get('start_date'),
        'end_date': request.args.get('end_date'),
        'bank_account_id': request.args.get('bank_account_id', 'all'),
        'payment_method': request.args.get('payment_method', 'all')
    }
    
    data = dashboard_crud.get_bank_account_analytics_data(company_id, filters)
    return jsonify(data)

@main.route('/dashboard/unified-financial', methods=['GET'])
@jwt_required()
def get_unified_financial_data():
    claims = get_jwt()
    company_id = claims['company_id']
    
    # Simplified filters — date range only
    filters = {
        'start_date': request.args.get('start_date'),
        'end_date': request.args.get('end_date'),
    }
    
    try:
        data = financial_dashboard_crud.get_financial_dashboard_data(company_id, filters)
        return jsonify(data), 200
    except Exception as e:
        logger.error(f"Error fetching financial dashboard data: {str(e)}")
        return jsonify({'error': 'Failed to fetch financial dashboard data'}), 500


@main.route('/dashboard/financial-intelligence', methods=['GET'])
@main.route('/dashboard/financial-intelligence-v2', methods=['GET'])
@jwt_required()
def get_financial_intelligence():
    """Financial intelligence contract endpoint."""
    claims = get_jwt()
    company_id = claims['company_id']

    filters = {
        'start_date': request.args.get('start_date'),
        'end_date': request.args.get('end_date'),
        'bank_account_id': request.args.get('bank_account_id', 'all'),
        'payment_method': request.args.get('payment_method', 'all'),
        'invoice_status': request.args.get('invoice_status', 'all'),
        'isp_payment_type': request.args.get('isp_payment_type', 'all'),
        'time_range': request.args.get('time_range', 'mtd'),
    }

    try:
        data = financial_dashboard_crud.get_financial_intelligence_v2(company_id, filters)
        if isinstance(data, dict) and data.get('error'):
            return jsonify(data), 500
        return jsonify(data), 200
    except Exception as e:
        logger.error(f"Error fetching financial intelligence: {str(e)}")
        return jsonify({'error': 'Failed to fetch financial intelligence data'}), 500

@main.route('/dashboard/ledger', methods=['GET'])
@jwt_required()
def get_ledger():
    claims = get_jwt()
    company_id = claims['company_id']
    filters = {
        'start_date': request.args.get('start_date'),
        'end_date': request.args.get('end_date'),
        'bank_account_id': request.args.get('bank_account_id', 'all'),
        'payment_method': request.args.get('payment_method', 'all'),
        'invoice_status': request.args.get('invoice_status', 'all'),
        'isp_payment_type': request.args.get('isp_payment_type', 'all'),
    }
    data = dashboard_crud.get_ledger_data(company_id, filters)
    return jsonify(data), 200

@main.route('/dashboard/ledger/export', methods=['GET'])
@jwt_required()
def export_ledger():
    """
    Export ledger data in CSV, XLSX, or PDF format.
    
    Query Parameters:
        - format: 'csv', 'xlsx', or 'pdf' (default: csv)
        - start_date, end_date: Date range (YYYY-MM-DD)
        - bank_account_id: Bank account filter UUID or 'all'
        - payment_method: Payment method filter or 'all'
        - invoice_status: Invoice status filter or 'all'
        - isp_payment_type: ISP payment type filter or 'all'
    """
    claims = get_jwt()
    company_id = claims['company_id']
    export_format = request.args.get('format', 'csv').lower()
    
    if export_format not in ['csv', 'xlsx', 'pdf']:
        return jsonify({'error': 'Invalid format. Supported formats: csv, xlsx, pdf'}), 400
    
    filters = {
        'start_date': request.args.get('start_date'),
        'end_date': request.args.get('end_date'),
        'bank_account_id': request.args.get('bank_account_id', 'all'),
        'payment_method': request.args.get('payment_method', 'all'),
        'invoice_status': request.args.get('invoice_status', 'all'),
        'isp_payment_type': request.args.get('isp_payment_type', 'all'),
    }
    
    try:
        file_obj, filename, mime_type = dashboard_crud.export_ledger_data(company_id, filters, export_format)
        return send_file(
            file_obj,
            mimetype=mime_type,
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        logger.error(f"Error exporting ledger: {str(e)}")
        return jsonify({'error': f'Failed to export ledger data: {str(e)}'}), 500


