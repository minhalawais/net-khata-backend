from flask import jsonify, request
from flask_jwt_extended import get_jwt, get_jwt_identity, jwt_required

from . import main
from ..crud import company_crud


def _ensure_super_admin():
    claims = get_jwt()
    if claims.get("role") != "super_admin":
        return None, (jsonify({"error": "Forbidden: super_admin required"}), 403)
    return claims, None


@main.route('/companies/list', methods=['GET'])
@jwt_required()
def get_companies():
    _, error_response = _ensure_super_admin()
    if error_response:
        return error_response

    search = request.args.get('search')
    companies = company_crud.list_companies(search)
    return jsonify(companies), 200


@main.route('/companies/add', methods=['POST'])
@jwt_required()
def add_new_company():
    _, error_response = _ensure_super_admin()
    if error_response:
        return error_response

    current_user_id = get_jwt_identity()
    ip_address = request.remote_addr
    user_agent = request.headers.get('User-Agent')
    data = request.json or {}

    try:
        company = company_crud.add_company(data, current_user_id, ip_address, user_agent)
        return jsonify({'message': 'Company created successfully', 'id': str(company.id)}), 201
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': 'Failed to create company', 'message': str(e)}), 500


@main.route('/companies/<string:company_id>', methods=['GET'])
@jwt_required()
def get_company(company_id):
    _, error_response = _ensure_super_admin()
    if error_response:
        return error_response

    company = company_crud.get_company(company_id)
    if not company:
        return jsonify({'error': 'Company not found'}), 404

    return jsonify(company), 200


@main.route('/companies/<string:company_id>/profile', methods=['GET'])
@jwt_required()
def get_company_profile(company_id):
    _, error_response = _ensure_super_admin()
    if error_response:
        return error_response

    profile = company_crud.get_company_profile(company_id)
    if not profile:
        return jsonify({'error': 'Company not found'}), 404

    return jsonify(profile), 200


@main.route('/companies/update/<string:company_id>', methods=['PUT'])
@jwt_required()
def update_existing_company(company_id):
    _, error_response = _ensure_super_admin()
    if error_response:
        return error_response

    current_user_id = get_jwt_identity()
    ip_address = request.remote_addr
    user_agent = request.headers.get('User-Agent')
    data = request.json or {}

    try:
        updated = company_crud.update_company(company_id, data, current_user_id, ip_address, user_agent)
        if not updated:
            return jsonify({'error': 'Company not found'}), 404
        return jsonify({'message': 'Company updated successfully'}), 200
    except Exception as e:
        return jsonify({'error': 'Failed to update company', 'message': str(e)}), 500


@main.route('/companies/delete/<string:company_id>', methods=['DELETE'])
@jwt_required()
def delete_existing_company(company_id):
    _, error_response = _ensure_super_admin()
    if error_response:
        return error_response

    current_user_id = get_jwt_identity()
    ip_address = request.remote_addr
    user_agent = request.headers.get('User-Agent')

    try:
        company = company_crud.deactivate_company(company_id, current_user_id, ip_address, user_agent)
        if not company:
            return jsonify({'error': 'Company not found'}), 404
        return jsonify({'message': 'Company deactivated successfully'}), 200
    except Exception as e:
        return jsonify({'error': 'Failed to deactivate company', 'message': str(e)}), 500


@main.route('/companies/<string:company_id>/users', methods=['GET'])
@jwt_required()
def get_company_users(company_id):
    _, error_response = _ensure_super_admin()
    if error_response:
        return error_response

    try:
        users = company_crud.get_company_users(company_id)
        return jsonify(users), 200
    except Exception as e:
        return jsonify({'error': 'Failed to fetch company users', 'message': str(e)}), 500


@main.route('/companies/<string:company_id>/users/add', methods=['POST'])
@jwt_required()
def add_company_user(company_id):
    _, error_response = _ensure_super_admin()
    if error_response:
        return error_response

    current_user_id = get_jwt_identity()
    ip_address = request.remote_addr
    user_agent = request.headers.get('User-Agent')
    data = request.json or {}

    try:
        user = company_crud.add_company_user(company_id, data, current_user_id, ip_address, user_agent)
        return jsonify({'message': 'Company user created successfully', 'id': str(user.id)}), 201
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': 'Failed to create company user', 'message': str(e)}), 500


@main.route('/companies/<string:company_id>/users/<string:user_id>/update', methods=['PUT'])
@jwt_required()
def update_company_user(company_id, user_id):
    _, error_response = _ensure_super_admin()
    if error_response:
        return error_response

    current_user_id = get_jwt_identity()
    ip_address = request.remote_addr
    user_agent = request.headers.get('User-Agent')
    data = request.json or {}

    try:
        user = company_crud.update_company_user(company_id, user_id, data, current_user_id, ip_address, user_agent)
        if not user:
            return jsonify({'error': 'User not found'}), 404
        return jsonify({'message': 'Company user updated successfully'}), 200
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': 'Failed to update company user', 'message': str(e)}), 500


@main.route('/companies/<string:company_id>/users/<string:user_id>/status', methods=['PATCH'])
@jwt_required()
def update_company_user_status(company_id, user_id):
    _, error_response = _ensure_super_admin()
    if error_response:
        return error_response

    current_user_id = get_jwt_identity()
    ip_address = request.remote_addr
    user_agent = request.headers.get('User-Agent')
    data = request.json or {}

    if 'is_active' not in data:
        return jsonify({'error': 'is_active is required'}), 400

    try:
        user = company_crud.set_company_user_status(
            company_id,
            user_id,
            bool(data.get('is_active')),
            current_user_id,
            ip_address,
            user_agent,
        )
        if not user:
            return jsonify({'error': 'User not found'}), 404
        return jsonify({'message': 'Company user status updated successfully'}), 200
    except Exception as e:
        return jsonify({'error': 'Failed to update user status', 'message': str(e)}), 500