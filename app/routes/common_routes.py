from flask import Blueprint, send_from_directory, current_app, abort
import os

common_bp = Blueprint('common', __name__)

@common_bp.route('/uploads/<path:filename>')
def serve_uploaded_file(filename):
    """Serve files from the uploads directory."""
    try:
        # Files are saved in api/app/uploads based on employee_crud logic
        # this file is in api/app/routes
        # so we go up one level to api/app, then into uploads
        
        current_dir = os.path.dirname(os.path.abspath(__file__)) # api/app/routes
        app_dir = os.path.dirname(current_dir) # api/app
        api_dir = os.path.dirname(app_dir) # api
        uploads_path = os.path.join(api_dir, 'uploads')
        
        return send_from_directory(uploads_path, filename)
    except FileNotFoundError:
        abort(404)
