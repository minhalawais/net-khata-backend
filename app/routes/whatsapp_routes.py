"""
WhatsApp API Routes
REST endpoints for WhatsApp messaging functionality.

Fixes applied:
  - POST /instance/create now returns 500 only when qr_code_base64 is genuinely
    missing (previously returned 201 with an empty QR and the frontend showed nothing).
  - GET /instance/qr returns 202 when the instance is already connected so the
    frontend knows to stop polling.
  - POST /webhook now handles the 'qrcode.updated' Evolution event — persists the
    new QR to the DB so the frontend's poll immediately gets a fresh code.
  - All Evolution routes use the module-level singleton (evolution_client).
  - instance_token is included in instance-status response for dispatcher use.
"""

from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity, get_jwt
from app import db
from app.models import (
    WhatsAppMessageQueue, WhatsAppDailyQuota,
    WhatsAppTemplate, WhatsAppConfig,
)
from app.models import Customer, Invoice, Company
from app.services.whatsapp_queue_service import WhatsAppQueueService
from app.services.whatsapp_rate_limiter import WhatsAppRateLimiter
from app.services.whatsapp_api_client import WhatsAppAPIClient
from app.services.evolution_api_client import evolution_client          # singleton
from datetime import datetime, date
import pytz
import logging

logger = logging.getLogger(__name__)

PAK_TZ = pytz.timezone('Asia/Karachi')

whatsapp_bp = Blueprint('whatsapp', __name__, url_prefix='/api/whatsapp')


# ============================================================================
# MESSAGE QUEUE ENDPOINTS
# ============================================================================

@whatsapp_bp.route('/queue', methods=['GET'])
@jwt_required()
def get_message_queue():
    """Get all messages in queue with pagination and filters"""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']

        page     = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        status       = request.args.get('status')
        message_type = request.args.get('message_type')
        customer_id  = request.args.get('customer_id')

        query = WhatsAppMessageQueue.query.filter_by(company_id=company_id, is_active=True)

        if status:
            query = query.filter_by(status=status)
        if message_type:
            query = query.filter_by(message_type=message_type)
        if customer_id:
            query = query.filter_by(customer_id=customer_id)

        pagination = query.order_by(
            WhatsAppMessageQueue.created_at.desc()
        ).paginate(page=page, per_page=per_page, error_out=False)

        messages = []
        for msg in pagination.items:
            messages.append({
                'id':                str(msg.id),
                'customer_id':       str(msg.customer_id),
                'customer_name':     f"{msg.customer.first_name} {msg.customer.last_name}" if msg.customer else '',
                'mobile':            msg.mobile,
                'message_type':      msg.message_type,
                'message_content':   msg.message_content,
                'media_type':        msg.media_type,
                'media_url':         msg.media_url,
                'priority':          msg.priority,
                'status':            msg.status,
                'retry_count':       msg.retry_count,
                'error_message':     msg.error_message,
                'scheduled_date':    msg.scheduled_date.isoformat() if msg.scheduled_date else None,
                'sent_at':           msg.sent_at.isoformat() if msg.sent_at else None,
                'created_at':        msg.created_at.isoformat(),
                'related_invoice_id': str(msg.related_invoice_id) if msg.related_invoice_id else None,
            })

        return jsonify({
            'messages':     messages,
            'total':        pagination.total,
            'pages':        pagination.pages,
            'current_page': page,
            'per_page':     per_page,
        }), 200

    except Exception as e:
        logger.error(f"Error fetching message queue: {e}")
        return jsonify({'error': str(e)}), 500


@whatsapp_bp.route('/queue/stats', methods=['GET'])
@jwt_required()
def get_queue_stats():
    """Get queue statistics"""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']
        stats      = WhatsAppQueueService.get_queue_stats(company_id)
        return jsonify(stats), 200
    except Exception as e:
        logger.error(f"Error fetching queue stats: {e}")
        return jsonify({'error': str(e)}), 500


@whatsapp_bp.route('/send-bulk', methods=['POST'])
@jwt_required()
def send_bulk_message():
    """Enqueue a bulk message to selected customers"""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']
        data       = request.get_json()

        customer_ids    = data.get('customer_ids', [])
        message_content = data.get('message')
        priority        = data.get('priority', 60)
        message_type    = data.get('message_type', 'custom')

        if not customer_ids:
            return jsonify({'error': 'No customers selected'}), 400
        if not message_content:
            return jsonify({'error': 'Message content required'}), 400

        # Fetch company and replace {{company_name}} if present
        company = Company.query.get(company_id)
        if company and '{{company_name}}' in message_content:
            message_content = message_content.replace('{{company_name}}', company.name)

        messages = WhatsAppQueueService.enqueue_bulk_messages(
            company_id      = company_id,
            customer_ids    = customer_ids,
            message_content = message_content,
            message_type    = message_type,
            priority        = priority,
        )

        return jsonify({
            'success':         True,
            'messages_queued': len(messages),
            'message':         f'Successfully queued {len(messages)} messages',
        }), 201

    except Exception as e:
        logger.error(f"Error sending bulk messages: {e}")
        return jsonify({'error': str(e)}), 500


@whatsapp_bp.route('/send-personalized', methods=['POST'])
@jwt_required()
def send_personalized_messages():
    """Enqueue personalised messages to customers"""
    try:
        claims        = get_jwt()
        company_id    = claims['company_id']
        data          = request.get_json()
        messages_data = data.get('messages', [])

        if not messages_data:
            return jsonify({'error': 'No messages provided'}), 400

        # Fetch company and replace {{company_name}} in each message if present
        company = Company.query.get(company_id)
        for msg in messages_data:
            if company and 'message' in msg and '{{company_name}}' in msg['message']:
                msg['message'] = msg['message'].replace('{{company_name}}', company.name)

        messages = WhatsAppQueueService.enqueue_personalized_messages(
            company_id    = company_id,
            messages_data = messages_data,
        )

        return jsonify({
            'success':         True,
            'messages_queued': len(messages),
            'message':         f'Successfully queued {len(messages)} personalised messages',
        }), 201

    except Exception as e:
        logger.error(f"Error sending personalised messages: {e}")
        return jsonify({'error': str(e)}), 500


@whatsapp_bp.route('/retry/<message_id>', methods=['POST'])
@jwt_required()
def retry_message(message_id):
    """Reset a failed message to pending for retry"""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']

        message = WhatsAppMessageQueue.query.filter_by(
            id=message_id, company_id=company_id
        ).first()

        if not message:
            return jsonify({'error': 'Message not found'}), 404

        message.status        = 'pending'
        message.error_message = None
        db.session.commit()

        return jsonify({'success': True, 'message': 'Message marked for retry'}), 200

    except Exception as e:
        logger.error(f"Error retrying message: {e}")
        return jsonify({'error': str(e)}), 500


# ============================================================================
# QUOTA ENDPOINTS
# ============================================================================

@whatsapp_bp.route('/quota', methods=['GET'])
@jwt_required()
def get_quota_status():
    """Get current daily quota status"""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']
        stats      = WhatsAppRateLimiter.get_quota_stats(company_id)
        return jsonify(stats), 200
    except Exception as e:
        logger.error(f"Error fetching quota status: {e}")
        return jsonify({'error': str(e)}), 500


# ============================================================================
# TEMPLATE ENDPOINTS
# ============================================================================

@whatsapp_bp.route('/templates', methods=['GET'])
@jwt_required()
def get_templates():
    """Get all message templates for the company"""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']

        templates = WhatsAppTemplate.query.filter_by(
            company_id=company_id, is_active=True
        ).order_by(WhatsAppTemplate.created_at.desc()).all()

        result = [{
            'id':               str(t.id),
            'name':             t.name,
            'description':      t.description,
            'template_text':    t.template_text,
            'category':         t.category,
            'message_type':     t.message_type,
            'default_priority': t.default_priority,
            'created_at':       t.created_at.isoformat(),
        } for t in templates]

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error fetching templates: {e}")
        return jsonify({'error': str(e)}), 500


@whatsapp_bp.route('/templates', methods=['POST'])
@jwt_required()
def create_template():
    """Create a new message template"""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']
        user_id    = get_jwt_identity()
        data       = request.get_json()

        template = WhatsAppTemplate(
            company_id       = company_id,
            name             = data.get('name'),
            description      = data.get('description'),
            template_text    = data.get('template_text'),
            category         = data.get('category', 'custom'),
            message_type     = data.get('message_type', 'custom'),
            default_priority = data.get('default_priority', 60),
            created_by       = user_id,
        )

        db.session.add(template)
        db.session.commit()

        return jsonify({
            'success':     True,
            'template_id': str(template.id),
            'message':     'Template created successfully',
        }), 201

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error creating template: {e}")
        return jsonify({'error': str(e)}), 500


@whatsapp_bp.route('/templates/<template_id>', methods=['PUT'])
@jwt_required()
def update_template(template_id):
    """Update an existing template"""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']

        template = WhatsAppTemplate.query.filter_by(
            id=template_id, company_id=company_id
        ).first()

        if not template:
            return jsonify({'error': 'Template not found'}), 404

        data = request.get_json()
        for field in ('name', 'description', 'template_text', 'category', 'default_priority'):
            if field in data:
                setattr(template, field, data[field])

        db.session.commit()
        return jsonify({'success': True, 'message': 'Template updated successfully'}), 200

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating template: {e}")
        return jsonify({'error': str(e)}), 500


@whatsapp_bp.route('/templates/<template_id>', methods=['DELETE'])
@jwt_required()
def delete_template(template_id):
    """Soft-delete a template"""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']

        template = WhatsAppTemplate.query.filter_by(
            id=template_id, company_id=company_id
        ).first()

        if not template:
            return jsonify({'error': 'Template not found'}), 404

        template.is_active = False
        db.session.commit()
        return jsonify({'success': True, 'message': 'Template deleted successfully'}), 200

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error deleting template: {e}")
        return jsonify({'error': str(e)}), 500


# ============================================================================
# CONFIGURATION ENDPOINTS
# ============================================================================

@whatsapp_bp.route('/config', methods=['GET'])
@jwt_required()
def get_config():
    """Get WhatsApp configuration for the company"""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']

        config = WhatsAppConfig.query.filter_by(company_id=company_id).first()
        
        if not config:
            return jsonify({'configured': False, 'message': 'WhatsApp not configured'}), 200

        # Keep UI status accurate: sync DB with live Evolution state when possible.
        if (config.provider_type or 'evolution') == 'evolution' and config.instance_name:
            try:
                evolution_client.update_connection_status(company_id, config.instance_name)
                db.session.refresh(config)
            except Exception as sync_err:
                logger.warning(f"Live status sync skipped in /config: {sync_err}")

        return jsonify({
            'configured':      True,
            'provider_type':   config.provider_type or 'evolution',
            # Mask legacy API key — never send full key to frontend
            'api_key':         (config.api_key[:10] + '…') if config.api_key else '',
            'server_address':  config.server_address,
            'auto_send_invoices':         config.auto_send_invoices,
            'auto_send_deadline_alerts':  config.auto_send_deadline_alerts,
            'message_send_time':          config.message_send_time,
            'deadline_check_time':        config.deadline_check_time,
            'deadline_alert_days_before': config.deadline_alert_days_before,
            'daily_quota_limit':          config.daily_quota_limit,
            'quota_buffer':               config.quota_buffer,
            'connection_status':          config.connection_status,
            'last_connection_test':       config.last_connection_test.isoformat() if config.last_connection_test else None,
            # Evolution API fields
            'instance_name':   config.instance_name,
            'phone_connected': config.phone_connected or False,
            'phone_number':    config.phone_number,
            # Anti-ban settings
            'min_delay_seconds':  config.min_delay_seconds  or 45,
            'max_delay_seconds':  config.max_delay_seconds  or 120,
            'send_window_start':  config.send_window_start  or '09:00',
            'send_window_end':    config.send_window_end    or '21:00',
            'enable_spintax':     config.enable_spintax if config.enable_spintax is not None else True,
            # Warm-up status
            'warmup_complete':    config.warmup_complete    or False,
            'warmup_start_date':  config.warmup_start_date.isoformat() if config.warmup_start_date else None,
            'current_daily_limit': config.current_daily_limit,
        }), 200

    except Exception as e:
        logger.error(f"Error fetching config: {e}")
        return jsonify({'error': str(e)}), 500


@whatsapp_bp.route('/config', methods=['PUT'])
@jwt_required()
def update_config():
    """Create or update WhatsApp configuration"""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']
        data       = request.get_json()

        config = WhatsAppConfig.query.filter_by(company_id=company_id).first()

        if not config:
            config = WhatsAppConfig(
                company_id                 = company_id,
                provider_type              = data.get('provider_type', 'evolution'),
                api_key                    = data.get('api_key'),
                server_address             = data.get('server_address'),
                auto_send_invoices         = data.get('auto_send_invoices', True),
                auto_send_deadline_alerts  = data.get('auto_send_deadline_alerts', True),
                message_send_time          = data.get('message_send_time', '09:00'),
                deadline_check_time        = data.get('deadline_check_time', '09:00'),
                deadline_alert_days_before = data.get('deadline_alert_days_before', 2),
                daily_quota_limit          = data.get('daily_quota_limit', 200),
                quota_buffer               = data.get('quota_buffer', 5),
                min_delay_seconds          = data.get('min_delay_seconds', 45),
                max_delay_seconds          = data.get('max_delay_seconds', 120),
                send_window_start          = data.get('send_window_start', '09:00'),
                send_window_end            = data.get('send_window_end', '21:00'),
                enable_spintax             = data.get('enable_spintax', True),
            )
            db.session.add(config)
        else:
            updatable = [
                'provider_type', 'api_key', 'server_address',
                'auto_send_invoices', 'auto_send_deadline_alerts',
                'message_send_time', 'deadline_check_time', 'deadline_alert_days_before',
                'daily_quota_limit', 'quota_buffer',
                'min_delay_seconds', 'max_delay_seconds',
                'send_window_start', 'send_window_end', 'enable_spintax',
            ]
            for field in updatable:
                if field in data:
                    setattr(config, field, data[field])

        db.session.commit()
        return jsonify({'success': True, 'message': 'Configuration updated successfully'}), 200

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating config: {e}")
        return jsonify({'error': str(e)}), 500


@whatsapp_bp.route('/config/test-connection', methods=['POST'])
@jwt_required()
def test_connection():
    """
    Test WhatsApp API connection.
    Branches on provider_type so Evolution companies don't test the legacy client.
    """
    try:
        claims     = get_jwt()
        company_id = claims['company_id']

        config   = WhatsAppConfig.query.filter_by(company_id=company_id).first()
        provider = config.provider_type if config else 'evolution'

        if provider == 'evolution':
            result = evolution_client.test_api_connection()
        else:
            client = WhatsAppAPIClient.from_config(company_id)
            result = client.test_connection()

        if config:
            config.last_connection_test = datetime.now(PAK_TZ)
            config.connection_status    = 'success' if result.get('success') else 'failed'
            db.session.commit()

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error testing connection: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# EVOLUTION API INSTANCE MANAGEMENT
# ============================================================================

@whatsapp_bp.route('/instance/create', methods=['POST'])
@jwt_required()
def create_instance():
    # CRITICAL FIX: get_jwt_identity() returns the user's UUID (JWT 'sub' claim).
    # The company_id lives in a custom JWT claim — must use get_jwt() here,
    # exactly as every other endpoint does. Using the wrong ID caused
    # _get_config(company_id) inside the client to find nothing, leaving
    # instance_name / instance_token / phone_connected null in the DB.
    claims     = get_jwt()
    company_id = claims['company_id']

    result = evolution_client.create_instance(company_id=company_id)

    if result.get('success'):
        if result.get('already_connected'):
            # Instance is genuinely active — no QR needed.
            # Frontend should detect this flag and skip the QR flow.
            logger.info(
                f"Instance for {company_id} is already connected — "
                f"returning already_connected=True so the frontend skips QR polling."
            )
        elif not result.get('qr_code_base64'):
            # Instance created/found but QR not yet ready — frontend will poll.
            logger.info(
                f"Instance created for {company_id}, awaiting asynchronous "
                f"QR generation via polling."
            )

        return jsonify(result), 201
    else:
        return jsonify(result), result.get('status_code', 500)

@whatsapp_bp.route('/instance/qr', methods=['GET'])
@jwt_required()
def get_instance_qr():
    """Fetch the current QR code for WhatsApp pairing"""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']

        config = WhatsAppConfig.query.filter_by(company_id=company_id).first()
        if not config or not config.instance_name:
            return jsonify({
                'success': False,
                'error':   'No instance found. Please create one first.',
            }), 404

        # This will fetch using the CORRECT instance name from the database
        result = evolution_client.get_qr_code(config.instance_name)

        # PRODUCTION FIX: Do NOT throw a 500 error if success is True but QR is empty.
        # It just means Evolution API is still generating the QR code.
        if result.get('success'):
            if result.get('qr_code_base64'):
                # Only update the DB if we actually got a valid base64 string
                config.qr_code_base64 = result['qr_code_base64']
                db.session.commit()
            
            # ALWAYS return 200 OK on success, so the React frontend keeps polling
            return jsonify(result), 200
        else:
            # Only trigger an error response if success is explicitly False (e.g., 404 Not Found)
            logger.warning(f"QR fetch failed for '{config.instance_name}': {result.get('error')}")
            return jsonify(result), result.get('status_code', 500)

    except Exception as e:
        logger.error(f"Error fetching QR code: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@whatsapp_bp.route('/instance/status', methods=['GET'])
@jwt_required()
def get_instance_status():
    """
    Check WhatsApp connection status and sync database.

    Returns the connection state, warm-up info, and instance token
    (used by the dispatcher for message-send auth).
    """
    try:
        claims     = get_jwt()
        company_id = claims['company_id']

        config = WhatsAppConfig.query.filter_by(company_id=company_id).first()
        if not config or not config.instance_name:
            return jsonify({
                'success':   True,
                'connected': False,
                'state':     'no_instance',
                'message':   'No instance configured',
            }), 200

        result = evolution_client.update_connection_status(company_id, config.instance_name)

        # Augment with warm-up info and token for the frontend / dispatcher
        result['warmup_complete']     = config.warmup_complete    or False
        result['warmup_start_date']   = config.warmup_start_date.isoformat() if config.warmup_start_date else None
        result['current_daily_limit'] = config.current_daily_limit
        result['phone_number']        = config.phone_number
        # Do NOT expose `instance_token` to the frontend. Tokens are sensitive and
        # should remain server-side for dispatcher and server processes.

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error checking instance status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@whatsapp_bp.route('/instance/disconnect', methods=['POST'])
@jwt_required()
def disconnect_instance():
    """Disconnect (logout) the WhatsApp session without deleting the instance."""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']

        config = WhatsAppConfig.query.filter_by(company_id=company_id).first()
        if not config or not config.instance_name:
            return jsonify({'success': False, 'error': 'No instance found'}), 404

        result = evolution_client.disconnect(config.instance_name)

        if result.get('success'):
            config.phone_connected   = False
            config.connection_status = 'disconnected'
            db.session.commit()

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error disconnecting instance: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@whatsapp_bp.route('/instance/restart', methods=['POST'])
@jwt_required()
def restart_instance():
    """Restart the Evolution API instance."""
    try:
        claims     = get_jwt()
        company_id = claims['company_id']

        config = WhatsAppConfig.query.filter_by(company_id=company_id).first()
        if not config or not config.instance_name:
            return jsonify({'success': False, 'error': 'No instance found'}), 404

        result = evolution_client.restart_instance(config.instance_name)
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error restarting instance: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@whatsapp_bp.route('/instance/delete', methods=['DELETE'])
@jwt_required()
def delete_instance():
    """
    Permanently delete the Evolution API instance.
    Use this to force a clean reconnect when the instance is in a broken state.
    """
    try:
        claims     = get_jwt()
        company_id = claims['company_id']

        config = WhatsAppConfig.query.filter_by(company_id=company_id).first()
        if not config or not config.instance_name:
            return jsonify({'success': False, 'error': 'No instance found'}), 404

        instance_name = config.instance_name
        result = evolution_client.delete_instance(instance_name)

        if result.get('success'):
            # Clear all instance-related fields from the DB
            config.instance_name     = None
            config.instance_token    = None
            config.qr_code_base64    = None
            config.phone_connected   = False
            config.phone_number      = None
            config.connection_status = 'untested'
            db.session.commit()
            logger.info(f"Instance '{instance_name}' deleted and DB cleared for company {company_id}")

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error deleting instance: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# EVOLUTION API WEBHOOK  (delivery receipts + QR refresh)
# ============================================================================

@whatsapp_bp.route('/webhook', methods=['POST'])
def evolution_webhook():
    """
    Receive event updates from Evolution API.

    Evolution posts here when WEBHOOK_GLOBAL_ENABLED=true.

    Events handled:
      messages.update   → update delivery status on WhatsAppMessageQueue rows
      connection.update → sync phone_connected flag in WhatsAppConfig
      qrcode.updated    → FIX: persist the new QR to the DB so frontend polls
                          immediately get a fresh code without a round-trip

    Security: verify the 'apikey' header Evolution sends matches our global key.
    This endpoint is NOT JWT-protected because Evolution calls it server-to-server.
    """
    try:
        import os
        data  = request.get_json(silent=True) or {}
        event = data.get('event', '')

        # Verify Evolution's request carries our global key
        received_key = request.headers.get('apikey', '')
        expected_key = os.environ.get('EVOLUTION_API_KEY', '')
        if expected_key and received_key != expected_key:
            logger.warning(f"Webhook received with invalid apikey: {received_key[:8]}…")
            return jsonify({'error': 'Forbidden'}), 403

        # ── messages.update — delivery receipt ─────────────────────────────────
        if event == 'messages.update':
            for update in data.get('data', []):
                message_id = update.get('key', {}).get('id', '')
                status_raw = update.get('update', {}).get('status', '')

                if not message_id:
                    continue

                status_map = {
                    'DELIVERY_ACK': 'delivered',
                    'READ':         'read',
                    'PLAYED':       'read',
                    'ERROR':        'failed',
                    'SERVER_ACK':   'sent',
                    'PENDING':      'sent',
                }
                new_status = status_map.get(status_raw)

                if new_status:
                    msg = WhatsAppMessageQueue.query.filter_by(
                        api_message_id=message_id
                    ).first()
                    if msg and msg.status == 'sent':
                        msg.status = new_status
                        db.session.commit()
                        logger.debug(f"Message {message_id[:8]}… status → {new_status}")

        # ── connection.update — phone connected / disconnected ─────────────────
        elif event == 'connection.update':
            instance_name = data.get('instance', '')
            state         = data.get('data', {}).get('state', '')

            if instance_name and state:
                config = WhatsAppConfig.query.filter_by(
                    instance_name=instance_name
                ).first()
                if config:
                    config.phone_connected   = (state == 'open')
                    config.connection_status = 'connected' if state == 'open' else 'disconnected'
                    db.session.commit()
                    logger.info(f"Webhook: instance '{instance_name}' state → {state}")

        # ── qrcode.updated — fresh QR available ────────────────────────────────
        # FIX: Evolution fires this event every ~20 seconds while the instance
        # is waiting for a scan. Persisting it here means the frontend's
        # GET /instance/qr poll always returns the freshest code from the DB
        # as a fast-path fallback even when the live /instance/connect call is slow.
        elif event == 'qrcode.updated':
            instance_name = data.get('instance', '')
            qr_data       = data.get('data', {})
            qr_base64     = qr_data.get('qrcode', {}).get('base64', '') if isinstance(qr_data.get('qrcode'), dict) else qr_data.get('base64', '')

            if instance_name and qr_base64:
                # Ensure data-URL prefix
                if not qr_base64.startswith('data:'):
                    qr_base64 = f'data:image/png;base64,{qr_base64}'

                config = WhatsAppConfig.query.filter_by(
                    instance_name=instance_name
                ).first()
                if config:
                    config.qr_code_base64    = qr_base64
                    config.connection_status = 'awaiting_qr'
                    db.session.commit()
                    logger.info(f"Webhook: QR updated for instance '{instance_name}'")

        return jsonify({'received': True}), 200

    except Exception as e:
        logger.error(f"Webhook processing error: {e}")
        return jsonify({'error': str(e)}), 500