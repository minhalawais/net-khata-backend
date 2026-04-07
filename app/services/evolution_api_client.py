"""
Evolution API Client (v2-compatible)
Self-hosted WhatsApp gateway using Baileys.

Fixes applied (cumulative):
  v1 → v2:
    - API key loaded from environment, never hardcoded
    - Global token vs instance token split
    - get_qr_code parses correct nested response structure
    - send_media includes required mimetype field
    - send_text includes optional delay field for typing indicator
    - Singleton session reuse pattern
    - v2 payload fields used consistently

  v2 → v3:
    - _handle_existing_instance: distinguishes between get_qr_code returning
      success=True/empty-QR (instance loading after container restart, not ready)
      vs success=False (actual API error). Previously both paths triggered
      delete-and-recreate immediately, causing the FK cascade bug below.
    - _handle_existing_instance: when QR is empty, checks if instance is already
      connected; if not, calls restart_instance + waits 4 s + retries get_qr_code
      before escalating to delete-and-recreate.
    - _delete_and_recreate: generates a NEW timestamp-suffixed instance name
      instead of reusing the old one. Evolution v2.2.3 has a cascade-delete bug:
      DELETE /instance/delete does NOT clean up Setting table rows. Recreating
      with the same name collides with the orphaned FK and returns 400.
      A fresh name = new UUID = no orphan conflict.
    - _create_fresh: separate internal method called by _delete_and_recreate to
      avoid going through the 403 handler recursively.
    - send_text / send_media: use self._message_session (pooled) instead of
      bare requests.post() per call.

  v3 → v4 (current):
    - _handle_existing_instance Step 2a: ZOMBIE STATE FIX.
      After user explicitly disconnects, Baileys keeps the WebSocket in a
      half-open state for ~60s while sending keep-alive probes that all time
      out. During this window, GET /instance/connectionState/{name} returns
      state='open' — a phantom / zombie connection. The old code trusted this
      and called _mark_connected_and_return(), setting phone_connected=True in
      the DB while the WhatsApp session was actually dead. This caused:
        a. DB in wrong state (thinks it's connected)
        b. Frontend starting QR polling loop that always got 404
        c. QR spinner stuck forever
      FIX: cross-check our own DB. If WhatsAppConfig.phone_connected is False,
      the user explicitly disconnected and Evolution's 'open' state is stale.
      We now treat this as NOT connected and proceed to restart the instance to
      regenerate a fresh QR, bypassing the phantom state entirely.
"""

import os
import time
import random
import requests
import logging
from typing import Dict, Any, Optional
from app.models import WhatsAppConfig
from app import db
from datetime import datetime, date
import pytz

logger = logging.getLogger(__name__)

PAK_TZ = pytz.timezone('Asia/Karachi')

# ── Configuration ──────────────────────────────────────────────────────────────
_EVOLUTION_BASE_URL = os.environ.get('EVOLUTION_API_URL', 'http://localhost:8081')
_EVOLUTION_API_KEY  = os.environ.get('EVOLUTION_API_KEY', '')

if not _EVOLUTION_API_KEY:
    logger.warning(
        "EVOLUTION_API_KEY environment variable is not set. "
        "Evolution API calls will fail authentication."
    )

_MIME_MAP: Dict[str, str] = {
    'document': 'application/pdf',
    'image':    'image/jpeg',
    'video':    'video/mp4',
    'audio':    'audio/ogg; codecs=opus',
}

# Seconds to wait after restart_instance before retrying get_qr_code.
# Evolution needs time to reload the session from its DB and generate a QR.
_RESTART_WAIT_SECONDS = 10


class EvolutionAPIClient:
    """
    Client for the self-hosted Evolution API v2.

    Auth:
      - _global_session  → instance-management endpoints (/instance/*)
                           global API key baked into session headers.
      - _message_session → message-sending endpoints (/message/*)
                           per-instance token supplied per-call.
                           Pooled so no TCP handshake per message under bulk load.
    """

    def __init__(self, base_url: str = None, global_api_key: str = None):
        self.base_url       = (base_url or _EVOLUTION_BASE_URL).rstrip('/')
        self.global_api_key = global_api_key or _EVOLUTION_API_KEY

        self._global_session = requests.Session()
        self._global_session.headers.update({
            'Content-Type': 'application/json',
            'apikey':       self.global_api_key,
        })

        self._message_session = requests.Session()
        self._message_session.headers.update({'Content-Type': 'application/json'})

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _message_headers(self, instance_token: str) -> Dict[str, str]:
        return {
            'Content-Type': 'application/json',
            'apikey': instance_token or self.global_api_key,
        }

    @staticmethod
    def _get_config(company_id: str) -> Optional[WhatsAppConfig]:
        return WhatsAppConfig.query.filter_by(company_id=company_id).first()

    @staticmethod
    def _ensure_qr_prefix(qr: str) -> str:
        """Guarantee the QR string carries the data-URL prefix browsers expect."""
        if qr and not qr.startswith('data:'):
            return f'data:image/png;base64,{qr}'
        return qr or ''

    @staticmethod
    def _base_instance_name(company_id: str) -> str:
        """Canonical name used on first-ever creation (no timestamp suffix)."""
        return f"netkhata_{str(company_id)[:8]}"

    @staticmethod
    def _new_instance_name(company_id: str) -> str:
        """
        Fresh unique name used when recreating after a delete.
        The 5-digit time suffix changes every second so it never collides with
        the old (possibly orphaned) name.
        """
        suffix = int(time.time()) % 100000
        return f"netkhata_{str(company_id)[:8]}_{suffix}"

    # ── Instance Management ────────────────────────────────────────────────────

    def create_instance(
        self,
        company_id: str,
        instance_name: str = None,
    ) -> Dict[str, Any]:
        """
        Create a new Evolution API v2 instance and return a QR code for pairing.

        403 handling is delegated to _handle_existing_instance which implements
        a three-step recovery (get QR → restart+retry → delete-and-recreate)
        before giving up.

        Returns:
            dict: success, instance_name, instance_token, qr_code_base64, message
        """
        try:
            if not instance_name:
                instance_name = self._base_instance_name(company_id)

            payload = {
                "instanceName":    instance_name,
                "integration":     "WHATSAPP-BAILEYS",
                "qrcode":          True,
                "rejectCall":      False,
                "msgCall":         "",
                "groupsIgnore":    True,
                "alwaysOnline":    False,
                "readMessages":    False,
                "readStatus":      False,
                "syncFullHistory": False,
            }

            logger.info(
                f"Creating Evolution instance '{instance_name}' for company {company_id}"
            )
            response = self._global_session.post(
                f"{self.base_url}/instance/create",
                json=payload,
                timeout=30,
            )

            if response.status_code in (200, 201):
                data           = response.json()
                instance_data  = data.get('instance', {})
                instance_token = instance_data.get('token', '')
                qr_base64      = self._extract_qr_from_response(data, instance_data)

                config = self._get_config(company_id)
                if config:
                    config.instance_name     = instance_name
                    config.instance_token    = instance_token
                    config.qr_code_base64    = qr_base64
                    config.phone_connected   = False
                    config.connection_status = 'awaiting_qr'
                    db.session.commit()

                logger.info(f"Instance '{instance_name}' created successfully")
                return {
                    'success':        True,
                    'instance_name':  instance_name,
                    'instance_token': instance_token,
                    'qr_code_base64': qr_base64,
                    'message':        'Instance created. Scan QR code to connect.',
                }

            elif response.status_code == 403:
                return self._handle_existing_instance(
                    company_id, instance_name, response
                )

            else:
                logger.error(
                    f"Failed to create instance: {response.status_code} — {response.text}"
                )
                return {
                    'success':     False,
                    'error':       response.text,
                    'status_code': response.status_code,
                }

        except requests.exceptions.ConnectionError:
            logger.error("Cannot connect to Evolution API. Is the Docker container running?")
            return {
                'success': False,
                'error':   'Cannot connect to Evolution API. Ensure the Docker container is running.',
            }
        except Exception as e:
            logger.error(f"Error creating instance: {e}")
            return {'success': False, 'error': str(e)}

    # ── 403 Recovery Logic ─────────────────────────────────────────────────────

    def _handle_existing_instance(
        self,
        company_id: str,
        instance_name: str,
        create_response,
    ) -> Dict[str, Any]:
        """
        Called when POST /instance/create returns 403 (name already in use).

        Three-step recovery cascade:

        Step 1 — get_qr_code() returns success + non-empty QR
            → Return QR immediately. Happy path.

        Step 2 — get_qr_code() returns success + EMPTY QR  (count=0)
            This is the common case after a container restart: Evolution loaded
            the instance from its PostgreSQL DB but the Baileys session hasn't
            initialised yet. Empty QR does NOT mean broken — it means "not ready".
            2a. check_connection() → already open  →  return already-connected.
            2b. Not connected → restart_instance() → sleep 4s → retry get_qr_code.

        Step 3 — get_qr_code() returns failure  OR  restart+retry still empty
            → _delete_and_recreate() with a NEW unique name.
        """
        logger.warning(
            f"Instance '{instance_name}' already exists in Evolution — "
            f"starting three-step QR recovery"
        )

        # ── Step 1: try to get a live QR ─────────────────────────────────────
        qr_result = self.get_qr_code(instance_name)

        if qr_result.get('success') and qr_result.get('qr_code_base64'):
            logger.info(f"Step 1 succeeded: got QR directly for '{instance_name}'")
            return self._persist_and_return_qr(
                company_id, instance_name, qr_result['qr_code_base64'],
                message='Instance already existed — fresh QR generated. Scan to connect.',
            )

        # ── Step 2: QR is empty (instance loading) or fetch failed ───────────
        if qr_result.get('success') and not qr_result.get('qr_code_base64'):
            # Evolution returned HTTP 200 but base64='', count=0.
            # Instance is in DB but Baileys hasn't generated a QR yet.
            logger.info(
                f"Step 2: '{instance_name}' returned empty QR "
                f"(count={qr_result.get('count', 0)}) — checking connection state"
            )

            conn = self.check_connection(instance_name)
            if conn.get('connected'):
                # ── ZOMBIE STATE GUARD ────────────────────────────────────────
                # Evolution can report state='open' for up to ~60 seconds after
                # a disconnect while Baileys exhausts its keep-alive probes.
                # This is a PHANTOM connection — the WhatsApp session is dead.
                #
                # Cross-check our own DB: if phone_connected=False the user
                # explicitly disconnected, so Evolution's 'open' is stale.
                # We skip 'already connected' and force a restart to get a fresh
                # QR code instead of leaving the user stuck with a dead session.
                # ─────────────────────────────────────────────────────────────
                config = self._get_config(company_id)
                db_says_connected = config.phone_connected if config else True

                if not db_says_connected:
                    logger.warning(
                        f"Step 2a ZOMBIE DETECTED: Evolution reports '{instance_name}' as "
                        f"'open' but DB says phone_connected=False (user explicitly "
                        f"disconnected). Treating as stale state — forcing restart to "
                        f"regenerate QR."
                    )
                    # Fall through to Step 2b (restart + retry QR)
                else:
                    logger.info(f"Step 2a: '{instance_name}' is genuinely connected — no QR needed")
                    return self._mark_connected_and_return(
                        company_id, instance_name, conn.get('phone_number', '')
                    )

            # Not connected — restart to force QR generation
            logger.info(
                f"Step 2b: '{instance_name}' not connected — "
                f"restarting to trigger QR generation"
            )
            restart = self.restart_instance(instance_name)

            if restart.get('success'):
                logger.info(
                    f"Restart issued — waiting {_RESTART_WAIT_SECONDS}s for QR generation"
                )
                time.sleep(_RESTART_WAIT_SECONDS)

                qr_retry = self.get_qr_code(instance_name)
                if qr_retry.get('success') and qr_retry.get('qr_code_base64'):
                    logger.info(f"Step 2b succeeded: QR obtained after restart")
                    return self._persist_and_return_qr(
                        company_id, instance_name, qr_retry['qr_code_base64'],
                        message='Instance restarted — fresh QR generated. Scan to connect.',
                    )

                logger.warning(
                    f"QR still empty after restart for '{instance_name}' "
                    f"(count={qr_retry.get('count', 0)}) — escalating to delete-and-recreate"
                )
            else:
                logger.warning(
                    f"Restart failed for '{instance_name}': {restart.get('error')} "
                    f"— escalating to delete-and-recreate"
                )

        else:
            # get_qr_code returned success=False (e.g. 404)
            logger.warning(
                f"get_qr_code returned an error for '{instance_name}': "
                f"{qr_result.get('error')} — escalating to delete-and-recreate"
            )

        # ── Step 3: delete and recreate with a NEW name ───────────────────────
        return self._delete_and_recreate(company_id, instance_name)

    def _persist_and_return_qr(
        self,
        company_id: str,
        instance_name: str,
        qr_base64: str,
        message: str = '',
    ) -> Dict[str, Any]:
        """Persist QR + awaiting_qr status to DB and return a success response."""
        config = self._get_config(company_id)
        if config:
            config.instance_name     = instance_name
            config.qr_code_base64    = qr_base64
            config.phone_connected   = False
            config.connection_status = 'awaiting_qr'
            db.session.commit()

        return {
            'success':        True,
            'instance_name':  instance_name,
            'instance_token': config.instance_token if config else '',
            'qr_code_base64': qr_base64,
            'message':        message or 'QR code ready. Scan to connect.',
        }

    def _mark_connected_and_return(
        self,
        company_id: str,
        instance_name: str,
        phone_number: str = '',
    ) -> Dict[str, Any]:
        """Persist connected status to DB and return an already-connected response."""
        config = self._get_config(company_id)
        if config:
            config.instance_name     = instance_name
            config.phone_connected   = True
            config.connection_status = 'connected'
            if phone_number:
                config.phone_number = phone_number
                if not config.warmup_start_date:
                    config.warmup_start_date = datetime.now(PAK_TZ).date()
            db.session.commit()

        return {
            'success':           True,
            'instance_name':     instance_name,
            'instance_token':    config.instance_token if config else '',
            'qr_code_base64':    '',
            'already_connected': True,
            'message':           'WhatsApp is already connected. No QR code needed.',
        }

    def _delete_and_recreate(
        self,
        company_id: str,
        old_instance_name: str,
    ) -> Dict[str, Any]:
        """
        Last-resort recovery: delete the broken instance, then create a fresh one
        under a NEW name.

        WHY a new name — Evolution v2.2.3 cascade-delete bug:
        DELETE /instance/delete removes the Instance record but leaves orphaned
        rows in the Setting (and IntegrationSession) tables. These orphaned rows
        still carry the old UUID as a foreign key. When create_instance is called
        with the SAME name it gets a new UUID but Evolution's Prisma layer tries
        to upsert Setting rows and collides with the orphans, producing:
            400 Bad Request — Foreign key constraint violated: Setting_instanceId_fkey

        Solution: use a fresh timestamp-suffixed name. New name = new UUID =
        completely clean Setting rows = no orphan conflict.
        """
        logger.info(
            f"_delete_and_recreate: deleting '{old_instance_name}' "
            f"and recreating with a new name"
        )

        # Best-effort delete — proceed even if it fails
        delete_result = self.delete_instance(old_instance_name)
        if delete_result.get('success'):
            logger.info(f"Deleted instance '{old_instance_name}'")
        else:
            logger.warning(
                f"Could not delete '{old_instance_name}': "
                f"{delete_result.get('error')} — will still create with new name"
            )

        new_name = self._new_instance_name(company_id)
        logger.info(
            f"Creating replacement instance as '{new_name}' "
            f"(new name bypasses Evolution v2.2.3 FK cascade bug)"
        )
        return self._create_fresh(company_id, new_name)

    def _create_fresh(self, company_id: str, instance_name: str) -> Dict[str, Any]:
        """
        Issue POST /instance/create for a brand-new name.
        Called only from _delete_and_recreate — NOT through create_instance()
        — to avoid going through the 403 handler recursively.
        """
        payload = {
            "instanceName":    instance_name,
            "integration":     "WHATSAPP-BAILEYS",
            "qrcode":          True,
            "rejectCall":      True,
            "msgCall":         "Sorry, we cannot take calls on this number.",
            "groupsIgnore":    True,
            "alwaysOnline":    False,
            "readMessages":    False,
            "readStatus":      False,
            "syncFullHistory": False,
        }

        try:
            response = self._global_session.post(
                f"{self.base_url}/instance/create",
                json=payload,
                timeout=30,
            )

            if response.status_code in (200, 201):
                data           = response.json()
                instance_data  = data.get('instance', {})
                instance_token = instance_data.get('token', '')
                qr_base64      = self._extract_qr_from_response(data, instance_data)

                config = self._get_config(company_id)
                if config:
                    config.instance_name     = instance_name
                    config.instance_token    = instance_token
                    config.qr_code_base64    = qr_base64
                    config.phone_connected   = False
                    config.connection_status = 'awaiting_qr'
                    db.session.commit()

                logger.info(f"Fresh instance '{instance_name}' created successfully")
                return {
                    'success':        True,
                    'instance_name':  instance_name,
                    'instance_token': instance_token,
                    'qr_code_base64': qr_base64,
                    'message':        'Instance recreated. Scan QR code to connect.',
                }
            else:
                logger.error(
                    f"_create_fresh failed for '{instance_name}': "
                    f"{response.status_code} — {response.text}"
                )
                return {
                    'success':     False,
                    'error':       response.text,
                    'status_code': response.status_code,
                }

        except requests.exceptions.ConnectionError:
            return {
                'success': False,
                'error':   'Cannot connect to Evolution API. Ensure the Docker container is running.',
            }
        except Exception as e:
            logger.error(f"_create_fresh error for '{instance_name}': {e}")
            return {'success': False, 'error': str(e)}

    # ── QR Code ────────────────────────────────────────────────────────────────

    def _extract_qr_from_response(self, data: dict, instance_data: dict) -> str:
        """
        Parse the QR base64 string from a /instance/create response.
        Handles multiple known response shapes across Evolution v2.x builds.
        """
        qr_base64 = ''

        qr_candidate = (
            data.get('qrcode')
            or instance_data.get('qrcode')
            or data.get('qr')
        )
        if isinstance(qr_candidate, dict):
            qr_base64 = qr_candidate.get('base64', '') or qr_candidate.get('data', '')
        elif isinstance(qr_candidate, str):
            qr_base64 = qr_candidate

        if not qr_base64:
            qr_base64 = data.get('base64', '')

        if not qr_base64:
            qr_base64 = self._deep_find_base64(data)

        return self._ensure_qr_prefix(qr_base64)

    @staticmethod
    def _deep_find_base64(obj) -> str:
        if isinstance(obj, dict):
            if 'base64' in obj and isinstance(obj['base64'], str):
                return obj['base64']
            for v in obj.values():
                found = EvolutionAPIClient._deep_find_base64(v)
                if found:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = EvolutionAPIClient._deep_find_base64(item)
                if found:
                    return found
        return ''

    def get_qr_code(self, instance_name: str) -> Dict[str, Any]:
        """
        Fetch the current QR code from Evolution API.
        Uses deep search to find the base64 string regardless of Evolution v2's nesting.
        """
        try:
            response = self._global_session.get(
                f"{self.base_url}/instance/connect/{instance_name}",
                timeout=15,
            )

            if response.status_code == 200:
                data = response.json()
                
                # Deep search helper to find the 'base64' string anywhere in the response
                def _find_base64(obj):
                    if isinstance(obj, dict):
                        if 'base64' in obj and isinstance(obj['base64'], str):
                            return obj['base64']
                        for v in obj.values():
                            found = _find_base64(v)
                            if found: return found
                    elif isinstance(obj, list):
                        for item in obj:
                            found = _find_base64(item)
                            if found: return found
                    return ''

                qr_base64 = _find_base64(data)

                # Format strictly for React <img> tag
                if qr_base64 and not qr_base64.startswith('data:'):
                    qr_base64 = f'data:image/png;base64,{qr_base64}'

                return {
                    'success':        True,
                    'qr_code_base64': qr_base64,
                    'count':          data.get('count', 0),
                }

            elif response.status_code == 404:
                return {'success': False, 'error': 'Instance not found in Evolution API.'}
            else:
                return {'success': False, 'error': response.text}

        except Exception as e:
            logger.error(f"Error fetching QR code: {e}")
            return {'success': False, 'error': str(e)}
    # ── Connection Status ──────────────────────────────────────────────────────

    def check_connection(self, instance_name: str) -> Dict[str, Any]:
        """
        Check the connection state of an instance.

        Evolution v2 endpoint: GET /instance/connectionState/{instanceName}
        Response: { "instance": { "instanceName": "...", "state": "open" } }

        Returns:
            dict: success, connected (bool), state (str), phone_number (str)
        """
        try:
            response = self._global_session.get(
                f"{self.base_url}/instance/connectionState/{instance_name}",
                timeout=15,
            )

            if response.status_code == 200:
                data          = response.json()
                instance_data = data.get('instance', data)
                state         = instance_data.get('state', 'close')
                connected     = state == 'open'

                return {
                    'success':      True,
                    'connected':    connected,
                    'state':        state,
                    'phone_number': instance_data.get('phoneNumber', ''),
                }
            else:
                return {
                    'success':   False,
                    'connected': False,
                    'state':     'error',
                    'error':     response.text,
                }

        except Exception as e:
            logger.error(f"Error checking connection: {e}")
            return {
                'success':   False,
                'connected': False,
                'state':     'error',
                'error':     str(e),
            }

    def update_connection_status(
        self, company_id: str, instance_name: str
    ) -> Dict[str, Any]:
        """Check connection state and sync the WhatsAppConfig DB row."""
        result = self.check_connection(instance_name)

        config = self._get_config(company_id)
        if config and result.get('success'):
            config.phone_connected      = result['connected']
            config.connection_status    = 'connected' if result['connected'] else 'disconnected'
            config.last_connection_test = datetime.now(PAK_TZ)

            if result['connected'] and result.get('phone_number'):
                config.phone_number = result['phone_number']
                if not config.warmup_start_date:
                    config.warmup_start_date = datetime.now(PAK_TZ).date()

            db.session.commit()

        return result

    # ── Instance Lifecycle ─────────────────────────────────────────────────────

    def disconnect(self, instance_name: str) -> Dict[str, Any]:
        """
        Logout (disconnect) the WhatsApp session without deleting the instance.
        Evolution v2 endpoint: DELETE /instance/logout/{instanceName}
        """
        try:
            response = self._global_session.delete(
                f"{self.base_url}/instance/logout/{instance_name}",
                timeout=15,
            )
            if response.status_code == 200:
                logger.info(f"Instance '{instance_name}' disconnected")
                return {'success': True, 'message': 'WhatsApp disconnected successfully'}
            return {
                'success':     False,
                'error':       response.text,
                'status_code': response.status_code,
            }
        except Exception as e:
            logger.error(f"Error disconnecting instance: {e}")
            return {'success': False, 'error': str(e)}

    def restart_instance(self, instance_name: str) -> Dict[str, Any]:
        """
        Restart an Evolution API instance.
        Evolution v2 endpoint: PUT /instance/restart/{instanceName}
        """
        try:
            response = self._global_session.put(
                f"{self.base_url}/instance/restart/{instance_name}",
                timeout=15,
            )
            if response.status_code == 200:
                logger.info(f"Instance '{instance_name}' restarted")
                return {'success': True, 'message': 'Instance restarted successfully'}
            return {
                'success':     False,
                'error':       response.text,
                'status_code': response.status_code,
            }
        except Exception as e:
            logger.error(f"Error restarting instance: {e}")
            return {'success': False, 'error': str(e)}

    def delete_instance(self, instance_name: str) -> Dict[str, Any]:
        """
        Permanently delete an Evolution API instance.
        Evolution v2 endpoint: DELETE /instance/delete/{instanceName}

        See _delete_and_recreate docstring for the v2.2.3 cascade-delete bug
        and why a new name must be used when recreating after deletion.
        """
        try:
            response = self._global_session.delete(
                f"{self.base_url}/instance/delete/{instance_name}",
                timeout=15,
            )
            if response.status_code == 200:
                logger.info(f"Instance '{instance_name}' deleted")
                return {'success': True, 'message': 'Instance deleted successfully'}
            return {
                'success':     False,
                'error':       response.text,
                'status_code': response.status_code,
            }
        except Exception as e:
            logger.error(f"Error deleting instance: {e}")
            return {'success': False, 'error': str(e)}

    # ── Message Sending ────────────────────────────────────────────────────────

    def send_text(
        self,
        instance_name: str,
        mobile: str,
        message: str,
        instance_token: str = None,
        typing_delay_ms: int = None,
    ) -> Dict[str, Any]:
        """
        Send a plain-text message via Evolution API v2.

        Evolution v2 endpoint: POST /message/sendText/{instanceName}
        Headers: apikey = instance_token  (NOT the global key)

        Returns:
            dict: success, message_id, response, status_code
        """
        try:
            clean_number = mobile.replace('+', '').replace(' ', '').replace('-', '')
            delay = (
                typing_delay_ms
                if typing_delay_ms is not None
                else random.randint(1000, 3000)
            )

            payload = {
                "number": clean_number,
                "text":   message,
                "delay":  delay,
            }

            response = self._message_session.post(
                f"{self.base_url}/message/sendText/{instance_name}",
                json=payload,
                headers=self._message_headers(instance_token),
                timeout=30,
            )

            if response.status_code in (200, 201):
                data = response.json()
                logger.info(f"Text sent to {mobile} via '{instance_name}'")
                return {
                    'success':     True,
                    'message_id':  data.get('key', {}).get('id', ''),
                    'response':    data,
                    'status_code': response.status_code,
                }
            elif response.status_code == 404:
                logger.error(f"Instance '{instance_name}' not found on send")
                return {
                    'success':         False,
                    'error':           'Instance not found. Please reconnect.',
                    'status_code':     404,
                    'needs_reconnect': True,
                }
            elif response.status_code == 401:
                logger.error(f"Unauthorized for instance '{instance_name}'")
                return {
                    'success':         False,
                    'error':           'Authentication failed. Check instance token.',
                    'status_code':     401,
                    'needs_reconnect': True,
                }
            else:
                logger.error(f"send_text failed: {response.status_code} — {response.text}")
                return {
                    'success':     False,
                    'error':       response.text,
                    'status_code': response.status_code,
                }

        except requests.exceptions.ConnectionError:
            return {'success': False, 'error': 'Cannot connect to Evolution API', 'status_code': None}
        except Exception as e:
            logger.error(f"Error sending text message: {e}")
            return {'success': False, 'error': str(e)}

    def send_media(
        self,
        instance_name: str,
        mobile: str,
        media_url: str,
        media_type: str = 'document',
        caption: str = '',
        filename: str = '',
        instance_token: str = None,
        mimetype: str = None,
    ) -> Dict[str, Any]:
        """
        Send a media message (image / document / video / audio) via Evolution API v2.

        Evolution v2 endpoint: POST /message/sendMedia/{instanceName}
        Headers: apikey = instance_token

        Returns:
            dict: success, message_id, response, status_code
        """
        try:
            clean_number  = mobile.replace('+', '').replace(' ', '').replace('-', '')
            resolved_mime = mimetype or _MIME_MAP.get(media_type, 'application/octet-stream')

            payload: Dict[str, Any] = {
                "number":    clean_number,
                "mediatype": media_type,
                "mimetype":  resolved_mime,
                "media":     media_url,
                "caption":   caption,
            }
            if filename:
                payload["fileName"] = filename

            response = self._message_session.post(
                f"{self.base_url}/message/sendMedia/{instance_name}",
                json=payload,
                headers=self._message_headers(instance_token),
                timeout=60,
            )

            if response.status_code in (200, 201):
                data = response.json()
                logger.info(f"Media ({media_type}) sent to {mobile} via '{instance_name}'")
                return {
                    'success':     True,
                    'message_id':  data.get('key', {}).get('id', ''),
                    'response':    data,
                    'status_code': response.status_code,
                }
            elif response.status_code == 404:
                return {
                    'success':         False,
                    'error':           'Instance not found. Please reconnect.',
                    'status_code':     404,
                    'needs_reconnect': True,
                }
            else:
                logger.error(f"send_media failed: {response.status_code} — {response.text}")
                return {
                    'success':     False,
                    'error':       response.text,
                    'status_code': response.status_code,
                }

        except Exception as e:
            logger.error(f"Error sending media message: {e}")
            return {'success': False, 'error': str(e)}

    # ── Connection Test ────────────────────────────────────────────────────────

    def test_api_connection(self) -> Dict[str, Any]:
        """
        Verify the Evolution API server is reachable.
        GET /instance/fetchInstances — requires global key.
        Tokens are NOT exposed here when AUTHENTICATION_EXPOSE_IN_FETCH_INSTANCES=false.
        Our code no longer relies on extracting tokens from this endpoint.
        """
        try:
            response = self._global_session.get(
                f"{self.base_url}/instance/fetchInstances",
                timeout=10,
            )
            return {
                'success':     True,
                'reachable':   True,
                'status_code': response.status_code,
                'message':     'Evolution API is reachable',
            }
        except requests.exceptions.ConnectionError:
            return {
                'success':   False,
                'reachable': False,
                'error':     'Cannot connect to Evolution API. Is Docker running?',
                'message':   'Evolution API is not reachable',
            }
        except Exception as e:
            return {'success': False, 'reachable': False, 'error': str(e)}


# ── Module-level singleton ─────────────────────────────────────────────────────
evolution_client = EvolutionAPIClient()