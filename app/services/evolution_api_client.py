"""
Evolution API Client
Wrapper for the self-hosted Evolution API (Baileys-based WhatsApp gateway).
Handles instance management, QR code pairing, and message sending.
"""

import requests
import logging
from typing import Dict, Any, Optional
from app.models import WhatsAppConfig
from app import db
from datetime import datetime, date

logger = logging.getLogger(__name__)


# Default Evolution API base URL (Docker container on port 8081)
DEFAULT_EVOLUTION_URL = 'http://localhost:8081'
DEFAULT_EVOLUTION_API_KEY = 'netkhata-evo-api-key-2025'


class EvolutionAPIClient:
    """Client for interacting with the self-hosted Evolution API"""
    
    def __init__(self, base_url: str = None, global_api_key: str = None):
        """
        Initialize Evolution API client.
        
        Args:
            base_url: Evolution API server URL (default: http://localhost:8081)
            global_api_key: Global API key for Evolution API authentication
        """
        self.base_url = (base_url or DEFAULT_EVOLUTION_URL).rstrip('/')
        self.global_api_key = global_api_key or DEFAULT_EVOLUTION_API_KEY
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'apikey': self.global_api_key
        })
    
    # ------------------------------------------------------------------
    # Instance Management
    # ------------------------------------------------------------------
    
    def create_instance(self, company_id: str, instance_name: str = None) -> Dict[str, Any]:
        """
        Create a new Evolution API instance for a company.
        Generates a QR code automatically for WhatsApp pairing.
        
        Args:
            company_id: Company UUID string
            instance_name: Optional custom instance name (default: netkhata_{company_id[:8]})
            
        Returns:
            dict with success status, instance_name, instance_token, and optional qr_code
        """
        try:
            if not instance_name:
                instance_name = f"netkhata_{str(company_id)[:8]}"
            
            payload = {
                "instanceName": instance_name,
                "integration": "WHATSAPP-BAILEYS",
                "qrcode": True,
                "rejectCall": True,
                "msgCall": "Sorry, we cannot take calls on this number.",
                "groupsIgnore": True,
                "alwaysOnline": False,
                "readMessages": False,
                "readStatus": False,
                "syncFullHistory": False
            }
            
            logger.info(f"Creating Evolution instance '{instance_name}' for company {company_id}")
            response = self.session.post(
                f"{self.base_url}/instance/create",
                json=payload,
                timeout=30
            )
            
            if response.status_code == 201 or response.status_code == 200:
                data = response.json()
                
                # Extract instance token and QR code
                instance_data = data.get('instance', data)
                instance_token = instance_data.get('token', '')
                qr_code = data.get('qrcode', {})
                qr_base64 = qr_code.get('base64', '') if isinstance(qr_code, dict) else ''
                
                # Save to database
                config = WhatsAppConfig.query.filter_by(company_id=company_id).first()
                if config:
                    config.instance_name = instance_name
                    config.instance_token = instance_token
                    config.qr_code_base64 = qr_base64
                    config.phone_connected = False
                    config.connection_status = 'awaiting_qr'
                    db.session.commit()
                
                logger.info(f"Instance '{instance_name}' created successfully")
                return {
                    'success': True,
                    'instance_name': instance_name,
                    'instance_token': instance_token,
                    'qr_code_base64': qr_base64,
                    'message': 'Instance created. Scan QR code to connect.'
                }
            else:
                error_msg = response.text
                logger.error(f"Failed to create instance: {response.status_code} - {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'status_code': response.status_code
                }
                
        except requests.exceptions.ConnectionError:
            logger.error("Cannot connect to Evolution API. Is Docker container running?")
            return {
                'success': False,
                'error': 'Cannot connect to Evolution API. Please ensure Docker container is running.',
                'status_code': None
            }
        except Exception as e:
            logger.error(f"Error creating instance: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def get_qr_code(self, instance_name: str) -> Dict[str, Any]:
        """
        Fetch the latest QR code for an instance.
        
        Args:
            instance_name: Evolution instance name
            
        Returns:
            dict with success status and qr_code_base64
        """
        try:
            response = self.session.get(
                f"{self.base_url}/instance/connect/{instance_name}",
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                qr_base64 = data.get('base64', '')
                
                return {
                    'success': True,
                    'qr_code_base64': qr_base64,
                    'state': data.get('state', 'unknown')
                }
            else:
                return {
                    'success': False,
                    'error': response.text,
                    'status_code': response.status_code
                }
                
        except Exception as e:
            logger.error(f"Error fetching QR code: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def check_connection(self, instance_name: str) -> Dict[str, Any]:
        """
        Check the connection state of an instance.
        
        Args:
            instance_name: Evolution instance name
            
        Returns:
            dict with connected status, phone_number, and state
        """
        try:
            response = self.session.get(
                f"{self.base_url}/instance/connectionState/{instance_name}",
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                instance_data = data.get('instance', data)
                state = instance_data.get('state', 'close')
                connected = state == 'open'
                
                return {
                    'success': True,
                    'connected': connected,
                    'state': state,
                    'phone_number': instance_data.get('phoneNumber', '')
                }
            else:
                return {
                    'success': False,
                    'connected': False,
                    'state': 'error',
                    'error': response.text
                }
                
        except Exception as e:
            logger.error(f"Error checking connection: {str(e)}")
            return {
                'success': False,
                'connected': False,
                'state': 'error',
                'error': str(e)
            }
    
    def update_connection_status(self, company_id: str, instance_name: str) -> Dict[str, Any]:
        """
        Check connection and update the database config accordingly.
        
        Args:
            company_id: Company UUID
            instance_name: Evolution instance name
            
        Returns:
            dict with connection status
        """
        result = self.check_connection(instance_name)
        
        config = WhatsAppConfig.query.filter_by(company_id=company_id).first()
        if config and result.get('success'):
            config.phone_connected = result['connected']
            config.connection_status = 'connected' if result['connected'] else 'disconnected'
            config.last_connection_test = datetime.now()
            
            if result['connected'] and result.get('phone_number'):
                config.phone_number = result['phone_number']
                # Start warm-up tracking on first connection
                if not config.warmup_start_date:
                    config.warmup_start_date = date.today()
            
            db.session.commit()
        
        return result
    
    def disconnect(self, instance_name: str) -> Dict[str, Any]:
        """
        Disconnect (logout) a WhatsApp session.
        
        Args:
            instance_name: Evolution instance name
            
        Returns:
            dict with success status
        """
        try:
            response = self.session.delete(
                f"{self.base_url}/instance/logout/{instance_name}",
                timeout=15
            )
            
            if response.status_code == 200:
                logger.info(f"Instance '{instance_name}' disconnected")
                return {'success': True, 'message': 'WhatsApp disconnected successfully'}
            else:
                return {
                    'success': False,
                    'error': response.text,
                    'status_code': response.status_code
                }
                
        except Exception as e:
            logger.error(f"Error disconnecting instance: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def restart_instance(self, instance_name: str) -> Dict[str, Any]:
        """
        Restart an Evolution API instance.
        
        Args:
            instance_name: Evolution instance name
            
        Returns:
            dict with success status
        """
        try:
            response = self.session.put(
                f"{self.base_url}/instance/restart/{instance_name}",
                timeout=15
            )
            
            if response.status_code == 200:
                logger.info(f"Instance '{instance_name}' restarted")
                return {'success': True, 'message': 'Instance restarted successfully'}
            else:
                return {
                    'success': False,
                    'error': response.text,
                    'status_code': response.status_code
                }
                
        except Exception as e:
            logger.error(f"Error restarting instance: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def delete_instance(self, instance_name: str) -> Dict[str, Any]:
        """
        Permanently delete an Evolution API instance.
        
        Args:
            instance_name: Evolution instance name
            
        Returns:
            dict with success status
        """
        try:
            response = self.session.delete(
                f"{self.base_url}/instance/delete/{instance_name}",
                timeout=15
            )
            
            if response.status_code == 200:
                logger.info(f"Instance '{instance_name}' deleted")
                return {'success': True, 'message': 'Instance deleted successfully'}
            else:
                return {
                    'success': False,
                    'error': response.text,
                    'status_code': response.status_code
                }
                
        except Exception as e:
            logger.error(f"Error deleting instance: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    # ------------------------------------------------------------------
    # Message Sending
    # ------------------------------------------------------------------
    
    def send_text(self, instance_name: str, mobile: str, message: str) -> Dict[str, Any]:
        """
        Send a text message via Evolution API.
        
        Args:
            instance_name: Evolution instance name
            mobile: Recipient phone number (international format, e.g., 923001234567)
            message: Message text content
            
        Returns:
            dict with success status and message_id
        """
        try:
            # Ensure number has @s.whatsapp.net format for Evolution API
            clean_number = mobile.replace('+', '').replace(' ', '').replace('-', '')
            
            payload = {
                "number": clean_number,
                "text": message
            }
            
            response = self.session.post(
                f"{self.base_url}/message/sendText/{instance_name}",
                json=payload,
                timeout=30
            )
            
            if response.status_code == 201 or response.status_code == 200:
                data = response.json()
                message_id = data.get('key', {}).get('id', '')
                
                logger.info(f"Text message sent to {mobile} via instance '{instance_name}'")
                return {
                    'success': True,
                    'message_id': message_id,
                    'response': data,
                    'status_code': response.status_code
                }
            elif response.status_code == 404:
                logger.error(f"Instance '{instance_name}' not found")
                return {
                    'success': False,
                    'error': 'Instance not found. Please reconnect.',
                    'status_code': 404,
                    'needs_reconnect': True
                }
            elif response.status_code == 401:
                logger.error(f"Unauthorized for instance '{instance_name}'")
                return {
                    'success': False,
                    'error': 'Authentication failed. Please check API key.',
                    'status_code': 401,
                    'needs_reconnect': True
                }
            else:
                logger.error(f"Failed to send message: {response.status_code} - {response.text}")
                return {
                    'success': False,
                    'error': response.text,
                    'status_code': response.status_code
                }
                
        except requests.exceptions.ConnectionError:
            return {
                'success': False,
                'error': 'Cannot connect to Evolution API',
                'status_code': None
            }
        except Exception as e:
            logger.error(f"Error sending text message: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def send_media(
        self,
        instance_name: str,
        mobile: str,
        media_url: str,
        media_type: str = 'document',
        caption: str = '',
        filename: str = ''
    ) -> Dict[str, Any]:
        """
        Send a media message (image/document) via Evolution API.
        
        Args:
            instance_name: Evolution instance name
            mobile: Recipient phone number
            media_url: URL of the media file
            media_type: 'image' or 'document'
            caption: Optional caption text
            filename: Optional filename for documents
            
        Returns:
            dict with success status
        """
        try:
            clean_number = mobile.replace('+', '').replace(' ', '').replace('-', '')
            
            payload = {
                "number": clean_number,
                "mediatype": media_type,
                "media": media_url,
                "caption": caption
            }
            
            if filename:
                payload["fileName"] = filename
            
            response = self.session.post(
                f"{self.base_url}/message/sendMedia/{instance_name}",
                json=payload,
                timeout=60
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                message_id = data.get('key', {}).get('id', '')
                
                logger.info(f"Media message sent to {mobile} via '{instance_name}'")
                return {
                    'success': True,
                    'message_id': message_id,
                    'response': data,
                    'status_code': response.status_code
                }
            else:
                logger.error(f"Failed to send media: {response.status_code} - {response.text}")
                return {
                    'success': False,
                    'error': response.text,
                    'status_code': response.status_code
                }
                
        except Exception as e:
            logger.error(f"Error sending media message: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    # ------------------------------------------------------------------
    # Connection Test
    # ------------------------------------------------------------------
    
    def test_api_connection(self) -> Dict[str, Any]:
        """
        Test if the Evolution API server is reachable.
        
        Returns:
            dict with reachable status
        """
        try:
            response = self.session.get(
                f"{self.base_url}/instance/fetchInstances",
                timeout=10
            )
            
            return {
                'success': True,
                'reachable': True,
                'status_code': response.status_code,
                'message': 'Evolution API is reachable'
            }
        except requests.exceptions.ConnectionError:
            return {
                'success': False,
                'reachable': False,
                'error': 'Cannot connect to Evolution API. Is Docker running?',
                'message': 'Evolution API is not reachable'
            }
        except Exception as e:
            return {
                'success': False,
                'reachable': False,
                'error': str(e),
                'message': 'Failed to reach Evolution API'
            }
