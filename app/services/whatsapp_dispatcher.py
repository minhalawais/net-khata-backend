"""
WhatsApp Message Dispatcher
Background queue processor that sends pending messages via Evolution API
with strict anti-ban protections: random delays, send windows, warm-up limits.

This service runs as a background thread and processes the WhatsAppMessageQueue
table, sending messages one at a time with randomized delays between each send.
"""

import random
import time
import threading
import logging
from datetime import datetime, date, timedelta
from typing import Optional

from app import db
from app.models import WhatsAppConfig, WhatsAppMessageQueue
from app.services.evolution_api_client import EvolutionAPIClient
from app.services.spintax_engine import process_spintax
from app.services.whatsapp_rate_limiter import WhatsAppRateLimiter
from sqlalchemy import and_, or_

logger = logging.getLogger(__name__)


class WhatsAppDispatcher:
    """
    Background dispatcher that processes the WhatsApp message queue
    with strict anti-ban safeguards.
    
    Safety features:
    - Random delay between messages (configurable, default 45-120s)
    - Send window enforcement (default 9 AM - 9 PM)
    - Daily quota with warm-up ramping (20 → 50 → 100 → 200 over 4 weeks)
    - Spintax message humanization
    - 5-minute cooldown after any send failure
    - Auto-disconnect after 3 consecutive failures
    """
    
    def __init__(self, app=None):
        """
        Initialize the dispatcher.
        
        Args:
            app: Flask application instance (needed for app context)
        """
        self.app = app
        self.evolution_client = EvolutionAPIClient()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._consecutive_failures = {}  # {company_id: failure_count}
        self._cooldown_until = {}  # {company_id: datetime}
    
    def start(self):
        """Start the dispatcher as a background daemon thread."""
        if self._running:
            logger.warning("Dispatcher is already running")
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.name = "WhatsAppDispatcher"
        self._thread.start()
        logger.info("WhatsApp Dispatcher started")
    
    def stop(self):
        """Gracefully stop the dispatcher."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        logger.info("WhatsApp Dispatcher stopped")
    
    # ------------------------------------------------------------------
    # Main Loop
    # ------------------------------------------------------------------
    
    def _run_loop(self):
        """Main dispatch loop — runs continuously as a background thread."""
        logger.info("Dispatcher loop starting...")
        
        while self._running:
            try:
                with self.app.app_context():
                    processed = self._process_all_companies()
                
                # If no messages were processed for any company, sleep longer
                if not processed:
                    time.sleep(30)
                    
            except Exception as e:
                logger.error(f"Dispatcher loop error: {str(e)}")
                time.sleep(60)  # Sleep longer on unexpected errors
    
    def _process_all_companies(self) -> bool:
        """
        Process pending messages for all companies with active Evolution configs.
        
        Returns:
            bool: True if at least one message was processed
        """
        processed_any = False
        
        # Get all companies using Evolution API
        configs = WhatsAppConfig.query.filter(
            WhatsAppConfig.provider_type == 'evolution',
            WhatsAppConfig.phone_connected == True,
            WhatsAppConfig.instance_name != None
        ).all()
        
        for config in configs:
            company_id = str(config.company_id)
            
            # Skip if in cooldown
            if self._is_in_cooldown(company_id):
                continue
            
            # Skip if outside send window
            if not self._is_within_send_window(config):
                continue
            
            # Skip if daily quota exhausted (warm-up aware)
            if not self._has_quota_remaining(config):
                continue
            
            # Get next pending message for this company
            message = self._get_next_message(company_id)
            if not message:
                continue
            
            # Process the message
            success = self._send_message(config, message)
            
            if success:
                processed_any = True
                self._consecutive_failures[company_id] = 0
                
                # Random delay between messages (THE anti-ban core)
                delay = random.randint(
                    config.min_delay_seconds or 45,
                    config.max_delay_seconds or 120
                )
                logger.info(f"Next message in {delay}s for company {company_id[:8]}...")
                time.sleep(delay)
            else:
                self._handle_failure(config, company_id)
        
        return processed_any
    
    # ------------------------------------------------------------------
    # Anti-Ban Guards
    # ------------------------------------------------------------------
    
    def _is_within_send_window(self, config: WhatsAppConfig) -> bool:
        """Check if current time is within the safe sending window."""
        now = datetime.now().time()
        
        try:
            start_parts = (config.send_window_start or '09:00').split(':')
            end_parts = (config.send_window_end or '21:00').split(':')
            
            start_time = datetime.now().replace(
                hour=int(start_parts[0]), minute=int(start_parts[1]),
                second=0, microsecond=0
            ).time()
            
            end_time = datetime.now().replace(
                hour=int(end_parts[0]), minute=int(end_parts[1]),
                second=0, microsecond=0
            ).time()
            
            return start_time <= now <= end_time
        except (ValueError, IndexError):
            # Default to 9 AM - 9 PM on parse error
            return 9 <= datetime.now().hour < 21
    
    def _has_quota_remaining(self, config: WhatsAppConfig) -> bool:
        """
        Check if the company still has daily quota remaining.
        Uses warm-up-aware limits for new numbers.
        """
        company_id = str(config.company_id)
        
        try:
            # Get warm-up-aware limit
            effective_limit = config.current_daily_limit
            
            # Get today's sent count
            quota = WhatsAppRateLimiter.get_or_create_today_quota(company_id)
            remaining = max(0, effective_limit - quota.messages_sent)
            
            if remaining <= 0:
                logger.debug(
                    f"Quota exhausted for company {company_id[:8]}: "
                    f"{quota.messages_sent}/{effective_limit} (warm-up limit)"
                )
            
            return remaining > 0
            
        except Exception as e:
            logger.error(f"Error checking quota: {str(e)}")
            return False
    
    def _is_in_cooldown(self, company_id: str) -> bool:
        """Check if company is in a failure cooldown period."""
        cooldown_until = self._cooldown_until.get(company_id)
        if cooldown_until and datetime.now() < cooldown_until:
            return True
        return False
    
    # ------------------------------------------------------------------
    # Message Processing
    # ------------------------------------------------------------------
    
    def _get_next_message(self, company_id: str) -> Optional[WhatsAppMessageQueue]:
        """
        Fetch the next pending message from the queue.
        Ordered by priority (ascending) then creation date (oldest first).
        """
        try:
            message = WhatsAppMessageQueue.query.filter(
                WhatsAppMessageQueue.company_id == company_id,
                WhatsAppMessageQueue.status == 'pending',
                WhatsAppMessageQueue.is_active == True,
                or_(
                    WhatsAppMessageQueue.scheduled_date == None,
                    WhatsAppMessageQueue.scheduled_date <= datetime.now()
                )
            ).order_by(
                WhatsAppMessageQueue.priority.asc(),
                WhatsAppMessageQueue.created_at.asc()
            ).first()
            
            return message
            
        except Exception as e:
            logger.error(f"Error fetching next message: {str(e)}")
            return None
    
    def _send_message(self, config: WhatsAppConfig, message: WhatsAppMessageQueue) -> bool:
        """
        Send a single message via Evolution API.
        Applies spintax processing if enabled.
        
        Args:
            config: WhatsApp configuration for the company
            message: Message queue entry to send
            
        Returns:
            bool: True if sent successfully
        """
        try:
            # Apply spintax humanization if enabled
            content = message.message_content
            if config.enable_spintax:
                content = process_spintax(content)
            
            # Route to appropriate send method based on media type
            if message.media_type == 'text' or not message.media_url:
                result = self.evolution_client.send_text(
                    instance_name=config.instance_name,
                    mobile=message.mobile,
                    message=content
                )
            else:
                # Document or image
                result = self.evolution_client.send_media(
                    instance_name=config.instance_name,
                    mobile=message.mobile,
                    media_url=message.media_url,
                    media_type=message.media_type or 'document',
                    caption=content if message.media_caption else '',
                    filename=''
                )
            
            # Update message status
            if result.get('success'):
                message.status = 'sent'
                message.sent_at = datetime.now()
                message.api_message_id = result.get('message_id', '')
                message.api_response = result.get('response', {})
                db.session.commit()
                
                # Increment daily quota counter
                WhatsAppRateLimiter.increment_sent_count(str(config.company_id))
                
                logger.info(
                    f"✓ Sent message {str(message.id)[:8]} to {message.mobile} "
                    f"via '{config.instance_name}'"
                )
                return True
            else:
                # Handle send failure
                error_msg = result.get('error', 'Unknown error')
                message.retry_count += 1
                message.error_message = error_msg
                
                if result.get('needs_reconnect'):
                    message.status = 'failed'
                    message.error_message = f"Reconnect needed: {error_msg}"
                elif message.retry_count >= message.max_retry:
                    message.status = 'failed_permanent'
                else:
                    message.status = 'pending'  # Will be retried
                
                db.session.commit()
                
                logger.warning(
                    f"✗ Failed to send message {str(message.id)[:8]}: {error_msg} "
                    f"(retry {message.retry_count}/{message.max_retry})"
                )
                return False
                
        except Exception as e:
            logger.error(f"Error in _send_message: {str(e)}")
            try:
                message.retry_count += 1
                message.error_message = str(e)
                if message.retry_count >= message.max_retry:
                    message.status = 'failed_permanent'
                db.session.commit()
            except Exception:
                db.session.rollback()
            return False
    
    def _handle_failure(self, config: WhatsAppConfig, company_id: str):
        """
        Handle a send failure with cooldown and auto-disconnect logic.
        
        - Apply 5-minute cooldown after each failure
        - After 3 consecutive failures, mark instance as disconnected
        """
        failures = self._consecutive_failures.get(company_id, 0) + 1
        self._consecutive_failures[company_id] = failures
        
        # 5-minute cooldown
        cooldown_minutes = 5
        self._cooldown_until[company_id] = datetime.now() + timedelta(minutes=cooldown_minutes)
        
        logger.warning(
            f"Company {company_id[:8]}: failure #{failures}, "
            f"cooldown until {self._cooldown_until[company_id].strftime('%H:%M:%S')}"
        )
        
        # After 3 consecutive failures, mark as disconnected
        if failures >= 3:
            logger.error(
                f"Company {company_id[:8]}: 3 consecutive failures, "
                f"marking instance as disconnected"
            )
            try:
                config.phone_connected = False
                config.connection_status = 'disconnected'
                db.session.commit()
            except Exception:
                db.session.rollback()
            
            # Reset failure counter
            self._consecutive_failures[company_id] = 0


# ------------------------------------------------------------------
# Global Dispatcher Instance
# ------------------------------------------------------------------
_dispatcher_instance: Optional[WhatsAppDispatcher] = None


def get_dispatcher() -> Optional[WhatsAppDispatcher]:
    """Get the global dispatcher instance."""
    return _dispatcher_instance


def init_dispatcher(app):
    """
    Initialize and start the global dispatcher.
    Call this from your Flask app startup (e.g., in run.py).
    
    Args:
        app: Flask application instance
    """
    global _dispatcher_instance
    
    if _dispatcher_instance and _dispatcher_instance._running:
        logger.warning("Dispatcher already initialized and running")
        return _dispatcher_instance
    
    _dispatcher_instance = WhatsAppDispatcher(app=app)
    _dispatcher_instance.start()
    
    logger.info("WhatsApp Dispatcher initialized and started")
    return _dispatcher_instance
from app import create_app
if __name__ == "__main__":
    print("🚀 Initializing WhatsApp Dispatcher Worker...")
    
    # 1. Create the Flask app instance
    app = create_app()  
    
    # 2. Push the application context
    with app.app_context():
        print("✅ Database context loaded. Dispatcher is now monitoring the queue...")
        
        dispatcher = None
        try:
            # 3. Call your actual initialization function (spawns the background thread)
            dispatcher = init_dispatcher(app)
            
            # 4. Keep the main script alive forever so the daemon thread can work
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n🛑 Dispatcher stopped manually by user.")
            if dispatcher:
                dispatcher.stop()  # Gracefully shut down your background thread
        except Exception as e:
            print(f"❌ Dispatcher crashed: {e}")