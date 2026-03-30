"""
WhatsApp Message Dispatcher
Background queue processor — sends pending messages via Evolution API
with strict anti-ban protections: random delays, send windows, warm-up limits.

Fixes applied vs original:
  - DB connections no longer held open during inter-message sleep
  - 'from app import create_app' moved inside __main__ guard (no circular import)
  - Instance token passed to send_text / send_media (not global key)
  - _process_all_companies returns the delay value so sleep is outside app context
"""

import random
import time
import threading
import logging
from datetime import datetime, date, timedelta
from typing import Optional
import pytz

from app import db
from app.models import WhatsAppConfig, WhatsAppMessageQueue
from app.services.evolution_api_client import evolution_client          # singleton
from app.services.spintax_engine import process_spintax
from app.services.whatsapp_rate_limiter import WhatsAppRateLimiter
from sqlalchemy import or_

# Pakistan timezone
PAK_TZ = pytz.timezone('Asia/Karachi')

logger = logging.getLogger(__name__)


class WhatsAppDispatcher:
    """
    Background daemon that processes the WhatsApp message queue
    with strict anti-ban safeguards.

    Safety features:
      - Random delay between messages (default 45-120 s, outside DB context)
      - Send window enforcement (default 9 AM – 9 PM)
      - Daily quota with warm-up ramping (40 → 60 → 100 → 200 over 4 weeks)
      - Spintax message humanisation
      - Native Evolution API typing-indicator delay
      - 5-minute cooldown after any send failure
      - Auto-disconnect after 3 consecutive failures
    """

    def __init__(self, app=None):
        self.app = app
        self._running              = False
        self._thread: Optional[threading.Thread] = None
        self._consecutive_failures: dict = {}   # {company_id: int}
        self._cooldown_until:       dict = {}   # {company_id: datetime}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self):
        """Start the dispatcher as a background daemon thread."""
        if self._running:
            logger.warning("Dispatcher is already running")
            return

        self._running = True
        self._thread  = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.name = "WhatsAppDispatcher"
        self._thread.start()
        logger.info("WhatsApp Dispatcher started")

    def stop(self):
        """Gracefully stop the dispatcher."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        logger.info("WhatsApp Dispatcher stopped")

    # ── Main Loop ─────────────────────────────────────────────────────────────

    def _run_loop(self):
        """
        Main dispatch loop.

        KEY FIX: the app context (and therefore the DB connection) is acquired
        only for the time needed to query and update records.  The inter-message
        sleep happens OUTSIDE the context so we don't hold a connection open
        for 45-120 seconds between every message.
        """
        logger.info("Dispatcher loop starting…")

        while self._running:
            sleep_seconds = 30  # default idle poll interval

            try:
                with self.app.app_context():
                    sleep_seconds = self._process_all_companies()

            except Exception as e:
                logger.error(f"Dispatcher loop error: {e}")
                sleep_seconds = 60   # longer backoff on unexpected errors

            # Sleep outside the app context — no DB connection held during wait
            time.sleep(sleep_seconds)

    def _process_all_companies(self) -> int:
        """
        Process one pending message per company that has an active Evolution config.

        Returns:
            int: seconds to sleep before the next iteration
        """
        configs = WhatsAppConfig.query.filter(
            WhatsAppConfig.provider_type   == 'evolution',
            WhatsAppConfig.phone_connected == True,
            WhatsAppConfig.instance_name   != None,
        ).all()

        if not configs:
            return 30  # No active companies — poll again in 30 s

        processed_any = False

        for config in configs:
            company_id = str(config.company_id)

            # Auto-complete warm-up if number has been active long enough
            try:
                if not config.warmup_complete and config.warmup_start_date:
                    days_active = (datetime.now(PAK_TZ).date() - config.warmup_start_date).days
                    if days_active >= 22:
                        config.warmup_complete = True
                        db.session.commit()
                        logger.info(f"Company {company_id[:8]}: warm-up auto-completed after {days_active} days")
            except Exception as e:
                db.session.rollback()
                logger.warning(f"Failed to auto-complete warmup for {company_id}: {e}")

            if self._is_in_cooldown(company_id):
                continue
            if not self._is_within_send_window(config):
                continue
            if not self._has_quota_remaining(config):
                continue

            message = self._get_next_message(company_id)
            if not message:
                continue

            success = self._send_message(config, message)

            if success:
                processed_any = True
                self._consecutive_failures[company_id] = 0
                # Return the configured delay — sleep will happen outside app context
                delay = random.randint(
                    config.min_delay_seconds or 45,
                    config.max_delay_seconds or 120,
                )
                logger.info(
                    f"✓ Message sent for company {company_id[:8]} — "
                    f"next send in {delay}s"
                )
                return delay
            else:
                self._handle_failure(config, company_id)

        return 30 if not processed_any else 5

    # ── Anti-Ban Guards ───────────────────────────────────────────────────────

    def _is_within_send_window(self, config: WhatsAppConfig) -> bool:
        # Get current time in Pakistan timezone
        now_pkt = datetime.now(PAK_TZ).time()
        try:
            start_h, start_m = map(int, (config.send_window_start or '09:00').split(':'))
            end_h,   end_m   = map(int, (config.send_window_end   or '21:00').split(':'))
            start = datetime.now(PAK_TZ).replace(hour=start_h, minute=start_m, second=0, microsecond=0).time()
            end   = datetime.now(PAK_TZ).replace(hour=end_h,   minute=end_m,   second=0, microsecond=0).time()
            return start <= now_pkt <= end
        except (ValueError, IndexError):
            # Fallback: 9 AM to 9 PM Pakistan time
            return 9 <= datetime.now(PAK_TZ).hour < 21

    def _has_quota_remaining(self, config: WhatsAppConfig) -> bool:
        company_id = str(config.company_id)
        try:
            effective_limit = config.current_daily_limit
            quota           = WhatsAppRateLimiter.get_or_create_today_quota(company_id)
            return max(0, effective_limit - quota.messages_sent) > 0
        except Exception as e:
            logger.error(f"Error checking quota: {e}")
            return False

    def _is_in_cooldown(self, company_id: str) -> bool:
        until = self._cooldown_until.get(company_id)
        return bool(until and datetime.now(PAK_TZ) < until)

    # ── Message Processing ────────────────────────────────────────────────────

    def _get_next_message(self, company_id: str) -> Optional[WhatsAppMessageQueue]:
        try:
            return WhatsAppMessageQueue.query.filter(
                WhatsAppMessageQueue.company_id == company_id,
                WhatsAppMessageQueue.status     == 'pending',
                WhatsAppMessageQueue.is_active  == True,
                or_(
                    WhatsAppMessageQueue.scheduled_date == None,
                    WhatsAppMessageQueue.scheduled_date <= datetime.now(PAK_TZ),
                ),
            ).order_by(
                WhatsAppMessageQueue.priority.asc(),
                WhatsAppMessageQueue.created_at.asc(),
            ).first()
        except Exception as e:
            logger.error(f"Error fetching next message: {e}")
            return None

    def _send_message(self, config: WhatsAppConfig, message: WhatsAppMessageQueue) -> bool:
        """
        Send a single queued message via the Evolution API singleton.

        KEY FIX: passes config.instance_token to send_text / send_media so that
        message-sending endpoints use the per-instance token, not the global key.
        """
        try:
            content = message.message_content
            if config.enable_spintax:
                content = process_spintax(content)

            instance_token = config.instance_token  # per-instance auth token

            if message.media_type == 'text' or not message.media_url:
                result = evolution_client.send_text(
                    instance_name  = config.instance_name,
                    mobile         = message.mobile,
                    message        = content,
                    instance_token = instance_token,
                    # Typing delay is already handled server-side;
                    # the dispatcher-level sleep provides the inter-message gap.
                )
            else:
                result = evolution_client.send_media(
                    instance_name  = config.instance_name,
                    mobile         = message.mobile,
                    media_url      = message.media_url,
                    media_type     = message.media_type or 'document',
                    caption        = content if message.media_caption else '',
                    filename       = '',
                    instance_token = instance_token,
                )

            if result.get('success'):
                message.status         = 'sent'
                message.sent_at        = datetime.now(PAK_TZ)
                message.api_message_id = result.get('message_id', '')
                message.api_response   = result.get('response', {})
                db.session.commit()

                WhatsAppRateLimiter.increment_sent_count(str(config.company_id))
                logger.info(
                    f"✓ Sent message {str(message.id)[:8]} to {message.mobile} "
                    f"via '{config.instance_name}'"
                )
                return True

            else:
                error_msg            = result.get('error', 'Unknown error')
                message.retry_count += 1
                message.error_message = error_msg

                if result.get('needs_reconnect'):
                    message.status        = 'failed'
                    message.error_message = f"Reconnect needed: {error_msg}"
                elif message.retry_count >= message.max_retry:
                    message.status = 'failed_permanent'
                else:
                    message.status = 'pending'

                db.session.commit()
                logger.warning(
                    f"✗ Failed message {str(message.id)[:8]}: {error_msg} "
                    f"(retry {message.retry_count}/{message.max_retry})"
                )
                return False

        except Exception as e:
            logger.error(f"Error in _send_message: {e}")
            try:
                message.retry_count  += 1
                message.error_message = str(e)
                if message.retry_count >= message.max_retry:
                    message.status = 'failed_permanent'
                db.session.commit()
            except Exception:
                db.session.rollback()
            return False

    def _handle_failure(self, config: WhatsAppConfig, company_id: str):
        """Apply cooldown and mark instance disconnected after 3 consecutive failures."""
        failures = self._consecutive_failures.get(company_id, 0) + 1
        self._consecutive_failures[company_id] = failures

        cooldown_until = datetime.now(PAK_TZ) + timedelta(minutes=5)
        self._cooldown_until[company_id] = cooldown_until

        logger.warning(
            f"Company {company_id[:8]}: failure #{failures}, "
            f"cooldown until {cooldown_until.strftime('%H:%M:%S')}"
        )

        if failures >= 3:
            logger.error(
                f"Company {company_id[:8]}: 3 consecutive failures — marking disconnected"
            )
            try:
                config.phone_connected   = False
                config.connection_status = 'disconnected'
                db.session.commit()
            except Exception:
                db.session.rollback()

            self._consecutive_failures[company_id] = 0


# ── Global Singleton ──────────────────────────────────────────────────────────

_dispatcher_instance: Optional[WhatsAppDispatcher] = None


def get_dispatcher() -> Optional[WhatsAppDispatcher]:
    """Return the running global dispatcher instance."""
    return _dispatcher_instance


def init_dispatcher(app) -> WhatsAppDispatcher:
    """
    Initialise and start the global dispatcher.
    Call this once from Flask app startup (e.g. in run.py or create_app).

    Args:
        app: Flask application instance
    """
    global _dispatcher_instance

    if _dispatcher_instance and _dispatcher_instance._running:
        logger.warning("Dispatcher already initialised and running")
        return _dispatcher_instance

    _dispatcher_instance = WhatsAppDispatcher(app=app)
    _dispatcher_instance.start()

    logger.info("WhatsApp Dispatcher initialised and started")
    return _dispatcher_instance


# ── Standalone Worker Entry Point ─────────────────────────────────────────────
# KEY FIX: 'from app import create_app' is now INSIDE the __main__ guard.
# Previously this import ran at module level on every Flask import, risking
# circular import errors and an unnecessary dependency at import time.

if __name__ == "__main__":
    # These imports only happen when running this file directly as a worker.
    import time as _time
    from app import create_app

    print("🚀 Initialising WhatsApp Dispatcher Worker…")

    app = create_app()

    with app.app_context():
        print("✅ Database context loaded. Dispatcher is now monitoring the queue…")

        dispatcher = None
        try:
            dispatcher = init_dispatcher(app)

            # Keep the main thread alive so the daemon thread can work
            while True:
                _time.sleep(1)

        except KeyboardInterrupt:
            print("\n🛑 Dispatcher stopped manually.")
            if dispatcher:
                dispatcher.stop()
        except Exception as e:
            print(f"❌ Dispatcher crashed: {e}")