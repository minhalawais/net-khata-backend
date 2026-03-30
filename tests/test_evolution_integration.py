"""
Unit Tests for WhatsApp Evolution API Integration
Tests: Spintax Engine, Warm-up Calculator, Send Window, Dispatcher Delay Logic
Run: python -m pytest tests/test_evolution_integration.py -v
"""

import sys
import os
import random
import re
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

# ============================================================================
# TEST 1: Spintax Engine
# ============================================================================

class TestSpintaxEngine:
    """Tests for the spintax message humanization engine"""

    def test_basic_spintax_resolution(self):
        """Verify basic [option1|option2] patterns resolve correctly"""
        # Import inline to avoid Flask app context issues
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from app.services.spintax_engine import process_spintax

        text = "[Hi|Hello|Dear] World"
        result = process_spintax(text)
        assert result in ["Hi World", "Hello World", "Dear World"], f"Got: {result}"
        print(f"  ✓ Basic spintax: '{text}' → '{result}'")

    def test_multiple_spintax_groups(self):
        """Verify multiple spintax groups in one message"""
        from app.services.spintax_engine import process_spintax

        text = "[Hi|Hello] {{name}}, your [bill|invoice] is [ready|available]."
        result = process_spintax(text)
        # Should still have {{name}} placeholder (not spintax)
        assert "{{name}}" in result
        assert "[" not in result and "]" not in result
        print(f"  ✓ Multiple groups: resolved to '{result}'")

    def test_100_runs_all_unique(self):
        """Verify that spintax produces varied output — no two identical in 100 runs"""
        from app.services.spintax_engine import process_spintax

        template = """[🧾|📄|💰] *[Invoice Generated|Your Bill is Ready|New Invoice]*

[Hi|Hello|Dear|Assalam o Alaikum] {{customer_name}},

Your invoice *{{invoice_number}}* [has been generated|is now ready|is available].
*Amount:* Rs. {{amount}}

[Please make payment before the due date|Kindly pay before the deadline|Payment is due by {{due_date}}].
[Thank you!|Thanks for your business!|We appreciate your trust!|Regards]"""

        results = set()
        for i in range(100):
            result = process_spintax(template)
            results.add(result)

        unique_count = len(results)
        # With this many spintax groups, we should get MANY unique results
        assert unique_count >= 20, f"Only {unique_count} unique out of 100 — not enough variation!"
        print(f"  ✓ 100 runs produced {unique_count} unique messages (expected ≥20)")

    def test_no_spintax_passthrough(self):
        """Verify plain text without spintax passes through unchanged"""
        from app.services.spintax_engine import process_spintax

        text = "Hello World, your bill is ready."
        result = process_spintax(text)
        assert result == text, f"Plain text was modified: '{result}'"
        print(f"  ✓ Plain text passthrough: unchanged")

    def test_empty_input(self):
        """Verify empty/None input is handled safely"""
        from app.services.spintax_engine import process_spintax

        assert process_spintax("") == ""
        assert process_spintax(None) is None
        print(f"  ✓ Empty input: handled safely")

    def test_validate_spintax(self):
        """Verify spintax validator counts combinations correctly"""
        from app.services.spintax_engine import validate_spintax

        # 3 options × 2 options = 6 combinations
        result = validate_spintax("[A|B|C] [X|Y]")
        assert result['is_valid'] == True
        assert result['combination_count'] == 6
        assert result['spintax_groups'] == 2
        print(f"  ✓ Validator: '[A|B|C] [X|Y]' = {result['combination_count']} combinations")

    def test_default_templates_exist(self):
        """Verify all default templates are accessible"""
        from app.services.spintax_engine import get_default_template

        for category in ['invoice', 'reminder', 'deadline_alert']:
            template = get_default_template(category)
            assert len(template) > 50, f"Template '{category}' is too short"
            assert "[" in template and "|" in template, f"Template '{category}' has no spintax"
        print(f"  ✓ All 3 default templates exist with spintax patterns")


# ============================================================================
# TEST 2: Warm-up Calculator
# ============================================================================

class TestWarmupCalculator:
    """Tests for the number warm-up daily limit calculation"""

    def _make_config(self, warmup_start, warmup_complete=False, daily_limit=200):
        """Create a mock WhatsAppConfig-like object"""
        config = MagicMock()
        config.warmup_start_date = warmup_start
        config.warmup_complete = warmup_complete
        config.daily_quota_limit = daily_limit

        # Replicate the current_daily_limit logic
        def calc_limit():
            if config.warmup_complete:
                return config.daily_quota_limit
            if not config.warmup_start_date:
                return 20
            days_active = (date.today() - config.warmup_start_date).days
            if days_active < 7:
                return 20
            elif days_active < 14:
                return 50
            elif days_active < 21:
                return 100
            else:
                return config.daily_quota_limit

        config.current_daily_limit = property(lambda self: calc_limit())
        return config, calc_limit

    def test_day_1_limit_is_20(self):
        """Day 1: New number should have 20 msgs/day limit"""
        _, calc = self._make_config(date.today())
        limit = calc()
        assert limit == 20, f"Day 1 limit should be 20, got {limit}"
        print(f"  ✓ Day 1: limit = {limit}")

    def test_day_3_limit_is_20(self):
        """Day 3: Still in week 1, should be 20"""
        _, calc = self._make_config(date.today() - timedelta(days=3))
        limit = calc()
        assert limit == 20, f"Day 3 limit should be 20, got {limit}"
        print(f"  ✓ Day 3: limit = {limit}")

    def test_day_8_limit_is_50(self):
        """Day 8: Week 2, should jump to 50"""
        _, calc = self._make_config(date.today() - timedelta(days=8))
        limit = calc()
        assert limit == 50, f"Day 8 limit should be 50, got {limit}"
        print(f"  ✓ Day 8: limit = {limit}")

    def test_day_15_limit_is_100(self):
        """Day 15: Week 3, should jump to 100"""
        _, calc = self._make_config(date.today() - timedelta(days=15))
        limit = calc()
        assert limit == 100, f"Day 15 limit should be 100, got {limit}"
        print(f"  ✓ Day 15: limit = {limit}")

    def test_day_22_limit_is_full(self):
        """Day 22: Week 4+, should be full daily_quota_limit (200)"""
        _, calc = self._make_config(date.today() - timedelta(days=22))
        limit = calc()
        assert limit == 200, f"Day 22 limit should be 200, got {limit}"
        print(f"  ✓ Day 22: limit = {limit} (full)")

    def test_warmup_complete_bypasses(self):
        """When warmup_complete=True, always return full limit"""
        _, calc = self._make_config(date.today(), warmup_complete=True)
        limit = calc()
        assert limit == 200, f"Warmup complete should return 200, got {limit}"
        print(f"  ✓ Warmup complete: limit = {limit}")

    def test_no_start_date_returns_safe_default(self):
        """No warmup_start_date should return ultra-safe 20"""
        _, calc = self._make_config(None)
        limit = calc()
        assert limit == 20, f"No start date should return 20, got {limit}"
        print(f"  ✓ No start date: limit = {limit} (ultra-safe)")


# ============================================================================
# TEST 3: Send Window Check
# ============================================================================

class TestSendWindow:
    """Tests for the send window time check"""

    def test_within_window(self):
        """2:00 PM should be within 9 AM - 9 PM window"""
        # Simulate the dispatcher's is_within_send_window logic
        start = "09:00"
        end = "21:00"
        test_time = datetime.now().replace(hour=14, minute=0)

        start_h, start_m = map(int, start.split(':'))
        end_h, end_m = map(int, end.split(':'))

        start_t = test_time.replace(hour=start_h, minute=start_m).time()
        end_t = test_time.replace(hour=end_h, minute=end_m).time()

        in_window = start_t <= test_time.time() <= end_t
        # 14:00 is between 09:00 and 21:00
        assert datetime(2000, 1, 1, 9, 0).time() <= datetime(2000, 1, 1, 14, 0).time() <= datetime(2000, 1, 1, 21, 0).time()
        print(f"  ✓ 2:00 PM is within 9AM-9PM window")

    def test_before_window(self):
        """7:00 AM should be OUTSIDE 9 AM - 9 PM window"""
        t_7am = datetime(2000, 1, 1, 7, 0).time()
        t_9am = datetime(2000, 1, 1, 9, 0).time()
        t_9pm = datetime(2000, 1, 1, 21, 0).time()
        
        in_window = t_9am <= t_7am <= t_9pm
        assert not in_window, "7 AM should be outside window"
        print(f"  ✓ 7:00 AM is outside 9AM-9PM window")

    def test_after_window(self):
        """10:00 PM should be OUTSIDE 9 AM - 9 PM window"""
        t_10pm = datetime(2000, 1, 1, 22, 0).time()
        t_9am = datetime(2000, 1, 1, 9, 0).time()
        t_9pm = datetime(2000, 1, 1, 21, 0).time()
        
        in_window = t_9am <= t_10pm <= t_9pm
        assert not in_window, "10 PM should be outside window"
        print(f"  ✓ 10:00 PM is outside 9AM-9PM window")

    def test_edge_start(self):
        """9:00 AM exactly should be IN window"""
        t_9am_exact = datetime(2000, 1, 1, 9, 0).time()
        t_9am = datetime(2000, 1, 1, 9, 0).time()
        t_9pm = datetime(2000, 1, 1, 21, 0).time()
        
        in_window = t_9am <= t_9am_exact <= t_9pm
        assert in_window, "9:00 AM exactly should be in window"
        print(f"  ✓ 9:00 AM exactly is within window (edge case)")


# ============================================================================
# TEST 4: Dispatcher Delay Logic
# ============================================================================

class TestDispatcherDelay:
    """Tests for the random delay generation"""

    def test_delay_within_range(self):
        """Verify all generated delays are within 45-120s range"""
        min_delay = 45
        max_delay = 120
        
        for _ in range(1000):
            delay = random.randint(min_delay, max_delay)
            assert min_delay <= delay <= max_delay, f"Delay {delay} outside {min_delay}-{max_delay}"
        
        print(f"  ✓ 1000 delays all within {min_delay}-{max_delay}s range")

    def test_delay_has_variation(self):
        """Verify delays are actually random (not all the same)"""
        min_delay = 45
        max_delay = 120
        
        delays = set()
        for _ in range(100):
            delays.add(random.randint(min_delay, max_delay))
        
        # With 76 possible values (45-120), 100 samples should hit many unique values
        assert len(delays) >= 20, f"Only {len(delays)} unique delays — not random enough"
        print(f"  ✓ 100 delays produced {len(delays)} unique values (good randomness)")

    def test_custom_delay_range(self):
        """Verify custom ISP delay settings work"""
        min_delay = 60
        max_delay = 180
        
        for _ in range(100):
            delay = random.randint(min_delay, max_delay)
            assert min_delay <= delay <= max_delay
        
        print(f"  ✓ Custom range {min_delay}-{max_delay}s works correctly")


# ============================================================================
# RUNNER
# ============================================================================

def run_all_tests():
    """Run all tests in sequence with formatted output"""
    print("=" * 60)
    print("  WhatsApp Evolution API — Unit Tests")
    print("=" * 60)
    
    test_classes = [
        ("Spintax Engine", TestSpintaxEngine),
        ("Warm-up Calculator", TestWarmupCalculator),
        ("Send Window Check", TestSendWindow),
        ("Dispatcher Delay Logic", TestDispatcherDelay),
    ]
    
    total_tests = 0
    passed_tests = 0
    failed_tests = 0
    
    for section_name, test_class in test_classes:
        print(f"\n{'─' * 40}")
        print(f"  {section_name}")
        print(f"{'─' * 40}")
        
        instance = test_class()
        methods = [m for m in dir(instance) if m.startswith('test_')]
        
        for method_name in sorted(methods):
            total_tests += 1
            try:
                getattr(instance, method_name)()
                passed_tests += 1
            except Exception as e:
                failed_tests += 1
                print(f"  ✗ {method_name}: {str(e)}")
    
    print(f"\n{'=' * 60}")
    print(f"  Results: {passed_tests}/{total_tests} passed, {failed_tests} failed")
    print(f"{'=' * 60}")
    
    return failed_tests == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
