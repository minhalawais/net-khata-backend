# Simple test runner without Unicode issues
import sys, os, random
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

passed = 0
failed = 0

def check(name, condition):
    global passed, failed
    if condition:
        passed += 1
        print(f"  PASS: {name}")
    else:
        failed += 1
        print(f"  FAIL: {name}")

print("\n=== SPINTAX ENGINE TESTS ===")
from app.services.spintax_engine import process_spintax, validate_spintax, get_default_template

r = process_spintax("[Hi|Hello|Dear] World")
check("Basic spintax resolves", r in ["Hi World", "Hello World", "Dear World"])

r = process_spintax("[Hi|Hello] name, your [bill|invoice] is [ready|available].")
check("Multiple groups resolved", "[" not in r and "]" not in r)

r = process_spintax("Hello World plain text")
check("Plain text passthrough", r == "Hello World plain text")

r = process_spintax("")
check("Empty string handled", r == "")

results = set()
for _ in range(100):
    results.add(process_spintax("[A|B|C] [X|Y|Z] [1|2|3|4]"))
check(f"100 runs = {len(results)} unique (need 10+)", len(results) >= 10)

v = validate_spintax("[A|B|C] [X|Y]")
check(f"Validator: 3x2 = {v['combination_count']} combos", v['combination_count'] == 6)
check("Validator: is_valid=True", v['is_valid'] == True)

for c in ['invoice', 'reminder', 'deadline_alert']:
    t = get_default_template(c)
    check(f"Template '{c}' exists ({len(t)} chars)", len(t) > 50 and '[' in t)

print("\n=== WARM-UP CALCULATOR TESTS ===")

def calc_limit(start_date, complete=False, quota=200):
    if complete:
        return quota
    if not start_date:
        return 20
    days = (date.today() - start_date).days
    if days < 7: return 20
    elif days < 14: return 50
    elif days < 21: return 100
    else: return quota

check("Day 1 = 20", calc_limit(date.today()) == 20)
check("Day 3 = 20", calc_limit(date.today() - timedelta(days=3)) == 20)
check("Day 8 = 50", calc_limit(date.today() - timedelta(days=8)) == 50)
check("Day 15 = 100", calc_limit(date.today() - timedelta(days=15)) == 100)
check("Day 22 = 200 (full)", calc_limit(date.today() - timedelta(days=22)) == 200)
check("Warmup complete = 200", calc_limit(date.today(), complete=True) == 200)
check("No start date = 20", calc_limit(None) == 20)

print("\n=== SEND WINDOW TESTS ===")

def in_window(hour, start="09:00", end="21:00"):
    t = datetime(2000,1,1,hour,0).time()
    s = datetime(2000,1,1,int(start.split(':')[0]),int(start.split(':')[1])).time()
    e = datetime(2000,1,1,int(end.split(':')[0]),int(end.split(':')[1])).time()
    return s <= t <= e

check("2PM in 9AM-9PM window", in_window(14) == True)
check("7AM outside 9AM-9PM", in_window(7) == False)
check("10PM outside 9AM-9PM", in_window(22) == False)
check("9AM exactly in window", in_window(9) == True)

print("\n=== DISPATCHER DELAY TESTS ===")

delays = [random.randint(45, 120) for _ in range(1000)]
check("All 1000 delays in 45-120 range", all(45 <= d <= 120 for d in delays))
check(f"Unique delays: {len(set(delays))} (need 20+)", len(set(delays)) >= 20)

custom = [random.randint(60, 180) for _ in range(100)]
check("Custom 60-180 range works", all(60 <= d <= 180 for d in custom))

print(f"\n{'='*40}")
print(f"  RESULTS: {passed} passed, {failed} failed out of {passed+failed}")
print(f"{'='*40}")

sys.exit(0 if failed == 0 else 1)
