# system-a-core/conftest.py
#
# Ensures the service package root is importable no matter where pytest is
# invoked from (repo root or the service directory), so `from core.models
# import ...` resolves consistently.

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def pytest_sessionfinish(session, exitstatus):
    """Close the DB connection pool (if opened) so the test process exits
    promptly instead of waiting on the pool's background worker threads."""
    try:
        from core import db

        db.close_pool()
    except Exception:
        pass
