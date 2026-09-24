# Tests for the recognized external-application model + scope handling.
# Pure validation (no DB / FastAPI), runnable with plain pytest.

import pytest
from pydantic import ValidationError

from core.models import ExternalAppCreate, ExternalAppRevoke


def test_default_scopes():
    a = ExternalAppCreate(name="Meridian")
    assert a.name == "Meridian"
    assert a.scopes == "credit,burn,balance"


def test_custom_scopes_and_trim():
    a = ExternalAppCreate(name="  Meridian  ", scopes="credit,burn")
    assert a.name == "Meridian"  # stripped
    assert a.scopes == "credit,burn"


def test_name_required():
    with pytest.raises(ValidationError):
        ExternalAppCreate(name="")


def test_name_length_bound():
    with pytest.raises(ValidationError):
        ExternalAppCreate(name="x" * 65)


def test_revoke_model():
    r = ExternalAppRevoke(name="Meridian")
    assert r.name == "Meridian"
    with pytest.raises(ValidationError):
        ExternalAppRevoke(name="")


# Mirror of require_scope()'s decision logic (kept in sync with core/main.py):
# accept if master key, else accept an active external key that includes scope.
def _authorized(presented, master, ext_lookup, scope):
    if presented == master:
        return True
    app = ext_lookup(presented)
    return bool(app and scope in app["scopes"])


def test_scope_authorization():
    master = "MASTER"
    apps = {"ext_meridian": {"name": "Meridian", "scopes": ["credit", "burn", "balance"]}}
    lookup = lambda k: apps.get(k)

    # master key: always allowed
    assert _authorized("MASTER", master, lookup, "credit") is True
    # external key with the scope
    assert _authorized("ext_meridian", master, lookup, "burn") is True
    # external key WITHOUT the scope
    apps["ext_readonly"] = {"name": "RO", "scopes": ["balance"]}
    assert _authorized("ext_readonly", master, lookup, "credit") is False
    assert _authorized("ext_readonly", master, lookup, "balance") is True
    # unknown key
    assert _authorized("nope", master, lookup, "credit") is False
