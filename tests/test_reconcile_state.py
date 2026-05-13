"""Tests for _reconcile_state_with_cc in provision_demo_pipelines.py.

The provisioner's skip-if-exists checks (topic / connector / Flink statement)
trust the local state file. When CC and state drift apart, those checks
silently skip recreation and leave the pipeline chain broken. The
reconciler runs at the start of provision() and prunes state entries
whose CC counterparts no longer exist.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture(scope="module")
def script_module():
    spec = importlib.util.spec_from_file_location(
        "_provdemo_reconcile_under_test",
        Path(__file__).resolve().parent.parent / "scripts" / "provision_demo_pipelines.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_drops_topics_missing_from_cc(script_module):
    state = {
        "topics":           ["ol-orders00-t0", "ol-orders00-t1", "ol-payments01-t3"],
        "connectors":       {},
        "flink_statements": [],
    }
    with patch.object(script_module, "_list_env_topics",
                      return_value=["ol-orders00-t0"]), \
         patch.object(script_module, "_list_env_connectors", return_value=[]), \
         patch.object(script_module, "_list_env_flink_statements", return_value=[]):
        script_module._reconcile_state_with_cc(state)
    assert state["topics"] == ["ol-orders00-t0"]


def test_drops_connectors_missing_from_cc(script_module):
    state = {
        "topics":           [],
        "connectors":       {"orders00": "lcc-aaa", "payments01": "lcc-stale"},
        "flink_statements": [],
    }
    with patch.object(script_module, "_list_env_topics", return_value=[]), \
         patch.object(script_module, "_list_env_connectors",
                      return_value=[("ol-orders00-datagen", "lcc-aaa")]), \
         patch.object(script_module, "_list_env_flink_statements", return_value=[]):
        script_module._reconcile_state_with_cc(state)
    assert state["connectors"] == {"orders00": "lcc-aaa"}


def test_drops_flink_statements_missing_from_cc(script_module):
    """The exact case behind the bug: state lists 6 stages, CC has only 3."""
    state = {
        "topics":     [],
        "connectors": {},
        "flink_statements": [
            "ol-payments01-enrich-0",     # stale
            "ol-payments01-filter-1",     # stale
            "ol-payments01-project-2",    # stale
            "ol-payments01-transform-3",  # exists
            "ol-payments01-route-4",      # exists
            "ol-payments01-score-5",      # exists
        ],
    }
    with patch.object(script_module, "_list_env_topics", return_value=[]), \
         patch.object(script_module, "_list_env_connectors", return_value=[]), \
         patch.object(script_module, "_list_env_flink_statements",
                      return_value=["ol-payments01-transform-3",
                                    "ol-payments01-route-4",
                                    "ol-payments01-score-5"]):
        script_module._reconcile_state_with_cc(state)
    assert state["flink_statements"] == [
        "ol-payments01-transform-3",
        "ol-payments01-route-4",
        "ol-payments01-score-5",
    ]


def test_preserves_state_when_cc_list_fails(script_module):
    """Transient CC-list failure (returns empty list) must NOT prune state.

    Empty actual-list could mean "really nothing in CC" or "API blip".
    Pruning on a blip would force unnecessary recreates the next cycle —
    safer to leave state intact and let the create attempt's own
    'already-exists' tolerance handle duplicates.
    """
    state = {
        "topics":           ["ol-orders00-t0"],
        "connectors":       {"orders00": "lcc-aaa"},
        "flink_statements": ["ol-orders00-enrich-0"],
    }
    with patch.object(script_module, "_list_env_topics", return_value=[]), \
         patch.object(script_module, "_list_env_connectors", return_value=[]), \
         patch.object(script_module, "_list_env_flink_statements", return_value=[]):
        script_module._reconcile_state_with_cc(state)
    assert state["topics"]           == ["ol-orders00-t0"]
    assert state["connectors"]       == {"orders00": "lcc-aaa"}
    assert state["flink_statements"] == ["ol-orders00-enrich-0"]


def test_keeps_state_aligned_with_cc(script_module):
    """When local state and CC fully agree, reconciliation is a no-op."""
    state = {
        "topics":           ["ol-orders00-t0", "ol-orders00-t1"],
        "connectors":       {"orders00": "lcc-aaa"},
        "flink_statements": ["ol-orders00-enrich-0"],
    }
    with patch.object(script_module, "_list_env_topics",
                      return_value=["ol-orders00-t0", "ol-orders00-t1", "other-topic"]), \
         patch.object(script_module, "_list_env_connectors",
                      return_value=[("ol-orders00-datagen", "lcc-aaa"),
                                    ("user-connector",      "lcc-zzz")]), \
         patch.object(script_module, "_list_env_flink_statements",
                      return_value=["ol-orders00-enrich-0", "user-statement"]):
        script_module._reconcile_state_with_cc(state)
    assert state["topics"]           == ["ol-orders00-t0", "ol-orders00-t1"]
    assert state["connectors"]       == {"orders00": "lcc-aaa"}
    assert state["flink_statements"] == ["ol-orders00-enrich-0"]
