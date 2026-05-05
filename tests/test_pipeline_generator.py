"""Tests for the demo-pipeline generator (scripts/provision_demo_pipelines.py).

The generator produces N independent Pipeline definitions of random length
in [2, max_nodes], with random end-stage choice (Flink vs consumer-group).
Tests pin behavior we care about: bounds, determinism via seed, naming
uniqueness, and that the variety knobs actually produce variety.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def script_module():
    """Import scripts/provision_demo_pipelines.py as a module."""
    spec = importlib.util.spec_from_file_location(
        "_provdemo_under_test",
        Path(__file__).resolve().parent.parent / "scripts" / "provision_demo_pipelines.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_generates_requested_count(script_module):
    pipes = script_module.generate_pipelines(num_pipelines=7, min_nodes=3, max_nodes=5, seed=1)
    assert len(pipes) == 7


def test_lengths_within_bounds(script_module):
    pipes = script_module.generate_pipelines(num_pipelines=50, min_nodes=3, max_nodes=8, seed=1)
    for p in pipes:
        assert 2 <= p.total_nodes <= 8, p


def test_max_nodes_clamped_to_minimum_3(script_module):
    """Even if caller passes max_nodes=1, generator clamps to 3 (the
    smallest end-to-end-connected pipeline: connector + topic + consumer)."""
    pipes = script_module.generate_pipelines(num_pipelines=5, min_nodes=3, max_nodes=1, seed=1)
    for p in pipes:
        assert p.total_nodes == 3


def test_exact_n_when_min_equals_max(script_module):
    """min_nodes == max_nodes must always produce that exact size."""
    for n in (3, 5, 7, 9, 11):
        pipes = script_module.generate_pipelines(
            num_pipelines=20, min_nodes=n, max_nodes=n, seed=42,
        )
        assert all(p.total_nodes == n for p in pipes), \
            f"min=max={n} should give exact-{n} pipelines, got {[p.total_nodes for p in pipes]}"


def test_seed_makes_output_deterministic(script_module):
    a = script_module.generate_pipelines(num_pipelines=10, min_nodes=3, max_nodes=6, seed=42)
    b = script_module.generate_pipelines(num_pipelines=10, min_nodes=3, max_nodes=6, seed=42)
    assert [(p.domain, p.flink_stages, p.ends_with_consumer) for p in a] == \
           [(p.domain, p.flink_stages, p.ends_with_consumer) for p in b]


def test_different_seeds_produce_different_output(script_module):
    a = script_module.generate_pipelines(num_pipelines=20, min_nodes=3, max_nodes=8, seed=1)
    b = script_module.generate_pipelines(num_pipelines=20, min_nodes=3, max_nodes=8, seed=2)
    a_shape = [(p.flink_stages, p.ends_with_consumer) for p in a]
    b_shape = [(p.flink_stages, p.ends_with_consumer) for p in b]
    assert a_shape != b_shape


def test_domain_names_are_unique(script_module):
    """Even when the prefix repeats (more than DOMAIN_PREFIXES count), names differ."""
    pipes = script_module.generate_pipelines(num_pipelines=40, min_nodes=3, max_nodes=4, seed=1)
    names = [p.domain for p in pipes]
    assert len(set(names)) == len(names)


def test_topics_count_matches_flink_stages(script_module):
    """Number of topics = 1 (Datagen output) + flink_stages (one output per stage)."""
    pipes = script_module.generate_pipelines(num_pipelines=15, min_nodes=3, max_nodes=6, seed=7)
    for p in pipes:
        assert len(p.topics) == 1 + p.flink_stages


def test_consumer_group_set_iff_ends_with_consumer(script_module):
    pipes = script_module.generate_pipelines(num_pipelines=30, min_nodes=3, max_nodes=6, seed=3)
    for p in pipes:
        if p.ends_with_consumer:
            assert p.consumer_group is not None
            assert p.consumer_group.startswith("ol-") and p.consumer_group.endswith("-cg")
        else:
            assert p.consumer_group is None


def test_variety_appears_in_a_realistic_run(script_module):
    """Across 30 pipelines: every one ends with a consumer (full end-to-end
    connectivity is required), and we should see a spread of chain lengths."""
    # max_nodes=8 → max_flink_stages=(8-3)//2=2 → possible totals: 3, 5, 7
    pipes = script_module.generate_pipelines(num_pipelines=30, min_nodes=3, max_nodes=8, seed=11)
    consumer_endings = sum(1 for p in pipes if p.ends_with_consumer)
    distinct_lengths = {p.total_nodes for p in pipes}
    assert consumer_endings == len(pipes), "every pipeline must end with a consumer"
    assert len(distinct_lengths) >= 2     # at least two different chain lengths
    # Lengths are always odd (connector + topic + 2N for Flink+topic + consumer)
    assert all(n % 2 == 1 for n in distinct_lengths), distinct_lengths


def test_first_flink_stage_is_enrich(script_module):
    """Stage 0 SQL projects + enriches (CASE risk_tier)."""
    import random
    rng = random.Random(0)
    sql = script_module._flink_sql(0, "in_topic", "out_topic", rng)
    assert "risk_tier" in sql
    assert "INSERT INTO `out_topic`" in sql
    assert "FROM `in_topic`" in sql


def test_later_flink_stages_use_select_star(script_module):
    """Stage 1+ uses identity SELECT * variants — schema-preserving."""
    import random
    rng = random.Random(0)
    sqls = [script_module._flink_sql(k, "i", "o", rng) for k in range(1, 6)]
    for sql in sqls:
        assert "SELECT *" in sql


def test_flink_names_distinct_within_pipeline(script_module):
    pipes = script_module.generate_pipelines(num_pipelines=10, min_nodes=3, max_nodes=10, seed=99)
    for p in pipes:
        assert len(set(p.flink_names)) == len(p.flink_names), p
