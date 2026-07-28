"""Integration metadata: the project file, the manifest, and the quality scale.

Two failures here are invisible until it is too late to matter. A version bumped in
one file but not the other passes every local check and then fails the release
workflow, which re-stamps the manifest from the tag and refuses to publish a
mismatch. And a quality scale file that claims a rule is done when it is not is
worse than no file at all, because it is the thing a reader trusts instead of
reading the code.
"""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from typing import Any

import yaml

from custom_components.coolblue_energy.const import SCAN_INTERVAL

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INTEGRATION = _REPO_ROOT / "custom_components" / "coolblue_energy"

# The 20 Bronze rules, transcribed from the published scale rather than derived
# from the file under test:
# https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/
BRONZE_RULES = frozenset(
    {
        "action-setup",
        "appropriate-polling",
        "brands",
        "common-modules",
        "config-flow",
        "config-flow-test-coverage",
        "dependency-transparency",
        "docs-actions",
        "docs-conditions",
        "docs-high-level-description",
        "docs-installation-instructions",
        "docs-removal-instructions",
        "docs-triggers",
        "entity-event-setup",
        "entity-unique-id",
        "has-entity-name",
        "runtime-data",
        "test-before-configure",
        "test-before-setup",
        "unique-config-entry",
    }
)

# Per ADR 0001 the integration creates no entities, so these three are exempt for
# one reason and have to give it in one wording.
ENTITY_RULES = frozenset({"entity-event-setup", "entity-unique-id", "has-entity-name"})


def _manifest() -> dict[str, Any]:
    return json.loads((_INTEGRATION / "manifest.json").read_text())


def _project() -> dict[str, Any]:
    return tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())


def _rules() -> dict[str, Any]:
    quality_scale = yaml.safe_load((_INTEGRATION / "quality_scale.yaml").read_text())
    return quality_scale["rules"]


def _comment(rule: Any) -> str:
    """The reason recorded for *rule*, or the empty string if it gives none."""
    return rule.get("comment", "") if isinstance(rule, dict) else ""


def _status(rule: Any) -> str:
    return rule["status"] if isinstance(rule, dict) else rule


def test_version_is_the_same_in_the_project_file_and_the_manifest() -> None:
    """A bump that lands in only one file breaks the release workflow."""
    assert _project()["project"]["version"] == _manifest()["version"]


def test_quality_scale_grades_every_bronze_rule() -> None:
    """No Bronze rule is silently omitted, and none is invented."""
    assert set(_rules()) == BRONZE_RULES


def test_every_bronze_rule_is_done_or_exempt() -> None:
    """Bronze is claimed, so nothing may still be outstanding."""
    outstanding = {
        name: _status(rule)
        for name, rule in _rules().items()
        if _status(rule) not in {"done", "exempt"}
    }
    assert outstanding == {}


def test_every_exemption_gives_a_reason() -> None:
    """An exemption without a reason is indistinguishable from a gap."""
    unexplained = [
        name
        for name, rule in _rules().items()
        if _status(rule) == "exempt" and not _comment(rule).strip()
    ]
    assert unexplained == []


def test_the_entity_rules_are_exempt_in_one_wording() -> None:
    """All three say the same thing because they are the same fact (ADR 0001)."""
    rules = _rules()
    reasons = {_comment(rules[name]) for name in ENTITY_RULES}
    assert len(reasons) == 1
    assert {_status(rules[name]) for name in ENTITY_RULES} == {"exempt"}


def test_the_polling_rule_records_the_interval_the_coordinator_uses() -> None:
    """The justification goes stale the moment SCAN_INTERVAL changes without it."""
    hours = int(SCAN_INTERVAL.total_seconds() // 3600)
    assert f"{hours} hours" in _comment(_rules()["appropriate-polling"])
