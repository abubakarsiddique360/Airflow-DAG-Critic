"""Reports for a DAG review."""

from __future__ import annotations

import json

from .dagfile import DagFile
from .rules import SEVERITY_ORDER, Review

BADGES = {"critical": "CRITICAL", "high": "HIGH", "medium": "MEDIUM", "low": "LOW"}

CATALOGUE = (
    ("hardcoded-secret", "critical", "a credential passed as a literal string"),
    ("no-dag", "high", "the file defines no DAG at all"),
    ("sensor-without-timeout", "high", "a sensor that can wait forever"),
    ("duplicate-task-id", "high", "two tasks share a task_id; one silently wins"),
    ("module-level-side-effect", "high", "network / disk / DB call at import time"),
    ("no-start-date", "medium", "no start_date, so history is undefined"),
    ("catchup-enabled", "medium", "catchup=True backfills everything since start_date"),
    ("catchup-default", "medium", "catchup unset, so it defaults to True"),
    ("dynamic-task-id", "medium", "task_id built with an f-string"),
    ("dynamic-dag-id", "medium", "dag_id built with an f-string"),
    ("missing-task-id", "medium", "operator instantiated without task_id"),
    ("no-retries", "medium", "tasks run with zero retries"),
    ("no-schedule", "low", "no schedule / schedule_interval"),
    ("no-owner", "low", "default_args has no owner"),
)


def render_review(result: Review) -> str:
    lines = [f"DAG review: {result.path}", "=" * 70]
    lines.append(
        f"risk {result.risk} points (grade {result.grade})   dags {result.dag_count}   "
        f"tasks {result.task_count}   critical {result.counts['critical']}   "
        f"high {result.counts['high']}   medium {result.counts['medium']}   "
        f"low {result.counts['low']}"
    )
    if result.clean:
        lines.append("")
        lines.append("No findings. Retries, timeouts, ownership and schedule are all explicit.")
        return "\n".join(lines)
    current = None
    for finding in result.findings:
        if finding.severity != current:
            current = finding.severity
            lines.append("")
            lines.append(BADGES[current])
        lines.append(f"  [{finding.code}] line {finding.line} ({finding.scope})")
        lines.append(f"      {finding.message}")
        lines.append(f"      fix: {finding.hint}")
    return "\n".join(lines)


def render_markdown(result: Review) -> str:
    lines = [
        f"### DAG review: `{result.path}`",
        "",
        f"**Risk {result.risk} points (grade {result.grade})** - "
        f"{result.dag_count} DAG(s), {result.task_count} task(s), "
        f"{result.counts['critical']} critical, {result.counts['high']} high, "
        f"{result.counts['medium']} medium, {result.counts['low']} low.",
        "",
        "| Severity | Line | Scope | Finding | Fix |",
        "| --- | ---: | --- | --- | --- |",
    ]
    if result.clean:
        lines.append("| - | - | - | No findings | - |")
    for finding in result.findings:
        message = finding.message.replace("|", "/")
        hint = finding.hint.replace("|", "/")
        lines.append(
            f"| {BADGES[finding.severity]} | {finding.line} | `{finding.scope}` | "
            f"{message} | {hint} |"
        )
    return "\n".join(lines)


def render_json(result: Review) -> str:
    return json.dumps(result.as_dict(), indent=2)


def render_summary(results: list[Review]) -> str:
    lines = ["Summary", "=" * 78]
    lines.append(f"{'file':<40}{'grade':>7}{'risk':>6}{'crit':>6}{'high':>6}{'med':>5}{'low':>5}")
    for result in results:
        name = result.path.replace("\\", "/").split("/")[-1]
        lines.append(
            f"{name[:39]:<40}{result.grade:>7}{result.risk:>6}"
            f"{result.counts['critical']:>6}{result.counts['high']:>6}"
            f"{result.counts['medium']:>5}{result.counts['low']:>5}"
        )
    total = sum(result.risk for result in results)
    lines.append("")
    lines.append(f"{len(results)} file(s)   combined risk {total} points")
    return "\n".join(lines)


def render_catalogue() -> str:
    lines = ["Rules", "=" * 92]
    for severity in SEVERITY_ORDER:
        rows = [item for item in CATALOGUE if item[1] == severity]
        if not rows:
            continue
        lines.append("")
        lines.append(BADGES[severity])
        for code, _severity, description in rows:
            lines.append(f"  {code:<26}{description}")
    return "\n".join(lines)


def _mermaid_id(name: str, used: dict[str, int]) -> str:
    cleaned = "".join(char if char.isalnum() or char == "_" else "_" for char in name) or "task"
    if cleaned in used:
        used[cleaned] += 1
        return f"{cleaned}_{used[cleaned]}"
    used[cleaned] = 0
    return cleaned


def render_mermaid(dagfile: DagFile) -> str:
    """Dependency graph as Mermaid, so the review can be read as a picture."""
    lines = ["```mermaid", "flowchart LR"]
    if not dagfile.tasks:
        lines.append('    empty["no operators found"]')
    used: dict[str, int] = {}
    node_ids: dict[str, str] = {}
    for task in dagfile.tasks:
        if task.label in node_ids:
            continue
        node_id = _mermaid_id(task.label, used)
        node_ids[task.label] = node_id
        lines.append(f'    {node_id}["{task.label}<br/><small>{task.operator}</small>"]')
    for source, target in dagfile.edges:
        from_id = node_ids.get(source, _mermaid_id(source, used))
        to_id = node_ids.get(target, _mermaid_id(target, used))
        lines.append(f"    {from_id} --> {to_id}")
    labels = {task.label for task in dagfile.tasks}
    unused = labels - {name for edge in dagfile.edges for name in edge}
    orphan_ids = [node_ids[name] for name in sorted(unused) if name in node_ids]
    if orphan_ids and dagfile.edges:
        lines.append("    classDef orphan stroke-dasharray: 4 3")
        for node_id in orphan_ids:
            lines.append(f"    class {node_id} orphan")
    lines.append("```")
    return "\n".join(lines)
