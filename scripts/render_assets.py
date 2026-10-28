"""Regenerate README assets from real runs.

Usage:  python scripts/render_assets.py
Writes: docs/results.json, docs/images/review.png,
        docs/images/findings-by-rule.svg, docs/images/task-graph.png
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402
from PIL import Image, ImageDraw, ImageFont  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from airflow_dag_critic.dagfile import DagFile, parse_file  # noqa: E402
from airflow_dag_critic.report import render_review, render_summary  # noqa: E402
from airflow_dag_critic.rules import SEVERITY_ORDER, Review, review  # noqa: E402

BG = "#0d1117"
CHROME = "#161b22"
FG = "#e6edf3"
MUTED = "#8b949e"
ACCENT = "#79c0ff"
SEVERITY_COLORS = {
    "critical": "#f85149",
    "high": "#d29922",
    "medium": "#bc4c00",
    "low": "#6e7781",
}


def load_font(size: int) -> ImageFont.FreeTypeFont:
    for name in ("consola.ttf", "cour.ttf", "DejaVuSansMono.ttf"):
        candidate = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts" / name
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size)
    return ImageFont.load_default()


def render_terminal(text: str, out_path: Path, title: str) -> None:
    font = load_font(15)
    lines = text.expandtabs(4).splitlines() or [""]
    probe = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    char_width = probe.textlength("M", font=font)
    line_height = 21
    pad = 24
    width = int(max(len(line) for line in lines) * char_width) + pad * 2
    height = pad * 2 + len(lines) * line_height + 44
    image = Image.new("RGB", (max(width, 720), height), BG)
    draw = ImageDraw.Draw(image)
    draw.rectangle([0, 0, image.width, 34], fill=CHROME)
    for index, color in enumerate(("#ff5f56", "#ffbd2e", "#27c93f")):
        draw.ellipse([16 + index * 22, 10, 28 + index * 22, 22], fill=color)
    draw.text((92, 9), title, font=load_font(13), fill=MUTED)

    for row, line in enumerate(lines):
        color = FG
        stripped = line.strip()
        if line.startswith("$"):
            color = ACCENT
        elif stripped in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
            color = SEVERITY_COLORS[stripped.lower()]
        elif stripped.startswith("fix:"):
            color = "#3fb950"
        elif stripped.startswith("["):
            color = MUTED
        elif "risk" in stripped:
            color = MUTED
        draw.text((pad, 50 + row * line_height), line, font=font, fill=color)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(out_path)


def render_rule_bars(bad: Review, good: Review, out_path: Path) -> None:
    counts: dict[str, int] = {}
    severities: dict[str, str] = {}
    for finding in bad.findings:
        counts[finding.code] = counts.get(finding.code, 0) + 1
        severities[finding.code] = finding.severity
    ordered = sorted(counts, key=lambda code: (SEVERITY_ORDER[severities[code]], -counts[code]))

    fig, ax = plt.subplots(figsize=(9.4, 4.0))
    fig.patch.set_facecolor("#ffffff")
    positions = range(len(ordered))
    values = [counts[code] for code in ordered]
    bars = ax.barh(
        list(positions),
        values,
        color=[SEVERITY_COLORS[severities[code]] for code in ordered],
        height=0.62,
    )
    for bar, value in zip(bars, values, strict=True):
        ax.text(value + 0.05, bar.get_y() + bar.get_height() / 2, str(value),
                va="center", fontsize=8, color="#24292f")
    ax.set_yticks(list(positions))
    ax.set_yticklabels(ordered, fontsize=8)
    for label, code in zip(ax.get_yticklabels(), ordered, strict=True):
        label.set_color(SEVERITY_COLORS[severities[code]])
        label.set_fontweight("bold")
    ax.invert_yaxis()
    ax.set_xlim(0, 1.15)
    ax.set_xlabel("findings in examples/bad_dag.py (colour = severity, see the labels)")
    ax.set_title(
        f"bad_dag.py: {bad.risk} points, grade {bad.grade}  |  "
        f"good_dag.py: {good.risk} points, grade {good.grade}",
        fontsize=11,
        loc="left",
    )
    ax.grid(axis="x", linestyle=":", alpha=0.45)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, format=out_path.suffix.lstrip("."))
    plt.close(fig)


def _levels(dagfile: DagFile) -> dict[str, int]:
    incoming: dict[str, set[str]] = {task.label: set() for task in dagfile.tasks}
    for source, target in dagfile.edges:
        if target in incoming:
            incoming[target].add(source)
    levels: dict[str, int] = {}
    pending = dict(incoming)
    while pending:
        ready = [name for name, sources in pending.items() if not (sources - set(levels))]
        if not ready:  # cycle: place the rest at one level instead of looping forever
            for name in pending:
                levels[name] = max(levels.values(), default=0) + 1
            break
        for name in ready:
            levels[name] = max((levels.get(source, 0) + 1 for source in incoming[name]), default=0)
            del pending[name]
    return levels


def draw_graph(axis, dagfile: DagFile, dismissed: dict[str, str], title: str) -> None:
    levels = _levels(dagfile)
    by_level: dict[int, list[str]] = {}
    for name, level in levels.items():
        by_level.setdefault(level, []).append(name)
    positions: dict[str, tuple[float, float]] = {}
    for level, names in by_level.items():
        for index, name in enumerate(sorted(names)):
            positions[name] = (level * 2.4, -index * 1.4)

    for source, target in dagfile.edges:
        if source not in positions or target not in positions:
            continue
        start = positions[source]
        end = positions[target]
        axis.add_patch(
            FancyArrowPatch(
                (start[0] + 1.05, start[1]),
                (end[0] - 1.05, end[1]),
                arrowstyle="-|>",
                mutation_scale=12,
                color="#57606a",
                linewidth=1.2,
                shrinkA=0,
                shrinkB=0,
            )
        )

    for name, (x, y) in positions.items():
        severity = dismissed.get(name)
        edge = SEVERITY_COLORS.get(severity, "#1a7f37") if severity else "#1a7f37"
        face = "#fff5f5" if severity in ("critical", "high") else "#f6f8fa"
        axis.add_patch(
            FancyBboxPatch(
                (x - 1.05, y - 0.32),
                2.1,
                0.64,
                boxstyle="round,pad=0.02,rounding_size=0.12",
                linewidth=2.0 if severity else 1.4,
                edgecolor=edge,
                facecolor=face,
            )
        )
        axis.text(x, y, name, ha="center", va="center", fontsize=8, color="#24292f")
        if severity:
            axis.text(x, y - 0.52, severity, ha="center", va="center", fontsize=6,
                      color=SEVERITY_COLORS[severity])

    axis.set_title(title, fontsize=10, loc="left")
    axis.set_xlim(-1.6, max((x for x, _ in positions.values()), default=1) + 1.6)
    axis.set_ylim(-1.0, 0.7)
    axis.axis("off")


def render_graph(bad_file: DagFile, bad: Review, good_file: DagFile, good: Review,
                 out_path: Path) -> None:
    def worst_by_scope(result: Review) -> dict[str, str]:
        ranking = list(SEVERITY_ORDER)
        found: dict[str, str] = {}
        for finding in result.findings:
            scope = finding.scope
            if scope not in found or ranking.index(finding.severity) < ranking.index(found[scope]):
                found[scope] = finding.severity
        return found

    fig, axes = plt.subplots(1, 2, figsize=(11.2, 2.6))
    fig.patch.set_facecolor("#ffffff")
    draw_graph(axes[0], bad_file, worst_by_scope(bad),
               f"examples/bad_dag.py - grade {bad.grade} ({bad.risk} points)")
    draw_graph(axes[1], good_file, worst_by_scope(good),
               f"examples/good_dag.py - grade {good.grade} ({good.risk} points)")
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, format="png", dpi=150)
    plt.close(fig)


def main() -> int:
    images = ROOT / "docs" / "images"
    images.mkdir(parents=True, exist_ok=True)
    # Relative paths keep the rendered review header readable in the README.
    os.chdir(ROOT)

    bad_file = parse_file("examples/bad_dag.py")
    good_file = parse_file("examples/good_dag.py")
    bad = review(bad_file)
    good = review(good_file)

    payload = {
        "bad_dag": bad.as_dict(),
        "good_dag": good.as_dict(),
        "rules_triggered": sorted({finding.code for finding in bad.findings}),
    }
    (ROOT / "docs" / "results.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    terminal = (
        "$ dag-critic review examples/bad_dag.py examples/good_dag.py --summary\n\n"
        + render_summary([bad, good])
        + "\n\n$ dag-critic review examples/bad_dag.py\n\n"
        + render_review(bad)
    )
    render_terminal(terminal, images / "review.png", "dag-critic  -  14 rules, no Airflow import")
    render_rule_bars(bad, good, images / "findings-by-rule.svg")
    render_graph(bad_file, bad, good_file, good, images / "task-graph.png")

    print(
        f"bad_dag risk={bad.risk} grade={bad.grade} counts={bad.counts} | "
        f"good_dag risk={good.risk} grade={good.grade}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
