"""Command line entry point for dag-critic."""

from __future__ import annotations

import argparse
import json
import sys

from . import __version__
from .dagfile import DagParseError, parse_file
from .report import (
    render_catalogue,
    render_markdown,
    render_mermaid,
    render_review,
    render_summary,
)
from .rules import SEVERITY_ORDER, review

SEVERITIES = tuple(SEVERITY_ORDER)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dag-critic",
        description="Review Airflow DAG files for the mistakes that survive code review.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    rev = sub.add_parser("review", help="score one or more DAG files")
    rev.add_argument("paths", nargs="+")
    rev.add_argument("--format", choices=("text", "markdown", "json"), default="text")
    rev.add_argument("--fail-on", choices=("never",) + SEVERITIES, default="high")
    rev.add_argument("--summary", action="store_true", help="print a per-file table instead")

    graph = sub.add_parser("graph", help="print the task graph of a file as Mermaid")
    graph.add_argument("path")

    stats = sub.add_parser("stats", help="count DAGs, tasks, and edges without judging them")
    stats.add_argument("paths", nargs="+")

    sub.add_parser("rules", help="list every rule and its severity")
    return parser


def _review_paths(paths: list[str]) -> tuple[list, str | None]:
    results = []
    for path in paths:
        try:
            results.append(review(parse_file(path)))
        except DagParseError as exc:
            return results, str(exc)
    return results, None


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "rules":
        print(render_catalogue())
        return 0

    if args.command == "graph":
        try:
            dagfile = parse_file(args.path)
        except DagParseError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        print(render_mermaid(dagfile))
        return 0

    if args.command == "stats":
        results, error = _review_paths(args.paths)
        if error:
            print(f"error: {error}", file=sys.stderr)
            return 2
        for result in results:
            print(f"{result.path}: dags {result.dag_count} tasks {result.task_count}")
        return 0

    results, error = _review_paths(args.paths)
    if error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    if args.format == "json":
        print(json.dumps([result.as_dict() for result in results], indent=2))
    elif args.format == "markdown":
        for result in results:
            print(render_markdown(result))
            print()
    elif args.summary:
        print(render_summary(results))
    else:
        for result in results:
            print(render_review(result))
            print()

    failing = [result for result in results if result.should_fail(args.fail_on)]
    return 1 if failing else 0


if __name__ == "__main__":
    raise SystemExit(main())
