"""Read a DAG file with :mod:`ast`, without importing Airflow.

Nothing in this module executes the target file. A DAG file is data: a set of ``DAG(...)``
calls, operator instantiations, and ``>>`` edges. Parsing it as syntax means the critic works
on a laptop with no Airflow installed, and cannot be fooled by an ``if`` that only runs on the
scheduler.
"""

from __future__ import annotations

import ast
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

OPERATOR_SUFFIXES = ("Operator", "Sensor")
DAG_FACTORY = "DAG"

# Calls that reach outside the process. At module scope these run on every scheduler parse,
# not once per task run - the classic cause of "the scheduler is slow" and duplicated writes.
SIDE_EFFECT_NAMES = frozenset(
    {
        "open",
        "requests.get",
        "requests.post",
        "requests.put",
        "requests.delete",
        "requests.request",
        "urllib.request.urlopen",
        "urlopen",
        "subprocess.run",
        "subprocess.Popen",
        "subprocess.check_output",
        "boto3.client",
        "boto3.resource",
        "smtplib.SMTP",
        "psycopg2.connect",
        "pymongo.MongoClient",
        "redis.Redis",
    }
)


class DagParseError(ValueError):
    """Raised when a file is not valid Python."""


@dataclass(frozen=True)
class DagSpec:
    dag_id: str
    line: int
    keywords: Mapping[str, str] = field(default_factory=dict)
    default_args: Mapping[str, str] = field(default_factory=dict)
    dynamic_id: bool = False

    def get(self, name: str) -> str | None:
        return self.keywords.get(name)

    def has(self, *names: str) -> bool:
        return any(name in self.keywords for name in names)


@dataclass(frozen=True)
class TaskSpec:
    task_id: str
    operator: str
    line: int
    keywords: Mapping[str, str] = field(default_factory=dict)
    dynamic_id: bool = False
    var: str = ""
    col: int = 0

    @property
    def is_sensor(self) -> bool:
        return self.operator.endswith("Sensor")

    @property
    def label(self) -> str:
        if self.dynamic_id or self.task_id == "<missing>":
            return f"{self.operator}@{self.line}"
        return self.task_id


@dataclass(frozen=True)
class DagFile:
    path: str
    dags: tuple[DagSpec, ...]
    tasks: tuple[TaskSpec, ...]
    edges: tuple[tuple[str, str], ...]
    module_calls: tuple[tuple[str, int], ...]

    @property
    def task_ids(self) -> tuple[str, ...]:
        return tuple(task.task_id for task in self.tasks)


def _call_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _call_name(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    if isinstance(node, ast.Call):
        return _call_name(node.func)
    return ""


def _short_name(node: ast.expr) -> str:
    name = _call_name(node)
    return name.rsplit(".", 1)[-1]


def _literal(node: ast.AST) -> tuple[str, bool]:
    """Return (rendered value, is_dynamic) for a keyword value."""
    if isinstance(node, ast.Constant):
        return repr(node.value), False
    if isinstance(node, ast.JoinedStr):
        return "<f-string>", True
    if isinstance(node, ast.Dict):
        parts = []
        for key, value in zip(node.keys, node.values, strict=False):
            key_text = _literal(key)[0] if key is not None else "..."
            parts.append(f"{key_text.strip(chr(39))}: {_literal(value)[0]}")
        return "{" + ", ".join(parts) + "}", False
    try:
        return ast.unparse(node), False
    except Exception:  # pragma: no cover - ast.unparse is very forgiving
        return "<?>", False


def _keywords(call: ast.Call) -> dict[str, str]:
    result: dict[str, str] = {}
    for keyword in call.keywords:
        if keyword.arg is None:
            continue
        result[keyword.arg] = _literal(keyword.value)[0]
    return result


def _dynamic_keyword(call: ast.Call, name: str) -> bool:
    for keyword in call.keywords:
        if keyword.arg == name:
            return isinstance(keyword.value, ast.JoinedStr)
    return False


def _flatten(node: ast.expr) -> list[str]:
    """Flatten ``a >> [b, c]`` style operands into task ids."""
    if isinstance(node, (ast.List, ast.Tuple)):
        names: list[str] = []
        for element in node.elts:
            names.extend(_flatten(element))
        return names
    if isinstance(node, (ast.Name, ast.Attribute)):
        return [_call_name(node)]
    return []


def _chain(node: ast.expr) -> tuple[list[tuple[str, str]], list[str]]:
    """Edges and tail task ids for one ``>>`` expression.

    ``a >> b >> c`` parses as ``(a >> b) >> c``, so the edge list has to be built from the
    tail of the left side to each name on the right side, not from a flat left/right pair.
    """
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
        left_edges, left_tail = _chain(node.left)
        right_edges, right_tail = _chain(node.right)
        cross = [(source, target) for source in left_tail for target in right_tail]
        return left_edges + right_edges + cross, right_tail
    return [], _flatten(node)


def _collect_edges(node: ast.AST) -> list[tuple[str, str]]:
    edges: list[tuple[str, str]] = []
    for child in ast.walk(node):
        if isinstance(child, ast.BinOp) and isinstance(child.op, ast.RShift):
            found, _tail = _chain(child)
            for edge in found:
                if edge not in edges and edge[0] != edge[1]:
                    edges.append(edge)
    return edges


class _Visitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.dags: list[DagSpec] = []
        self.tasks: list[TaskSpec] = []
        self.module_calls: list[tuple[str, int]] = []
        self._depth = 0

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 - ast API
        self._depth += 1
        self.generic_visit(node)
        self._depth -= 1

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802 - ast API
        self._depth += 1
        self.generic_visit(node)
        self._depth -= 1

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802 - ast API
        self._depth += 1
        self.generic_visit(node)
        self._depth -= 1

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 - ast API
        name = _call_name(node.func)
        short = name.rsplit(".", 1)[-1]
        keywords = _keywords(node)

        if short == DAG_FACTORY:
            default_args: dict[str, str] = {}
            for keyword in node.keywords:
                if keyword.arg == "default_args" and isinstance(keyword.value, ast.Name):
                    for sibling in ast.walk(self._root):
                        if (
                            isinstance(sibling, ast.Assign)
                            and isinstance(sibling.targets[0], ast.Name)
                            and sibling.targets[0].id == keyword.value.id
                            and isinstance(sibling.value, ast.Dict)
                        ):
                            for key, value in zip(
                                sibling.value.keys, sibling.value.values, strict=False
                            ):
                                if isinstance(key, ast.Constant):
                                    default_args[str(key.value)] = _literal(value)[0]
            dag_id = keywords.get("dag_id", "'<unnamed>'").strip("'\"")
            self.dags.append(
                DagSpec(
                    dag_id=dag_id,
                    line=node.lineno,
                    keywords=keywords,
                    default_args=default_args,
                    dynamic_id=_dynamic_keyword(node, "dag_id"),
                )
            )
        elif short.endswith(OPERATOR_SUFFIXES):
            task_id = keywords.get("task_id", "<missing>").strip("'\"")
            self.tasks.append(
                TaskSpec(
                    task_id=task_id,
                    operator=short,
                    line=node.lineno,
                    keywords=keywords,
                    dynamic_id=_dynamic_keyword(node, "task_id"),
                    col=node.col_offset,
                )
            )

        if self._depth == 0 and (name in SIDE_EFFECT_NAMES or short in SIDE_EFFECT_NAMES):
            self.module_calls.append((name or short, node.lineno))

        self.generic_visit(node)

    _root: ast.Module

    def run(self, tree: ast.Module) -> None:
        self._root = tree
        self.generic_visit(tree)


def _assignment_names(tree: ast.Module) -> dict[tuple[int, int], str]:
    """Map an operator call position to the variable it is assigned to.

    ``wait = ExternalTaskSensor(task_id="wait_for_upstream")`` means ``>>`` edges use
    ``wait`` while Airflow uses ``wait_for_upstream``; the review should speak Airflow's names.
    """
    mapping: dict[tuple[int, int], str] = {}
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Call)
            and _short_name(node.value).endswith(OPERATOR_SUFFIXES)
        ):
            mapping[(node.value.lineno, node.value.col_offset)] = node.targets[0].id
    return mapping


def parse_source(text: str, path: str = "<string>") -> DagFile:
    try:
        tree = ast.parse(text, filename=path)
    except SyntaxError as exc:
        raise DagParseError(f"{path}:{exc.lineno}: {exc.msg}") from exc
    visitor = _Visitor()
    visitor.run(tree)

    names = _assignment_names(tree)
    tasks = tuple(
        replace(task, var=names.get((task.line, task.col), "")) for task in visitor.tasks
    )
    var_to_id = {task.var: task.label for task in tasks if task.var}
    raw_edges = _collect_edges(tree)
    edges = tuple(
        (var_to_id.get(source, source), var_to_id.get(target, target))
        for source, target in raw_edges
    )
    return DagFile(
        path=path,
        dags=tuple(visitor.dags),
        tasks=tasks,
        edges=edges,
        module_calls=tuple(visitor.module_calls),
    )


def parse_file(path: Path | str) -> DagFile:
    source = Path(path)
    if not source.exists():
        raise DagParseError(f"file not found: {source}")
    return parse_source(source.read_text(encoding="utf-8"), str(source))


def caller_kwargs(task: TaskSpec) -> Mapping[str, Any]:
    return task.keywords


def value_of(rendered: str) -> str:
    """Strip the repr quotes so messages read like the source."""
    text = rendered.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "'\"":
        return text[1:-1]
    return text
