"""Rules for reviewing a parsed DAG file."""

from __future__ import annotations

from dataclasses import dataclass

from .dagfile import DagFile, DagSpec, TaskSpec, value_of

SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}
SEVERITY_POINTS = {"critical": 30, "high": 15, "medium": 8, "low": 3}

SECRET_KEYWORDS = ("password", "secret", "secret_key", "token", "api_key", "aws_secret_access_key")
SCHEDULE_KEYS = ("schedule", "schedule_interval", "timetable")
START_DATE_KEYS = ("start_date",)


@dataclass(frozen=True)
class Finding:
    code: str
    severity: str
    line: int
    scope: str
    message: str
    hint: str

    @property
    def points(self) -> int:
        return SEVERITY_POINTS[self.severity]

    def as_dict(self) -> dict:
        return {
            "code": self.code,
            "severity": self.severity,
            "line": self.line,
            "scope": self.scope,
            "message": self.message,
            "hint": self.hint,
            "points": self.points,
        }


@dataclass(frozen=True)
class Review:
    path: str
    findings: tuple[Finding, ...]
    risk: int
    grade: str
    counts: dict[str, int]
    dag_count: int
    task_count: int

    @property
    def clean(self) -> bool:
        return not self.findings

    def at_or_above(self, severity: str) -> list[Finding]:
        limit = SEVERITY_ORDER[severity]
        return [item for item in self.findings if SEVERITY_ORDER[item.severity] <= limit]

    def should_fail(self, fail_on: str) -> bool:
        if fail_on == "never":
            return False
        return bool(self.at_or_above(fail_on))

    def as_dict(self) -> dict:
        return {
            "path": self.path,
            "risk": self.risk,
            "grade": self.grade,
            "counts": self.counts,
            "dags": self.dag_count,
            "tasks": self.task_count,
            "findings": [item.as_dict() for item in self.findings],
        }


def _grade(risk: int) -> str:
    if risk == 0:
        return "A"
    if risk <= 10:
        return "B"
    if risk <= 30:
        return "C"
    if risk <= 60:
        return "D"
    return "F"


def review(dagfile: DagFile) -> Review:
    findings: list[Finding] = []

    if not dagfile.dags:
        findings.append(
            Finding(
                "no-dag",
                "high",
                0,
                "<file>",
                "no DAG() call found - the scheduler would create nothing from this file",
                "define the DAG in this file, or point the critic at the file that does",
            )
        )

    for dag in dagfile.dags:
        findings.extend(_review_dag(dag))
    if dagfile.dags:
        findings.extend(_review_every_task(dagfile))
    findings.extend(_review_duplicates(dagfile))
    findings.extend(_review_module_calls(dagfile))

    findings.sort(key=lambda item: (SEVERITY_ORDER[item.severity], item.line, item.code))
    risk = sum(item.points for item in findings)
    counts = {severity: 0 for severity in SEVERITY_ORDER}
    for item in findings:
        counts[item.severity] += 1
    return Review(
        path=dagfile.path,
        findings=tuple(findings),
        risk=risk,
        grade=_grade(risk),
        counts=counts,
        dag_count=len(dagfile.dags),
        task_count=len(dagfile.tasks),
    )


def _review_dag(dag: DagSpec) -> list[Finding]:
    findings: list[Finding] = []
    scope = dag.dag_id

    if not dag.has(*START_DATE_KEYS):
        findings.append(
            Finding(
                "no-start-date",
                "medium",
                dag.line,
                scope,
                "no start_date on the DAG or in default_args",
                "set start_date; without it the scheduler has no idea when history begins",
            )
        )

    catchup = dag.get("catchup")
    if catchup is None:
        findings.append(
            Finding(
                "catchup-default",
                "medium",
                dag.line,
                scope,
                "catchup is not set, so it defaults to True and the DAG backfills all history",
                "set catchup=False unless you really want every missed interval run",
            )
        )
    elif catchup == "True":
        findings.append(
            Finding(
                "catchup-enabled",
                "medium",
                dag.line,
                scope,
                "catchup=True will backfill every interval since start_date on first deploy",
                "set catchup=False, or narrow start_date to the moment you want to begin",
            )
        )

    if not dag.has(*SCHEDULE_KEYS):
        findings.append(
            Finding(
                "no-schedule",
                "low",
                dag.line,
                scope,
                "no schedule / schedule_interval given",
                "state the schedule explicitly so the DAG's cadence is reviewable",
            )
        )

    if "owner" not in dag.default_args:
        findings.append(
            Finding(
                "no-owner",
                "low",
                dag.line,
                scope,
                "default_args has no owner",
                "set owner so failures are routed to a team instead of unowned@example.com",
            )
        )

    if dag.dynamic_id:
        findings.append(
            Finding(
                "dynamic-dag-id",
                "medium",
                dag.line,
                scope,
                "dag_id is an f-string, so the DAG identity changes between parses",
                "use a literal dag_id; parametrise inside the DAG instead",
            )
        )
    return findings


def _review_every_task(dagfile: DagFile) -> list[Finding]:
    findings: list[Finding] = []
    default_retries = any("retries" in dag.default_args for dag in dagfile.dags)

    if not default_retries:
        without = [task for task in dagfile.tasks if "retries" not in task.keywords]
        if without:
            labels = list(dict.fromkeys(task.label for task in without))
            findings.append(
                Finding(
                    "no-retries",
                    "medium",
                    without[0].line,
                    f"{len(without)} task(s)",
                    f"{len(without)} task(s) set no retries and default_args has none: "
                    + ", ".join(labels[:6]),
                    "set retries in default_args so one blip does not page a human",
                )
            )

    for task in dagfile.tasks:
        findings.extend(_review_task(task))
    return findings


def _review_task(task: TaskSpec) -> list[Finding]:
    findings: list[Finding] = []
    scope = task.label

    if task.is_sensor and "timeout" not in task.keywords:
        findings.append(
            Finding(
                "sensor-without-timeout",
                "high",
                task.line,
                scope,
                f"{task.operator} has no timeout, so it can hold a slot forever",
                "set timeout (and mode='reschedule' for long waits)",
            )
        )

    if task.dynamic_id:
        findings.append(
            Finding(
                "dynamic-task-id",
                "medium",
                task.line,
                scope,
                "task_id is an f-string, so task identity changes on every parse",
                "use a literal task_id and pass the varying value as an argument",
            )
        )

    if task.task_id == "<missing>":
        findings.append(
            Finding(
                "missing-task-id",
                "medium",
                task.line,
                scope,
                f"{task.operator} has no task_id",
                "give every task an explicit, stable task_id",
            )
        )

    for keyword, rendered in task.keywords.items():
        if keyword.lower() in SECRET_KEYWORDS and rendered.startswith(("'", '"')) and value_of(
            rendered
        ):
            findings.append(
                Finding(
                    "hardcoded-secret",
                    "critical",
                    task.line,
                    scope,
                    f"{task.operator} passes {keyword}= as a literal string",
                    "load it from a connection, a Variable, or a secret backend",
                )
            )
            break
    return findings


def _review_duplicates(dagfile: DagFile) -> list[Finding]:
    findings: list[Finding] = []
    seen: dict[str, TaskSpec] = {}
    reported: set[str] = set()
    for task in dagfile.tasks:
        if task.dynamic_id:
            # An f-string task_id is reported as dynamic-task-id; calling it a duplicate
            # would be an artefact of the critic not being able to evaluate the f-string.
            continue
        if task.task_id in seen and task.task_id not in reported:
            reported.add(task.task_id)
            first = seen[task.task_id]
            findings.append(
                Finding(
                    "duplicate-task-id",
                    "high",
                    task.line,
                    task.task_id,
                    f"task_id {task.task_id!r} is defined twice (lines {first.line} and "
                    f"{task.line}); the second silently replaces the first",
                    "give each task a distinct task_id",
                )
            )
        else:
            seen.setdefault(task.task_id, task)
    return findings


def _review_module_calls(dagfile: DagFile) -> list[Finding]:
    findings: list[Finding] = []
    reported: set[str] = set()
    for name, line in dagfile.module_calls:
        if name in reported:
            continue
        reported.add(name)
        findings.append(
            Finding(
                "module-level-side-effect",
                "high",
                line,
                "<module>",
                f"{name}() runs at import time, i.e. on every scheduler parse",
                "move the call inside the task callable so it runs once per task run",
            )
        )
    return findings
