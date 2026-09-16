"""Control-flow guarantees for plan_topup_round.

Both are regressions of bugs seen live:

  * An early `return` from inside the `with session_scope()` block skipped the
    finishing work that runs after the transaction, so a job that had reached
    its target sat in RUNNING forever instead of trimming and completing.
  * Celery tasks dispatched from inside the open transaction raced the commit.
"""

import ast
import inspect
from pathlib import Path


def _function_source(name: str) -> str:
    source = Path("app/tasks/orchestration_tasks.py").read_text()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(source, node) or ""
    raise AssertionError(f"{name} not found")


class TestPlanTopupRoundControlFlow:
    def test_finishing_work_is_not_skipped_by_an_early_return(self):
        """The trim + completion path must sit after the transaction block.

        If it were inside, the `return` that ends the no-more-rounds branch
        would skip it and strand the job in RUNNING -- which is exactly what
        happened: a job reached 160/120 and then never went terminal.
        """
        src = _function_source("plan_topup_round")
        tree = ast.parse(src)
        fn = tree.body[0]

        with_nodes = [n for n in ast.walk(fn) if isinstance(n, ast.With)]
        session_with = [
            n for n in with_nodes
            if any("session_scope" in ast.dump(item.context_expr) for item in n.items)
        ]
        assert session_with, "expected a session_scope block"

        inside = {id(n) for n in ast.walk(session_with[0])}
        trim_calls = [
            n for n in ast.walk(fn)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "trim_excess_assets"
        ]
        assert trim_calls, "expected trim_excess_assets to be called"
        for call in trim_calls:
            assert id(call) not in inside, "trim must run after the transaction commits"

    def test_redispatch_happens_outside_the_transaction(self):
        """_resume_task dispatches Celery tasks; doing that inside the
        transaction races them against the commit, so a re-dispatched
        discovery task can read a stale PENDING/quota state."""
        src = _function_source("plan_topup_round")
        fn = ast.parse(src).body[0]

        session_with = [
            n for n in ast.walk(fn)
            if isinstance(n, ast.With)
            and any("session_scope" in ast.dump(i.context_expr) for i in n.items)
        ][0]
        inside = {id(n) for n in ast.walk(session_with)}

        resume_calls = [
            n for n in ast.walk(fn)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "_resume_task"
        ]
        assert resume_calls
        for call in resume_calls:
            assert id(call) not in inside

    def test_stop_flags_are_cleared_before_a_new_round(self):
        """Without this a new round fetches nothing at all.

        should_stop() consults the job-wide target flag and each source's cap
        flag; both were set when the previous round hit its ceiling, so every
        connector would stop on its first iteration.
        """
        src = _function_source("plan_topup_round")
        assert "clear_target_reached" in src
        assert "clear_source_cap_reached" in src
