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

    def test_plan_topup_round_no_longer_triggers_offload_itself(self):
        """NAS offload must not be dispatched from plan_topup_round any more --
        it now only fires from evaluate_job_completion, once the job's status
        has actually committed to COMPLETED/COMPLETED_PARTIAL (see
        TestEvaluateJobCompletionTriggersOffload below)."""
        src = _function_source("plan_topup_round")
        assert "schedule_offload_for_job" not in src


class TestEvaluateJobCompletionTriggersOffload:
    """NAS offload must only start once the job is actually marked
    COMPLETED/COMPLETED_PARTIAL -- never while it's still RUNNING/DISCOVERING,
    and never from inside the open transaction (it must race the commit the
    same way the other post-transaction dispatches in this module do)."""

    def test_offload_call_is_gated_on_completed_statuses(self):
        src = _function_source("evaluate_job_completion")
        fn = ast.parse(src).body[0]

        calls = [
            n for n in ast.walk(fn)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "schedule_offload_for_job"
        ]
        assert calls, "expected schedule_offload_for_job to be called"
        assert len(calls) == 1, "offload must be dispatched exactly once per evaluation"

        # It must sit inside an `if new_status in (...COMPLETED...):` guard,
        # not unconditionally.
        guard = None
        for node in ast.walk(fn):
            if isinstance(node, ast.If) and any(
                id(n) == id(calls[0]) for n in ast.walk(node)
            ):
                guard = node
                break
        assert guard is not None, "offload call must be inside an if-guard"
        test_src = ast.unparse(guard.test)
        assert "new_status" in test_src
        assert "COMPLETED" in test_src

    def test_offload_call_is_outside_the_open_transaction(self):
        """Dispatched after `with session_scope()` exits, i.e. after the status
        transition has committed -- not from inside the transaction, which
        would race the commit the same way plan_topup_round's redispatch does."""
        src = _function_source("evaluate_job_completion")
        fn = ast.parse(src).body[0]

        session_with = [
            n for n in ast.walk(fn)
            if isinstance(n, ast.With)
            and any("session_scope" in ast.dump(i.context_expr) for i in n.items)
        ]
        assert session_with, "expected a session_scope block"
        inside = {id(n) for n in ast.walk(session_with[0])}

        calls = [
            n for n in ast.walk(fn)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "schedule_offload_for_job"
        ]
        assert calls
        for call in calls:
            assert id(call) not in inside, "offload must run after the transaction commits"


class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.expiries = {}

    def incr(self, key):
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    def expire(self, key, seconds):
        self.expiries[key] = seconds

    def delete(self, key):
        self.store.pop(key, None)


class TestDeferUntilFiltered:
    """When the filter could not judge a round, retry it instead of planning."""

    def _patch(self, monkeypatch):
        import app.tasks.filtering_tasks as ft
        import app.tasks.orchestration_tasks as ot

        redis = _FakeRedis()
        scheduled = []

        class _Task:
            def apply_async(self, args, countdown):
                scheduled.append((tuple(args), countdown))

        monkeypatch.setattr(ot, "get_redis", lambda: redis)
        monkeypatch.setattr(ft, "run_relevance_filter", _Task())
        return ot, redis, scheduled

    def test_schedules_a_filter_retry_after_a_delay(self, monkeypatch):
        ot, _redis, scheduled = self._patch(monkeypatch)

        ot._defer_until_filtered("job-1", 1854, 1854)

        assert scheduled == [(("job-1",), ot._REFILTER_DELAY_S)]

    def test_counts_attempts_and_lets_the_counter_expire(self, monkeypatch):
        ot, redis, _ = self._patch(monkeypatch)

        ot._defer_until_filtered("job-1", 10, 10)
        ot._defer_until_filtered("job-1", 10, 10)

        assert redis.store["job:job-1:refilter_attempts"] == 2
        assert redis.expiries["job:job-1:refilter_attempts"] > 0

    def test_gives_up_retrying_after_the_cap_and_leaves_it_to_the_watchdog(self, monkeypatch):
        ot, _redis, scheduled = self._patch(monkeypatch)

        for _ in range(ot._REFILTER_MAX_ATTEMPTS + 5):
            ot._defer_until_filtered("job-1", 10, 10)

        assert len(scheduled) == ot._REFILTER_MAX_ATTEMPTS

    def test_jobs_are_counted_separately(self, monkeypatch):
        ot, _redis, scheduled = self._patch(monkeypatch)

        for _ in range(ot._REFILTER_MAX_ATTEMPTS):
            ot._defer_until_filtered("job-1", 10, 10)
        ot._defer_until_filtered("job-2", 10, 10)

        assert scheduled[-1][0] == ("job-2",)

    def test_the_retry_window_covers_a_realistic_outage(self):
        import app.tasks.orchestration_tasks as ot

        assert ot._REFILTER_DELAY_S * ot._REFILTER_MAX_ATTEMPTS >= 3600


class TestPlanTopupRoundWaitsForTheFilter:
    def test_it_checks_for_undecided_assets_before_planning(self):
        src = _function_source("plan_topup_round")

        assert "filtering_incomplete(" in src
        assert "_defer_until_filtered(" in src
        # ... and the check comes before the planner is asked for a round.
        assert src.index("filtering_incomplete(") < src.index("TopupPlanner.plan(")

    def test_it_does_not_fall_through_to_completion_while_filtering_is_incomplete(self):
        src = _function_source("plan_topup_round")
        after_defer = src[src.index("_defer_until_filtered(") :]

        # The very next statement after deferring is a bare `return`: the job is not done.
        assert after_defer.split("\n")[1].strip() == "return"

    def test_the_yield_denominator_excludes_undecided_assets(self):
        src = _function_source("plan_topup_round")

        assert "promoted_total = total_assets - undecided" in src

    def test_the_retry_counter_resets_once_filtering_completes(self):
        src = _function_source("plan_topup_round")

        assert "refilter_attempts" in src[src.index("filtering_incomplete(") :]


class TestDiscoveryKeepsTheWholeShard:
    """discover_* rebuilt each shard by hand and dropped the "unlabelled" flag, so every
    scanned image was tagged matched_by="label". Each dataset task must pass the whole
    external_ref through."""

    def test_every_dataset_discovery_passes_the_shard_through_unchanged(self):
        source = Path("app/tasks/discovery_tasks.py").read_text()
        tree = ast.parse(source)
        checked = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in {
                "discover_coco", "discover_pascal_voc", "discover_davis", "discover_nuscenes",
                "discover_kitti", "discover_sun_rgbd", "discover_open_images",
            }:
                body = ast.get_source_segment(source, node) or ""
                assert "s.external_ref" in body, node.name
                # the old hand-built form kept only named keys
                assert '"external_ref": {"image_ids": s.external_ref["image_ids"]' not in body, node.name
                assert '"external_ref": {"frames": s.external_ref["frames"]}' not in body, node.name
                assert '"external_ref": {"sample_data_tokens": s.external_ref["sample_data_tokens"]}' not in body, node.name
                checked.append(node.name)
        assert len(checked) == 7
