import pytest

from app.domain.enums import JobStatus
from app.domain.state_machine import IllegalTransitionError, JobStateMachine


class TestJobStateMachine:
    def test_draft_can_start(self):
        assert JobStateMachine.transition(JobStatus.DRAFT, "start") == JobStatus.DISCOVERING

    def test_draft_can_schedule(self):
        assert JobStateMachine.transition(JobStatus.DRAFT, "schedule") == JobStatus.SCHEDULED

    def test_scheduled_can_start(self):
        assert JobStateMachine.transition(JobStatus.SCHEDULED, "start") == JobStatus.DISCOVERING

    def test_running_can_pause_and_resume(self):
        paused = JobStateMachine.transition(JobStatus.RUNNING, "pause")
        assert paused == JobStatus.PAUSED
        assert JobStateMachine.transition(paused, "resume") == JobStatus.RUNNING

    def test_running_cancel_goes_through_cancelling(self):
        assert JobStateMachine.transition(JobStatus.RUNNING, "cancel") == JobStatus.CANCELLING
        assert JobStateMachine.transition(JobStatus.CANCELLING, "finish_cancel") == JobStatus.CANCELLED

    def test_draft_cancel_is_immediate(self):
        # A job that never started has no in-flight work to wind down, so it
        # goes straight to cancelled rather than through cancelling.
        assert JobStateMachine.transition(JobStatus.DRAFT, "cancel") == JobStatus.CANCELLED

    @pytest.mark.parametrize(
        "current,event",
        [
            (JobStatus.CANCELLED, "resume"),
            (JobStatus.CANCELLED, "start"),
            (JobStatus.COMPLETED, "pause"),
            (JobStatus.DRAFT, "resume"),
            (JobStatus.PAUSED, "start"),
            (JobStatus.RUNNING, "schedule"),
        ],
    )
    def test_illegal_transitions_are_rejected(self, current, event):
        with pytest.raises(IllegalTransitionError):
            JobStateMachine.transition(current, event)

    def test_can_transition_reports_without_raising(self):
        assert JobStateMachine.can_transition(JobStatus.RUNNING, "pause") is True
        assert JobStateMachine.can_transition(JobStatus.RUNNING, "schedule") is False
