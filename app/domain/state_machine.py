"""JobStateMachine — the single source of truth for legal Job status transitions.

Services and Celery orchestration tasks call through transition() rather than
setting job.status directly, so an illegal transition (e.g. cancelled -> running)
is impossible from any code path.
"""

from app.domain.enums import JobStatus

# event -> {current_status: next_status}
_TRANSITIONS: dict[str, dict[JobStatus, JobStatus]] = {
    "schedule": {JobStatus.DRAFT: JobStatus.SCHEDULED},
    "start": {
        JobStatus.DRAFT: JobStatus.DISCOVERING,
        JobStatus.SCHEDULED: JobStatus.DISCOVERING,
    },
    "begin_running": {JobStatus.DISCOVERING: JobStatus.RUNNING},
    "pause": {
        JobStatus.DISCOVERING: JobStatus.PAUSED,
        JobStatus.RUNNING: JobStatus.PAUSED,
    },
    "resume": {
        JobStatus.PAUSED: JobStatus.RUNNING,
    },
    "cancel": {
        JobStatus.DRAFT: JobStatus.CANCELLED,
        JobStatus.SCHEDULED: JobStatus.CANCELLED,
        JobStatus.DISCOVERING: JobStatus.CANCELLING,
        JobStatus.RUNNING: JobStatus.CANCELLING,
        JobStatus.PAUSED: JobStatus.CANCELLING,
    },
    "finish_cancel": {JobStatus.CANCELLING: JobStatus.CANCELLED},
    "complete": {
        JobStatus.DISCOVERING: JobStatus.COMPLETED,
        JobStatus.RUNNING: JobStatus.COMPLETED,
    },
    "complete_partial": {
        JobStatus.DISCOVERING: JobStatus.COMPLETED_PARTIAL,
        JobStatus.RUNNING: JobStatus.COMPLETED_PARTIAL,
    },
    "fail": {
        JobStatus.DISCOVERING: JobStatus.FAILED,
        JobStatus.RUNNING: JobStatus.FAILED,
        JobStatus.SCHEDULED: JobStatus.FAILED,
    },
}


class IllegalTransitionError(Exception):
    def __init__(self, event: str, current_status: JobStatus):
        super().__init__(f"Cannot apply event '{event}' to job in status '{current_status.value}'")
        self.event = event
        self.current_status = current_status


class JobStateMachine:
    @staticmethod
    def can_transition(current_status: JobStatus, event: str) -> bool:
        return current_status in _TRANSITIONS.get(event, {})

    @staticmethod
    def transition(current_status: JobStatus, event: str) -> JobStatus:
        mapping = _TRANSITIONS.get(event)
        if mapping is None or current_status not in mapping:
            raise IllegalTransitionError(event, current_status)
        return mapping[current_status]
