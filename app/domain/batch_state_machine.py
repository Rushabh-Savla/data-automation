"""BatchStateMachine — formal legal transitions and validation for ImageBatch lifecycle."""

from __future__ import annotations

import logging
from app.domain.enums import BatchStatus

logger = logging.getLogger(__name__)


class IllegalBatchTransitionError(ValueError):
    """Raised when a batch status transition is requested that violates the state machine."""

    def __init__(self, current: BatchStatus, target: BatchStatus, reason: str = ""):
        msg = f"Illegal batch transition from {current.value} to {target.value}"
        if reason:
            msg += f": {reason}"
        super().__init__(msg)
        self.current = current
        self.target = target


# Legal transitions mapping: current_status -> set of allowed next_statuses
_LEGAL_BATCH_TRANSITIONS: dict[BatchStatus, set[BatchStatus]] = {
    BatchStatus.CREATED: {
        BatchStatus.DISCOVERING,
        BatchStatus.DOWNLOADING,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.DISCOVERING: {
        BatchStatus.DOWNLOADING,
        BatchStatus.PROCESSING,
        BatchStatus.READY_FOR_UPLOAD,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.DOWNLOADING: {
        BatchStatus.PROCESSING,
        BatchStatus.DISCOVERING,
        BatchStatus.REPLENISHING,
        BatchStatus.FILLING_SHORTFALL,
        BatchStatus.READY_FOR_UPLOAD,
        BatchStatus.UPLOADING,
        BatchStatus.VERIFYING,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.PROCESSING: {
        BatchStatus.FILTERING,
        BatchStatus.REPLENISHING,
        BatchStatus.FILLING_SHORTFALL,
        BatchStatus.READY_FOR_UPLOAD,
        BatchStatus.UPLOADING,
        BatchStatus.PARTIAL,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.FILTERING: {
        BatchStatus.PROCESSING,
        BatchStatus.REPLENISHING,
        BatchStatus.FILLING_SHORTFALL,
        BatchStatus.READY_FOR_UPLOAD,
        BatchStatus.PARTIAL,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.REPLENISHING: {
        BatchStatus.DOWNLOADING,
        BatchStatus.DISCOVERING,
        BatchStatus.PROCESSING,
        BatchStatus.READY_FOR_UPLOAD,
        BatchStatus.UPLOADING,
        BatchStatus.VERIFYING,
        BatchStatus.PARTIAL,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.FILLING_SHORTFALL: {
        BatchStatus.DOWNLOADING,
        BatchStatus.DISCOVERING,
        BatchStatus.PROCESSING,
        BatchStatus.READY_FOR_UPLOAD,
        BatchStatus.PARTIAL,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.READY_FOR_UPLOAD: {
        BatchStatus.PROCESSING,
        BatchStatus.UPLOADING,
        BatchStatus.NAS_UPLOADING,
        BatchStatus.NAS_PENDING,
        BatchStatus.VERIFYING,
        BatchStatus.NAS_VERIFIED,
        BatchStatus.COMPLETED,
        BatchStatus.PARTIAL,
        BatchStatus.REPLENISHING,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.UPLOADING: {
        BatchStatus.VERIFYING,
        BatchStatus.NAS_VERIFIED,
        BatchStatus.READY_FOR_UPLOAD,  # Re-attempt on transient failure
        BatchStatus.COMPLETED,
        BatchStatus.REPLENISHING,
        BatchStatus.DOWNLOADING,
        BatchStatus.PARTIAL,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.NAS_UPLOADING: {
        BatchStatus.VERIFYING,
        BatchStatus.NAS_VERIFIED,
        BatchStatus.READY_FOR_UPLOAD,
        BatchStatus.COMPLETED,
        BatchStatus.PARTIAL,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.NAS_PENDING: {
        BatchStatus.UPLOADING,
        BatchStatus.NAS_UPLOADING,
        BatchStatus.READY_FOR_UPLOAD,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.VERIFYING: {
        BatchStatus.COMPLETED,
        BatchStatus.PARTIAL,
        BatchStatus.UPLOADING,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.NAS_VERIFIED: {
        BatchStatus.COMPLETED,
        BatchStatus.PARTIAL,
        BatchStatus.UPLOADING,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.PAUSED: {
        # Resumes back to its active phase once backpressure clears
        BatchStatus.DISCOVERING,
        BatchStatus.DOWNLOADING,
        BatchStatus.PROCESSING,
        BatchStatus.REPLENISHING,
        BatchStatus.FILLING_SHORTFALL,
        BatchStatus.READY_FOR_UPLOAD,
        BatchStatus.UPLOADING,
        BatchStatus.NAS_UPLOADING,
        BatchStatus.FAILED,
    },
    BatchStatus.PARTIAL: {
        # Can be uploaded, verified, finalized, paused, or reopened
        BatchStatus.COMPLETED,
        BatchStatus.READY_FOR_UPLOAD,
        BatchStatus.UPLOADING,
        BatchStatus.NAS_UPLOADING,
        BatchStatus.VERIFYING,
        BatchStatus.NAS_VERIFIED,
        BatchStatus.DISCOVERING,
        BatchStatus.DOWNLOADING,
        BatchStatus.PAUSED,
        BatchStatus.FAILED,
    },
    BatchStatus.FAILED: {
        # Recovery attempts
        BatchStatus.DISCOVERING,
        BatchStatus.DOWNLOADING,
        BatchStatus.PROCESSING,
        BatchStatus.REPLENISHING,
        BatchStatus.READY_FOR_UPLOAD,
    },
    BatchStatus.COMPLETED: set(),  # Terminal state
}


class BatchStateMachine:
    """Validator and executor for BatchStatus lifecycle transitions."""

    @staticmethod
    def can_transition(current: BatchStatus, target: BatchStatus) -> bool:
        if current == target:
            return True
        allowed = _LEGAL_BATCH_TRANSITIONS.get(current, set())
        return target in allowed

    @staticmethod
    def transition(current: BatchStatus, target: BatchStatus, reason: str = "") -> BatchStatus:
        if not BatchStateMachine.can_transition(current, target):
            logger.error(
                f"[BATCH STATE MACHINE] Illegal transition: {current.value} -> {target.value}"
                + (f" ({reason})" if reason else "")
            )
            raise IllegalBatchTransitionError(current, target, reason)
        logger.info(
            f"[BATCH STATE MACHINE] Transitioned {current.value} -> {target.value}"
            + (f" ({reason})" if reason else "")
        )
        return target
