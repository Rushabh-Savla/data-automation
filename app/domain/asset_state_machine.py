"""AssetStateMachine — formal legal transitions for individual Asset records."""

from __future__ import annotations

from app.domain.enums import AssetStatus


import logging

logger = logging.getLogger(__name__)


class IllegalAssetTransitionError(ValueError):
    """Raised when an asset status transition is requested that violates the state machine."""

    def __init__(self, current: AssetStatus, target: AssetStatus, reason: str = ""):
        msg = f"Illegal asset transition from {current.value} to {target.value}"
        if reason:
            msg += f": {reason}"
        super().__init__(msg)
        self.current = current
        self.target = target


# Legal transitions mapping: current_status -> set of allowed next_statuses
_LEGAL_ASSET_TRANSITIONS: dict[AssetStatus, set[AssetStatus]] = {
    AssetStatus.DISCOVERED: {AssetStatus.DOWNLOADING, AssetStatus.DOWNLOADED, AssetStatus.FAILED},
    AssetStatus.DOWNLOADING: {
        AssetStatus.DOWNLOADED,
        AssetStatus.STANDARDIZING,
        AssetStatus.STANDARDIZED,
        AssetStatus.PROMOTED,
        AssetStatus.RETRY_PENDING,
        AssetStatus.FAILED,
    },
    AssetStatus.DOWNLOADED: {
        AssetStatus.STANDARDIZING,
        AssetStatus.STANDARDIZED,
        AssetStatus.PROMOTED,
        AssetStatus.RETRY_PENDING,
        AssetStatus.FAILED,
    },
    AssetStatus.STANDARDIZING: {
        AssetStatus.STANDARDIZED,
        AssetStatus.PROMOTED,
        AssetStatus.RETRY_PENDING,
        AssetStatus.FAILED,
    },
    AssetStatus.STANDARDIZED: {
        AssetStatus.PROMOTED,
        AssetStatus.DUPLICATE,
        AssetStatus.RELEVANCE_PENDING,
        AssetStatus.RELEVANCE_PASSED,
        AssetStatus.FAILED,
    },
    AssetStatus.PROMOTED: {
        AssetStatus.RELEVANCE_PENDING,
        AssetStatus.RELEVANCE_PASSED,
        AssetStatus.RELEVANCE_REJECTED,
        AssetStatus.REVIEW_PENDING,
        AssetStatus.RETRY_PENDING,
        AssetStatus.DUPLICATE,
        AssetStatus.FAILED,
    },
    AssetStatus.RELEVANCE_PENDING: {
        AssetStatus.RELEVANCE_PASSED,
        AssetStatus.RELEVANCE_REJECTED,
        AssetStatus.REVIEW_PENDING,
        AssetStatus.RETRY_PENDING,
        AssetStatus.PROMOTED,  # Refilter reset
        AssetStatus.FAILED,
    },
    AssetStatus.REVIEW_PENDING: {
        AssetStatus.RELEVANCE_PASSED,
        AssetStatus.RELEVANCE_REJECTED,
        AssetStatus.PROMOTED,
        AssetStatus.FAILED,
    },
    AssetStatus.RETRY_PENDING: {
        AssetStatus.DOWNLOADING,
        AssetStatus.RELEVANCE_PENDING,
        AssetStatus.RELEVANCE_PASSED,
        AssetStatus.RELEVANCE_REJECTED,
        AssetStatus.REVIEW_PENDING,
        AssetStatus.PROMOTED,
        AssetStatus.FAILED,
    },
    AssetStatus.RELEVANCE_PASSED: {
        AssetStatus.READY_FOR_UPLOAD,
        AssetStatus.UPLOADING,
        AssetStatus.NAS_VERIFIED,
        AssetStatus.COMPLETED,
        AssetStatus.DUPLICATE,  # Late cross-batch / near-duplicate dedup pass
        AssetStatus.RELEVANCE_REJECTED,  # Trim excess assets
        AssetStatus.SURPLUS,  # Trimmed surplus beyond target
        AssetStatus.PROMOTED,  # Forced refilter pass
        AssetStatus.FAILED,
    },
    AssetStatus.READY_FOR_UPLOAD: {
        AssetStatus.UPLOADING,
        AssetStatus.NAS_VERIFIED,
        AssetStatus.COMPLETED,
        AssetStatus.SURPLUS,
        AssetStatus.FAILED,
    },
    AssetStatus.UPLOADING: {
        AssetStatus.READY_FOR_UPLOAD,  # Retry upon upload interruption
        AssetStatus.NAS_VERIFIED,
        AssetStatus.COMPLETED,
        AssetStatus.SURPLUS,
        AssetStatus.FAILED,
    },
    AssetStatus.NAS_VERIFIED: {
        AssetStatus.COMPLETED,
        AssetStatus.SURPLUS,
        AssetStatus.PROMOTED,
        AssetStatus.FAILED,
    },
    AssetStatus.COMPLETED: {
        AssetStatus.SURPLUS,  # Surplus trimmed during consolidation
    },
    AssetStatus.SURPLUS: {
        AssetStatus.PROMOTED,  # Refilter or job reopen reset
    },
    AssetStatus.RELEVANCE_REJECTED: {
        AssetStatus.PROMOTED,  # Refilter reset
    },
    AssetStatus.DUPLICATE: {
        AssetStatus.PROMOTED,  # Deduplication re-evaluation / canonical reassignment
    },
    AssetStatus.FAILED: {
        AssetStatus.PROMOTED,
        AssetStatus.DOWNLOADING,
        AssetStatus.RETRY_PENDING,
    },
}


class AssetStateMachine:
    @staticmethod
    def can_transition(current: AssetStatus, target: AssetStatus) -> bool:
        if current == target:
            return True
        allowed = _LEGAL_ASSET_TRANSITIONS.get(current, set())
        return target in allowed

    @staticmethod
    def transition(current: AssetStatus, target: AssetStatus, reason: str = "") -> AssetStatus:
        if not AssetStateMachine.can_transition(current, target):
            logger.error(
                f"[ASSET STATE MACHINE] Illegal transition: {current.value} -> {target.value}"
                + (f" ({reason})" if reason else "")
            )
            raise IllegalAssetTransitionError(current, target, reason)
        logger.debug(
            f"[ASSET STATE MACHINE] Transitioned {current.value} -> {target.value}"
            + (f" ({reason})" if reason else "")
        )
        return target

