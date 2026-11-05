"""Intelligent Resource Orchestration Domain Module.

Manages Automatic Mode:
  Tier 1: Web Images (SourceType.WEB)
  Tier 2: Videos (SourceType.YOUTUBE)
  Tier 3: Dataset Resources (Kaggle, Roboflow, COCO, Open Images, Pascal VOC,
                             Visual Genome, SUN RGB-D, DAVIS, nuScenes, KITTI)

And Manual Mode:
  Concurrently executes all user-selected resources without tier restrictions.
"""

from __future__ import annotations

from typing import Sequence
from app.domain.enums import SourceSelectionMode, SourceType

TIER_1_SOURCES: tuple[SourceType, ...] = (SourceType.WEB,)
TIER_2_SOURCES: tuple[SourceType, ...] = (SourceType.YOUTUBE,)
TIER_3_SOURCES: tuple[SourceType, ...] = (
    SourceType.KAGGLE,
    SourceType.ROBOFLOW,
    SourceType.COCO,
    SourceType.OPEN_IMAGES,
    SourceType.PASCAL_VOC,
    SourceType.VISUAL_GENOME,
    SourceType.SUN_RGBD,
    SourceType.DAVIS,
    SourceType.NUSCENES,
    SourceType.KITTI,
)


def get_tier_for_source(source: SourceType) -> int:
    """Returns the priority tier (1, 2, or 3) for a given source connector."""
    if source in TIER_1_SOURCES:
        return 1
    if source in TIER_2_SOURCES:
        return 2
    return 3


def build_orchestration_tiers(
    available_sources: Sequence[SourceType],
) -> list[list[SourceType]]:
    """Partitions available sources into strict sequential tiers for Automatic Mode.

    Tier 1 (Web) -> Tier 2 (Videos) -> Tier 3 (Datasets).
    """
    tier1 = [s for s in TIER_1_SOURCES if s in available_sources]
    tier2 = [s for s in TIER_2_SOURCES if s in available_sources]
    tier3 = [s for s in TIER_3_SOURCES if s in available_sources]

    tiers: list[list[SourceType]] = []
    if tier1:
        tiers.append(tier1)
    if tier2:
        tiers.append(tier2)
    if tier3:
        tiers.append(tier3)
    return tiers

