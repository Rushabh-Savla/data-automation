"""Shared response envelopes."""

from typing import Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class PageResponse(BaseModel, Generic[T]):
    """A page of results plus enough to render pagination controls without a
    second round-trip: `total` is the full filtered/searched count (not just
    len(items)), so the frontend can compute page count as
    ceil(total / limit) directly from one response."""

    items: list[T]
    total: int
    limit: int
    offset: int
