"""
Token-Bucket Rate Limiter & Priority Scheduler.
Per-queue rate limits with priority ordering (HIGH, NORMAL, LOW) and exponential backoff retries with jitter.
"""

import asyncio
import time
import random
import heapq
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from enum import Enum


class Priority(Enum):
    HIGH = 0
    NORMAL = 1
    LOW = 2

    @classmethod
    def from_string(cls, s: str) -> 'Priority':
        return {'HIGH': cls.HIGH, 'NORMAL': cls.NORMAL, 'LOW': cls.LOW}.get(s.upper(), cls.NORMAL)


@dataclass(order=True)
class ScheduledJob:
    """Job with scheduling priority for the heap."""
    priority: int
    scheduled_at: float
    job_id: str = field(compare=False)
    queue: str = field(compare=False)
    # These are for tie-breaking only
    _counter: int = field(default=0, compare=False)


class TokenBucket:
    """Token bucket rate limiter for a single queue."""

    def __init__(self, rate: float, burst: Optional[int] = None):
        self.rate = rate  # tokens per second
        self.burst = burst or max(1, int(rate))  # no burst by default
        self._tokens = float(self.burst)
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def take(self, tokens: int = 1) -> float:
        """Take tokens, returning the wait time required (0 if immediate)."""
        async with self._lock:
            now = time.monotonic()
            # Add tokens based on elapsed time
            elapsed = now - self._last_update
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._last_update = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return 0.0

            # Need to wait for tokens
            deficit = tokens - self._tokens
            wait_time = deficit / self.rate
            self._tokens = 0
            return wait_time

    async def try_take(self, tokens: int = 1) -> bool:
        """Try to take tokens without waiting. Returns True if successful."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_update
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._last_update = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def set_rate(self, rate: float) -> None:
        self.rate = rate
        self.burst = max(1, int(rate * 2))

    @property
    def available_tokens(self) -> float:
        now = time.monotonic()
        elapsed = now - self._last_update
        return min(self.burst, self._tokens + elapsed * self.rate)


class RateLimiter:
    """Manages token buckets per queue."""

    def __init__(self):
        self._buckets: Dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()

    async def get_bucket(self, queue: str, rate: float) -> TokenBucket:
        async with self._lock:
            if queue not in self._buckets:
                self._buckets[queue] = TokenBucket(rate)
            else:
                # Update rate if changed
                self._buckets[queue].set_rate(rate)
            return self._buckets[queue]

    async def acquire(self, queue: str, rate: float, tokens: int = 1) -> float:
        """Acquire tokens for a queue. Returns wait time in seconds."""
        bucket = await self.get_bucket(queue, rate)
        return await bucket.take(tokens)

    async def try_acquire(self, queue: str, rate: float, tokens: int = 1) -> bool:
        """Try to acquire tokens without waiting."""
        bucket = await self.get_bucket(queue, rate)
        return await bucket.try_take(tokens)

    def get_queue_rate(self, queue: str) -> Optional[float]:
        bucket = self._buckets.get(queue)
        return bucket.rate if bucket else None


class PriorityScheduler:
    """Priority scheduler with per-queue rate limiting."""

    def __init__(self, rate_limiter: RateLimiter):
        self.rate_limiter = rate_limiter
        self._queue_heaps: Dict[str, List[ScheduledJob]] = defaultdict(list)
        self._job_counter = 0
        self._lock = asyncio.Lock()
        self._queue_rates: Dict[str, float] = {}

    def set_queue_rate(self, queue: str, rate: float) -> None:
        self._queue_rates[queue] = rate

    def get_queue_rate(self, queue: str) -> float:
        return self._queue_rates.get(queue, 10.0)

    async def schedule_job(self, job_id: str, queue: str, priority: Priority, delay: float = 0) -> None:
        """Schedule a job for execution."""
        scheduled_at = time.monotonic() + delay
        async with self._lock:
            self._job_counter += 1
            heapq.heappush(
                self._queue_heaps[queue],
                ScheduledJob(
                    priority=priority.value,
                    scheduled_at=scheduled_at,
                    job_id=job_id,
                    queue=queue,
                    _counter=self._job_counter
                )
            )

    async def get_next_job(self, queues: List[str]) -> Optional[Tuple[str, str]]:
        """Get the next job to execute across all queues.
        Returns (job_id, queue) or None if no job ready.
        """
        now = time.monotonic()

        async with self._lock:
            # Find ALL ready jobs across all queues
            ready_jobs = []
            for queue in queues:
                heap = self._queue_heaps[queue]
                if not heap:
                    continue

                # Check all ready jobs in this queue
                for job in heap:
                    if job.scheduled_at <= now:
                        ready_jobs.append((job, queue))
                    else:
                        break  # Heap is ordered by scheduled_at

            if not ready_jobs:
                return None

            # Sort by priority (lower value = higher priority), then by scheduled_at, then counter
            ready_jobs.sort(key=lambda x: (x[0].priority, x[0].scheduled_at, x[0]._counter))
            best_job, best_queue = ready_jobs[0]

            # Check rate limit
            rate = self.get_queue_rate(best_queue)
            can_acquire = await self.rate_limiter.try_acquire(best_queue, rate)
            if not can_acquire:
                return None

            # Pop the specific job from its queue's heap
            heap = self._queue_heaps[best_queue]
            for i, job in enumerate(heap):
                if job.job_id == best_job.job_id:
                    heapq.heappop(heap)
                    break

            return (best_job.job_id, best_queue)

    async def get_next_ready_time(self, queues: List[str]) -> Optional[float]:
        """Get the earliest time a job will be ready."""
        async with self._lock:
            earliest = None
            for queue in queues:
                heap = self._queue_heaps[queue]
                if heap:
                    job = heap[0]
                    if earliest is None or job.scheduled_at < earliest:
                        earliest = job.scheduled_at
            return earliest

    def requeue_job(self, job_id: str, queue: str, priority: Priority, delay: float = 0) -> None:
        """Requeue a job (e.g., after failure). Called from outside async context."""
        scheduled_at = time.monotonic() + delay
        self._job_counter += 1
        heapq.heappush(
            self._queue_heaps[queue],
            ScheduledJob(
                priority=priority.value,
                scheduled_at=scheduled_at,
                job_id=job_id,
                queue=queue,
                _counter=self._job_counter
            )
        )

    def get_pending_count(self, queue: str) -> int:
        return len(self._queue_heaps.get(queue, []))

    def get_total_pending(self) -> int:
        return sum(len(h) for h in self._queue_heaps.values())


def calculate_backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0, jitter: float = 0.25) -> float:
    """Calculate exponential backoff with jitter.

    Args:
        attempt: The attempt number (0-indexed)
        base_delay: Base delay in seconds
        max_delay: Maximum delay cap
        jitter: Jitter factor (0.25 = ±25%)

    Returns:
        Delay in seconds with jitter applied
    """
    delay = min(base_delay * (2 ** attempt), max_delay)
    # Apply jitter: random value in [delay * (1-jitter), delay * (1+jitter)]
    jitter_range = delay * jitter
    return delay - jitter_range + random.random() * (2 * jitter_range)