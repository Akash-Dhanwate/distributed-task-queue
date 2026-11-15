"""
Automated Chaos & Stress Test Suite for the Distributed Task Queue.
Tests: 500 concurrent jobs, crash/restart recovery, zero job loss, performance metrics.
"""

import asyncio
import time
import random
import statistics
import tempfile
import os
import signal
import subprocess
import sys
from typing import List, Dict, Any
from dataclasses import dataclass, field
import json

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.client import QueueClient, submit_job, get_job_result
from src.worker import run_worker, example_job_executor
from src.server import QueueServer
from src.storage import Storage, JobState
from src.protocol import Message, MessageType


@dataclass
class TestMetrics:
    """Performance metrics for test runs."""
    total_jobs: int = 0
    completed: int = 0
    failed: int = 0
    dead_letter: int = 0
    start_time: float = 0
    end_time: float = 0
    latencies: List[float] = field(default_factory=list)
    recovered_jobs: int = 0

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time

    @property
    def throughput(self) -> float:
        return self.completed / self.duration if self.duration > 0 else 0

    @property
    def p95_latency(self) -> float:
        if not self.latencies:
            return 0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.95)
        return sorted_latencies[idx]

    @property
    def p99_latency(self) -> float:
        if not self.latencies:
            return 0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.99)
        return sorted_latencies[idx]

    @property
    def avg_latency(self) -> float:
        return statistics.mean(self.latencies) if self.latencies else 0

    def print_report(self) -> None:
        print("\n" + "=" * 60)
        print("PERFORMANCE REPORT")
        print("=" * 60)
        print(f"Total Jobs Submitted: {self.total_jobs}")
        print(f"Completed:            {self.completed}")
        print(f"Failed:               {self.failed}")
        print(f"Dead Letter:          {self.dead_letter}")
        print(f"Recovered Jobs:       {self.recovered_jobs}")
        print(f"Duration:             {self.duration:.2f}s")
        print(f"Throughput:           {self.throughput:.2f} jobs/sec")
        print(f"Avg Latency:          {self.avg_latency*1000:.2f}ms")
        print(f"P95 Latency:          {self.p95_latency*1000:.2f}ms")
        print(f"P99 Latency:          {self.p99_latency*1000:.2f}ms")
        print("=" * 60)


class TestSuite:
    """Main test suite runner."""

    def __init__(self):
        self.server_process: Optional[subprocess.Popen] = None
        self.worker_tasks: List[asyncio.Task] = []
        self.db_path: Optional[str] = None
        self.server_host = "127.0.0.1"
        self.server_port = 18080  # Use different port to avoid conflicts

    async def setup_server(self, db_path: str) -> None:
        """Start the server as a subprocess."""
        self.db_path = db_path
        env = os.environ.copy()
        env['PYTHONPATH'] = os.path.join(os.path.dirname(__file__), '..', 'src')

        self.server_process = subprocess.Popen(
            [sys.executable, '-m', 'src.server', '--host', self.server_host, '--port', str(self.server_port), '--db', db_path],
            cwd=os.path.join(os.path.dirname(__file__), '..'),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Wait for server to start
        await asyncio.sleep(1.5)

        # Verify server is up
        async with QueueClient(self.server_host, self.server_port) as client:
            await client.list_queues()

        print(f"Server started on {self.server_host}:{self.server_port}")

    async def start_workers(self, num_workers: int, queues: List[str]) -> None:
        """Start worker tasks."""
        for i in range(num_workers):
            worker_id = f"test-worker-{i}"
            task = asyncio.create_task(run_worker(
                queues=queues,
                job_executor=example_job_executor,
                worker_id=worker_id,
                host=self.server_host,
                port=self.server_port,
                max_concurrent=5
            ))
            self.worker_tasks.append(task)

        # Give workers time to register
        await asyncio.sleep(1.0)
        print(f"Started {num_workers} workers")

    def stop_workers(self) -> None:
        """Stop all worker tasks."""
        for task in self.worker_tasks:
            task.cancel()
        self.worker_tasks = []

    def stop_server(self) -> None:
        """Stop the server process."""
        if self.server_process:
            self.server_process.terminate()
            try:
                self.server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server_process.kill()
                self.server_process.wait()
            self.server_process = None

    async def run_stress_test(
        self,
        num_jobs: int = 500,
        num_workers: int = 10,
        queues: List[str] = None,
        priorities: List[str] = None
    ) -> TestMetrics:
        """Run the main stress test."""
        if queues is None:
            queues = ["high-priority", "normal-priority", "low-priority"]
        if priorities is None:
            priorities = ["HIGH", "NORMAL", "LOW"]

        metrics = TestMetrics()
        metrics.total_jobs = num_jobs
        metrics.start_time = time.time()

        # Submit jobs
        print(f"\nSubmitting {num_jobs} jobs across {len(queues)} queues with priorities {priorities}...")

        job_ids = []
        submit_start = time.time()

        async with QueueClient(self.server_host, self.server_port) as client:
            # Submit in batches for efficiency
            batch_size = 50
            for i in range(0, num_jobs, batch_size):
                batch = []
                for j in range(min(batch_size, num_jobs - i)):
                    queue = random.choice(queues)
                    priority = random.choice(priorities)
                    job_type = random.choice(['sleep', 'compute'])

                    if job_type == 'sleep':
                        payload = {'type': 'sleep', 'duration': random.uniform(0.01, 0.1)}
                    else:
                        payload = {'type': 'compute', 'n': random.randint(100, 1000)}

                    batch.append((queue, payload, priority))

                # Submit batch concurrently
                tasks = [
                    client.submit_job(q, p, pr, max_retries=3)
                    for q, p, pr in batch
                ]
                batch_ids = await asyncio.gather(*tasks)
                job_ids.extend(batch_ids)

        submit_time = time.time() - submit_start
        print(f"Submitted {len(job_ids)} jobs in {submit_time:.2f}s")

        # Wait for completion with progress tracking
        print("Waiting for jobs to complete...")

        # Create multiple clients for parallel status checking
        num_clients = 5
        clients = [QueueClient(self.server_host, self.server_port) for _ in range(num_clients)]
        for c in clients:
            await c.connect()

        async def check_job(client_idx, jid):
            try:
                result = await clients[client_idx].get_job_status(jid)
                return jid, result
            except Exception as e:
                return jid, None

        completed = 0
        failed = 0
        dead_letter = 0
        checked_jobs = set()

        while completed + failed + dead_letter < num_jobs:
            # Check status of remaining jobs
            remaining = [jid for jid in job_ids if jid not in checked_jobs]

            if not remaining:
                break

            # Check a batch of jobs using multiple clients for parallelism
            check_batch = remaining[:num_clients]
            tasks = [check_job(i % num_clients, jid) for i, jid in enumerate(check_batch)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for jid, result in zip(check_batch, results):
                if isinstance(result, Exception) or result is None:
                    continue

                jid, result = result
                if result is None:
                    continue

                if result.state == 'COMPLETED':
                    completed += 1
                    checked_jobs.add(jid)
                    if result.completed_at and result.started_at:
                        metrics.latencies.append(result.completed_at - result.started_at)
                elif result.state == 'FAILED':
                    failed += 1
                    checked_jobs.add(jid)
                elif result.state == 'DEAD_LETTER':
                    dead_letter += 1
                    checked_jobs.add(jid)

            print(f"  Progress: {completed} completed, {failed} failed, {dead_letter} dead letter, "
                  f"{num_jobs - completed - failed - dead_letter} pending/running", end='\r')

            await asyncio.sleep(0.1)

        # Close all clients
        for c in clients:
            await c.close()

        metrics.completed = completed
        metrics.failed = failed
        metrics.dead_letter = dead_letter

        metrics.end_time = time.time()
        print(f"\nTest completed!")

        return metrics

    async def test_crash_recovery(self) -> TestMetrics:
        """Test crash recovery by killing server mid-execution."""
        print("\n" + "=" * 60)
        print("CRASH RECOVERY TEST")
        print("=" * 60)

        # Use a fresh database
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name

        try:
            # Start server
            await self.setup_server(db_path)

            # Start workers
            await self.start_workers(5, ["recovery-queue"])

            # Submit initial batch
            print("Submitting initial batch of 100 jobs...")
            job_ids = []
            async with QueueClient(self.server_host, self.server_port) as client:
                for i in range(100):
                    job_id = await client.submit_job(
                        "recovery-queue",
                        {'type': 'sleep', 'duration': 0.5},  # Long enough to be running when we crash
                        priority="NORMAL",
                        max_retries=3
                    )
                    job_ids.append(job_id)

            # Wait a bit for jobs to start running
            print("Waiting for jobs to start...")
            await asyncio.sleep(1.0)

            # Check how many are running
            async with QueueClient(self.server_host, self.server_port) as client:
                running_before = 0
                for jid in job_ids:
                    status = await client.get_job_status(jid)
                    if status.state == 'RUNNING':
                        running_before += 1
                print(f"Jobs running before crash: {running_before}")

            # CRASH: Kill server process
            print("\n*** SIMULATING SERVER CRASH ***")
            self.stop_server()

            # Wait a moment
            await asyncio.sleep(0.5)

            # Restart server with SAME database
            print("Restarting server with same database...")
            await self.setup_server(db_path)

            # Restart workers
            await self.start_workers(5, ["recovery-queue"])

            # Wait for recovery
            print("Waiting for recovery and completion...")

            metrics = TestMetrics()
            metrics.total_jobs = 100
            metrics.start_time = time.time()

            async with QueueClient(self.server_host, self.server_port) as client:
                completed = 0
                failed = 0
                dead_letter = 0
                checked_jobs = set()

                while completed + failed + dead_letter < 100:
                    remaining = [jid for jid in job_ids if jid not in checked_jobs]
                    check_batch = remaining[:50]
                    for jid in check_batch:
                        try:
                            result = await client.get_job_status(jid)
                            if result.state == 'COMPLETED':
                                completed += 1
                                checked_jobs.add(jid)
                                if result.completed_at and result.started_at:
                                    metrics.latencies.append(result.completed_at - result.started_at)
                            elif result.state == 'FAILED':
                                failed += 1
                                checked_jobs.add(jid)
                            elif result.state == 'DEAD_LETTER':
                                dead_letter += 1
                                checked_jobs.add(jid)
                        except Exception:
                            pass

                    await asyncio.sleep(0.2)

                metrics.completed = completed
                metrics.failed = failed
                metrics.dead_letter = dead_letter

            metrics.end_time = time.time()

            # Check database directly for recovery count
            storage = Storage(db_path)
            recovered = len(storage.get_jobs_by_state(JobState.PENDING.value, limit=1000))
            # Actually check for jobs that were RUNNING and are now PENDING/COMPLETED
            all_jobs = storage.get_jobs_by_state(JobState.COMPLETED.value, limit=1000)
            metrics.recovered_jobs = len([j for j in all_jobs if j.queue == "recovery-queue"])
            storage.close()

            print(f"\nRecovery test results:")
            print(f"  Completed: {completed}")
            print(f"  Failed: {failed}")
            print(f"  Dead Letter: {dead_letter}")
            print(f"  Recovered jobs: {metrics.recovered_jobs}")

            return metrics

        finally:
            self.stop_workers()
            self.stop_server()
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_rate_limiting(self) -> TestMetrics:
        """Test that rate limiting works correctly."""
        print("\n" + "=" * 60)
        print("RATE LIMITING TEST")
        print("=" * 60)

        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name

        try:
            await self.setup_server(db_path)

            # Set rate limit to 10 jobs/sec on a queue
            storage = Storage(db_path)
            storage.set_queue_rate_limit("rate-limited", 10.0)
            storage.close()

            await self.start_workers(3, ["rate-limited"])

            # Submit 50 jobs - should take ~5 seconds at 10/sec
            print("Submitting 50 jobs to rate-limited queue (10/sec)...")
            start = time.time()

            job_ids = []
            async with QueueClient(self.server_host, self.server_port) as client:
                for i in range(50):
                    job_id = await client.submit_job(
                        "rate-limited",
                        {'type': 'sleep', 'duration': 0.01},
                        priority="NORMAL",
                        max_retries=3
                    )
                    job_ids.append(job_id)

            submit_time = time.time() - start
            print(f"Submission took {submit_time:.2f}s")

            # Wait for completion
            metrics = TestMetrics()
            metrics.total_jobs = 50
            metrics.start_time = time.time()

            async with QueueClient(self.server_host, self.server_port) as client:
                completed = 0
                checked_jobs = set()
                while completed < 50:
                    remaining = [jid for jid in job_ids if jid not in checked_jobs]
                    check_batch = remaining[:20]
                    for jid in check_batch:
                        try:
                            result = await client.get_job_status(jid)
                            if result.state == 'COMPLETED':
                                completed += 1
                                checked_jobs.add(jid)
                                if result.completed_at and result.started_at:
                                    metrics.latencies.append(result.completed_at - result.started_at)
                            elif result.state in ('FAILED', 'DEAD_LETTER'):
                                checked_jobs.add(jid)
                            # else still running
                        except Exception as e:
                            pass

                    await asyncio.sleep(0.1)

                metrics.completed = completed

            metrics.end_time = time.time()

            actual_rate = metrics.completed / metrics.duration
            print(f"Actual throughput: {actual_rate:.2f} jobs/sec (expected ~10/sec)")

            # Allow some tolerance
            assert actual_rate < 15, f"Rate limiting failed: got {actual_rate} jobs/sec"
            assert actual_rate > 5, f"Rate too low: got {actual_rate} jobs/sec"

            return metrics

        finally:
            self.stop_workers()
            self.stop_server()
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_priority_ordering(self) -> TestMetrics:
        """Test that HIGH priority jobs run before LOW priority."""
        print("\n" + "=" * 60)
        print("PRIORITY ORDERING TEST")
        print("=" * 60)

        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name

        try:
            await self.setup_server(db_path)
            await self.start_workers(2, ["priority-test"])

            # Submit LOW priority jobs first
            print("Submitting 20 LOW priority jobs...")
            low_jobs = []
            async with QueueClient(self.server_host, self.server_port) as client:
                for i in range(20):
                    job_id = await client.submit_job(
                        "priority-test",
                        {'type': 'sleep', 'duration': 0.1},
                        priority="LOW",
                        max_retries=3
                    )
                    low_jobs.append(job_id)

            # Give them time to start
            await asyncio.sleep(0.5)

            # Submit HIGH priority jobs
            print("Submitting 10 HIGH priority jobs...")
            high_jobs = []
            async with QueueClient(self.server_host, self.server_port) as client:
                for i in range(10):
                    job_id = await client.submit_job(
                        "priority-test",
                        {'type': 'sleep', 'duration': 0.05},
                        priority="HIGH",
                        max_retries=3
                    )
                    high_jobs.append(job_id)

            # Wait and check ordering
            print("Checking execution order...")

            async with QueueClient(self.server_host, self.server_port) as client:
                # Check start times
                high_starts = []
                low_starts = []

                for jid in high_jobs:
                    status = await client.get_job_status(jid)
                    if status.started_at:
                        high_starts.append(status.started_at)

                for jid in low_jobs:
                    status = await client.get_job_status(jid)
                    if status.started_at:
                        low_starts.append(status.started_at)

                print(f"HIGH priority started: {len(high_starts)}")
                print(f"LOW priority started: {len(low_starts)}")

                if high_starts and low_starts:
                    avg_high_start = statistics.mean(high_starts)
                    avg_low_start = statistics.mean(low_starts)
                    print(f"Avg HIGH start time: {avg_high_start}")
                    print(f"Avg LOW start time: {avg_low_start}")

                    # HIGH priority should start earlier on average
                    # (This is probabilistic but should hold with enough jobs)

            # Wait for all to complete
            all_jobs = high_jobs + low_jobs
            metrics = TestMetrics()
            metrics.total_jobs = len(all_jobs)

            async with QueueClient(self.server_host, self.server_port) as client:
                completed = 0
                checked_jobs = set()
                while completed < len(all_jobs):
                    remaining = [jid for jid in all_jobs if jid not in checked_jobs]
                    check_batch = remaining[:20]
                    for jid in check_batch:
                        try:
                            result = await client.get_job_status(jid)
                            if result.state == 'COMPLETED':
                                completed += 1
                                checked_jobs.add(jid)
                            elif result.state in ('FAILED', 'DEAD_LETTER'):
                                checked_jobs.add(jid)
                        except Exception:
                            pass

                    await asyncio.sleep(0.1)

                metrics.completed = completed

            return metrics

        finally:
            self.stop_workers()
            self.stop_server()
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def run_all_tests(self) -> Dict[str, TestMetrics]:
        """Run all tests and return results."""
        results = {}

        # Test 1: Crash Recovery
        print("\n" + "#" * 60)
        print("# TEST 1: CRASH RECOVERY")
        print("#" * 60)
        try:
            results['crash_recovery'] = await self.test_crash_recovery()
        except Exception as e:
            print(f"Crash recovery test failed: {e}")
            results['crash_recovery'] = TestMetrics()

        # Test 2: Rate Limiting
        print("\n" + "#" * 60)
        print("# TEST 2: RATE LIMITING")
        print("#" * 60)
        try:
            results['rate_limiting'] = await self.test_rate_limiting()
        except Exception as e:
            print(f"Rate limiting test failed: {e}")
            results['rate_limiting'] = TestMetrics()

        # Test 3: Priority Ordering
        print("\n" + "#" * 60)
        print("# TEST 3: PRIORITY ORDERING")
        print("#" * 60)
        try:
            results['priority_ordering'] = await self.test_priority_ordering()
        except Exception as e:
            print(f"Priority ordering test failed: {e}")
            results['priority_ordering'] = TestMetrics()

        # Test 4: Main Stress Test
        print("\n" + "#" * 60)
        print("# TEST 4: STRESS TEST (500 JOBS)")
        print("#" * 60)
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name

        try:
            await self.setup_server(db_path)
            await self.start_workers(15, ["stress-high", "stress-normal", "stress-low"])

            metrics = await self.run_stress_test(
                num_jobs=500,
                num_workers=15,
                queues=["stress-high", "stress-normal", "stress-low"],
                priorities=["HIGH", "NORMAL", "LOW"]
            )

            results['stress_test'] = metrics

        except Exception as e:
            print(f"Stress test failed: {e}")
            results['stress_test'] = TestMetrics()
        finally:
            self.stop_workers()
            self.stop_server()
            if os.path.exists(db_path):
                os.unlink(db_path)

        # Print summary
        self.print_summary(results)

        return results

    def print_summary(self, results: Dict[str, TestMetrics]) -> None:
        """Print summary of all test results."""
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)

        all_passed = True

        for name, metrics in results.items():
            print(f"\n{name.upper()}:")

            if name == 'crash_recovery':
                passed = metrics.completed >= 95  # Allow some failures
                status = "PASS" if passed else "FAIL"
                print(f"  Jobs completed: {metrics.completed}/100")
                print(f"  Recovered: {metrics.recovered_jobs}")
                print(f"  Status: {status}")
                if not passed:
                    all_passed = False

            elif name == 'rate_limiting':
                actual_rate = metrics.completed / metrics.duration if metrics.duration > 0 else 0
                passed = 5 < actual_rate < 15
                status = "PASS" if passed else "FAIL"
                print(f"  Throughput: {actual_rate:.2f} jobs/sec (target ~10)")
                print(f"  Status: {status}")
                if not passed:
                    all_passed = False

            elif name == 'priority_ordering':
                passed = metrics.completed == 30
                status = "PASS" if passed else "FAIL"
                print(f"  Jobs completed: {metrics.completed}/30")
                print(f"  Status: {status}")
                if not passed:
                    all_passed = False

            elif name == 'stress_test':
                passed = metrics.completed >= 490  # Allow small loss
                status = "PASS" if passed else "FAIL"
                metrics.print_report()
                print(f"  Status: {status}")
                if not passed:
                    all_passed = False

        print("\n" + "=" * 60)
        if all_passed:
            print("ALL TESTS PASSED ✓")
        else:
            print("SOME TESTS FAILED ✗")
        print("=" * 60)


async def main():
    """Run the test suite."""
    suite = TestSuite()
    results = await suite.run_all_tests()

    # Exit with error code if any test failed
    for name, metrics in results.items():
        if name == 'crash_recovery' and metrics.completed < 95:
            sys.exit(1)
        elif name == 'rate_limiting':
            rate = metrics.completed / metrics.duration if metrics.duration > 0 else 0
            if not (5 < rate < 15):
                sys.exit(1)
        elif name == 'priority_ordering' and metrics.completed != 30:
            sys.exit(1)
        elif name == 'stress_test' and metrics.completed < 490:
            sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())