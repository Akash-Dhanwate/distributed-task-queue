"""
Worker implementation for the Distributed Task Queue.
Handles job leasing, heartbeats, job execution, and graceful shutdown.
"""

import asyncio
import signal
import time
import uuid
import json
import logging
from typing import Dict, List, Optional, Callable, Any, Awaitable
from dataclasses import dataclass

from .protocol import (
    ProtocolHandler, Message, MessageType, FrameProtocol,
    make_register_worker, make_heartbeat, make_lease_job,
    make_complete_job, make_fail_job
)
from .storage import JobState

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


JobExecutor = Callable[[Dict[str, Any]], Awaitable[Any]]


@dataclass
class WorkerConfig:
    worker_id: str
    queues: List[str]
    server_host: str = "127.0.0.1"
    server_port: int = 8080
    heartbeat_interval: float = 5.0
    lease_poll_interval: float = 0.01  # Slightly slower polling
    visibility_timeout: int = 30
    max_concurrent_jobs: int = 1


class Worker:
    """Async worker that processes jobs from the queue server."""

    def __init__(self, config: WorkerConfig, job_executor: JobExecutor):
        self.config = config
        self.job_executor = job_executor
        self._handler: Optional[ProtocolHandler] = None
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._current_jobs: Dict[str, asyncio.Task] = {}
        self._shutdown_event = asyncio.Event()
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._incoming_messages: asyncio.Queue = asyncio.Queue()
        self._response_futures: Dict[str, asyncio.Future] = {}
        self._request_counter = 0

    async def start(self) -> None:
        """Connect to server and start working."""
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self.config.server_host, self.config.server_port
            )
        except Exception as e:
            logger.error(f"Failed to connect to server: {e}")
            raise

        self._handler = ProtocolHandler(self._reader, self._writer)
        self._running = True

        # Start message pump (single reader)
        self._tasks = [
            asyncio.create_task(self._message_pump()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._lease_loop()),
        ]

        # Register with server
        await self._register()

        logger.info(f"Worker {self.config.worker_id} started, queues: {self.config.queues}")

        # Wait for shutdown
        await self._shutdown_event.wait()
        await self._shutdown()

    async def _message_pump(self) -> None:
        """Single message pump that reads from socket and dispatches."""
        while self._running:
            try:
                message = await self._handler.recv()
                if message is None:
                    logger.info("Server connection closed")
                    self._shutdown_event.set()
                    break

                # Check if this is a response to a request
                if message.request_id and message.request_id in self._response_futures:
                    future = self._response_futures.pop(message.request_id)
                    if not future.done():
                        future.set_result(message)
                else:
                    # Unsolicited message (job_assigned, server_shutdown)
                    await self._incoming_messages.put(message)

            except Exception as e:
                logger.error(f"Message pump error: {e}")
                if self._running:
                    self._shutdown_event.set()
                break

    def _next_request_id(self) -> str:
        self._request_counter += 1
        return f"{self.config.worker_id}-req-{self._request_counter}"

    async def _send_request(self, message: Message) -> Message:
        """Send a request and wait for response."""
        request_id = self._next_request_id()
        message.request_id = request_id

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._response_futures[request_id] = future

        await self._handler.send(message)

        try:
            response = await asyncio.wait_for(future, timeout=10.0)
            return response
        except asyncio.TimeoutError:
            self._response_futures.pop(request_id, None)
            raise RuntimeError("Request timeout")

    async def _register(self) -> None:
        """Register worker with server."""
        msg = make_register_worker(self.config.worker_id, self.config.queues)
        response = await self._send_request(msg)

        if response.type == "error":
            error = response.payload.get('error', 'Unknown error')
            raise RuntimeError(f"Registration failed: {error}")

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats."""
        while self._running:
            try:
                msg = make_heartbeat(self.config.worker_id)
                response = await self._send_request(msg)

                if response.type == "error":
                    logger.warning(f"Heartbeat error: {response.payload.get('error')}")
            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")
                if self._running:
                    self._shutdown_event.set()
                break

            await asyncio.sleep(self.config.heartbeat_interval)

    async def _lease_loop(self) -> None:
        """Continuously request jobs from server."""
        while self._running:
            # Check if we can accept more jobs
            if len(self._current_jobs) < self.config.max_concurrent_jobs:
                try:
                    msg = make_lease_job(
                        self.config.worker_id,
                        self.config.queues,
                        self.config.visibility_timeout
                    )
                    response = await self._send_request(msg)

                    if response.type == "response" and response.payload.get('success') and response.payload.get('data'):
                        # Server sends job data in 'data' field
                        job_data = response.payload['data']
                        if job_data.get('job_id'):
                            await self._execute_job(job_data)
                        # else no job available
                    elif response.type == "job_assigned":
                        # Legacy format
                        job_data = response.payload
                        await self._execute_job(job_data)
                    elif response.type == "error":
                        logger.error(f"Lease error: {response.payload.get('error')}")
                except Exception as e:
                    logger.error(f"Lease error: {e}")

            await asyncio.sleep(self.config.lease_poll_interval)

    async def _execute_job(self, job_data: Dict[str, Any]) -> None:
        """Execute a job."""
        job_id = job_data['job_id']
        queue = job_data['queue']
        payload = job_data['payload']
        priority = job_data['priority']
        max_retries = job_data['max_retries']
        attempt = job_data['attempt']

        logger.info(f"Executing job {job_id} (attempt {attempt}/{max_retries})")

        # Create task for job execution
        task = asyncio.create_task(self._run_job(job_id, payload, attempt, max_retries))
        self._current_jobs[job_id] = task

        # Clean up when done
        def cleanup(t):
            self._current_jobs.pop(job_id, None)
            if t.cancelled():
                logger.info(f"Job {job_id} was cancelled")
            elif t.exception():
                logger.error(f"Job {job_id} failed with exception: {t.exception()}")

        task.add_done_callback(cleanup)

    async def _run_job(self, job_id: str, payload: Dict, attempt: int, max_retries: int) -> None:
        """Run the actual job with error handling."""
        try:
            result = await self.job_executor(payload)

            # Report success
            msg = make_complete_job(self.config.worker_id, job_id, result)
            response = await self._send_request(msg)

            if response.type == "error":
                logger.error(f"Failed to report completion: {response.payload.get('error')}")

            logger.info(f"Job {job_id} completed successfully")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Job {job_id} failed: {error_msg}")

            # Determine if we should retry
            retry = attempt < max_retries

            msg = make_fail_job(self.config.worker_id, job_id, error_msg, retry)
            response = await self._send_request(msg)

            if response.type == "error":
                logger.error(f"Failed to report failure: {response.payload.get('error')}")

    async def _shutdown(self) -> None:
        """Graceful shutdown - wait for active jobs to complete."""
        logger.info(f"Worker {self.config.worker_id} shutting down...")
        self._running = False

        # Cancel background tasks
        for task in self._tasks:
            task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        # Wait for active jobs to complete (with timeout)
        if self._current_jobs:
            logger.info(f"Waiting for {len(self._current_jobs)} active jobs to complete...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._current_jobs.values(), return_exceptions=True),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for jobs, forcing shutdown")

        # Close connection
        if self._handler:
            self._handler.close()
            await self._handler.wait_closed()

        logger.info(f"Worker {self.config.worker_id} shutdown complete")

    def shutdown(self) -> None:
        """Trigger shutdown from signal handler."""
        self._shutdown_event.set()


async def run_worker(
    queues: List[str],
    job_executor: JobExecutor,
    worker_id: Optional[str] = None,
    host: str = "127.0.0.1",
    port: int = 8080,
    max_concurrent: int = 1
) -> None:
    """Convenience function to run a worker."""
    config = WorkerConfig(
        worker_id=worker_id or str(uuid.uuid4()),
        queues=queues,
        server_host=host,
        server_port=port,
        max_concurrent_jobs=max_concurrent
    )

    worker = Worker(config, job_executor)

    # Handle signals
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.shutdown)

    await worker.start()


# Example job executor for testing
async def example_job_executor(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Example job executor that simulates work."""
    job_type = payload.get('type', 'sleep')

    if job_type == 'sleep':
        duration = payload.get('duration', 0.1)
        await asyncio.sleep(duration)
        return {'status': 'completed', 'slept': duration}
    elif job_type == 'compute':
        n = payload.get('n', 1000)
        # Simple CPU work
        result = sum(i * i for i in range(n))
        return {'status': 'completed', 'result': result}
    elif job_type == 'fail':
        raise ValueError(payload.get('error', 'Intentional failure'))
    else:
        return {'status': 'completed', 'echo': payload}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Distributed Task Queue Worker")
    parser.add_argument("--queues", nargs="+", required=True, help="Queues to work on")
    parser.add_argument("--worker-id", help="Worker ID (auto-generated if not provided)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--concurrent", type=int, default=1)
    args = parser.parse_args()

    asyncio.run(run_worker(
        queues=args.queues,
        job_executor=example_job_executor,
        worker_id=args.worker_id,
        host=args.host,
        port=args.port,
        max_concurrent=args.concurrent
    ))