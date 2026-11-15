"""
TCP Server for the Distributed Task Queue.
Handles client connections, worker management, job scheduling, and crash recovery.
"""

import asyncio
import signal
import json
import time
import uuid
from typing import Dict, Set, Optional, List, Any
from dataclasses import dataclass, field
import logging

from .protocol import (
    ProtocolHandler, Message, MessageType, FrameProtocol,
    make_response, make_error, make_job_assigned, make_server_shutdown
)
from .storage import Storage, Job, JobState, QueueConfig
from .rate_limiter import RateLimiter, PriorityScheduler, Priority, calculate_backoff

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class WorkerInfo:
    worker_id: str
    queues: List[str]
    last_heartbeat: float
    current_job_id: Optional[str] = None
    handler: Optional[ProtocolHandler] = None


class QueueServer:
    """Main queue server."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8080, db_path: str = "queue.db"):
        self.host = host
        self.port = port
        self.storage = Storage(db_path)
        self.rate_limiter = RateLimiter()
        self.scheduler = PriorityScheduler(self.rate_limiter)

        self._workers: Dict[str, WorkerInfo] = {}
        self._client_handlers: Dict[str, ProtocolHandler] = {}
        self._running = False
        self._server: Optional[asyncio.Server] = None
        self._tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()

        # Initialize queue rates from storage
        for queue_name in self.storage.list_queues():
            config = self.storage.get_queue_config(queue_name)
            if config:
                self.scheduler.set_queue_rate(queue_name, config.rate_limit)

    async def start(self) -> None:
        """Start the server."""
        self._running = True

        # Recover orphaned jobs from previous runs
        recovered = self.storage.requeue_orphaned_jobs()
        if recovered:
            logger.info(f"Recovered {recovered} orphaned jobs from previous run")
            # Re-add recovered jobs to scheduler
            for queue_name in self.storage.list_queues():
                jobs = self.storage.get_jobs_by_state(JobState.PENDING.value, limit=1000)
                for job in jobs:
                    if job.queue == queue_name:
                        priority = Priority(job.priority)
                        self.scheduler.requeue_job(job.id, job.queue, priority)

        # Start background tasks
        self._tasks.add(asyncio.create_task(self._worker_lease_loop()))
        self._tasks.add(asyncio.create_task(self._heartbeat_monitor()))
        self._tasks.add(asyncio.create_task(self._cleanup_stale_leases()))

        # Start TCP server
        self._server = await asyncio.start_server(
            self._handle_connection, self.host, self.port
        )

        logger.info(f"Server listening on {self.host}:{self.port}")

        # Wait for shutdown
        await self._shutdown_event.wait()
        await self._shutdown()

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle a new client connection."""
        handler = ProtocolHandler(reader, writer)
        client_id = str(uuid.uuid4())
        self._client_handlers[client_id] = handler

        try:
            while self._running:
                message = await handler.recv()
                if message is None:
                    break

                await self._process_message(client_id, handler, message)
        except Exception as e:
            logger.error(f"Connection error: {e}")
        finally:
            self._client_handlers.pop(client_id, None)
            handler.close()
            await handler.wait_closed()

    async def _process_message(self, client_id: str, handler: ProtocolHandler, message: Message) -> None:
        """Process an incoming message."""
        try:
            if message.type == MessageType.SUBMIT_JOB.value:
                await self._handle_submit_job(client_id, handler, message)
            elif message.type == MessageType.GET_JOB_STATUS.value:
                await self._handle_get_job_status(client_id, handler, message)
            elif message.type == MessageType.CANCEL_JOB.value:
                await self._handle_cancel_job(client_id, handler, message)
            elif message.type == MessageType.LIST_QUEUES.value:
                await self._handle_list_queues(client_id, handler, message)
            elif message.type == MessageType.GET_QUEUE_STATS.value:
                await self._handle_get_queue_stats(client_id, handler, message)
            elif message.type == MessageType.REGISTER_WORKER.value:
                await self._handle_register_worker(client_id, handler, message)
            elif message.type == MessageType.HEARTBEAT.value:
                await self._handle_heartbeat(client_id, handler, message)
            elif message.type == MessageType.LEASE_JOB.value:
                await self._handle_lease_job(client_id, handler, message)
            elif message.type == MessageType.COMPLETE_JOB.value:
                await self._handle_complete_job(client_id, handler, message)
            elif message.type == MessageType.FAIL_JOB.value:
                await self._handle_fail_job(client_id, handler, message)
            else:
                await handler.send(make_error(message.request_id, f"Unknown message type: {message.type}"))
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            await handler.send(make_error(message.request_id, str(e)))

    async def _handle_submit_job(self, client_id: str, handler: ProtocolHandler, message: Message) -> None:
        payload = message.payload
        queue = payload['queue']
        job_payload = payload['payload']
        priority_str = payload.get('priority', 'NORMAL')
        max_retries = payload.get('max_retries', 3)
        visibility_timeout = payload.get('visibility_timeout', 30)

        job = self.storage.create_job(queue, job_payload, priority_str, max_retries, visibility_timeout)

        # Add to scheduler
        priority = Priority.from_string(priority_str)
        self.scheduler.requeue_job(job.id, queue, priority)

        # Ensure queue rate limit is set
        if queue not in self.scheduler._queue_rates:
            self.scheduler.set_queue_rate(queue, 10.0)

        await handler.send(make_response(message.request_id, True, {'job_id': job.id}))

    async def _handle_get_job_status(self, client_id: str, handler: ProtocolHandler, message: Message) -> None:
        job_id = message.payload['job_id']
        job = self.storage.get_job(job_id)

        if job:
            data = {
                'job_id': job.id,
                'queue': job.queue,
                'state': job.state,
                'attempt': job.attempt,
                'max_retries': job.max_retries,
                'created_at': job.created_at,
                'started_at': job.started_at,
                'completed_at': job.completed_at,
                'worker_id': job.worker_id,
                'result': job.result,
                'error': job.error
            }
            await handler.send(make_response(message.request_id, True, data))
        else:
            await handler.send(make_error(message.request_id, "Job not found"))

    async def _handle_cancel_job(self, client_id: str, handler: ProtocolHandler, message: Message) -> None:
        job_id = message.payload['job_id']
        job = self.storage.get_job(job_id)

        if job and job.state == JobState.PENDING.value:
            # Remove from scheduler by marking as cancelled (using DEAD_LETTER)
            with self.storage.transaction() as conn:
                conn.execute(
                    "UPDATE jobs SET state = ?, updated_at = ? WHERE id = ?",
                    (JobState.DEAD_LETTER.value, time.time(), job_id)
                )
            await handler.send(make_response(message.request_id, True, {'cancelled': True}))
        else:
            await handler.send(make_error(message.request_id, "Job cannot be cancelled"))

    async def _handle_list_queues(self, client_id: str, handler: ProtocolHandler, message: Message) -> None:
        queues = self.storage.list_queues()
        await handler.send(make_response(message.request_id, True, {'queues': queues}))

    async def _handle_get_queue_stats(self, client_id: str, handler: ProtocolHandler, message: Message) -> None:
        queue = message.payload['queue']
        stats = self.storage.get_queue_stats(queue)
        await handler.send(make_response(message.request_id, True, stats))

    async def _handle_register_worker(self, client_id: str, handler: ProtocolHandler, message: Message) -> None:
        worker_id = message.payload['worker_id']
        queues = message.payload['queues']

        self.storage.register_worker(worker_id, queues)

        worker_info = WorkerInfo(
            worker_id=worker_id,
            queues=queues,
            last_heartbeat=time.time(),
            handler=handler
        )
        self._workers[worker_id] = worker_info

        # Ensure queue rates are set
        for queue in queues:
            if queue not in self.scheduler._queue_rates:
                config = self.storage.get_queue_config(queue)
                rate = config.rate_limit if config else 10.0
                self.scheduler.set_queue_rate(queue, rate)

        await handler.send(make_response(message.request_id, True, {'registered': True}))

    async def _handle_heartbeat(self, client_id: str, handler: ProtocolHandler, message: Message) -> None:
        worker_id = message.payload['worker_id']

        if worker_id in self._workers:
            self._workers[worker_id].last_heartbeat = time.time()
            self.storage.worker_heartbeat(worker_id)
            await handler.send(make_response(message.request_id, True, {'ok': True}))
        else:
            await handler.send(make_error(message.request_id, "Worker not registered"))

    async def _handle_lease_job(self, client_id: str, handler: ProtocolHandler, message: Message) -> None:
        worker_id = message.payload['worker_id']
        queues = message.payload['queues']
        visibility_timeout = message.payload.get('visibility_timeout', 30)

        if worker_id not in self._workers:
            await handler.send(make_error(message.request_id, "Worker not registered"))
            return

        # Try to get next job from scheduler
        result = await self.scheduler.get_next_job(queues)

        if result:
            job_id, queue = result
            job = self.storage.lease_job(job_id, worker_id, visibility_timeout)

            if job:
                self._workers[worker_id].current_job_id = job_id
                # Send response with job data
                data = {
                    'job_id': job.id,
                    'queue': job.queue,
                    'payload': job.payload,
                    'priority': Priority(job.priority).name,
                    'max_retries': job.max_retries,
                    'attempt': job.attempt
                }
                await handler.send(make_response(message.request_id, True, data))
            else:
                # Job was taken by another worker, try again
                await handler.send(make_response(message.request_id, True, {'job_id': None}))
        else:
            await handler.send(make_response(message.request_id, True, {'job_id': None}))

    async def _handle_complete_job(self, client_id: str, handler: ProtocolHandler, message: Message) -> None:
        worker_id = message.payload['worker_id']
        job_id = message.payload['job_id']
        result = message.payload.get('result')

        success = self.storage.complete_job(job_id, worker_id, result)

        if worker_id in self._workers:
            self._workers[worker_id].current_job_id = None

        if success:
            await handler.send(make_response(message.request_id, True, {'completed': True}))
        else:
            await handler.send(make_error(message.request_id, "Job not found or not owned by worker"))

    async def _handle_fail_job(self, client_id: str, handler: ProtocolHandler, message: Message) -> None:
        worker_id = message.payload['worker_id']
        job_id = message.payload['job_id']
        error = message.payload['error']
        retry = message.payload.get('retry', True)

        job = self.storage.fail_job(job_id, worker_id, error, retry)

        if worker_id in self._workers:
            self._workers[worker_id].current_job_id = None

        if job:
            if job.state == JobState.PENDING.value:
                # Requeue with backoff
                delay = calculate_backoff(job.attempt - 1)
                priority = Priority(job.priority)
                self.scheduler.requeue_job(job.id, job.queue, priority, delay)
                await handler.send(make_response(message.request_id, True, {'requeued': True, 'attempt': job.attempt}))
            else:
                await handler.send(make_response(message.request_id, True, {'dead_letter': True}))
        else:
            await handler.send(make_error(message.request_id, "Job not found or not owned by worker"))

    async def _worker_lease_loop(self) -> None:
        """Background task - disabled. Workers poll for jobs via lease_job request."""
        while self._running:
            await asyncio.sleep(1.0)  # Just keep the task alive

    async def _heartbeat_monitor(self) -> None:
        """Monitor worker heartbeats and clean up dead workers."""
        while self._running:
            try:
                now = time.time()
                dead_workers = []

                for worker_id, worker in self._workers.items():
                    if now - worker.last_heartbeat > 60:  # 60 second timeout
                        dead_workers.append(worker_id)

                for worker_id in dead_workers:
                    logger.info(f"Worker {worker_id} timed out, cleaning up")
                    worker = self._workers.pop(worker_id, None)
                    if worker and worker.current_job_id:
                        # Job will be recovered by cleanup task
                        pass
            except Exception as e:
                logger.error(f"Error in heartbeat monitor: {e}")

            await asyncio.sleep(10)

    async def _cleanup_stale_leases(self) -> None:
        """Periodically clean up expired job leases (crash recovery)."""
        while self._running:
            try:
                recovered = self.storage.requeue_orphaned_jobs()
                if recovered > 0:
                    logger.info(f"Recovered {recovered} orphaned jobs")
                    # Re-add recovered jobs to scheduler
                    for queue_name in self.storage.list_queues():
                        jobs = self.storage.get_jobs_by_state(JobState.PENDING.value, limit=1000)
                        for job in jobs:
                            if job.queue == queue_name:
                                priority = Priority(job.priority)
                                self.scheduler.requeue_job(job.id, job.queue, priority)
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")

            await asyncio.sleep(5)

    async def _shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("Shutting down server...")
        self._running = False

        # Notify workers
        shutdown_msg = make_server_shutdown()
        for worker in self._workers.values():
            if worker.handler:
                try:
                    await worker.handler.send(shutdown_msg)
                except Exception:
                    pass

        # Cancel background tasks
        for task in self._tasks:
            task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        # Close server
        if self._server:
            self._server.close()
            await self._server.wait_closed()

        # Close storage
        self.storage.close()

        logger.info("Server shutdown complete")

    def shutdown(self) -> None:
        """Trigger shutdown from signal handler."""
        self._shutdown_event.set()


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Distributed Task Queue Server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db", default="queue.db")
    args = parser.parse_args()

    server = QueueServer(args.host, args.port, args.db)

    # Handle signals
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, server.shutdown)

    await server.start()


if __name__ == "__main__":
    asyncio.run(main())