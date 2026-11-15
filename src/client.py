"""
Client library for the Distributed Task Queue.
Simple interface for submitting jobs and checking status.
"""

import asyncio
import uuid
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from .protocol import (
    ProtocolHandler, Message, MessageType, FrameProtocol,
    make_submit_job, make_get_job_status, make_cancel_job,
    make_list_queues, make_get_queue_stats
)


@dataclass
class JobResult:
    job_id: str
    queue: str
    state: str
    attempt: int
    max_retries: int
    created_at: float
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    worker_id: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None


class QueueClient:
    """Async client for the task queue."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8080, timeout: float = 10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._handler: Optional[ProtocolHandler] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._request_id = 0

    async def connect(self) -> None:
        """Connect to the server."""
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=self.timeout
        )
        self._handler = ProtocolHandler(self._reader, self._writer)

    async def close(self) -> None:
        """Close the connection."""
        if self._handler:
            self._handler.close()
            await self._handler.wait_closed()
        self._handler = None
        self._reader = None
        self._writer = None

    def _next_request_id(self) -> str:
        self._request_id += 1
        return f"req-{self._request_id}-{uuid.uuid4().hex[:8]}"

    async def _send_request(self, message: Message) -> Message:
        """Send a request and wait for response."""
        if not self._handler:
            raise RuntimeError("Not connected")

        # Use a lock to prevent concurrent reads on the same connection
        if not hasattr(self, '_send_lock'):
            self._send_lock = asyncio.Lock()

        async with self._send_lock:
            await self._handler.send(message)
            response = await asyncio.wait_for(self._handler.recv(), timeout=self.timeout)

            if response is None:
                raise RuntimeError("Connection closed by server")

            return response

    async def submit_job(
        self,
        queue: str,
        payload: Dict[str, Any],
        priority: str = "NORMAL",
        max_retries: int = 3,
        visibility_timeout: int = 30
    ) -> str:
        """Submit a job to the queue. Returns job_id."""
        msg = make_submit_job(queue, payload, priority, max_retries)
        msg.request_id = self._next_request_id()
        msg.payload['visibility_timeout'] = visibility_timeout

        response = await self._send_request(msg)

        if response.type == "error":
            raise RuntimeError(response.payload.get('error', 'Unknown error'))

        return response.payload['data']['job_id']

    async def get_job_status(self, job_id: str) -> JobResult:
        """Get the status of a job."""
        msg = make_get_job_status(job_id)
        msg.request_id = self._next_request_id()

        response = await self._send_request(msg)

        if response.type == "error":
            raise RuntimeError(response.payload.get('error', 'Unknown error'))

        # Server sends job data wrapped in 'data' for get_job_status
        data = response.payload.get('data', response.payload)
        return JobResult(
            job_id=data.get('job_id', data.get('id', '')),
            queue=data['queue'],
            state=data['state'],
            attempt=data['attempt'],
            max_retries=data['max_retries'],
            created_at=data['created_at'],
            started_at=data.get('started_at'),
            completed_at=data.get('completed_at'),
            worker_id=data.get('worker_id'),
            result=data.get('result'),
            error=data.get('error')
        )

    async def wait_for_job(
        self,
        job_id: str,
        timeout: float = 60.0,
        poll_interval: float = 0.5
    ) -> JobResult:
        """Wait for a job to complete (COMPLETED, FAILED, or DEAD_LETTER)."""
        start = time.time()

        while time.time() - start < timeout:
            result = await self.get_job_status(job_id)

            if result.state in ('COMPLETED', 'FAILED', 'DEAD_LETTER'):
                return result

            await asyncio.sleep(poll_interval)

        raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending job."""
        from .protocol import Message
        msg = Message(
            type=MessageType.CANCEL_JOB.value,
            payload={'job_id': job_id},
            request_id=self._next_request_id()
        )

        response = await self._send_request(msg)

        if response.type == "error":
            raise RuntimeError(response.payload.get('error', 'Unknown error'))

        return response.payload.get('cancelled', False)

    async def list_queues(self) -> List[str]:
        """List all queues."""
        msg = make_list_queues()
        msg.request_id = self._next_request_id()

        response = await self._send_request(msg)

        if response.type == "error":
            raise RuntimeError(response.payload.get('error', 'Unknown error'))

        return response.payload.get('data', response.payload).get('queues', [])

    async def get_queue_stats(self, queue: str) -> Dict[str, int]:
        """Get statistics for a queue."""
        msg = make_get_queue_stats(queue)
        msg.request_id = self._next_request_id()

        response = await self._send_request(msg)

        if response.type == "error":
            raise RuntimeError(response.payload.get('error', 'Unknown error'))

        return response.payload

    async def submit_batch(
        self,
        queue: str,
        payloads: List[Dict[str, Any]],
        priority: str = "NORMAL",
        max_retries: int = 3,
        visibility_timeout: int = 30
    ) -> List[str]:
        """Submit multiple jobs efficiently."""
        job_ids = []
        for payload in payloads:
            job_id = await self.submit_job(
                queue, payload, priority, max_retries, visibility_timeout
            )
            job_ids.append(job_id)
        return job_ids

    async def __aenter__(self) -> 'QueueClient':
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


# Convenience function for simple usage
async def submit_job(
    queue: str,
    payload: Dict[str, Any],
    host: str = "127.0.0.1",
    port: int = 8080,
    priority: str = "NORMAL",
    max_retries: int = 3
) -> str:
    """Submit a single job and return job_id."""
    async with QueueClient(host, port) as client:
        return await client.submit_job(queue, payload, priority, max_retries)


async def get_job_result(
    job_id: str,
    host: str = "127.0.0.1",
    port: int = 8080,
    timeout: float = 60.0
) -> JobResult:
    """Get final result of a job, waiting for completion."""
    async with QueueClient(host, port) as client:
        return await client.wait_for_job(job_id, timeout=timeout)


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Task Queue Client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Submit command
    submit_parser = subparsers.add_parser("submit", help="Submit a job")
    submit_parser.add_argument("queue")
    submit_parser.add_argument("payload", help="JSON payload")
    submit_parser.add_argument("--priority", default="NORMAL", choices=["HIGH", "NORMAL", "LOW"])
    submit_parser.add_argument("--retries", type=int, default=3)
    submit_parser.add_argument("--wait", action="store_true", help="Wait for completion")

    # Status command
    status_parser = subparsers.add_parser("status", help="Check job status")
    status_parser.add_argument("job_id")
    status_parser.add_argument("--wait", action="store_true", help="Wait for completion")

    # List queues
    subparsers.add_parser("queues", help="List queues")

    # Queue stats
    stats_parser = subparsers.add_parser("stats", help="Get queue stats")
    stats_parser.add_argument("queue")

    args = parser.parse_args()

    async def main():
        async with QueueClient(args.host, args.port) as client:
            if args.command == "submit":
                payload = json.loads(args.payload)
                job_id = await client.submit_job(args.queue, payload, args.priority, args.retries)
                print(f"Submitted job: {job_id}")

                if args.wait:
                    result = await client.wait_for_job(job_id)
                    print(f"Job completed: {result.state}")
                    if result.result:
                        print(f"Result: {json.dumps(result.result)}")
                    if result.error:
                        print(f"Error: {result.error}")

            elif args.command == "status":
                result = await client.get_job_status(args.job_id)
                print(f"Job: {result.job_id}")
                print(f"  Queue: {result.queue}")
                print(f"  State: {result.state}")
                print(f"  Attempt: {result.attempt}/{result.max_retries}")
                if args.wait and result.state not in ('COMPLETED', 'FAILED', 'DEAD_LETTER'):
                    result = await client.wait_for_job(args.job_id)
                    print(f"Final state: {result.state}")
                    if result.result:
                        print(f"Result: {json.dumps(result.result)}")

            elif args.command == "queues":
                queues = await client.list_queues()
                print("Queues:", ", ".join(queues))

            elif args.command == "stats":
                stats = await client.get_queue_stats(args.queue)
                print(f"Queue: {args.queue}")
                for k, v in stats.items():
                    print(f"  {k}: {v}")

    asyncio.run(main())