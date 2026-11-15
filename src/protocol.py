"""
TCP Wire Protocol: Length-prefixed JSON frames (4-byte big-endian length header + UTF-8 JSON payload).
Handles partial reads and coalesced packets without corruption.
"""

import json
import struct
import asyncio
from typing import Any, Optional, Dict
from dataclasses import dataclass, asdict
from enum import Enum


class MessageType(Enum):
    # Client -> Server
    SUBMIT_JOB = "submit_job"
    GET_JOB_STATUS = "get_job_status"
    CANCEL_JOB = "cancel_job"
    LIST_QUEUES = "list_queues"
    GET_QUEUE_STATS = "get_queue_stats"

    # Worker -> Server
    REGISTER_WORKER = "register_worker"
    HEARTBEAT = "heartbeat"
    LEASE_JOB = "lease_job"
    COMPLETE_JOB = "complete_job"
    FAIL_JOB = "fail_job"
    REQUEUE_JOB = "requeue_job"

    # Server -> Client/Worker
    RESPONSE = "response"
    ERROR = "error"
    JOB_ASSIGNED = "job_assigned"
    SERVER_SHUTDOWN = "server_shutdown"


@dataclass
class Message:
    type: str
    payload: Dict[str, Any]
    request_id: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(',', ':'))

    @classmethod
    def from_json(cls, data: str) -> 'Message':
        obj = json.loads(data)
        return cls(
            type=obj['type'],
            payload=obj['payload'],
            request_id=obj.get('request_id')
        )


class ProtocolError(Exception):
    pass


class FrameProtocol:
    """Handles length-prefixed JSON frame encoding/decoding."""

    HEADER_SIZE = 4
    MAX_FRAME_SIZE = 10 * 1024 * 1024  # 10 MB

    @staticmethod
    def encode(message: Message) -> bytes:
        """Encode a message to length-prefixed JSON bytes."""
        json_bytes = message.to_json().encode('utf-8')
        length = len(json_bytes)
        if length > FrameProtocol.MAX_FRAME_SIZE:
            raise ProtocolError(f"Frame too large: {length} bytes (max {FrameProtocol.MAX_FRAME_SIZE})")
        header = struct.pack('>I', length)
        return header + json_bytes

    @staticmethod
    def decode_frame(buffer: bytearray) -> Optional[Message]:
        """Try to decode a complete frame from buffer. Returns Message or None if incomplete."""
        if len(buffer) < FrameProtocol.HEADER_SIZE:
            return None

        length = struct.unpack('>I', buffer[:FrameProtocol.HEADER_SIZE])[0]
        if length > FrameProtocol.MAX_FRAME_SIZE:
            raise ProtocolError(f"Frame too large: {length} bytes")

        total_size = FrameProtocol.HEADER_SIZE + length
        if len(buffer) < total_size:
            return None

        json_bytes = buffer[FrameProtocol.HEADER_SIZE:total_size]
        try:
            message = Message.from_json(json_bytes.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError, KeyError) as e:
            raise ProtocolError(f"Invalid frame payload: {e}")

        # Remove consumed bytes from buffer
        del buffer[:total_size]
        return message


class ProtocolHandler:
    """Async protocol handler for reading/writing frames over a stream."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self._buffer = bytearray()
        self._closed = False

    async def send(self, message: Message) -> None:
        """Send a message."""
        if self._closed:
            raise ProtocolError("Connection closed")
        data = FrameProtocol.encode(message)
        self.writer.write(data)
        await self.writer.drain()

    async def recv(self) -> Optional[Message]:
        """Receive a complete message."""
        while not self._closed:
            # Try to decode from existing buffer
            try:
                message = FrameProtocol.decode_frame(self._buffer)
                if message is not None:
                    return message
            except ProtocolError:
                raise

            # Need more data
            chunk = await self.reader.read(8192)
            if not chunk:
                self._closed = True
                return None
            self._buffer.extend(chunk)
        return None

    def close(self) -> None:
        self._closed = True
        if not self.writer.is_closing():
            self.writer.close()

    async def wait_closed(self) -> None:
        await self.writer.wait_closed()


# Convenience functions for common message types
def make_response(request_id: str, success: bool, data: Any = None, error: str = None) -> Message:
    return Message(
        type=MessageType.RESPONSE.value,
        payload={'success': success, 'data': data, 'error': error},
        request_id=request_id
    )


def make_error(request_id: str, error: str) -> Message:
    return Message(
        type=MessageType.ERROR.value,
        payload={'error': error},
        request_id=request_id
    )


def make_submit_job(queue: str, payload: Dict, priority: str = "NORMAL", max_retries: int = 3) -> Message:
    return Message(
        type=MessageType.SUBMIT_JOB.value,
        payload={'queue': queue, 'payload': payload, 'priority': priority, 'max_retries': max_retries}
    )


def make_get_job_status(job_id: str) -> Message:
    return Message(
        type=MessageType.GET_JOB_STATUS.value,
        payload={'job_id': job_id}
    )


def make_cancel_job(job_id: str) -> Message:
    return Message(
        type=MessageType.CANCEL_JOB.value,
        payload={'job_id': job_id}
    )


def make_list_queues() -> Message:
    return Message(
        type=MessageType.LIST_QUEUES.value,
        payload={}
    )


def make_get_queue_stats(queue: str) -> Message:
    return Message(
        type=MessageType.GET_QUEUE_STATS.value,
        payload={'queue': queue}
    )


def make_register_worker(worker_id: str, queues: list) -> Message:
    return Message(
        type=MessageType.REGISTER_WORKER.value,
        payload={'worker_id': worker_id, 'queues': queues}
    )


def make_heartbeat(worker_id: str) -> Message:
    return Message(
        type=MessageType.HEARTBEAT.value,
        payload={'worker_id': worker_id}
    )


def make_lease_job(worker_id: str, queues: list, visibility_timeout: int = 30) -> Message:
    return Message(
        type=MessageType.LEASE_JOB.value,
        payload={'worker_id': worker_id, 'queues': queues, 'visibility_timeout': visibility_timeout}
    )


def make_complete_job(worker_id: str, job_id: str, result: Any = None) -> Message:
    return Message(
        type=MessageType.COMPLETE_JOB.value,
        payload={'worker_id': worker_id, 'job_id': job_id, 'result': result}
    )


def make_fail_job(worker_id: str, job_id: str, error: str, retry: bool = True) -> Message:
    return Message(
        type=MessageType.FAIL_JOB.value,
        payload={'worker_id': worker_id, 'job_id': job_id, 'error': error, 'retry': retry}
    )


def make_job_assigned(job_id: str, queue: str, payload: Dict, priority: str, max_retries: int, attempt: int) -> Message:
    return Message(
        type=MessageType.JOB_ASSIGNED.value,
        payload={
            'job_id': job_id,
            'queue': queue,
            'payload': payload,
            'priority': priority,
            'max_retries': max_retries,
            'attempt': attempt
        }
    )


def make_server_shutdown() -> Message:
    return Message(
        type=MessageType.SERVER_SHUTDOWN.value,
        payload={}
    )