from __future__ import annotations

from collections import deque

from infra.queue.events import EventStatus, SystemEvent


class InMemoryEventQueue:
    def __init__(self) -> None:
        self._queue: deque[SystemEvent] = deque()
        self._consumed: list[SystemEvent] = []

    def publish(self, event: SystemEvent) -> None:
        self._queue.append(event)

    def consume(self, limit: int = 100) -> list[SystemEvent]:
        events: list[SystemEvent] = []
        for _ in range(min(limit, len(self._queue))):
            event = self._queue.popleft()
            event.status = EventStatus.CONSUMED
            events.append(event)
            self._consumed.append(event)
        return events

    def pending(self, limit: int = 100) -> list[SystemEvent]:
        return list(self._queue)[:limit]

    def consumed(self, limit: int = 100) -> list[SystemEvent]:
        return self._consumed[-limit:]

    def size(self) -> int:
        return len(self._queue)
