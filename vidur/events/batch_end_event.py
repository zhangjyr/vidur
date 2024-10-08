from typing import List

from vidur.entities import Batch
from vidur.events import BaseEvent, RequestEndEvent
from vidur.logger import init_logger
from vidur.metrics import MetricsStore
from vidur.scheduler import BaseGlobalScheduler
from vidur.types import EventType

logger = init_logger(__name__)


class BatchEndEvent(BaseEvent):
    def __init__(self, time: float, replica_id: int, batch: Batch):
        super().__init__(time, EventType.BATCH_END)

        self._replica_id = replica_id
        self._batch = batch

    def handle_event(
        self, scheduler: BaseGlobalScheduler, metrics_store: MetricsStore
    ) -> List[BaseEvent]:
        from vidur.events.replica_schedule_event import ReplicaScheduleEvent

        self._batch.on_batch_end(self.time)
        replica_scheduler = scheduler.get_replica_scheduler(self._replica_id)
        replica_scheduler.on_batch_end(self._batch)

        memory_usage_percent = replica_scheduler.memory_usage_percent
        metrics_store.on_batch_end(
            self.time, self._batch, self._replica_id, memory_usage_percent
        )

        if len(self._batch.completed_requests) == 0:
            return [ReplicaScheduleEvent(self.time, self._replica_id)]
        
        # Generate request completion events.
        ret = [None] * (len(self._batch.completed_requests) + 1)
        for i, request in enumerate(self._batch.completed_requests):
            ret[i] = RequestEndEvent(self.time, request)
        ret[-1] = ReplicaScheduleEvent(self.time, self._replica_id)

        return ret

    def to_dict(self):
        return {
            "time": self.time,
            "event_type": self.event_type,
            "batch_id": self._batch.id,
            "requests": len(self._batch.requests),
            "completed": len(self._batch.completed_requests)
        }
