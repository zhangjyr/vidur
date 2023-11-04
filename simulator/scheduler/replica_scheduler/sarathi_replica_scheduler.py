from simulator.entities.batch import Batch, Request
from simulator.scheduler.replica_scheduler.orca_replica_scheduler import (
    OrcaReplicaScheduler,
)


class SarathiReplicaScheduler(OrcaReplicaScheduler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._chunk_size = self._config.sarathi_scheduler_chunk_size
        # club multiple prefills to ensure uniform chunk size
        self._enable_rolling_prefills = (
            self._config.sarathi_scheduler_enable_rolling_prefills
        )
        # when we are packing multiple prefills in a batch, we need to ensure
        # that we don't end up packing a very small prefill chunk just to make batch full
        # because that will lead to reduced number of schedulable prefill requests
        self._prefill_fitting_tolerance = (
            self._config.sarathi_scheduler_prefill_fitting_tolerance
        )

    def _get_request_next_num_tokens(
        self, request: Request, batch_contains_prefill: bool, num_batch_tokens: int
    ) -> int:
        assert not request.completed

        if request.is_prefill_complete:
            return 1

        next_num_tokens = min(
            request.num_prefill_tokens - request.num_processed_tokens, self._chunk_size
        )

        if not batch_contains_prefill:
            return next_num_tokens

        if self._enable_rolling_prefills and num_batch_tokens < self._chunk_size * (
            1 - self._prefill_fitting_tolerance
        ):
            # we can have multiple prefills per batch
            # but the total number of tokens should not exceed
            # the max batch size
            return min(next_num_tokens, self._chunk_size - num_batch_tokens)
        else:
            # we will only allow one prefill per batch
            return 0

    def _get_next_batch(self) -> Batch:
        requests = []
        num_tokens = []
        skipped_requests = []
        contains_prefill = False
        num_batch_tokens = 0

        # preempted requests could contain multiple requests which have
        # partial prefills completed, so we need to be careful
        while self._preempted_requests:
            if len(requests) == self._max_batch_size:
                break

            request = self._preempted_requests.pop(0)
            next_num_tokens = self._get_request_next_num_tokens(
                request, contains_prefill, num_batch_tokens
            )

            if next_num_tokens == 0:
                skipped_requests.append(request)
                continue

            if not request.is_prefill_complete:
                contains_prefill = True

            num_batch_tokens += next_num_tokens
            requests.append(request)
            num_tokens.append(next_num_tokens)

        # re-add the skipped requests, but make sure that we add them to the
        # front of the queue so that they are scheduled first and we maintain FIFO ordering
        self._preempted_requests = skipped_requests + self._preempted_requests
        skipped_requests = []

        while self._request_queue:
            if len(requests) == self._max_batch_size:
                break

            if not self.can_allocate(self._max_blocks_per_sequence):
                break

            next_num_tokens = self._get_request_next_num_tokens(
                self._request_queue[0], contains_prefill, num_batch_tokens
            )

            if next_num_tokens == 0:
                break

            request = self._request_queue.pop(0)
            self.allocate(request.id, self._max_blocks_per_sequence)

            # all new requests will have a prefill
            contains_prefill = True
            num_batch_tokens += next_num_tokens
            requests.append(request)
            num_tokens.append(next_num_tokens)

        if not requests:
            return

        return Batch(self._replica_id, requests, num_tokens)
