from queue import PriorityQueue


# TODO: should we use PQ from asyncio instead?


class ImmutableObjectsPriorityQueue(PriorityQueue):

    @classmethod
    def from_args(cls, *arg) -> "ImmutableObjectsPriorityQueue":
        pq = cls()
        for item in arg:
            pq.put(item)
        return pq

    def clone(self) -> "ImmutableObjectsPriorityQueue":
        # Don't use copy.deepcopy(), because content is intended to be immutable.
        clone = ImmutableObjectsPriorityQueue()
        for item in self.queue:
            clone.put(item)
        return clone

    def to_list(self) -> list:
        clone = self.clone()
        converted = []
        while clone.qsize() > 0:
            converted.append(clone.get())
        return converted

    def __eq__(self, other):
        return self.to_list() == other.to_list()
