import abc
from collections import deque
from pyspark.sql import DataFrame
from typing import List, Union, Dict, Iterable


DataStreamType = Union[DataFrame, Dict[str, DataFrame]]


class Task(metaclass=abc.ABCMeta):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f'<Task {self.name}>'

    @abc.abstractmethod
    def perform(self, data: DataStreamType) -> DataStreamType:
        ...

    def __rshift__(self, other: 'Task') -> 'DAG':
        r = DAG()
        r.add(self)
        r.add(other, inputs=[self])
        return r

    def __floordiv__(self, other: 'DAG') -> 'DAG':
        r = DAG()
        r.add(self)
        r.add(other)
        return r

    __and__ = __floordiv__


class DAG:
    def __init__(self):
        self._stages = set()
        self._edges = {}
        self._dependencies = {}

    def copy(self) -> 'DAG':
        r = DAG()
        r._stages = self._stages.copy()
        r._edges = self._edges.copy()
        r._dependencies = self._dependencies.copy()
        return r

    def update(self, other: 'DAG') -> 'DAG':
        self._stages.update(other._stages)
        self._edges.update(other._edges)
        self._dependencies.update(other._dependencies)

    @property
    def endpoints(self) -> Iterable[Task]:
        return [s for s in self._stages if s not in self._edges]

    @property
    def startpoints(self) -> Iterable[Task]:
        return [s for s in self._stages if s not in self._dependencies]

    def add(self, node: Task, inputs: List[Task] = None):
        self._stages.add(node)
        if inputs is None:
            return
        for s in inputs:
            if s not in self._stages:
                self._stages.add(s)
            if s not in self._edges:
                self._edges[s] = set()
            self._edges[s].add(node)
        if node not in self._dependencies:
            self._dependencies[node] = set()
        self._dependencies[node].update(inputs)

    def perform(self, data: DataStreamType) -> DataStreamType:
        q = deque()
        output = {}
        for s in self.startpoints:
            q.appendleft(s)

        while len(q) > 0:
            stage = q.pop()
            if stage in self._dependencies:
                if len(self._dependencies[stage]) == 1:
                    d = output[list(self._dependencies[stage])[0]]
                else:
                    d = {dep.name: output[dep] for dep in self._dependencies[stage]}
            elif isinstance(data, dict):
                d = data[stage.name]
            else:
                d = data

            output[stage] = stage.perform(d)
            if stage in self._edges:
                q.extendleft((e for e in self._edges[stage] if self._dependencies[e].issubset(output.keys())))

        if len(self.endpoints) == 1:
            return output[self.endpoints[0]]
        return {e.name: output[e] for e in self.endpoints}

    def __floordiv__(self, other: 'DAG') -> 'DAG':
        r = self.copy()
        r.update(other)
        return r

    __and__ = __floordiv__

    def __rshift__(self, other: 'Task') -> 'DAG':
        r = self.copy()
        r.add(other, self.endpoints)
        return r
