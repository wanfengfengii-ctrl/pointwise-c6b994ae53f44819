"""Quality-isolated execution over the shared strict traversal.

IsolatedRunner reuses ExecutionRunner.execute_scope frame by frame, so
traversal order, laziness, short-circuiting, caching and function call
effects are exactly the strict ones.  Candidate details are buffered in
memory instead of being staged immediately.  A quality rejection
(FunctionFailure, MissingField, TypeMismatch) is handled by the frame
whose occurrence owns the failing expression: candidate details produced
inside that occurrence are withdrawn, the rest of its subtree is skipped,
and unrelated occurrences continue in the original depth-first order.
Already discovered rejections are never withdrawn, issued tickets are
never recalled, and surviving details are renumbered from the buffer
without any re-evaluation.  Non-quality errors propagate and keep the
whole-run failure boundary.
"""
from dataclasses import dataclass
from warehouse.execution.runner import ExecutionRunner, OutputRow
from warehouse.execution.environments import Environment
from warehouse.expressions.errors import (
    EvaluationError, FunctionFailure, MissingField, TypeMismatch)

QUALITY_ERRORS = (FunctionFailure, MissingField, TypeMismatch)


@dataclass(frozen=True)
class Rejection:
    owner: object
    error: EvaluationError


@dataclass(frozen=True)
class IsolatedOutcome:
    rows: tuple
    rejections: tuple


def _within(coordinate, owner):
    return (coordinate.source == owner.source and
            coordinate.elements[:len(owner.elements)] == owner.elements)


class _IsolationState:
    def __init__(self):
        self.rows = []
        self.rejections = []
        self._seen = []

    def reject(self, owner, error):
        self.rows = [row for row in self.rows if not _within(row[0], owner)]
        if any(seen is error for seen in self._seen):
            return
        self._seen.append(error)
        self.rejections.append(Rejection(owner, error))


class IsolatedRunner(ExecutionRunner):
    _state = None

    def execute_isolated(self, sources, plan, context):
        state = _IsolationState()
        self._state = state
        try:
            for source, record in sources:
                environment = Environment.root(plan.root, source, record)
                for coordinate, values in self.execute_scope(environment, context, ()):
                    state.rows.append((coordinate, values))
        finally:
            self._state = None
        rows = tuple(OutputRow(index, coordinate, values)
                     for index, (coordinate, values) in enumerate(state.rows))
        return IsolatedOutcome(rows, tuple(state.rejections))

    def execute_scope(self, environment, context, ancestors):
        try:
            yield from super().execute_scope(environment, context, ancestors)
        except EvaluationError as error:
            state = self._state
            if (state is not None and isinstance(error, QUALITY_ERRORS)
                    and error.owner == environment.coordinate):
                state.reject(environment.coordinate, error)
                return
            raise
