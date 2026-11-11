# Copyright 2026 by Falconry contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Opt-in routing settlement sessions.

The routing decision in Falcon spans several planes that each keep their
own matching semantics:

* the resource plane (the compiled router, URI field converters and the
  per-resource ``method_map``);
* the sink plane (regex prefixes matched regardless of the HTTP method);
* the static-route plane;
* and the implicit not-found fallback.

This module adds an opt-in observability layer that labels the plane that
actually settled each request, and freezes an immutable snapshot of the
observable terminal state (status, body text and ``Allow`` header). The
decision logic itself is not changed; settlement only records what the
existing carriers already decided. When no :class:`RouteSettlement` is
attached, the app behaves exactly as it did before.

Frozen records can never be written through: re-calling the router,
reordering sinks versus static routes, or swapping the converter table
only affects requests settled in later epochs.
"""

from __future__ import annotations

from contextvars import ContextVar
from threading import Lock
from typing import Any

__all__ = (
    'CONVERTER_REJECTED',
    'NO_TEMPLATE_MATCH',
    'OUTCOME_BAD_REQUEST',
    'OUTCOME_METHOD_NOT_ALLOWED',
    'OUTCOME_OPTIONS_DEFAULT',
    'OUTCOME_RESOURCE_HIT',
    'OUTCOME_ROUTE_NOT_FOUND',
    'OUTCOME_SINK_MATCHED',
    'OUTCOME_STATIC_MATCHED',
    'PLANE_NOT_FOUND',
    'PLANE_RESOURCE',
    'PLANE_SINK',
    'PLANE_STATIC',
    'RouteSettlement',
    'SettlementFrozenError',
    'SettlementRecord',
    'SETTLEMENT_ALLOWED_ATTR',
    'SETTLEMENT_OUTCOME_ATTR',
    'TABLE_REORDER',
    'TABLE_SINK',
    'TABLE_STATIC',
    'current_record',
    'tag_default_responder',
)

# ---------------------------------------------------------------------
# Plane labels: which routing carrier settled the request.
# ---------------------------------------------------------------------
PLANE_RESOURCE = 'resource'
PLANE_SINK = 'sink'
PLANE_STATIC = 'static'
PLANE_NOT_FOUND = 'not_found'

# ---------------------------------------------------------------------
# Outcome labels: how the chosen plane resolved the request.
# ---------------------------------------------------------------------
OUTCOME_RESOURCE_HIT = 'resource_hit'
OUTCOME_METHOD_NOT_ALLOWED = 'method_not_allowed'
OUTCOME_OPTIONS_DEFAULT = 'options_default'
OUTCOME_BAD_REQUEST = 'bad_request'
OUTCOME_SINK_MATCHED = 'sink_matched'
OUTCOME_STATIC_MATCHED = 'static_matched'
OUTCOME_ROUTE_NOT_FOUND = 'route_not_found'

# ---------------------------------------------------------------------
# Router diagnostics: why a resource template failed to match.
# ---------------------------------------------------------------------
CONVERTER_REJECTED = 'converter_rejected'
NO_TEMPLATE_MATCH = 'no_template_match'

# ---------------------------------------------------------------------
# Route-table change kinds reported by the app registration carriers.
# ---------------------------------------------------------------------
TABLE_SINK = 'sink_registered'
TABLE_STATIC = 'static_registered'
TABLE_REORDER = 'sink_static_reordered'

# Markers stamped on the default responders created by
# ``set_default_responders()`` so that the resource plane can distinguish
# an explicitly implemented responder from the framework-generated 405
# and default OPTIONS exits without enumerating method_map key names.
SETTLEMENT_OUTCOME_ATTR = '_falcon_settlement_outcome'
SETTLEMENT_ALLOWED_ATTR = '_falcon_settlement_allowed'

_current_record: ContextVar[SettlementRecord | None] = ContextVar(
    'falcon_route_settlement_record', default=None
)


def current_record() -> SettlementRecord | None:
    """Return the record of the request currently being settled, if any."""
    return _current_record.get()


def tag_default_responder(
    responder: Any, outcome: str, allowed_methods: tuple[str, ...] | None
) -> None:
    """Stamp a framework-generated responder with its settlement outcome.

    Args:
        responder: The default responder callable created for an
            unimplemented HTTP method or for the implicit OPTIONS handler.
        outcome: One of the ``OUTCOME_*`` labels describing the exit.
        allowed_methods: Snapshot of the methods that the resource
            explicitly implements, or ``None`` when no ``Allow`` value
            applies.
    """

    setattr(responder, SETTLEMENT_OUTCOME_ATTR, outcome)
    setattr(responder, SETTLEMENT_ALLOWED_ATTR, allowed_methods)


class SettlementFrozenError(RuntimeError):
    """Raised when attempting to mutate a frozen settlement record."""


class SettlementRecord:
    """Immutable-after-freeze snapshot of a single settled request.

    Args:
        method: HTTP method of the settled request.
        path: Path portion of the settled request URI.
        plane: One of the ``PLANE_*`` labels identifying the carrier that
            settled the request.
        outcome: One of the ``OUTCOME_*`` labels describing how the plane
            resolved the request.
        params: Snapshot of the keyword arguments passed to the responder
            (URI template fields or sink named groups).
        epoch: Route-table epoch in effect when the request was settled.
        converter_failure: Router diagnostic for resource-plane misses
            (``CONVERTER_REJECTED`` or ``NO_TEMPLATE_MATCH``), or ``None``
            for resource-plane hits.
        allowed_methods: Explicitly implemented methods carried by a
            default 405 responder, if any.
        uri_template: The matching URI template for resource-plane hits.
    """

    __slots__ = (
        'method',
        'path',
        'plane',
        'outcome',
        'params',
        'epoch',
        'converter_failure',
        'allowed_methods',
        'uri_template',
        'status',
        'allow_header',
        'body_text',
        '_frozen',
    )

    def __init__(
        self,
        method: str,
        path: str,
        plane: str,
        outcome: str,
        params: dict[str, Any],
        epoch: int,
        converter_failure: str | None = None,
        allowed_methods: tuple[str, ...] | None = None,
        uri_template: str | None = None,
    ) -> None:
        self.method = method
        self.path = path
        self.plane = plane
        self.outcome = outcome
        # NOTE: take a copy so later mutation of the live params dict can
        # not write through the record.
        self.params = dict(params)
        self.epoch = epoch
        self.converter_failure = converter_failure
        self.allowed_methods = allowed_methods
        self.uri_template = uri_template
        self.status: str | None = None
        self.allow_header: str | None = None
        self.body_text: str | None = None
        self._frozen = False

    @property
    def frozen(self) -> bool:
        """``True`` once the epoch owning this record has been advanced."""
        return self._frozen

    def finalize(
        self, status: str, allow_header: str | None, body_text: str | None
    ) -> None:
        """Snapshot the observable terminal state of the response.

        Args:
            status: The response status line.
            allow_header: Value of the response ``Allow`` header, if any.
            body_text: Best-effort text snapshot of the response body.

        Raises:
            SettlementFrozenError: If the record was already frozen.
        """

        if self._frozen:
            raise SettlementFrozenError(
                f'settlement record for {self.method} {self.path} is frozen'
            )

        self.status = status
        self.allow_header = allow_header
        self.body_text = body_text

    def freeze(self) -> None:
        """Freeze this record, rejecting any further terminal snapshots."""
        self._frozen = True

    def __repr__(self) -> str:
        return (
            f'SettlementRecord({self.method!r}, {self.path!r}, '
            f'plane={self.plane!r}, outcome={self.outcome!r}, '
            f'epoch={self.epoch}, frozen={self._frozen})'
        )


class RouteSettlement:
    """Opt-in ledger that settles and freezes routing decisions per epoch.

    A settlement is attached to an app via
    :meth:`~falcon.App.enable_route_settlement`. Every request handled
    while the settlement is active appends one :class:`SettlementRecord`
    to the current, open epoch. Calling :meth:`advance` freezes every
    record collected so far and opens a fresh epoch; frozen records are
    kept as immutable snapshots and can never be rewritten by subsequent
    router calls, sink/static reordering, or converter-table changes.

    Args:
        app: The :class:`~falcon.App` (or ASGI counterpart) whose routing
            decisions are being settled.
    """

    def __init__(self, app: Any) -> None:
        self._app = app
        self._lock = Lock()
        self._active = False
        self._epoch = 0
        self._open: list[SettlementRecord] = []
        self._frozen: list[tuple[SettlementRecord, ...]] = []
        self._table_events: list[tuple[str, Any]] = []
        self._find_calls = 0
        self._matched_finds = 0

    # -----------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------
    def attach(self) -> RouteSettlement:
        """Wire this settlement into its app and its default router.

        Attaching is idempotent; a custom router that does not expose the
        settlement hook is left untouched.
        """

        if self._active:
            return self

        self._app._settlement = self
        router = getattr(self._app, '_router', None)
        if router is not None and hasattr(router, '_settlement'):
            router._settlement = self
        self._active = True
        return self

    def detach(self) -> None:
        """Unwire this settlement, restoring the app's default behavior."""

        if not self._active:
            return

        router = getattr(self._app, '_router', None)
        if router is not None and getattr(router, '_settlement', None) is self:
            router._settlement = None
        if getattr(self._app, '_settlement', None) is self:
            self._app._settlement = None
        self._active = False
        _current_record.set(None)

    def __enter__(self) -> RouteSettlement:
        return self.attach()

    def __exit__(self, ex_type: Any, ex: Any, traceback: Any) -> None:
        self.detach()

    @property
    def active(self) -> bool:
        """Whether this settlement is currently attached to its app."""
        return self._active

    # -----------------------------------------------------------------
    # Epoch management
    # -----------------------------------------------------------------
    @property
    def epoch(self) -> int:
        """Sequence number of the currently open epoch."""
        return self._epoch

    def advance(self) -> int:
        """Freeze the open epoch and open a new one.

        Returns:
            int: Sequence number of the newly opened epoch.
        """

        with self._lock:
            frozen_epoch = tuple(self._open)
            for record in frozen_epoch:
                record.freeze()
            self._frozen.append(frozen_epoch)
            self._open = []
            self._epoch += 1
            _current_record.set(None)
            return self._epoch

    @property
    def frozen(self) -> tuple[SettlementRecord, ...]:
        """All records frozen by previous :meth:`advance` calls."""
        with self._lock:
            return tuple(
                record for epoch_records in self._frozen for record in epoch_records
            )

    @property
    def open_records(self) -> tuple[SettlementRecord, ...]:
        """Records collected by the epoch that is currently open."""
        with self._lock:
            return tuple(self._open)

    @property
    def records(self) -> tuple[SettlementRecord, ...]:
        """Frozen records followed by the open epoch's records."""
        return self.frozen + self.open_records

    @property
    def table_events(self) -> tuple[tuple[str, Any], ...]:
        """Observed route-table registration and reordering events."""
        with self._lock:
            return tuple(self._table_events)

    @property
    def find_calls(self) -> int:
        """Number of router ``find()`` calls observed while attached."""
        return self._find_calls

    @property
    def matched_finds(self) -> int:
        """Number of observed ``find()`` calls that returned a resource."""
        return self._matched_finds

    def get(self, method: str, path: str) -> SettlementRecord | None:
        """Return the newest record for a method/path pair, if settled.

        Open-epoch records take precedence over frozen ones so that callers
        always observe the most recent settlement of a given request.
        """

        target = (method, path)
        with self._lock:
            for record in reversed(self._open):
                if (record.method, record.path) == target:
                    return record
            for epoch_records in reversed(self._frozen):
                for record in reversed(epoch_records):
                    if (record.method, record.path) == target:
                        return record
        return None

    # -----------------------------------------------------------------
    # Carrier hooks (invoked by the routing/app internals)
    # -----------------------------------------------------------------
    def open_record(
        self,
        *,
        method: str,
        path: str,
        plane: str,
        outcome: str,
        params: dict[str, Any],
        converter_failure: str | None = None,
        allowed_methods: tuple[str, ...] | None = None,
        uri_template: str | None = None,
    ) -> SettlementRecord:
        """Open a record in the current epoch for a newly routed request."""

        record = SettlementRecord(
            method=method,
            path=path,
            plane=plane,
            outcome=outcome,
            params=params,
            epoch=self._epoch,
            converter_failure=converter_failure,
            allowed_methods=allowed_methods,
            uri_template=uri_template,
        )
        with self._lock:
            self._open.append(record)
        _current_record.set(record)
        return record

    def finalize_record(
        self,
        record: SettlementRecord,
        status: str,
        allow_header: str | None,
        body_text: str | None,
    ) -> None:
        """Snapshot the response state, unless the record is already frozen.

        A frozen record is deliberately ignored instead of raising, so a
        late response callback can never write through an advanced epoch.
        """

        with self._lock:
            if record.frozen:
                _current_record.set(None)
                return
            record.finalize(status, allow_header, body_text)
        _current_record.set(None)

    def note_router_find(self, uri: str, matched: bool) -> None:
        """Record an observed call into the compiled router's ``find()``."""

        with self._lock:
            self._find_calls += 1
            if matched:
                self._matched_finds += 1

    def note_route_table(self, kind: str, token: Any = None) -> None:
        """Record a sink/static registration or reordering event."""

        with self._lock:
            self._table_events.append((kind, token))
