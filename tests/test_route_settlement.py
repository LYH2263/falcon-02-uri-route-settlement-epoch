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

"""Acceptance tests for opt-in route settlement sessions.

The same app instance always carries a resource route, a prefix-competing
sink, and a static route. Tests drive sequential requests through the WSGI
and ASGI surfaces and assert the four routing planes (resource, sink,
static, not-found) plus the converter/method divergences, and then verify
that frozen epochs cannot be written through by later table changes.
"""

import pytest

import falcon
from falcon import routing
from falcon import testing
from falcon.routing import settlement


# ---------------------------------------------------------------------
# Resources and sinks, in sync and async flavors.
# ---------------------------------------------------------------------
class _ItemsResource:
    def on_get(self, req, resp, id):
        resp.text = f'item:{id}'

    def on_post(self, req, resp, id):
        resp.text = f'created:{id}'


class _AsyncItemsResource:
    async def on_get(self, req, resp, id):
        resp.text = f'item:{id}'

    async def on_post(self, req, resp, id):
        resp.text = f'created:{id}'


class _FilesResource:
    def on_get(self, req, resp, matched_path):
        resp.text = f'files:{matched_path}'


class _AsyncFilesResource:
    async def on_get(self, req, resp, matched_path):
        resp.text = f'files:{matched_path}'


class _JournalResource:
    def on_get(self, req, resp, year, month):
        resp.text = f'journal:{year}:{month}'


class _AsyncJournalResource:
    async def on_get(self, req, resp, year, month):
        resp.text = f'journal:{year}:{month}'


class _PingResource:
    def on_get(self, req, resp, n):
        resp.text = f'ping:{n}'


class _AsyncPingResource:
    async def on_get(self, req, resp, n):
        resp.text = f'ping:{n}'


class _TagsResource:
    def on_get(self, req, resp, name):
        resp.text = f'tag:{name}'


class _AsyncTagsResource:
    async def on_get(self, req, resp, name):
        resp.text = f'tag:{name}'


def _sink(req, resp, **kw):
    resp.text = 'sink:' + kw.get('tail', '')


async def _sink_async(req, resp, **kw):
    resp.text = 'sink:' + kw.get('tail', '')


class _LenientInt(routing.IntConverter):
    """Int converter that accepts values the stock converter rejects."""

    def convert(self, value):
        result = super().convert(value)
        if result is None:
            return -1
        return result


class _RejectingPathConverter(routing.PathConverter):
    """Multi-segment converter that rejects everything it is offered."""

    def convert(self, value):
        return None


class _NullRouter:
    """Minimal custom router without the settlement diagnostic hook."""

    def __init__(self, method_map):
        self._method_map = method_map
        self.added = []

    def add_route(self, uri_template, resource, **kwargs):
        self.added.append(uri_template)

    def find(self, uri, req=None):
        if uri == '/fake-resource':
            return object(), self._method_map, {}, None
        return None


def _fake_responder(req, resp):
    resp.text = 'fake'


async def _fake_responder_async(req, resp):
    resp.text = 'fake'


class _ShortCircuitMiddleware:
    def process_request(self, req, resp):
        resp.complete = True


class _AsyncShortCircuitMiddleware:
    async def process_request(self, req, resp):
        resp.complete = True


# ---------------------------------------------------------------------
# Fixtures / builders
# ---------------------------------------------------------------------
def _build_app(asgi, util, tmp_path, sink_before_static_route=True):
    (tmp_path / 'file.txt').write_text('static-body')

    app = util.create_app(asgi, sink_before_static_route=sink_before_static_route)
    app.add_route(
        '/items/{id:int}',
        _AsyncItemsResource() if asgi else _ItemsResource(),
    )
    app.add_route(
        '/files/{matched_path:path}',
        _AsyncFilesResource() if asgi else _FilesResource(),
    )
    app.add_route(
        '/journal/{year:int}-{month:int}',
        _AsyncJournalResource() if asgi else _JournalResource(),
    )
    app.add_route(
        '/tags/{name}',
        _AsyncTagsResource() if asgi else _TagsResource(),
    )
    app.add_sink(
        _sink_async if asgi else _sink,
        r'/items/(?P<tail>.+)',
    )
    app.add_static_route('/items/static', str(tmp_path))
    return app


@pytest.fixture
def settled(asgi, util, tmp_path):
    def make(sink_before_static_route=True):
        app = _build_app(asgi, util, tmp_path, sink_before_static_route)
        session = app.enable_route_settlement()
        return app, testing.TestClient(app), session

    return make


def _snapshot(record):
    # NOTE: the frozen flag is asserted separately because advancing an
    # epoch flips it without rewriting any of the settled evidence.
    return (
        record.method,
        record.path,
        record.plane,
        record.outcome,
        record.converter_failure,
        tuple(sorted(record.params.items())),
        record.allowed_methods,
        record.uri_template,
        record.status,
        record.allow_header,
        record.body_text,
        record.epoch,
    )


# ---------------------------------------------------------------------
# Four terminal states in one session, same app instance.
# ---------------------------------------------------------------------
def test_four_terminal_states_in_one_session(settled):
    app, client, session = settled()

    # 1. Resource hit (converter accepts; explicit responder).
    result = client.simulate_get('/items/42')
    assert result.status == falcon.HTTP_200
    assert result.text == 'item:42'
    record = session.get('GET', '/items/42')
    assert record is not None
    assert record.plane == settlement.PLANE_RESOURCE
    assert record.outcome == settlement.OUTCOME_RESOURCE_HIT
    assert record.params == {'id': 42}
    assert record.uri_template == '/items/{id:int}'
    assert record.status == falcon.HTTP_200
    assert record.body_text == 'item:42'
    assert record.allow_header is None
    assert record.epoch == 0 and not record.frozen

    # 2. Converter failure -> no resource -> the competing sink drains it.
    result = client.simulate_get('/items/not-an-int')
    assert result.status == falcon.HTTP_200
    assert result.text == 'sink:not-an-int'
    record = session.get('GET', '/items/not-an-int')
    assert record.plane == settlement.PLANE_SINK
    assert record.outcome == settlement.OUTCOME_SINK_MATCHED
    assert record.converter_failure == settlement.CONVERTER_REJECTED
    assert record.params == {'tail': 'not-an-int'}
    assert record.body_text == 'sink:not-an-int'

    # 3. Resource path matches, but the method is not implemented -> 405.
    result = client.simulate_delete('/items/42')
    assert result.status == falcon.HTTP_405
    assert result.headers['Allow'] == 'GET, POST, OPTIONS'
    record = session.get('DELETE', '/items/42')
    assert record.plane == settlement.PLANE_RESOURCE
    assert record.outcome == settlement.OUTCOME_METHOD_NOT_ALLOWED
    assert record.allowed_methods == ('GET', 'POST', 'OPTIONS')
    assert record.allow_header == 'GET, POST, OPTIONS'
    assert record.status == falcon.HTTP_405

    # 4. Nothing matches -> 404, distinct from the method-level 405.
    result = client.simulate_get('/nowhere/missing')
    assert result.status == falcon.HTTP_404
    record = session.get('GET', '/nowhere/missing')
    assert record.plane == settlement.PLANE_NOT_FOUND
    assert record.outcome == settlement.OUTCOME_ROUTE_NOT_FOUND
    assert record.converter_failure == settlement.NO_TEMPLATE_MATCH
    assert record.status == falcon.HTTP_404

    planes = {record.plane for record in session.records}
    outcomes = {record.outcome for record in session.records}
    assert planes == {
        settlement.PLANE_RESOURCE,
        settlement.PLANE_SINK,
        settlement.PLANE_NOT_FOUND,
    }
    assert {
        settlement.OUTCOME_RESOURCE_HIT,
        settlement.OUTCOME_SINK_MATCHED,
        settlement.OUTCOME_METHOD_NOT_ALLOWED,
        settlement.OUTCOME_ROUTE_NOT_FOUND,
    }.issubset(outcomes)

    # Every record was finalized with an observable status snapshot.
    assert all(record.status is not None for record in session.records)


def test_options_default_is_distinct_from_405(settled):
    app, client, session = settled()

    # The default OPTIONS responder advertises the explicit methods only;
    # OPTIONS itself is intentionally excluded from its Allow header.
    result = client.simulate_options('/items/42')
    assert result.status == falcon.HTTP_200
    assert result.headers['Allow'] == 'GET, POST'
    record = session.get('OPTIONS', '/items/42')
    assert record.outcome == settlement.OUTCOME_OPTIONS_DEFAULT
    assert record.plane == settlement.PLANE_RESOURCE
    assert record.allow_header == 'GET, POST'
    assert record.allowed_methods is None

    # The default OPTIONS exit and the missing-method 405 exit are labeled
    # differently even though both come from generated responders.
    client.simulate_delete('/items/42')
    mna = session.get('DELETE', '/items/42')
    assert mna.outcome == settlement.OUTCOME_METHOD_NOT_ALLOWED
    assert mna.allow_header == 'GET, POST, OPTIONS'
    assert mna.outcome != record.outcome


def test_sink_is_method_agnostic_unlike_resource_method_map(settled):
    app, client, session = settled()

    # Same POST method: resource plane honors method_map, sink does not.
    result = client.simulate_post('/items/42')
    assert result.status == falcon.HTTP_200
    assert result.text == 'created:42'
    assert session.get('POST', '/items/42').outcome == (settlement.OUTCOME_RESOURCE_HIT)

    result = client.simulate_post('/items/not-an-int')
    assert result.status == falcon.HTTP_200
    assert result.text == 'sink:not-an-int'
    sink_record = session.get('POST', '/items/not-an-int')
    assert sink_record.plane == settlement.PLANE_SINK

    # DELETE on the resource is 405; the same verb on the sink is accepted.
    result = client.simulate_delete('/items/42')
    assert result.status == falcon.HTTP_405
    result = client.simulate_delete('/items/not-an-int')
    assert result.status == falcon.HTTP_200
    assert result.text == 'sink:not-an-int'
    assert session.get('DELETE', '/items/not-an-int').plane == (settlement.PLANE_SINK)


def test_path_converter_multi_segment_vs_int_rejection(settled):
    app, client, session = settled()

    # PathConverter consumes several segments and still hits the resource.
    result = client.simulate_get('/files/a/b/c')
    assert result.status == falcon.HTTP_200
    assert result.text == 'files:a/b/c'
    record = session.get('GET', '/files/a/b/c')
    assert record.outcome == settlement.OUTCOME_RESOURCE_HIT
    assert record.params == {'matched_path': 'a/b/c'}

    # The single-segment int failure is a different cut: it falls through.
    result = client.simulate_get('/items/not-an-int')
    assert result.text == 'sink:not-an-int'
    record = session.get('GET', '/items/not-an-int')
    assert record.converter_failure == settlement.CONVERTER_REJECTED

    # Extra segments past the template are structural misses, not rejects.
    result = client.simulate_get('/items/42/extra')
    assert result.text == 'sink:42/extra'
    record = session.get('GET', '/items/42/extra')
    assert record.converter_failure == settlement.NO_TEMPLATE_MATCH


def test_complex_template_converter_rejection(settled):
    app, client, session = settled()

    result = client.simulate_get('/journal/2026-11')
    assert result.status == falcon.HTTP_200
    assert result.text == 'journal:2026:11'

    # One of the two inline int converters rejects -> no resource -> 404.
    result = client.simulate_get('/journal/foo-11')
    assert result.status == falcon.HTTP_404
    record = session.get('GET', '/journal/foo-11')
    assert record.converter_failure == settlement.CONVERTER_REJECTED
    assert record.plane == settlement.PLANE_NOT_FOUND

    # A segment that the complex regex cannot parse is a structural miss.
    result = client.simulate_get('/journal/nodash')
    assert result.status == falcon.HTTP_404
    record = session.get('GET', '/journal/nodash')
    assert record.converter_failure == settlement.NO_TEMPLATE_MATCH


def test_settlement_diagnose_walker_labels(settled):
    app, client, session = settled()
    router = app._router

    assert router.settlement_diagnose('/items/not-an-int') == (
        settlement.CONVERTER_REJECTED
    )
    assert router.settlement_diagnose('/journal/foo-11') == (
        settlement.CONVERTER_REJECTED
    )
    assert router.settlement_diagnose('/journal/nodash') == (
        settlement.NO_TEMPLATE_MATCH
    )
    assert router.settlement_diagnose('/nowhere/missing') == (
        settlement.NO_TEMPLATE_MATCH
    )
    assert router.settlement_diagnose('/items/42/extra') == (
        settlement.NO_TEMPLATE_MATCH
    )
    # Paths that the compiled finder would match exercise the walker's
    # completion branches (including the multi-segment converter).
    assert router.settlement_diagnose('/items/42') == (settlement.NO_TEMPLATE_MATCH)
    assert router.settlement_diagnose('/journal/2026-11') == (
        settlement.NO_TEMPLATE_MATCH
    )
    assert router.settlement_diagnose('/files/a/b') == (settlement.NO_TEMPLATE_MATCH)
    # A path ending at an intermediate node exercises the depth guard.
    assert router.settlement_diagnose('/items') == (settlement.NO_TEMPLATE_MATCH)
    # A matching complex node with leftover segments cannot complete.
    assert router.settlement_diagnose('/journal/2026-11/extra') == (
        settlement.NO_TEMPLATE_MATCH
    )
    # A plain variable node (no converter) still walks to completion.
    assert router.settlement_diagnose('/tags/whatever') == (
        settlement.NO_TEMPLATE_MATCH
    )
    result = client.simulate_get('/tags/whatever')
    assert result.text == 'tag:whatever'
    assert session.get('GET', '/tags/whatever').outcome == (
        settlement.OUTCOME_RESOURCE_HIT
    )


# ---------------------------------------------------------------------
# Freezing: advanced epochs must never be written through.
# ---------------------------------------------------------------------
def test_frozen_settlement_not_written_through(settled):
    app, client, session = settled()

    client.simulate_get('/items/42')
    client.simulate_get('/items/static/file.txt')
    client.simulate_delete('/items/42')
    client.simulate_get('/items/not-an-int')
    client.simulate_get('/nowhere/missing')

    before = [_snapshot(record) for record in session.records]
    find_calls = session.find_calls

    epoch = session.advance()
    assert epoch == 1
    assert session.epoch == 1
    assert len(session.frozen) == len(before)
    assert all(record.frozen for record in session.frozen)
    assert session.open_records == ()

    # Re-calling find() must not touch the frozen records.
    app._router.find('/items/42')
    app._router.find('/items/static/file.txt')
    app._router.find('/nowhere/missing')
    assert session.find_calls == find_calls + 3
    assert [_snapshot(record) for record in session.frozen] == before

    # Flipping sink/static order only settles new requests in the new epoch.
    app._sink_before_static_route = False
    app._update_sink_and_static_routes()

    result = client.simulate_get('/items/static/file.txt')
    assert result.text == 'static-body'
    new_record = session.get('GET', '/items/static/file.txt')
    assert new_record.plane == settlement.PLANE_STATIC
    assert new_record.epoch == 1 and not new_record.frozen

    frozen_match = next(
        record
        for record in session.frozen
        if record.method == 'GET' and record.path == '/items/static/file.txt'
    )
    assert frozen_match.plane == settlement.PLANE_SINK
    assert frozen_match.body_text == 'sink:static/file.txt'
    assert _snapshot(frozen_match) in before

    # Swapping the converter table and forcing a recompile likewise only
    # affects newly settled requests.
    app.router_options.converters['int'] = _LenientInt
    app.add_route(
        '/ping/{n:int}',
        _AsyncPingResource() if app._ASGI else _PingResource(),
    )

    result = client.simulate_get('/items/not-an-int')
    assert result.text == 'item:-1'
    new_record = session.get('GET', '/items/not-an-int')
    assert new_record.plane == settlement.PLANE_RESOURCE
    assert new_record.outcome == settlement.OUTCOME_RESOURCE_HIT
    assert new_record.params == {'id': -1}

    frozen_int = next(
        record
        for record in session.frozen
        if record.method == 'GET' and record.path == '/items/not-an-int'
    )
    assert frozen_int.plane == settlement.PLANE_SINK
    assert frozen_int.converter_failure == settlement.CONVERTER_REJECTED

    # Lookup walks the open epoch past the newest record to an older one.
    older = session.get('GET', '/items/static/file.txt')
    assert older is not None and older.plane == settlement.PLANE_STATIC

    # Advancing again freezes the new epoch; the original snapshot holds.
    assert session.advance() == 2
    assert len(session.frozen) == len(before) + 2
    assert [_snapshot(record) for record in session.frozen[: len(before)]] == (before)

    # With no open epoch records, get() searches the frozen epochs.
    frozen_hit = session.get('GET', '/items/42')
    assert frozen_hit is not None and frozen_hit.frozen
    assert frozen_hit.plane == settlement.PLANE_RESOURCE


def test_sink_static_order_divergence_on_same_app(settled):
    app, client, session = settled(sink_before_static_route=True)

    result = client.simulate_get('/items/static/file.txt')
    assert result.text == 'sink:static/file.txt'
    first = session.get('GET', '/items/static/file.txt')
    assert first.plane == settlement.PLANE_SINK

    session.advance()
    app._sink_before_static_route = False
    app._update_sink_and_static_routes()

    result = client.simulate_get('/items/static/file.txt')
    assert result.text == 'static-body'
    second = session.get('GET', '/items/static/file.txt')
    assert second.plane == settlement.PLANE_STATIC
    assert second.epoch == 1

    # Same app, same path, two epochs: the frozen label survives the flip.
    assert first.frozen and first.plane != second.plane
    assert session.get('GET', '/items/static/file.txt') is second


def test_route_table_events_observed(settled, tmp_path):
    app, client, session = settled()

    # Registration events are only collected while a settlement is active.
    extra_dir = tmp_path / 'extra'
    extra_dir.mkdir()
    app.add_sink(_sink_async if app._ASGI else _sink, r'/extra/(?P<tail>.+)')
    app.add_static_route('/extra-static', str(extra_dir))

    kinds = [kind for kind, _token in session.table_events]
    assert settlement.TABLE_SINK in kinds
    assert settlement.TABLE_STATIC in kinds
    assert settlement.TABLE_REORDER in kinds

    client.simulate_get('/items/42')
    client.simulate_get('/nowhere/missing')
    assert session.matched_finds >= 1
    assert session.find_calls >= session.matched_finds


# ---------------------------------------------------------------------
# Lifecycle, guards and disabled pin behavior.
# ---------------------------------------------------------------------
def test_disabled_pin_behavior(asgi, util, tmp_path):
    app = _build_app(asgi, util, tmp_path)
    assert app._settlement is None
    assert app._router._settlement is None

    client = testing.TestClient(app)

    assert client.simulate_get('/items/42').text == 'item:42'
    assert client.simulate_get('/items/not-an-int').text == 'sink:not-an-int'
    result = client.simulate_delete('/items/42')
    assert result.status == falcon.HTTP_405
    assert result.headers['Allow'] == 'GET, POST, OPTIONS'
    result = client.simulate_get('/nowhere/missing')
    assert result.status == falcon.HTTP_404

    route = app._router.find('/items/42')
    assert route is not None and len(route) == 4
    assert app._router.find('/nowhere/missing') is None


def test_enable_idempotent_context_manager_and_disable(settled, asgi, util):
    app, client, session = settled()
    assert session.active
    assert app.enable_route_settlement() is session

    with app.enable_route_settlement() as contextual:
        assert contextual is session
        result = client.simulate_get('/items/42')
        assert result.status == falcon.HTTP_200
        assert len(session.open_records) >= 1

    app.disable_route_settlement()
    assert app._settlement is None
    assert app._router._settlement is None

    # Detaching is idempotent: an inactive session returns immediately.
    session.detach()
    # Detaching is idempotent and stops recording new requests.
    app.disable_route_settlement()
    recorded = len(session.records)
    client.simulate_get('/items/42')
    assert len(session.records) == recorded


def test_replacing_settlement_detaches_without_touching_new_session(settled):
    app, client, session = settled()

    replacement = routing.RouteSettlement(app).attach()
    assert app._settlement is replacement
    assert app._router._settlement is replacement

    # The stale session must not unset the replacement's wiring.
    session.detach()
    assert app._settlement is replacement
    assert app._router._settlement is replacement
    assert not session.active

    result = client.simulate_get('/items/42')
    assert result.status == falcon.HTTP_200
    assert len(replacement.open_records) == 1
    replacement.detach()


def test_custom_router_guard_and_bad_request_outcome(asgi, util, tmp_path):
    method_map = {
        'GET': _fake_responder_async if asgi else _fake_responder,
    }
    app = util.create_app(asgi, router=_NullRouter(method_map))
    app.add_sink(
        _sink_async if asgi else _sink,
        r'/items/(?P<tail>.+)',
    )
    session = app.enable_route_settlement()
    client = testing.TestClient(app)

    # The custom router lacks settlement_diagnose; the guard must not fail.
    result = client.simulate_get('/items/not-an-int')
    assert result.text == 'sink:not-an-int'
    record = session.get('GET', '/items/not-an-int')
    assert record.plane == settlement.PLANE_SINK
    assert record.converter_failure == settlement.NO_TEMPLATE_MATCH

    result = client.simulate_get('/nope')
    assert result.status == falcon.HTTP_404
    assert session.get('GET', '/nope').plane == settlement.PLANE_NOT_FOUND

    # A sparse custom method_map surfaces the bad-request fallback branch.
    result = client.simulate_get('/fake-resource')
    assert result.status == falcon.HTTP_200
    assert result.text == 'fake'
    result = client.simulate_put('/fake-resource')
    assert result.status == falcon.HTTP_400
    record = session.get('PUT', '/fake-resource')
    assert record.outcome == settlement.OUTCOME_BAD_REQUEST

    # Detaching must tolerate the router lacking a settlement slot.
    app.disable_route_settlement()
    assert app._settlement is None
    assert not hasattr(app._router, '_settlement')


def test_finalize_after_freeze_is_ignored(settled):
    app, client, session = settled()

    record = session.open_record(
        method='GET',
        path='/unfinished',
        plane=settlement.PLANE_NOT_FOUND,
        outcome=settlement.OUTCOME_ROUTE_NOT_FOUND,
        params={},
    )
    session.advance()
    assert record.frozen

    with pytest.raises(settlement.SettlementFrozenError):
        record.finalize(falcon.HTTP_200, None, 'body')

    # The session swallows late finalization instead of writing through.
    session.finalize_record(record, falcon.HTTP_200, None, 'body')
    assert record.status is None
    assert record.body_text is None
    assert repr(record).startswith('SettlementRecord(')
    assert session.get('GET', '/never-settled') is None


def test_short_circuit_without_record_is_finalized_safely(asgi, util, tmp_path):
    middleware = _AsyncShortCircuitMiddleware() if asgi else _ShortCircuitMiddleware()
    app = util.create_app(asgi, middleware=[middleware])
    app.add_route(
        '/items/{id:int}',
        _AsyncItemsResource() if asgi else _ItemsResource(),
    )
    session = app.enable_route_settlement()
    client = testing.TestClient(app)

    result = client.simulate_get('/items/42')
    assert result.status == falcon.HTTP_200
    # resp.complete short-circuits before routing: no record is opened, and
    # the finalize hook must tolerate the missing record.
    assert session.open_records == ()
    assert settlement.current_record() is None


def test_bytes_body_snapshot_and_type_guard(settled):
    app, client, session = settled()

    class _BytesResource:
        def on_get(self, req, resp):
            resp.data = b'bytes-body'

    class _AsyncBytesResource:
        async def on_get(self, req, resp):
            resp.data = b'bytes-body'

    app.add_route(
        '/bytes',
        _AsyncBytesResource() if app._ASGI else _BytesResource(),
    )
    result = client.simulate_get('/bytes')
    assert result.status == falcon.HTTP_200
    record = session.get('GET', '/bytes')
    assert record.body_text == 'bytes-body'

    # Non-bytes chunks must not break the best-effort text snapshot.
    record = session.open_record(
        method='GET',
        path='/guarded',
        plane=settlement.PLANE_NOT_FOUND,
        outcome=settlement.OUTCOME_ROUTE_NOT_FOUND,
        params={},
    )
    resp = app._response_type(options=app.resp_options)
    app._settlement_finalize(resp, [object()])
    assert record.body_text is None
    assert settlement.current_record() is None


def test_diagnose_handles_rejecting_multi_segment_converter(asgi, util):
    # The diagnostic must stay honest when a multi-segment converter
    # rejects even though it normally consumes the whole remaining path.
    app = util.create_app(asgi)
    app.router_options.converters['brokenpath'] = _RejectingPathConverter
    app.add_route(
        '/broken/{rest:brokenpath}',
        _AsyncFilesResource() if asgi else _FilesResource(),
    )

    assert app._router.settlement_diagnose('/broken/a/b') == (
        settlement.NO_TEMPLATE_MATCH
    )
