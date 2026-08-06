"""Direct tests of LivehostSessionRegistry.claim() -- the atomic check-and-set
that closes the session_id race ws.py's own check-then-register used to
leave open across `await upstream.connect()`.

These are deliberately unit-level rather than driven through the WebSocket
route: the race claim() closes is a same-event-loop TOCTOU window a few
microseconds wide, and there is no reliable way to force two real WS
connections to straddle it without either instrumenting production code for
test-only synchronization or accepting a flaky test (see the arbitration
test in test_ws_social.py for what that costs). A direct test of the atomic
primitive proves the invariant `claim()` is supposed to hold; test_ws_social.py's
`test_a_different_identity_cannot_hijack_an_existing_session_id` still covers
the post-registration case end to end through the real route.
"""

from livehost.registry import LivehostSession, LivehostSessionRegistry


def _session(user_id):
    # scheduler/ingestor are irrelevant to claim()'s ownership logic -- it
    # never touches them -- so plain placeholders keep this test about the
    # registry's own semantics only.
    return LivehostSession(scheduler=object(), ingestor=object(), user_id=user_id)


def test_claim_succeeds_on_a_fresh_id():
    registry = LivehostSessionRegistry()
    session = _session("user-1")
    assert registry.claim("s1", session) is True
    assert registry.get("s1") is session


def test_a_different_owner_is_refused_and_the_original_survives():
    """This is the race window itself, collapsed to a single-threaded
    reproduction: two LivehostSessions for the same session_id, the second
    with a different owner, claimed back to back with no await between them
    (exactly what two coroutines racing the same session_id would each
    individually observe if claim() -- unlike the old get()-then-register()
    pair -- did not hold the check and the write atomic)."""
    registry = LivehostSessionRegistry()
    first = _session("user-1")
    second = _session("user-2")

    assert registry.claim("s1", first) is True
    assert registry.claim("s1", second) is False

    # Not merely "some session with user-1's data" -- the *original* object,
    # untouched. A test that only checked user_id would pass even if claim()
    # replaced the entry and then merely reported failure.
    assert registry.get("s1") is first


def test_a_reclaim_by_the_same_owner_succeeds():
    """Preserves register()'s old overwrite-on-collision behavior for the
    one case it was legitimate: the same owner reconnecting under the same
    session_id."""
    registry = LivehostSessionRegistry()
    first = _session("user-1")
    second = _session("user-1")

    assert registry.claim("s1", first) is True
    assert registry.claim("s1", second) is True
    assert registry.get("s1") is second


def test_two_anonymous_callers_can_both_claim_the_same_id():
    """Documents the accepted caveat, not a bug: with auth disabled, every
    caller's user_id normalizes to the same value (None -- see ws.py's
    `user_id or None`), so two different anonymous callers are
    indistinguishable to claim() and either can take over the other's
    session_id. That mirrors the gateway's own WsIdentity.unauthenticated:
    with no ownership model to enforce in that mode, there is nothing for
    this guard to enforce either. This test exists so a future change that
    accidentally makes claim() reject None-vs-None (breaking a legitimate
    anonymous reconnect) gets caught, not so this behavior is relied on as a
    security boundary in dev mode -- it deliberately is not one.
    """
    registry = LivehostSessionRegistry()
    anon_a = _session(None)
    anon_b = _session(None)

    assert registry.claim("s1", anon_a) is True
    assert registry.claim("s1", anon_b) is True
    assert registry.get("s1") is anon_b
