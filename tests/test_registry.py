"""Direct tests of LivehostSessionRegistry.claim() and .release() -- the
atomic check-and-set/compare-and-delete pair that replaced the old
get()-then-register()/unregister() calls, which left two races open: a
TOCTOU window across `await upstream.connect()` (closed by claim()), and an
ABA bug where a superseded connection's teardown could delete-by-id a live
session a same-owner reclaim had already replaced it with (closed by
release()).

These are deliberately unit-level rather than driven through the WebSocket
route: both races are same-event-loop windows a few microseconds wide (or,
for release()'s ABA case, no timing window at all -- just the wrong
comparison), and there is no reliable way to force real WS connections to
reproduce them without either instrumenting production code for test-only
synchronization or accepting a flaky test (see the arbitration test in
test_ws_social.py for what that costs). Direct tests of the atomic
primitives prove the invariants they're supposed to hold; test_ws_social.py's
`test_a_different_identity_cannot_hijack_an_existing_session_id` still covers
the post-registration hijack case end to end through the real route.
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


def test_a_superseded_reclaim_does_not_evict_the_new_session():
    """ABA bug reproduction: the owner reconnects under the same session_id
    (claim() correctly lets the same owner replace their own entry -- see
    test_a_reclaim_by_the_same_owner_succeeds), then the *superseded*
    connection's own teardown runs and tries to release what it thinks is
    still its entry. release() must refuse -- it removes what THIS caller
    put there, not whatever is stored under the id right now -- or a live,
    working session (B) ends up with no registry entry at all, and /status,
    /connect, /disconnect on it all start 404ing.
    """
    registry = LivehostSessionRegistry()
    session_a = _session("user-1")
    session_b = _session("user-1")  # same owner reconnecting

    assert registry.claim("s1", session_a) is True
    assert registry.claim("s1", session_b) is True  # legitimate reclaim; registry now holds B

    # A's teardown, still winding down from before B connected, releases
    # its own (stale) reference -- must be a no-op against the live entry.
    assert registry.release("s1", session_a) is False
    assert registry.get("s1") is session_b

    # B's own teardown still works normally.
    assert registry.release("s1", session_b) is True
    assert registry.get("s1") is None


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
