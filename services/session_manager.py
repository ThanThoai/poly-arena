"""
SessionManager — Orchestrator for multi-session matching architecture.

Replaces MatchingEngine as the top-level registry/dispatcher in WS Feed Service.
Manages SessionEngine instances keyed by session_id, provides:
  - Session CRUD with lifecycle transitions
  - Token index for WS event fan-out (token_id → [session_ids])
  - Active queue keys for multi-key BRPOP
  - Candle boundary handling (old ACTIVE → SETTLING, new PREFETCH → ACTIVE)
  - Compatibility shims (add_valid_token, is_valid_token, best_ask/bid)
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Callable, Optional

from services.matching_engine import (
    ShadowOrderbook,
    SimulatedOrder,
    BracketFillResult,
    OrderSide,
    OrderStatus,
    OrderStateChangeEvent,
)
from services.session_engine import SessionEngine, SessionState

logger = logging.getLogger(__name__)


class SessionManager:
    """
    Top-level orchestrator that replaces MatchingEngine in WS Feed Service.

    Thread-safety: ``_lock`` protects ``_engines`` and ``_token_index`` mutations.
    SessionEngine-level operations are protected by each engine's own lock.
    """

    def __init__(self) -> None:
        self._engines: dict[str, SessionEngine] = {}         # session_id → engine
        self._token_index: dict[str, list[str]] = {}         # token_id → [session_ids]
        self._lock = threading.Lock()
        self._on_market_resolved: Optional[Callable] = None  # callback(asset_id, orders)

    # ── Session CRUD ──────────────────────────────────────────────────────────

    def create_session(
        self,
        session_id: str,
        tokens: dict[str, str],
        initial_state: SessionState = SessionState.PREFETCH,
    ) -> SessionEngine:
        """
        Create a new SessionEngine and register it.

        Args:
            session_id: e.g. "BTC:M5:1709313000"
            tokens: {"UP": "0xabc...", "DOWN": "0xdef..."}
            initial_state: Starting lifecycle state.
        """
        with self._lock:
            if session_id in self._engines:
                existing = self._engines[session_id]
                logger.debug("Session %s already exists (state=%s)", session_id, existing.state.value)
                return existing

            engine = SessionEngine(session_id, tokens)
            if initial_state != SessionState.PREFETCH:
                engine.transition(initial_state)
            self._engines[session_id] = engine

            # Build token index
            for token_id in engine.token_ids:
                self._token_index.setdefault(token_id, []).append(session_id)

            logger.info(
                "Created session %s with %d token(s), state=%s",
                session_id, len(tokens), engine.state.value,
            )
            return engine

    def get_session(self, session_id: str) -> Optional[SessionEngine]:
        with self._lock:
            return self._engines.get(session_id)

    def get_sessions_for_token(self, token_id: str) -> list[SessionEngine]:
        """Return all sessions that own a given token_id."""
        with self._lock:
            session_ids = self._token_index.get(token_id, [])
            return [self._engines[sid] for sid in session_ids if sid in self._engines]

    def transition_session(self, session_id: str, new_state: SessionState) -> None:
        """Transition a session to a new state. ARCHIVED removes from registries."""
        with self._lock:
            engine = self._engines.get(session_id)
            if engine is None:
                logger.warning("Cannot transition unknown session %s", session_id)
                return

            # Capture token_ids before potential clear
            old_token_ids = list(engine.token_ids)

            engine.transition(new_state)

            if new_state == SessionState.ARCHIVED:
                # Remove from token index (prevent event dispatch)
                # but keep in _engines for ARCHIVED_RETENTION_S
                for token_id in old_token_ids:
                    sids = self._token_index.get(token_id, [])
                    if session_id in sids:
                        sids.remove(session_id)
                    if not sids:
                        self._token_index.pop(token_id, None)

    def list_sessions(self) -> list[SessionEngine]:
        """Return all non-ARCHIVED sessions."""
        with self._lock:
            return [e for e in self._engines.values() if e.state != SessionState.ARCHIVED]

    def list_archived_sessions(self) -> list[SessionEngine]:
        """Return all ARCHIVED sessions still retained in the registry."""
        with self._lock:
            return [e for e in self._engines.values() if e.state == SessionState.ARCHIVED]

    def purge_session(self, session_id: str) -> None:
        """Permanently remove an ARCHIVED session from the registry."""
        with self._lock:
            engine = self._engines.get(session_id)
            if engine is None:
                return
            if engine.state != SessionState.ARCHIVED:
                logger.warning(
                    "Cannot purge non-ARCHIVED session %s (state=%s)",
                    session_id, engine.state.value,
                )
                return
            del self._engines[session_id]
            logger.debug("Purged ARCHIVED session %s", session_id)

    # ── WS Event dispatch ─────────────────────────────────────────────────────

    def dispatch_event(self, event: dict) -> None:
        """
        Fan-out a WS event to all sessions owning the token.

        market_resolved is handled here (callback to publish affected bo_ids).
        All other event types are dispatched to matching SessionEngine(s).
        """
        etype = event.get("event_type", "")
        asset_id = event.get("asset_id", "")

        if etype == "market_resolved":
            self._handle_market_resolved(asset_id)
            return

        # Fan-out to all sessions owning this token
        sessions = self.get_sessions_for_token(asset_id)
        if not sessions and asset_id:
            logger.warning(
                "No sessions for token %s (event_type=%s) — event dropped",
                asset_id[:16], etype,
            )
        for session in sessions:
            try:
                session.dispatch_ws_event(event)
            except Exception as exc:
                logger.error(
                    "Error dispatching %s to session %s: %s",
                    etype, session.session_id, exc,
                )

    def _handle_market_resolved(self, asset_id: str) -> None:
        """Handle market_resolved: cancel TP/SL, mark positions closed across all sessions."""
        sessions = self.get_sessions_for_token(asset_id)
        all_resolved: list[SimulatedOrder] = []

        for session in sessions:
            book = session.get_book_for_token(asset_id)
            if book is None:
                continue
            with book._lock:
                for order in book._virtual_orders:
                    if order.position_closed:
                        continue
                    if order.status in (OrderStatus.PENDING, OrderStatus.PARTIAL):
                        order.status = OrderStatus.CANCELED
                    order.tp_price = None
                    order.sl_price = None
                    order.position_closed = True
                    all_resolved.append(order)

        if all_resolved:
            logger.info(
                "Market resolved %s: marked %d order(s) as position_closed across %d session(s)",
                asset_id[:16], len(all_resolved), len(sessions),
            )

        if self._on_market_resolved is not None:
            try:
                self._on_market_resolved(asset_id, all_resolved)
            except Exception as exc:
                logger.error("market_resolved callback error: %s", exc, exc_info=True)

    # ── OrderConsumer support ─────────────────────────────────────────────────

    def active_queue_keys(self) -> list[str]:
        """Return queue keys for all non-ARCHIVED sessions."""
        with self._lock:
            return [
                engine.queue_key
                for engine in self._engines.values()
                if engine.state != SessionState.ARCHIVED
            ]

    # ── Query helpers ─────────────────────────────────────────────────────────

    def get_book(self, token_id: str) -> Optional[ShadowOrderbook]:
        """Find the ShadowOrderbook for a token across all sessions."""
        sessions = self.get_sessions_for_token(token_id)
        # Prefer ACTIVE session, then PREFETCH, then SETTLING
        for preferred_state in (SessionState.ACTIVE, SessionState.PREFETCH, SessionState.SETTLING):
            for session in sessions:
                if session.state == preferred_state:
                    book = session.get_book_for_token(token_id)
                    if book is not None:
                        return book
        # Fallback: any session
        for session in sessions:
            book = session.get_book_for_token(token_id)
            if book is not None:
                return book
        return None

    def best_ask(self, token_id: str) -> Optional[float]:
        sessions = self.get_sessions_for_token(token_id)
        for session in sessions:
            if session.state in (SessionState.ACTIVE, SessionState.PREFETCH):
                result = session.best_ask(token_id)
                if result is not None:
                    return result
        return None

    def best_bid(self, token_id: str) -> Optional[float]:
        sessions = self.get_sessions_for_token(token_id)
        for session in sessions:
            if session.state in (SessionState.ACTIVE, SessionState.PREFETCH):
                result = session.best_bid(token_id)
                if result is not None:
                    return result
        return None

    # ── Periodic maintenance ──────────────────────────────────────────────────

    def expire_all_pending(self) -> int:
        """Expire TTL-elapsed orders across ALL sessions."""
        with self._lock:
            engines = list(self._engines.values())
        total = 0
        for engine in engines:
            if engine.state != SessionState.ARCHIVED:
                total += engine.expire_all_pending()
        return total

    def archive_settling_sessions(self, grace_seconds: float = 30) -> list[str]:
        """Archive sessions that have been SETTLING longer than grace_seconds."""
        now = datetime.now(timezone.utc)
        to_archive: list[str] = []

        with self._lock:
            for sid, engine in list(self._engines.items()):
                if engine.state == SessionState.SETTLING and engine.settling_at is not None:
                    elapsed = (now - engine.settling_at).total_seconds()
                    if elapsed > grace_seconds:
                        to_archive.append(sid)

        for sid in to_archive:
            self.transition_session(sid, SessionState.ARCHIVED)

        if to_archive:
            logger.info("Archived %d settling session(s): %s", len(to_archive), to_archive)
        return to_archive

    # ── Candle boundary lifecycle ─────────────────────────────────────────────

    def on_candle_boundary(self, symbol: str, timeframe: str, new_candle_ts: int) -> None:
        """
        Handle a candle boundary:
          - Old session (current ACTIVE for this sym:tf): ACTIVE → SETTLING
          - New session (PREFETCH with matching candle_open): PREFETCH → ACTIVE
        """
        with self._lock:
            engines = list(self._engines.values())

        for engine in engines:
            if engine.symbol != symbol or engine.timeframe != timeframe:
                continue
            if engine.state == SessionState.ACTIVE and engine.candle_open < new_candle_ts:
                self.transition_session(engine.session_id, SessionState.SETTLING)
            elif engine.state == SessionState.PREFETCH and engine.candle_open == new_candle_ts:
                self.transition_session(engine.session_id, SessionState.ACTIVE)

    # ── Compatibility shims ───────────────────────────────────────────────────

    def add_valid_token(self, token_id: str) -> None:
        """No-op — tokens are validated by session ownership."""
        pass

    def is_valid_token(self, token_id: str) -> bool:
        """Check if any session owns this token."""
        with self._lock:
            return token_id in self._token_index and len(self._token_index[token_id]) > 0

    def register_valid_tokens(self, token_ids: list[str]) -> None:
        """No-op compatibility shim — tokens registered via create_session."""
        pass

    # ── Shutdown ──────────────────────────────────────────────────────────────

    def shutdown(self) -> None:
        """Archive all sessions, firing state-change callbacks for balance refunds."""
        with self._lock:
            session_ids = list(self._engines.keys())

        for sid in session_ids:
            engine = self.get_session(sid)
            if engine is None:
                continue
            # Cancel all orders in all books and fire callbacks
            for book in list(engine.books.values()):
                with book._lock:
                    for order in book._virtual_orders:
                        if order.status in (OrderStatus.PENDING, OrderStatus.PARTIAL):
                            order.status = OrderStatus.CANCELED
                    state_events = book.collect_state_changes()
                book._fire_state_change_callbacks(state_events)
            self.transition_session(sid, SessionState.ARCHIVED)

        logger.info("SessionManager shut down (%d sessions archived)", len(session_ids))


# ── Module-level singleton ───────────────────────────────────────────────────

_session_manager: Optional[SessionManager] = None


def get_session_manager() -> SessionManager:
    """Return the global SessionManager singleton (create on first call)."""
    global _session_manager
    if _session_manager is None:
        _session_manager = SessionManager()
    return _session_manager
