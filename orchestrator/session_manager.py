"""
Session Manager — in-memory session boshqaruvi.
- Guardrail prompt har bir sessiyaga qo'shiladi
- Context oynasi config'dan keladi (max_history, token_limit)
- Zero-log: diskka yozilmaydi
"""
import time
import threading
import re
from typing import Dict, List

from orchestrator.security.guardrail import guardrail


class SessionManager:
    VALID_ROLES = {"system", "user", "assistant", "tool"}

    def __init__(self, ttl_seconds: int = 900, max_history: int = 20, max_sessions: int = 5000):
        self.sessions: Dict[str, Dict] = {}
        self.ttl_seconds = ttl_seconds
        self.max_history = max_history
        self.max_sessions = max_sessions
        self._lock = threading.RLock()

    def _build_system_messages(self, caller_name: str = "", language: str = "uz") -> list:
        """System prompt + FAQ + Guardrail ni yig'ish."""
        from orchestrator.profile_manager import profile_manager

        # Asosiy prompt (template to'ldirilgan)
        main_prompt = profile_manager.get_system_prompt_with_faq(caller_name, language)

        # Guardrail qo'shish (ENDI HAQIQATDA ISHLATILADI!)
        guardrail_prompt = guardrail.get_system_guardrail_prompt()

        return [
            {"role": "system", "content": main_prompt},
            {"role": "system", "content": guardrail_prompt},
        ]

    def get_session(self, caller_id: str, caller_name: str = "", language: str = "uz") -> List[Dict]:
        """Sessiyani olish yoki yangisini yaratish."""
        safe_id = _sanitize_id(caller_id)
        with self._lock:
            if safe_id in self.sessions:
                self.sessions[safe_id]["last_active"] = time.time()
                return self.sessions[safe_id]["history"]

            system_msgs = self._build_system_messages(caller_name, language)
            self.sessions[safe_id] = {
                "history": system_msgs,
                "last_active": time.time(),
                "caller_name": caller_name,
                "language": language,
            }
            return self.sessions[safe_id]["history"]

    def add_message(self, caller_id: str, role: str, content: str | None, **kwargs):
        """Xabarni sessiyaga qo'shish."""
        if role not in self.VALID_ROLES:
            role = "user"
        if caller_id is None:
            return
        safe_id = _sanitize_id(caller_id)
        with self._lock:
            if len(self.sessions) >= self.max_sessions and safe_id not in self.sessions:
                self._evict_oldest()
            history = self.get_session(safe_id)

            msg = {"role": role}
            if content is not None:
                msg["content"] = content
            msg.update(kwargs)
            history.append(msg)

            self._trim_history(safe_id)

    def _trim_history(self, safe_id: str):
        """System xabarlarni saqlab, kontekst oynasini cheklash."""
        history = self.sessions[safe_id]["history"]
        if len(history) <= self.max_history:
            return
        # System xabarlar sonini topish
        system_count = sum(1 for m in history if m["role"] == "system")
        # System'larni saqlab, qolganini max_history - system_count gacha kesish
        non_system = [m for m in history if m["role"] != "system"]
        keep_count = self.max_history - system_count
        self.sessions[safe_id]["history"] = (
            [m for m in history if m["role"] == "system"] + non_system[-keep_count:]
        )

    def clear_session(self, caller_id: str):
        if caller_id is None:
            return
        with self._lock:
            self.sessions.pop(_sanitize_id(caller_id), None)

    def get_active_count(self) -> int:
        with self._lock:
            return len(self.sessions)

    def cleanup_expired_now(self) -> int:
        with self._lock:
            now = time.time()
            expired = [c for c, s in self.sessions.items() if now - s["last_active"] > self.ttl_seconds]
            for c in expired:
                self.sessions.pop(c, None)
            return len(expired)

    def _evict_oldest(self):
        if not self.sessions:
            return
        oldest = min(self.sessions.items(), key=lambda kv: kv[1]["last_active"])[0]
        self.sessions.pop(oldest, None)

    def update_ttl_from_config(self):
        """Config'dan TTL va max_history ni yangilash."""
        from orchestrator.profile_manager import profile_manager
        ctx = profile_manager.get_context_config()
        self.ttl_seconds = ctx.get("session_ttl_seconds", 900)
        self.max_history = ctx.get("max_history", 20)


def _sanitize_id(value) -> str:
    # #11 fix: : va . belgilari olib tashlandi (path traversal / injection xavfi)
    return re.sub(r"[^A-Za-z0-9_\-]", "_", str(value or ""))[:64]


# Global seans menejeri — config'dan parametrlar bilan
session_manager = SessionManager(ttl_seconds=900, max_history=20)
