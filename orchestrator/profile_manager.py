"""
Profile Manager — markaziy profil boshqaruvchisi.
- Dinamik template o'zgaruvchilar (company_name, bot_name, sanasi, vaqti, caller_name)
- FAQ yuklash (alohida fayldan)
- Prompt versiyalash (prompt_v2.txt)
- Hot-reload (qayta yuklash)
- LLM parametrlarini config'dan o'qish
"""
import json
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

log = logging.getLogger("profile_manager")


class ProfileManager:
    def __init__(self, base_path: str = "/home/ubuntu/ai-operator/profiles"):
        self.base_path = base_path
        self.active_profile_name = None
        self.system_prompt_template = ""
        self.faq_content = ""
        self.config = {}
        self.tools = []
        self.crm_instance = None
        self._prompt_version = "v1"

    # ─────────────────── YUKLASH ───────────────────

    def load_profile(self, profile_name: str):
        """Profilni diskdan o'qib aktivlashtirish."""
        profile_dir = os.path.join(self.base_path, profile_name)
        if not os.path.isdir(profile_dir):
            raise FileNotFoundError(f"Profil topilmadi: {profile_dir}")

        # 1. Config
        self.config = self._read_json(os.path.join(profile_dir, "config.json"), {})
        self._prompt_version = self.config.get("prompt_version", "v1")

        # 2. System prompt (versiyalangan)
        prompt_file = self._find_prompt_file(profile_dir)
        self.system_prompt_template = self._read_text(prompt_file)

        # 3. FAQ
        faq_file = os.path.join(profile_dir, "faq.txt")
        if os.path.isfile(faq_file):
            self.faq_content = self._read_text(faq_file)
        else:
            self.faq_content = ""

        # 4. Tools
        self.tools = self._read_json(os.path.join(profile_dir, "tools.json"), [])

        # 5. CRM
        crm_type = self.config.get("crm_type", "dummy")
        self.crm_instance = self._create_crm(crm_type)

        self.active_profile_name = profile_name
        log.info(f"[ProfileManager] '{profile_name}' profili yuklandi (v{self._prompt_version})")

    def reload(self):
        """Hot-reload: aktiv profilni qayta yuklash."""
        if self.active_profile_name:
            self.load_profile(self.active_profile_name)

    def auto_select(self, language: str = "uz"):
        """Tilga qarab avtomatik profil tanlash."""
        lang_map = {"uz": "isp_beta", "ru": "isp_ru", "en": "isp_en"}
        profile = lang_map.get(language, "isp_beta")
        if self.active_profile_name != profile:
            self.load_profile(profile)

    # ─────────────────── TEMPLATE ───────────────────

    def get_system_prompt(self, caller_name: str = "", language: str = "uz") -> str:
        """System prompt'ni yig'ish: template + guardrail + FAQ."""
        now = datetime.now()
        variables = {
            "company_name": self.config.get("company_name", "Kompaniya"),
            "bot_name": self.config.get("bot_name", "Operator"),
            "current_date": now.strftime("%Y-%m-%d"),
            "current_time": now.strftime("%H:%M"),
            "caller_name": caller_name or "Hurmatli mijoz",
        }

        # Templat'ni to'ldirish
        prompt = self.system_prompt_template
        for key, val in variables.items():
            prompt = prompt.replace("{" + key + "}", str(val))

        return prompt.strip()

    def get_system_prompt_with_faq(self, caller_name: str = "", language: str = "uz") -> str:
        """FAQ bilan birga system prompt."""
        base = self.get_system_prompt(caller_name, language)
        if self.faq_content:
            base += "\n\n--- FAQ (zarur bo'lganda foydalaning) ---\n" + self.faq_content
        return base

    # ─────────────────── GETTERS ───────────────────

    def get_tools(self) -> list:
        return self.tools

    def get_crm(self):
        return self.crm_instance

    def get_llm_params(self) -> dict:
        """LLM parametrlarini config'dan olish."""
        return self.config.get("llm", {"temperature": 0.3, "max_tokens": 512})

    def get_context_config(self) -> dict:
        return self.config.get("context", {"max_history": 20, "token_limit": 3000})

    def get_greeting(self, language: str = "uz") -> str:
        greetings = self.config.get("greeting", {})
        return greetings.get(language, greetings.get("uz", "Salom"))

    def get_language(self) -> str:
        return self.config.get("language", "uz")

    # ─────────────────── HELPERS ───────────────────

    def _find_prompt_file(self, profile_dir: str) -> str:
        """Versiyalangan prompt faylni topish."""
        if self._prompt_version != "v1":
            vfile = os.path.join(profile_dir, f"prompt_{self._prompt_version}.txt")
            if os.path.isfile(vfile):
                return vfile
            log.warning(f"Versiya {self._prompt_version} topilmadi, v1 ishlatilmoqda.")
        return os.path.join(profile_dir, "prompt.txt")

    @staticmethod
    def _read_text(path: str) -> str:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
        return ""

    @staticmethod
    def _read_json(path: str, default: Any) -> Any:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        return default

    def _create_crm(self, crm_type: str):
        if crm_type == "dummy":
            from orchestrator.crm.dummy_sql import DummySQLCRM
            return DummySQLCRM()
        else:
            log.warning(f"Noma'lum CRM tipi: {crm_type}. Dummy ishlatilmoqda.")
            from orchestrator.crm.dummy_sql import DummySQLCRM
            return DummySQLCRM()


# Global singleton
profile_manager = ProfileManager()
