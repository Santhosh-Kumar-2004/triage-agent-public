from __future__ import annotations
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Airflow connection (mock or real)
    airflow_url: str = "http://127.0.0.1:8080"
    airflow_username: str = "admin"
    airflow_password: str = "admin"
    poll_interval_seconds: int = 60

    # Email (SMTP — sending you failure + fix notifications)
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    alert_to: str = ""

    # DAG source (local repo path; falls back to GitHub if token is real)
    github_token: str = "dummy"
    github_repo: str = "dummy/repo"
    github_repo_local_path: str = ""
    github_base_branch: str = "main"

    # ── LLM ───────────────────────────────────────────────────────────────────
    # "deepinfra"  → any OpenAI-compatible endpoint. DeepInfra is the default
    #                host, but Together, Fireworks, Groq, OpenRouter and a
    #                local Ollama / vLLM server all speak the same API: point
    #                LLM_BASE_URL at them and the code does not change.
    # "anthropic"  → Anthropic SDK (native)
    # "gemini"     → Google GenAI SDK (native)
    llm_provider: str = "deepinfra"

    # DeepInfra — open-weight models, OpenAI-compatible API.
    # Model catalogue + live pricing: https://deepinfra.com/models/text-generation
    #   deepseek-ai/DeepSeek-V4-Flash-0731    $0.06/$0.18 per 1M  — cheap workhorse
    #   deepseek-ai/DeepSeek-V4.1-Flash-1017  $0.098/$0.294       — better at coding
    #   deepseek-ai/DeepSeek-V4-Pro-0813      $1.30/$2.60         — for hard cases
    deepinfra_api_key: str = ""
    deepinfra_model: str = "deepseek-ai/DeepSeek-V4.1-Flash-1017"
    llm_base_url: str = "https://api.deepinfra.com/v1/openai"

    # Native-SDK providers (kept as fallbacks)
    gemini_api_key: str = ""
    gemini_model: str = "gemini-3.8-flash"
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-sonnet-4-6"

    # Burr tracking UI (optional)
    enable_burr_tracker: bool = False

    class Config:
        env_file = ".env"
        extra = "ignore"

    # ── helpers ───────────────────────────────────────────────────────────────

    def llm_credentials(self) -> tuple[str, str, str]:
        """Return (api_key, model, base_url) for the configured provider.

        Keeps provider branching out of main.py / scripts/fix.py — they unpack
        this and hand it straight to generate_fix().
        """
        if self.llm_provider == "anthropic":
            return self.anthropic_api_key, self.anthropic_model, ""
        if self.llm_provider == "gemini":
            return self.gemini_api_key, self.gemini_model, ""
        # default: deepinfra / any OpenAI-compatible host
        return self.deepinfra_api_key, self.deepinfra_model, self.llm_base_url
