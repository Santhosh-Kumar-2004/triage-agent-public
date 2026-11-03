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

    # LLM provider: "gemini" (default) or "anthropic"
    llm_provider: str = "gemini"
    gemini_api_key: str = ""
    gemini_model: str = "gemini-3.8-flash"
    anthropic_api_key: str = ""

    # Burr tracking UI (optional)
    enable_burr_tracker: bool = False

    class Config:
        env_file = ".env"
