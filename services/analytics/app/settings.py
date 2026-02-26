"""Application settings using pydantic-settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    database_url: str = ""
    database_url_test: str = ""

    # Session / Auth
    session_max_age_hours: int = 720
    login_rate_limit_per_minute: int = 10

    # Token encryption (Fernet key for Oura tokens at rest)
    token_encryption_key: str = ""

    # Auto-migration (dev-only, default false)
    enable_auto_migrate: bool = False

    # Oura OAuth
    oura_client_id: str = ""
    oura_client_secret: str = ""
    oura_redirect_uri: str = "http://localhost:3000/api/oura/callback"

    # Oura API
    oura_api_base_url: str = "https://api.ouraring.com/v2"
    oura_auth_url: str = "https://cloud.ouraring.com/oauth/authorize"
    oura_token_url: str = "https://api.ouraring.com/oauth/token"

    # CORS
    cors_origins: str = "http://localhost:3000"

    # Scopes for Oura API
    oura_scopes: str = "daily heartrate tag session workout personal spo2 heart_health"

    # OpenAI (chat agent)
    openai_api_key: str = ""
    chat_enabled: bool = False
    chat_max_tool_calls_per_turn: int = 10
    chat_timeout_seconds: int = 60
    chat_max_tokens: int = 4096


settings = Settings()
