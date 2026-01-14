import os
from typing import Optional, Literal
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import PostgresDsn, computed_field


class Settings(BaseSettings):
    """
    Application Configuration Class.

    This class loads environment variables from the .env file and provides
    typed access to configuration settings throughout the application.
    It uses Pydantic for validation to ensure all required variables are present.
    """

    # --------------------------------------------------------------------------
    # Application Settings
    # --------------------------------------------------------------------------
    PROJECT_NAME: str = "QuantLake Core"
    API_V1_STR: str = "/api/v1"

    # Environment Mode: 'development', 'production', or 'testing'
    APP_ENV: Literal["development", "production", "testing"] = "development"

    # --------------------------------------------------------------------------
    # Database Settings (Loaded from .env)
    # --------------------------------------------------------------------------
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str
    DB_PORT: int = 5432

    # Default to 'localhost' for local scripts, but Docker can override this to 'timescaledb'
    DB_HOST: str = "localhost"

    # Optional: Allow direct injection of the full connection string (useful for Docker Compose)
    DATABASE_URL: Optional[str] = None

    # --------------------------------------------------------------------------
    # Pydantic Configuration
    # --------------------------------------------------------------------------
    model_config = SettingsConfigDict(
        # Location of the .env file
        env_file=".env",
        # Encoding of the .env file
        env_file_encoding="utf-8",
        # Case sensitivity for environment variables (True = distinct cases)
        case_sensitive=True,
        # Ignore extra fields in .env that are not defined here
        extra="ignore",
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        """
        Constructs the SQLAlchemy connection string dynamically.

        Priority:
        1. If DATABASE_URL is explicitly set (e.g., by Docker), use it.
        2. Otherwise, build it from individual DB_* components.

        Returns:
            str: The full PostgreSQL connection URI.
        """
        if self.DATABASE_URL:
            return self.DATABASE_URL

        # Build the DSN (Data Source Name) manually using Pydantic's Url builder
        # Format: postgresql://user:password@host:port/dbname
        return str(
            PostgresDsn.build(
                scheme="postgresql",
                username=self.DB_USER,
                password=self.DB_PASSWORD,
                host=self.DB_HOST,
                port=self.DB_PORT,
                path=self.DB_NAME,
            )
        )


# --------------------------------------------------------------------------
# Singleton Instance
# --------------------------------------------------------------------------
# Create a single instance of Settings to be imported and used across the app.
settings = Settings()
