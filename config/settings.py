#!/usr/bin/env python
"""
Configuration Management Module
==============================

Handles all configuration loading, validation, and default creation
for the CURIA scraper system.

Features:
- Pydantic-based configuration validation
- Automatic config file creation with defaults
- Environment variable override support
- Configuration backup and migration
"""

import os
import toml
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field, validator
from datetime import datetime


class GeneralSettings(BaseModel):
    """General scraper configuration settings"""

    output_dir: str = Field(default="./output", description="Directory for output files")
    checkpoint_file: str = Field(default="./checkpoint.json", description="Checkpoint file path")
    headless: bool = Field(default=True, description="Run browser in headless mode")
    throttle_delay_ms: int = Field(default=2000, ge=500, le=10000, description="Delay between requests in ms")
    preferred_language: str = Field(default="EN", description="Preferred document language (EN, FR, DE, etc.)")
    max_documents: Optional[int] = Field(default=None, description="Maximum documents to process (None for unlimited)")
    concurrent_pages: int = Field(default=1, ge=1, le=5, description="Number of concurrent browser pages")
    retry_attempts: int = Field(default=3, ge=1, le=10, description="Number of retry attempts for failed requests")
    timeout_seconds: int = Field(default=30, ge=10, le=120, description="Page load timeout in seconds")

    @validator('preferred_language')
    def validate_language(cls, v):
        """Validate language code format"""
        if not v.isupper() or len(v) != 2:
            raise ValueError('Language code must be 2 uppercase letters (e.g., EN, FR, DE)')
        return v


class SiteSettings(BaseModel):
    """CURIA website-specific configuration"""

    listing_url: str = Field(
        default="https://curia.europa.eu/juris/recherche.jsf?language=en",
        description="CURIA search results URL"
    )
    base_url: str = Field(default="https://curia.europa.eu", description="Base URL for relative links")
    document_content_selector: str = Field(default="body", description="CSS selector for document content")
    start_print_button_text: str = Field(default="Start Printing", description="Text of the print button")
    document_link_selector: str = Field(
        default="div#docHtml a[href*='document.jsf']",
        description="CSS selector for document links"
    )
    next_page_selector: str = Field(
        default="a[title='Next Page']",
        description="CSS selector for next page button"
    )

    # Advanced selectors for better document detection
    alternative_link_selectors: list[str] = Field(
        default=["a[href*='document.jsf']", "a[href*='docid=']"],
        description="Alternative selectors for document links"
    )
    alternative_next_selectors: list[str] = Field(
        default=["a[title*='Next']", "a:has-text('Next')", "a:has-text('»')"],
        description="Alternative selectors for next page buttons"
    )


class LoggingSettings(BaseModel):
    """Logging configuration settings"""

    level: str = Field(default="INFO", description="Logging level")
    format: str = Field(
        default="[%(asctime)s] %(name)s.%(funcName)s:%(lineno)d %(levelname)s %(message)s",
        description="Log message format"
    )
    file_path: Optional[str] = Field(default="scraper.log", description="Log file path (None for console only)")
    max_file_size_mb: int = Field(default=10, description="Maximum log file size in MB")
    backup_count: int = Field(default=3, description="Number of backup log files to keep")


class Settings(BaseModel):
    """Main configuration container"""

    general: GeneralSettings = Field(default_factory=GeneralSettings)
    site: SiteSettings = Field(default_factory=SiteSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)


class ConfigManager:
    """Configuration manager with advanced features"""

    def __init__(self, config_path: str = "config.toml"):
        self.config_path = Path(config_path)
        self.backup_path = self.config_path.with_suffix('.toml.backup')

    def load_config(self) -> Settings:
        """
        Load configuration with fallback to defaults and environment variables

        Returns:
            Settings: Validated configuration object
        """
        if not self.config_path.exists():
            return self._create_default_config()

        try:
            # Backup current config before loading
            self._backup_config()

            # Load and parse TOML
            config_data = toml.load(self.config_path)

            # Apply environment variable overrides
            config_data = self._apply_env_overrides(config_data)

            # Validate and return
            settings = Settings(**config_data)

            # Update config file with any new fields
            self._update_config_file(settings)

            return settings

        except Exception as e:
            print(f"⚠️ Error loading config: {e}")
            print("🔄 Creating fresh configuration...")
            return self._create_default_config()

    def _create_default_config(self) -> Settings:
        """Create default configuration file"""
        print("📝 Creating default config.toml...")

        settings = Settings()

        # Convert to dict for TOML serialization
        config_dict = {
            "general": settings.general.dict(),
            "site": settings.site.dict(),
            "logging": settings.logging.dict()
        }

        # Write to file
        with open(self.config_path, "w", encoding="utf-8") as f:
            toml.dump(config_dict, f)

        print(f"✅ Created {self.config_path} with CURIA-optimized defaults")
        return settings

    def _backup_config(self):
        """Create backup of current config"""
        if self.config_path.exists():
            import shutil
            shutil.copy2(self.config_path, self.backup_path)

    def _apply_env_overrides(self, config_data: dict) -> dict:
        """Apply environment variable overrides"""
        # Example: CURIA_GENERAL_HEADLESS=false
        env_mappings = {
            'CURIA_GENERAL_HEADLESS': ('general', 'headless', bool),
            'CURIA_GENERAL_OUTPUT_DIR': ('general', 'output_dir', str),
            'CURIA_GENERAL_LANGUAGE': ('general', 'preferred_language', str),
            'CURIA_SITE_LISTING_URL': ('site', 'listing_url', str),
            'CURIA_LOGGING_LEVEL': ('logging', 'level', str),
        }

        for env_var, (section, key, type_func) in env_mappings.items():
            if env_var in os.environ:
                value = os.environ[env_var]

                # Convert boolean strings
                if type_func == bool:
                    value = value.lower() in ('true', '1', 'yes', 'on')
                else:
                    value = type_func(value)

                # Apply to config
                if section not in config_data:
                    config_data[section] = {}
                config_data[section][key] = value

        return config_data

    def _update_config_file(self, settings: Settings):
        """Update config file with any new fields from defaults"""
        try:
            current_config = toml.load(self.config_path)
            new_config = {
                "general": settings.general.dict(),
                "site": settings.site.dict(),
                "logging": settings.logging.dict()
            }

            # Check if update is needed
            if current_config != new_config:
                # Add timestamp comment
                comment = f"# Updated: {datetime.now().isoformat()}\n"

                with open(self.config_path, "w", encoding="utf-8") as f:
                    f.write(comment)
                    toml.dump(new_config, f)

                print("🔄 Configuration file updated with new fields")

        except Exception as e:
            print(f"⚠️ Warning: Could not update config file: {e}")


# Global config manager instance
config_manager = ConfigManager()

def get_settings() -> Settings:
    """Get the current configuration settings"""
    return config_manager.load_config()


if __name__ == "__main__":
    # Test configuration loading
    settings = get_settings()
    print("Configuration loaded successfully!")
    print(f"Output directory: {settings.general.output_dir}")
    print(f"Listing URL: {settings.site.listing_url}")
    print(f"Language: {settings.general.preferred_language}")