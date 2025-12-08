"""
Template Configuration
=====================

Configuration for the template scraper application.

INSTRUCTIONS:
1. Replace all 'template' references with your site name
2. Update base_url and start_url with your target site
3. Customize CSS selectors to match your target site's HTML structure
4. Add any site-specific configuration fields you need
5. Implement validate_config() with your validation logic
"""

import sys
from pathlib import Path

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scraper.core.base_config import BaseConfig
from pydantic import Field, validator
from typing import Dict, Any, List, Optional


class TemplateConfig(BaseConfig):
    """
    Configuration for Template website scraper.
    
    Extends BaseConfig with site-specific settings.
    """
    
    # ========================================================================
    # REQUIRED: Update these URLs for your target site
    # ========================================================================
    
    base_url: str = Field(
        default="https://example.com",
        description="Base URL of the target website"
    )
    
    start_url: str = Field(
        default="https://example.com/documents",
        description="Starting URL for document discovery"
    )
    
    # ========================================================================
    # SITE-SPECIFIC SETTINGS
    # Add any custom settings your scraper needs
    # ========================================================================
    
    # Example: Language preference
    preferred_language: str = Field(
        default="EN",
        description="Preferred document language (EN, FR, DE, etc.)"
    )
    
    # Example: Document format
    preferred_format: str = Field(
        default="HTML",
        description="Preferred document format (HTML, PDF, etc.)"
    )
    
    # Example: Authentication (if needed)
    api_key: Optional[str] = Field(
        default=None,
        description="API key for authenticated access (optional)"
    )
    
    # ========================================================================
    # CSS SELECTORS
    # Update these to match your target site's HTML structure
    # ========================================================================
    
    document_link_selector: str = Field(
        default="a.document-link",
        description="Primary CSS selector for document links"
    )
    
    next_page_selector: str = Field(
        default="a.next-page",
        description="Primary CSS selector for next page button"
    )
    
    # Alternative selectors (fallbacks if primary fails)
    alternative_link_selectors: List[str] = Field(
        default=[
            "a[href*='/doc/']",
            "a.doc-link",
            "div.document-item a"
        ],
        description="Alternative CSS selectors for document links"
    )
    
    alternative_next_selectors: List[str] = Field(
        default=[
            "a[title*='Next']",
            "button.next",
            "a:has-text('Next')"
        ],
        description="Alternative CSS selectors for next page"
    )
    
    document_content_selector: str = Field(
        default="div.content",
        description="CSS selector for main document content"
    )
    
    # ========================================================================
    # OPTIONAL: Additional selectors for metadata extraction
    # ========================================================================
    
    title_selector: str = Field(
        default="h1.title",
        description="CSS selector for document title"
    )
    
    author_selector: str = Field(
        default="span.author",
        description="CSS selector for document author"
    )
    
    date_selector: str = Field(
        default="span.date",
        description="CSS selector for document date"
    )
    
    # ========================================================================
    # VALIDATORS
    # Add custom validation logic for your configuration
    # ========================================================================
    
    @validator('preferred_language')
    def validate_language(cls, v):
        """Ensure language code is uppercase"""
        return v.upper()
    
    @validator('base_url', 'start_url')
    def validate_urls(cls, v):
        """Ensure URLs start with http:// or https://"""
        if not v.startswith(('http://', 'https://')):
            raise ValueError(f"URL must start with http:// or https://: {v}")
        return v
    
    # ========================================================================
    # REQUIRED METHODS
    # ========================================================================
    
    def validate_config(self) -> bool:
        """
        Validate site-specific configuration.
        
        Override this method to add custom validation logic.
        For example, check that base_url matches your expected domain.
        
        Returns:
            bool: True if configuration is valid
            
        Raises:
            ValueError: If configuration is invalid
        """
        # Example validation: Check domain
        if "example.com" not in self.base_url:
            raise ValueError(
                f"base_url must be an example.com domain, got: {self.base_url}"
            )
        
        # Add more validation as needed
        if self.concurrent_pages > 5:
            raise ValueError(
                "concurrent_pages should not exceed 5 to avoid overwhelming the server"
            )
        
        return True
    
    def get_selectors(self) -> Dict[str, Any]:
        """
        Get all CSS selectors as a dictionary.
        
        This is useful for passing selectors to the scraper.
        
        Returns:
            Dictionary containing all CSS selectors
        """
        return {
            "document_links": self.document_link_selector,
            "alternative_links": self.alternative_link_selectors,
            "next_page": self.next_page_selector,
            "alternative_next": self.alternative_next_selectors,
            "content": self.document_content_selector,
            "title": self.title_selector,
            "author": self.author_selector,
            "date": self.date_selector,
        }


# ========================================================================
# USAGE EXAMPLE
# ========================================================================

if __name__ == "__main__":
    """
    Test the configuration by running this file directly:
    
        python applications/template/config.py
    """
    import json
    
    # Create config with defaults
    config = TemplateConfig()
    
    # Validate
    try:
        config.validate_config()
        print("✅ Configuration is valid!")
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        exit(1)
    
    # Display configuration
    print("\nConfiguration:")
    print(json.dumps(config.model_dump(), indent=2))
    
    # Display selectors
    print("\nSelectors:")
    print(json.dumps(config.get_selectors(), indent=2))
