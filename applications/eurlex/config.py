"""
EUR-Lex Configuration
====================

Configuration specific to the EUR-Lex website scraper.
"""

import sys
from pathlib import Path

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scraper.core.base_config import BaseConfig
from pydantic import Field
from typing import Dict, Any, List


class EurlexConfig(BaseConfig):
    """EUR-Lex-specific configuration"""
    
    # Override defaults for EUR-Lex
    base_url: str = Field(
        default="https://eur-lex.europa.eu",
        description="EUR-Lex base URL"
    )
    start_url: str = Field(
        default="https://eur-lex.europa.eu/search.html?DTA=2024&DTS_SUBDOM=EU_CASE_LAW&DTS_DOM=EU_LAW&CASE_LAW_SUMMARY=true&type=advanced",
        description="EUR-Lex search page URL"
    )
    
    # EUR-Lex-specific settings
    preferred_language: str = Field(
        default="EN",
        description="Preferred document language (EN, FR, DE, etc.)"
    )
    
    # CSS Selectors
    document_link_selector: str = Field(
        default="a[title*='html CELEX']",
        description="CSS selector for document links"
    )
    next_page_selector: str = Field(
        default="a[title='Next Page']",
        description="CSS selector for next page button"
    )
    alternative_link_selectors: List[str] = Field(
        default=[
            "a[href*='legal-content/EN/TXT/HTML']",
            "a.piwik_download[href*='HTML']",
        ],
        description="Alternative selectors for document links"
    )
    alternative_next_selectors: List[str] = Field(
        default=[
            "a.btn.btn-primary[title='Next Page']",
            "a[href*='page='][class*='btn']",
            "a:has-text('Next')",
        ],
        description="Alternative selectors for next page buttons"
    )
    
    document_content_selector: str = Field(
        default="body",
        description="CSS selector for document content"
    )
    start_print_button_text: str = Field(
        default="Ready to print",
        description="Text indicating document is ready for printing"
    )
    
    def validate_config(self) -> bool:
        """Validate EUR-Lex-specific configuration"""
        if "eur-lex.europa.eu" not in self.base_url:
            raise ValueError("base_url must be a EUR-Lex domain")
        
        if not self.preferred_language or len(self.preferred_language) != 2:
            raise ValueError("preferred_language must be a 2-letter code")
        
        return True
    
    def get_selectors(self) -> Dict[str, Any]:
        """Get CSS selectors for EUR-Lex site"""
        return {
            "document_links": self.document_link_selector,
            "alternative_links": self.alternative_link_selectors,
            "next_page": self.next_page_selector,
            "alternative_next": self.alternative_next_selectors,
            "content": self.document_content_selector,
        }
