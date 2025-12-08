"""
CURIA Configuration
==================

Configuration specific to the CURIA website scraper.
"""

import sys
from pathlib import Path

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scraper.core.base_config import BaseConfig
from pydantic import Field
from typing import Dict, Any, List


class CuriaConfig(BaseConfig):
    """CURIA-specific configuration"""
    
    # Override defaults for CURIA
    base_url: str = Field(
        default="https://curia.europa.eu",
        description="CURIA base URL"
    )
    start_url: str = Field(
        default="https://curia.europa.eu/juris/recherche.jsf?language=en",
        description="CURIA search/listing page URL"
    )
    
    # CURIA-specific settings
    preferred_language: str = Field(
        default="EN",
        description="Preferred document language (EN, FR, DE, etc.)"
    )
    
    # CSS Selectors
    document_link_selector: str = Field(
        default="div#docHtml a[href*='document.jsf']",
        description="CSS selector for document links"
    )
    next_page_selector: str = Field(
        default="a[title='Next Page']",
        description="CSS selector for next page button"
    )
    alternative_link_selectors: List[str] = Field(
        default=["a[href*='document.jsf']", "a[href*='docid=']"],
        description="Alternative selectors for document links"
    )
    alternative_next_selectors: List[str] = Field(
        default=["a[title*='Next']", "a:has-text('Next')", "a:has-text('»')"],
        description="Alternative selectors for next page buttons"
    )
    
    document_content_selector: str = Field(
        default="body",
        description="CSS selector for document content"
    )
    start_print_button_text: str = Field(
        default="Start Printing",
        description="Text of the print button"
    )
    
    def validate_config(self) -> bool:
        """Validate CURIA-specific configuration"""
        if "curia.europa.eu" not in self.base_url:
            raise ValueError("base_url must be a CURIA domain")
        
        if not self.preferred_language or len(self.preferred_language) != 2:
            raise ValueError("preferred_language must be a 2-letter code")
        
        return True
    
    def get_selectors(self) -> Dict[str, Any]:
        """Get CSS selectors for CURIA site"""
        return {
            "document_links": self.document_link_selector,
            "alternative_links": self.alternative_link_selectors,
            "next_page": self.next_page_selector,
            "alternative_next": self.alternative_next_selectors,
            "content": self.document_content_selector,
        }
