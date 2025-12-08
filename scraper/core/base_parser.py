"""
Base Parser Module
=================

Abstract base class for site-specific HTML parsers.
All parser implementations should extend this class.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict
from datetime import datetime


@dataclass
class BaseDocumentMetadata:
    """
    Base metadata structure for parsed documents.
    
    Site-specific implementations can extend this with additional fields.
    """
    
    doc_id: Optional[str] = None
    url: Optional[str] = None
    title: Optional[str] = None
    content_length: int = 0
    language: Optional[str] = None
    extracted_at: Optional[str] = None
    processing_method: Optional[str] = None
    custom_fields: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if self.extracted_at is None:
            self.extracted_at = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)


class BaseParser(ABC):
    """
    Abstract base parser class.
    
    Provides common parsing utilities and defines the interface
    that all site-specific parsers must implement.
    """
    
    def __init__(self, logger=None):
        """
        Initialize parser with optional logger.
        
        Args:
            logger: Logger instance for debugging and monitoring
        """
        self.logger = logger
    
    @abstractmethod
    def parse_document(
        self,
        html_content: str,
        url: str,
        doc_id: Optional[str] = None,
        processing_method: str = "html"
    ) -> Dict[str, Any]:
        """
        Parse HTML content and extract metadata.
        
        Args:
            html_content: Raw HTML content
            url: URL of the document
            doc_id: Optional document identifier
            processing_method: Method used to obtain content (html, pdf, etc.)
            
        Returns:
            Dictionary containing extracted metadata
        """
        pass
    
    @abstractmethod
    def extract_links(self, html_content: str, base_url: str) -> List[str]:
        """
        Extract relevant links from HTML content.
        
        Args:
            html_content: Raw HTML content
            base_url: Base URL for resolving relative links
            
        Returns:
            List of absolute URLs
        """
        pass
    
    def clean_text(self, text: str) -> str:
        """
        Clean and normalize text content.
        
        Args:
            text: Raw text to clean
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        # Remove extra whitespace
        text = " ".join(text.split())
        
        # Remove common artifacts
        text = text.replace("\xa0", " ")
        text = text.replace("\u200b", "")
        
        return text.strip()
    
    def normalize_url(self, url: str, base_url: str) -> str:
        """
        Convert relative URLs to absolute URLs.
        
        Args:
            url: URL to normalize (can be relative or absolute)
            base_url: Base URL for resolving relative URLs
            
        Returns:
            Absolute URL
        """
        if url.startswith("http"):
            return url
        elif url.startswith("/"):
            return f"{base_url.rstrip('/')}{url}"
        else:
            return f"{base_url.rstrip('/')}/{url}"
    
    def log_info(self, message: str, **kwargs):
        """Log info message if logger available"""
        if self.logger:
            self.logger.info(message, **kwargs)
    
    def log_debug(self, message: str, **kwargs):
        """Log debug message if logger available"""
        if self.logger:
            self.logger.debug(message, **kwargs)
    
    def log_warning(self, message: str, **kwargs):
        """Log warning message if logger available"""
        if self.logger:
            self.logger.warning(message, **kwargs)
    
    def log_error(self, message: str, **kwargs):
        """Log error message if logger available"""
        if self.logger:
            self.logger.error(message, **kwargs)
