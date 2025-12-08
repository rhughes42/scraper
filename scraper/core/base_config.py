"""
Base Configuration Module
========================

Abstract base class for site-specific configurations.
All site implementations should extend this class.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


class BaseConfig(ABC, BaseModel):
    """
    Abstract base configuration class.
    
    Site-specific implementations should extend this class
    and add their own configuration fields.
    """
    
    class Config:
        """Pydantic configuration"""
        arbitrary_types_allowed = True
        
    # Common configuration fields
    output_dir: str = Field(default="./output", description="Directory for output files")
    checkpoint_file: str = Field(default="./checkpoint.json", description="Checkpoint file path")
    headless: bool = Field(default=True, description="Run browser in headless mode")
    throttle_delay_ms: int = Field(default=2000, ge=500, le=10000, description="Delay between requests in ms")
    concurrent_pages: int = Field(default=1, ge=1, le=10, description="Number of concurrent browser pages")
    retry_attempts: int = Field(default=3, ge=1, le=10, description="Number of retry attempts")
    timeout_seconds: int = Field(default=30, ge=10, le=120, description="Page load timeout")
    max_documents: Optional[int] = Field(default=None, description="Maximum documents to process")
    
    # Site-specific fields
    base_url: str = Field(..., description="Base URL of the target site")
    start_url: str = Field(..., description="Starting URL for scraping")
    
    @abstractmethod
    def validate_config(self) -> bool:
        """
        Validate site-specific configuration.
        
        Returns:
            bool: True if configuration is valid
            
        Raises:
            ValueError: If configuration is invalid
        """
        pass
    
    @abstractmethod
    def get_selectors(self) -> Dict[str, Any]:
        """
        Get CSS selectors for the target site.
        
        Returns:
            Dict containing CSS selectors for various elements
        """
        pass
