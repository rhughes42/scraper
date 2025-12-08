"""
Base Scraper Module
==================

Abstract base class for site-specific web scrapers.
All scraper implementations should extend this class.
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from pathlib import Path
import asyncio


class BaseScraper(ABC):
    """
    Abstract base scraper class.
    
    Provides the core scraping workflow and defines the interface
    that all site-specific scrapers must implement.
    """
    
    def __init__(self, config, logger=None, storage=None, browser_manager=None, parser=None):
        """
        Initialize scraper with required components.
        
        Args:
            config: Configuration object (extends BaseConfig)
            logger: Logger instance
            storage: Storage manager instance
            browser_manager: Browser manager instance
            parser: Parser instance (extends BaseParser)
        """
        self.config = config
        self.logger = logger
        self.storage = storage
        self.browser_manager = browser_manager
        self.parser = parser
        
        self.session_id: Optional[str] = None
        self.processed_count = 0
    
    async def start(
        self,
        resume: bool = False,
        max_documents: Optional[int] = None
    ):
        """
        Start the scraping process.
        
        Args:
            resume: Whether to resume from checkpoint
            max_documents: Maximum documents to process (overrides config)
        """
        try:
            # Initialize session
            if self.storage:
                self.session_id = self.storage.initialize_session()
            
            # Override max documents if specified
            if max_documents is not None:
                self.config.max_documents = max_documents
            
            self.log_info(
                "🚀 Starting scraping session",
                session_id=self.session_id,
                max_documents=self.config.max_documents,
            )
            
            # Execute scraping workflow
            await self.scrape()
            
            # Finalize session
            await self.finalize()
            
        except KeyboardInterrupt:
            self.log_info("🛑 Scraping interrupted by user")
            await self.emergency_shutdown()
        except Exception as e:
            self.log_error(f"💥 Fatal error in scraping session: {e}", exc_info=True)
            await self.emergency_shutdown()
            raise
    
    @abstractmethod
    async def scrape(self):
        """
        Main scraping logic.
        
        This method should implement the site-specific scraping workflow.
        """
        pass
    
    @abstractmethod
    async def process_page(self, page, url: str) -> Dict[str, Any]:
        """
        Process a single page.
        
        Args:
            page: Browser page object
            url: URL to process
            
        Returns:
            Dictionary containing processing results
        """
        pass
    
    @abstractmethod
    async def extract_document_links(self, page) -> List[str]:
        """
        Extract document links from the current page.
        
        Args:
            page: Browser page object
            
        Returns:
            List of document URLs
        """
        pass
    
    @abstractmethod
    def should_process_url(self, url: str) -> bool:
        """
        Determine if a URL should be processed.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL should be processed, False otherwise
        """
        pass
    
    async def finalize(self):
        """Finalize scraping session"""
        self.log_info("🏁 Finalizing scraping session")
        
        if self.storage:
            self.storage.cleanup_session()
        
        if self.logger:
            self.logger.log_final_summary()
        
        self.log_info("✅ Scraping session completed successfully")
    
    async def emergency_shutdown(self):
        """Emergency shutdown procedures"""
        self.log_warning("⚠️ Performing emergency shutdown")
        
        try:
            if self.storage:
                self.storage.cleanup_session()
        except Exception as e:
            self.log_error(f"Error during emergency shutdown: {e}")
    
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
