"""Browser management module for CURIA scraper"""

from .manager import create_browser_manager, BrowserPool, EnhancedBrowserManager, RetryablePageManager

__all__ = ["create_browser_manager", "BrowserPool", "EnhancedBrowserManager", "RetryablePageManager"]