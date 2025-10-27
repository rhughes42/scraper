"""
Browser Management Module
========================

Advanced Playwright browser management with connection pooling,
session persistence, and intelligent resource handling.

Features:
- Connection pooling for better performance
- Session persistence and recovery
- Smart retry mechanisms
- Resource monitoring and cleanup
- Browser configuration optimization
"""

import asyncio
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright
from utils.logging import ScraperLogger


class BrowserPool:
    """Manages a pool of browser instances for concurrent processing"""

    def __init__(self, max_browsers: int = 2, headless: bool = True):
        self.max_browsers = max_browsers
        self.headless = headless
        self.browsers: List[Browser] = []
        self.available_browsers: asyncio.Queue = asyncio.Queue()
        self.playwright: Optional[Playwright] = None
        self._initialized = False

    async def initialize(self):
        """Initialize the browser pool"""
        if self._initialized:
            return

        self.playwright = await async_playwright().start()

        # Create browser instances
        for i in range(self.max_browsers):
            browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-web-security',
                    '--disable-features=TranslateUI',
                    '--disable-ipc-flooding-protection',
                    '--disable-renderer-backgrounding',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-field-trial-config'
                ]
            )
            self.browsers.append(browser)
            await self.available_browsers.put(browser)

        self._initialized = True

    async def get_browser(self) -> Browser:
        """Get an available browser from the pool"""
        if not self._initialized:
            await self.initialize()
        return await self.available_browsers.get()

    async def return_browser(self, browser: Browser):
        """Return a browser to the pool"""
        await self.available_browsers.put(browser)

    async def cleanup(self):
        """Clean up all browser instances"""
        for browser in self.browsers:
            await browser.close()
        if self.playwright:
            await self.playwright.stop()
        self._initialized = False


class EnhancedBrowserManager:
    """Enhanced browser manager with advanced features"""

    def __init__(
        self,
        settings,
        logger: ScraperLogger,
        downloads_path: Optional[str] = None
    ):
        self.settings = settings
        self.logger = logger
        self.downloads_path = downloads_path
        self.browser_pool = BrowserPool(
            max_browsers=settings.general.concurrent_pages,
            headless=settings.general.headless
        )
        self.active_contexts: Dict[str, BrowserContext] = {}
        self.session_data_path = Path(settings.general.output_dir) / "browser_session.json"

    async def __aenter__(self):
        """Async context manager entry"""
        await self.browser_pool.initialize()
        self.logger.info("Browser pool initialized", pool_size=self.browser_pool.max_browsers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with cleanup"""
        await self._cleanup_contexts()
        await self.browser_pool.cleanup()
        self.logger.info("Browser resources cleaned up")

    async def create_page(self, session_id: str = "default") -> Page:
        """
        Create a new page with optimized settings

        Args:
            session_id: Identifier for session persistence

        Returns:
            Page: Configured Playwright page instance
        """
        browser = await self.browser_pool.get_browser()

        try:
            # Create or reuse context
            if session_id not in self.active_contexts:
                context = await self._create_context(browser, session_id)
                self.active_contexts[session_id] = context
            else:
                context = self.active_contexts[session_id]

            # Create page with optimizations
            page = await context.new_page()
            await self._optimize_page(page)

            self.logger.debug(f"Created page for session: {session_id}")
            return page

        finally:
            # Return browser to pool
            await self.browser_pool.return_browser(browser)

    async def _create_context(self, browser: Browser, session_id: str) -> BrowserContext:
        """Create browser context with optimized settings"""
        context_args = {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "locale": "en-US",
            "timezone_id": "UTC",
            "ignore_https_errors": True,
        }

        # Add download configuration if needed (commented out for compatibility)
        # Note: downloads_path may not be supported in all Playwright versions
        # if self.downloads_path:
        #     context_args.update({
        #         "accept_downloads": True,
        #         "downloads_path": self.downloads_path
        #     })

        # Load persistent session data if available
        session_file = self.session_data_path.parent / f"session_{session_id}.json"
        if session_file.exists():
            try:
                with open(session_file, 'r') as f:
                    session_data = json.load(f)
                    if 'cookies' in session_data:
                        context_args['storage_state'] = session_data
            except Exception as e:
                self.logger.warning(f"Could not load session data: {e}")

        context = await browser.new_context(**context_args)

        # Set up request interception for monitoring
        context.on("request", self._on_request)
        context.on("response", self._on_response)

        return context

    async def _optimize_page(self, page: Page):
        """Apply performance optimizations to page"""
        # Block unnecessary resources to speed up loading
        await page.route("**/*", self._route_handler)

        # Set timeouts
        page.set_default_timeout(self.settings.general.timeout_seconds * 1000)
        page.set_default_navigation_timeout(self.settings.general.timeout_seconds * 1000)

        # Add error handling
        page.on("pageerror", self._on_page_error)
        page.on("console", self._on_console_message)

    async def _route_handler(self, route):
        """Handle resource routing to block unnecessary content"""
        resource_type = route.request.resource_type

        # Block images, fonts, and other non-essential resources for faster loading
        if resource_type in ["image", "font", "media"]:
            await route.abort()
        else:
            await route.continue_()

    def _on_request(self, request):
        """Handle request events for monitoring"""
        self.logger.debug(f"Request: {request.method} {request.url}")

    def _on_response(self, response):
        """Handle response events for monitoring"""
        self.logger.log_network_request(
            url=response.url,
            status_code=response.status,
            response_size=len(response.headers.get('content-length', '0'))
        )

    def _on_page_error(self, error):
        """Handle page JavaScript errors"""
        self.logger.warning(f"Page error: {error}")

    def _on_console_message(self, msg):
        """Handle console messages from page"""
        if msg.type == "error":
            self.logger.debug(f"Console error: {msg.text}")

    async def save_session(self, session_id: str = "default"):
        """Save browser session state for persistence"""
        if session_id in self.active_contexts:
            try:
                context = self.active_contexts[session_id]
                storage_state = await context.storage_state()

                session_file = self.session_data_path.parent / f"session_{session_id}.json"
                session_file.parent.mkdir(parents=True, exist_ok=True)

                with open(session_file, 'w') as f:
                    json.dump(storage_state, f, indent=2)

                self.logger.debug(f"Session saved: {session_id}")

            except Exception as e:
                self.logger.error(f"Failed to save session: {e}")

    async def _cleanup_contexts(self):
        """Clean up all browser contexts"""
        for session_id, context in self.active_contexts.items():
            try:
                await self.save_session(session_id)
                await context.close()
            except Exception as e:
                self.logger.error(f"Error cleaning up context {session_id}: {e}")
        self.active_contexts.clear()


class RetryablePageManager:
    """Page manager with intelligent retry mechanisms"""

    def __init__(self, browser_manager: EnhancedBrowserManager, logger: ScraperLogger):
        self.browser_manager = browser_manager
        self.logger = logger
        self.retry_attempts = browser_manager.settings.general.retry_attempts

    async def navigate_with_retry(self, page: Page, url: str) -> bool:
        """
        Navigate to URL with retry logic

        Args:
            page: Playwright page instance
            url: URL to navigate to

        Returns:
            bool: True if navigation successful, False otherwise
        """
        for attempt in range(self.retry_attempts):
            try:
                await page.goto(url, wait_until="networkidle")
                self.logger.debug(f"Successfully navigated to {url}")
                return True

            except Exception as e:
                self.logger.warning(
                    f"Navigation attempt {attempt + 1} failed: {e}",
                    url=url,
                    attempt=attempt + 1
                )

                if attempt < self.retry_attempts - 1:
                    # Wait before retry with exponential backoff
                    wait_time = (2 ** attempt) * 1000  # ms
                    await page.wait_for_timeout(wait_time)

        self.logger.error(f"Failed to navigate to {url} after {self.retry_attempts} attempts")
        return False

    async def wait_for_selector_with_retry(
        self,
        page: Page,
        selector: str,
        timeout: int = 10000
    ) -> Optional[Any]:
        """
        Wait for selector with retry logic

        Args:
            page: Playwright page instance
            selector: CSS selector to wait for
            timeout: Timeout in milliseconds

        Returns:
            Element if found, None otherwise
        """
        for attempt in range(self.retry_attempts):
            try:
                element = await page.wait_for_selector(selector, timeout=timeout)
                return element

            except Exception as e:
                self.logger.debug(
                    f"Selector wait attempt {attempt + 1} failed: {e}",
                    selector=selector,
                    attempt=attempt + 1
                )

                if attempt < self.retry_attempts - 1:
                    await page.wait_for_timeout(1000)  # Wait 1 second before retry

        return None


def create_browser_manager(settings, logger: ScraperLogger, downloads_path: Optional[str] = None):
    """Factory function to create browser manager"""
    return EnhancedBrowserManager(settings, logger, downloads_path)


if __name__ == "__main__":
    # Test browser manager
    import asyncio
    from config.settings import get_settings
    from utils.logging import setup_logger

    async def test_browser():
        settings = get_settings()
        logger = setup_logger(settings)

        async with create_browser_manager(settings, logger) as browser_mgr:
            page = await browser_mgr.create_page("test_session")

            retry_mgr = RetryablePageManager(browser_mgr, logger)
            success = await retry_mgr.navigate_with_retry(page, "https://example.com")

            if success:
                print("Navigation successful!")
                await page.screenshot(path="test_screenshot.png")

            await page.close()

    asyncio.run(test_browser())