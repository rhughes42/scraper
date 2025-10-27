#!/usr/bin/env python
"""
CURIA Scraper - Optimized Modular Version
=========================================

High-performance, modular CURIA legal document scraper with advanced features:

- Modular architecture for maintainability
- Concurrent processing for speed
- Intelligent retry mechanisms
- Comprehensive error handling
- Progress tracking and resumption
- Data deduplication and integrity
- Advanced logging and monitoring

Usage:
    python main.py [--config config.toml] [--resume] [--max-docs N] [--headless]

Author: Ryan Hughes
Version: 2.0.0
"""

import asyncio
import argparse
import sys
from pathlib import Path
from typing import List, Optional

# Add local modules to path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import get_settings, ConfigManager
from utils.logging import setup_logger
from browser.manager import create_browser_manager, RetryablePageManager
from parsers.curia_parser import create_parser as create_curia_parser
from parsers.eurlex_parser import create_eurlex_parser
from storage.manager import create_storage_manager


class CuriaScraperEngine:
    """Main scraper engine with advanced orchestration"""

    def __init__(self, config_path: str = "config.toml"):
        # Load configuration
        self.config_manager = ConfigManager(config_path)
        self.settings = self.config_manager.load_config()

        # Initialize components
        self.logger = setup_logger(self.settings)

        # Choose parser based on site URL
        if "eur-lex.europa.eu" in self.settings.site.base_url:
            self.parser = create_eurlex_parser(self.logger)
            self.logger.info("Using EUR-Lex parser for document processing")
        else:
            self.parser = create_curia_parser(self.logger)
            self.logger.info("Using CURIA parser for document processing")

        self.storage = create_storage_manager(self.settings, self.logger)

        # Session tracking
        self.session_id: Optional[str] = None
        self.processed_count = 0

    async def start_scraping(
        self, resume: bool = False, max_documents: Optional[int] = None
    ):
        """
        Start the scraping process

        Args:
            resume: Whether to resume from checkpoint
            max_documents: Maximum documents to process (overrides config)
        """
        try:
            # Initialize session
            self.session_id = self.storage.initialize_session()

            # Override max documents if specified
            if max_documents is not None:
                self.settings.general.max_documents = max_documents

            self.logger.info(
                "🚀 Starting CURIA scraper session",
                session_id=self.session_id,
                config_file=self.config_manager.config_path,
                max_documents=self.settings.general.max_documents,
                headless=self.settings.general.headless,
                concurrent_pages=self.settings.general.concurrent_pages,
            )

            # Create browser manager
            async with create_browser_manager(
                self.settings, self.logger, str(self.storage.pdfs_dir)
            ) as browser_mgr:

                retry_mgr = RetryablePageManager(browser_mgr, self.logger)

                # Process documents
                await self._process_listing_pages(browser_mgr, retry_mgr)

            # Finalize session
            await self._finalize_session()

        except KeyboardInterrupt:
            self.logger.info("🛑 Scraping interrupted by user")
            await self._emergency_shutdown()
        except Exception as e:
            self.logger.error(f"💥 Fatal error in scraping session: {e}", exc_info=True)
            await self._emergency_shutdown()
            raise

    async def _process_listing_pages(self, browser_mgr, retry_mgr):
        """Process all listing pages and extract documents"""
        page = await browser_mgr.create_page("main_session")

        try:
            # Navigate to listing URL
            self.logger.info(f"🌐 Navigating to: {self.settings.site.listing_url}")

            success = await retry_mgr.navigate_with_retry(
                page, self.settings.site.listing_url
            )
            if not success:
                raise Exception(
                    f"Failed to navigate to {self.settings.site.listing_url}"
                )

            page_num = 1
            total_processed = 0

            while True:
                with self.logger.log_processing_time(
                    "page_processing", page_num=page_num
                ):
                    self.logger.info(f"📄 Processing page {page_num}")

                    # Find document links on current page
                    document_links = await self._extract_document_links(page)

                    if not document_links:
                        self.logger.warning(
                            f"No document links found on page {page_num}"
                        )
                        break

                    self.logger.log_page_processed(page_num, len(document_links))

                    # Process documents (with concurrency if enabled)
                    processed_on_page = await self._process_document_batch(
                        browser_mgr, document_links, total_processed
                    )

                    total_processed += processed_on_page
                    self.storage.update_page_progress(page_num, page.url)

                    # Check if we've hit the maximum document limit
                    if (
                        self.settings.general.max_documents
                        and total_processed >= self.settings.general.max_documents
                    ):
                        self.logger.info(
                            f"📊 Reached maximum document limit: {self.settings.general.max_documents}"
                        )
                        break

                    # Try to navigate to next page
                    if not await self._navigate_to_next_page(page, retry_mgr):
                        self.logger.info("🏁 No more pages to process")
                        break

                    page_num += 1

                    # Throttle between pages
                    await page.wait_for_timeout(self.settings.general.throttle_delay_ms)

            self.logger.info(
                f"✅ Processing completed",
                total_pages=page_num,
                total_documents=total_processed,
            )

        finally:
            await page.close()

    async def _extract_document_links(self, page) -> List[str]:
        """Extract document links from current page"""
        document_links = []

        # Try primary selector first
        primary_selector = self.settings.site.document_link_selector
        alternative_selectors = self.settings.site.alternative_link_selectors

        all_selectors = [primary_selector] + alternative_selectors

        for selector in all_selectors:
            try:
                links = await page.query_selector_all(selector)

                if links:
                    self.logger.debug(
                        f"Found {len(links)} links with selector: {selector}"
                    )

                    for link in links:
                        href = await link.get_attribute("href")
                        if href:
                            full_url = self._normalize_url(href)
                            self.logger.debug(f"Processing URL: {full_url}")

                            # Apply language filtering
                            if self._should_include_document(full_url):
                                document_links.append(full_url)
                                self.logger.debug(f"Included URL: {full_url}")
                            else:
                                self.logger.debug(f"Filtered out URL: {full_url}")

                    break  # Use first successful selector

            except Exception as e:
                self.logger.debug(f"Selector '{selector}' failed: {e}")
                continue

        # Remove duplicates while preserving order
        return self._deduplicate_urls(document_links)

    def _normalize_url(self, href: str) -> str:
        """Convert relative URLs to absolute URLs"""
        if href.startswith("http"):
            return href
        elif href.startswith("/"):
            return f"{self.settings.site.base_url}{href}"
        else:
            return f"{self.settings.site.base_url}/{href}"

    def _should_include_document(self, url: str) -> bool:
        """Check if document should be included based on language preference"""
        preferred_lang = self.settings.general.preferred_language
        self.logger.debug(f"Checking URL '{url}' against preferred language '{preferred_lang}'")

        if not preferred_lang:
            self.logger.debug("No preferred language set, including document")
            return True

        # Handle EUR-Lex URLs (language in path: /EN/, /FR/, etc.)
        if "eur-lex.europa.eu" in url:
            self.logger.debug("EUR-Lex URL detected")
            # EUR-Lex URLs have language in path like "/EN/TXT/HTML/"
            if f"/{preferred_lang}/" in url:
                self.logger.debug(f"Found preferred language /{preferred_lang}/ in URL path")
                return True
            # Check for different language in path
            import re
            lang_match = re.search(r'/([A-Z]{2})/', url)
            if lang_match and lang_match.group(1) != preferred_lang:
                self.logger.debug(f"Found different language /{lang_match.group(1)}/ in URL path")
                return False  # Different language
            self.logger.debug("No specific language found in URL path, including")
            return True  # No specific language or matches

        # Handle CURIA URLs (language as query parameter: doclang=EN)
        if f"doclang={preferred_lang}" in url:
            self.logger.debug(f"Found preferred language doclang={preferred_lang} in URL")
            return True

        # Check if URL has any language parameter
        if "doclang=" in url:
            self.logger.debug("Found different language doclang parameter in URL")
            return False  # Different language

        self.logger.debug("No language specification found, including document")
        return True  # No language specified, can be modified

    def _deduplicate_urls(self, urls: List[str]) -> List[str]:
        """Remove duplicate URLs based on document ID"""
        self.logger.debug(f"Deduplicating {len(urls)} URLs")
        seen_ids = set()
        unique_urls = []

        for url in urls:
            # Extract document ID for deduplication
            import re

            # Handle CURIA URLs with docid parameter
            doc_match = re.search(r"docid=(\d+)", url)
            if doc_match:
                doc_id = doc_match.group(1)
                self.logger.debug(f"Found CURIA docid: {doc_id}")
                if doc_id not in seen_ids:
                    seen_ids.add(doc_id)

                    # Add language parameter if needed
                    if (
                        self.settings.general.preferred_language
                        and "doclang=" not in url
                    ):
                        url += f"&doclang={self.settings.general.preferred_language}"

                    unique_urls.append(url)
                    self.logger.debug(f"Added CURIA URL: {url}")
                else:
                    self.logger.debug(f"Duplicate CURIA docid {doc_id}, skipping")
            else:
                # Handle EUR-Lex URLs with CELEX numbers
                celex_match = re.search(r"CELEX:([^&]+)", url)
                if celex_match:
                    celex_id = celex_match.group(1)
                    self.logger.debug(f"Found EUR-Lex CELEX: {celex_id}")
                    if celex_id not in seen_ids:
                        seen_ids.add(celex_id)
                        unique_urls.append(url)
                        self.logger.debug(f"Added EUR-Lex URL: {url}")
                    else:
                        self.logger.debug(f"Duplicate CELEX {celex_id}, skipping")
                else:
                    # Fallback: use full URL as identifier if no specific ID found
                    self.logger.debug(f"No specific ID found, using full URL")
                    if url not in seen_ids:
                        seen_ids.add(url)
                        unique_urls.append(url)
                        self.logger.debug(f"Added fallback URL: {url}")

        self.logger.debug(f"Deduplication complete: {len(unique_urls)} unique URLs")
        return unique_urls

    async def _process_document_batch(
        self, browser_mgr, document_links: List[str], start_index: int
    ) -> int:
        """Process a batch of documents with optional concurrency"""
        processed_count = 0

        if self.settings.general.concurrent_pages > 1:
            # Concurrent processing
            processed_count = await self._process_documents_concurrent(
                browser_mgr, document_links, start_index
            )
        else:
            # Sequential processing
            processed_count = await self._process_documents_sequential(
                browser_mgr, document_links, start_index
            )

        return processed_count

    async def _process_documents_sequential(
        self, browser_mgr, document_links: List[str], start_index: int
    ) -> int:
        """Process documents sequentially"""
        processed_count = 0

        for i, doc_link in enumerate(document_links):
            doc_index = start_index + i + 1

            # Check if already processed (resume functionality)
            doc_id = self._extract_doc_id_from_url(doc_link)
            if doc_id and self.storage.is_document_processed(doc_id):
                self.logger.debug(f"Skipping already processed document: {doc_id}")
                continue

            # Check document limit
            if (
                self.settings.general.max_documents
                and processed_count >= self.settings.general.max_documents
            ):
                break

            try:
                await self._process_single_document(browser_mgr, doc_link, doc_index)
                processed_count += 1

            except Exception as e:
                self.logger.error(
                    f"Failed to process document {doc_index}: {e}",
                    doc_index=doc_index,
                    url=doc_link,
                )
                self.storage.save_error_info(doc_index, doc_link, str(e))

        return processed_count

    async def _process_documents_concurrent(
        self, browser_mgr, document_links: List[str], start_index: int
    ) -> int:
        """Process documents concurrently"""
        # Create semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.settings.general.concurrent_pages)

        async def process_with_semaphore(doc_link: str, doc_index: int):
            async with semaphore:
                return await self._process_single_document(
                    browser_mgr, doc_link, doc_index
                )

        # Create tasks for concurrent execution
        tasks = []
        for i, doc_link in enumerate(document_links):
            doc_index = start_index + i + 1

            # Check limits and duplicates
            doc_id = self._extract_doc_id_from_url(doc_link)
            if doc_id and self.storage.is_document_processed(doc_id):
                continue

            if (
                self.settings.general.max_documents
                and len(tasks) >= self.settings.general.max_documents
            ):
                break

            task = asyncio.create_task(process_with_semaphore(doc_link, doc_index))
            tasks.append(task)

        # Execute tasks and collect results
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successful processing
        processed_count = sum(
            1 for result in results if not isinstance(result, Exception)
        )

        # Log any exceptions
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                doc_index = start_index + i + 1
                doc_link = document_links[i] if i < len(document_links) else "unknown"
                self.logger.error(
                    f"Concurrent processing failed for document {doc_index}: {result}",
                    doc_index=doc_index,
                    url=doc_link,
                )
                self.storage.save_error_info(doc_index, doc_link, str(result))

        return processed_count

    async def _process_single_document(
        self, browser_mgr, doc_link: str, doc_index: int
    ):
        """Process a single document"""
        doc_id = self._extract_doc_id_from_url(doc_link)

        with self.logger.log_processing_time("document_processing", doc_id=doc_id):
            # Create new page for this document
            page = await browser_mgr.create_page(f"doc_{doc_index}")

            try:
                retry_mgr = RetryablePageManager(browser_mgr, self.logger)

                # Navigate to document
                success = await retry_mgr.navigate_with_retry(page, doc_link)
                if not success:
                    raise Exception(f"Failed to navigate to document: {doc_link}")

                # Try to generate PDF first
                pdf_success = await self._try_generate_pdf(
                    page, doc_id or str(doc_index), doc_index
                )

                if pdf_success:
                    processing_method = "pdf_generation"
                    # For PDF, save minimal metadata
                    metadata = self.parser.parse_document(
                        await page.content(), doc_link, doc_id, processing_method
                    )
                else:
                    processing_method = "html_extraction"
                    # Full HTML parsing and metadata extraction
                    html_content = await page.content()
                    metadata = self.parser.parse_document(
                        html_content, doc_link, doc_id, processing_method
                    )

                # Save metadata
                self.storage.save_document_metadata(metadata, doc_index)

                # Log successful processing
                self.logger.log_document_processed(
                    doc_id or str(doc_index), doc_link, processing_method
                )

            finally:
                await page.close()

    async def _try_generate_pdf(self, page, doc_id: str, doc_index: int) -> bool:
        """Try to generate PDF by clicking print button or direct PDF generation"""

        # Check if this is EUR-Lex (ready to print)
        if "eur-lex.europa.eu" in self.settings.site.base_url:
            return await self._generate_eurlex_pdf(page, doc_id, doc_index)
        else:
            return await self._generate_curia_pdf(page, doc_id, doc_index)

    async def _generate_eurlex_pdf(self, page, doc_id: str, doc_index: int) -> bool:
        """Generate PDF for EUR-Lex documents (ready to print)"""
        try:
            # EUR-Lex pages are already ready for printing
            filename = f"eurlex-doc-{doc_id}.pdf"
            pdf_path = self.storage.pdfs_dir / filename

            # Wait a moment for the page to fully load
            await page.wait_for_timeout(2000)

            # Generate PDF directly
            await page.pdf(
                path=str(pdf_path),
                format="A4",
                print_background=True,
                margin={"top": "1cm", "right": "1cm", "bottom": "1cm", "left": "1cm"},
            )

            # Save PDF info
            self.storage.save_pdf_info(doc_id, pdf_path, doc_index)

            self.logger.debug(f"Generated EUR-Lex PDF: {filename}", doc_id=doc_id)
            return True

        except Exception as e:
            self.logger.debug(f"EUR-Lex PDF generation failed: {e}", doc_id=doc_id)
            return False

    async def _generate_curia_pdf(self, page, doc_id: str, doc_index: int) -> bool:
        """Generate PDF for CURIA documents (requires print button click)"""
        print_button_selectors = [
            "input[value*='Start Printing']",
            "button:has-text('Start Printing')",
            "a:has-text('Start Printing')",
            "input[type='submit'][value*='Print']",
        ]

        # Look for print button
        for selector in print_button_selectors:
            try:
                print_btn = await page.wait_for_selector(selector, timeout=5000)
                if print_btn:
                    self.logger.debug(f"Found print button: {selector}", doc_id=doc_id)

                    # Click print button
                    await print_btn.click()
                    await page.wait_for_load_state("networkidle")
                    await page.wait_for_timeout(2000)  # Wait for print view

                    # Generate PDF
                    filename = f"curia-doc-{doc_id}.pdf"
                    pdf_path = self.storage.pdfs_dir / filename

                    await page.pdf(
                        path=str(pdf_path),
                        format="A4",
                        print_background=True,
                        margin={
                            "top": "1cm",
                            "right": "1cm",
                            "bottom": "1cm",
                            "left": "1cm",
                        },
                    )

                    # Save PDF info
                    self.storage.save_pdf_info(doc_id, pdf_path, doc_index)

                    return True

            except Exception as e:
                self.logger.debug(f"Print button selector failed: {selector} - {e}")
                continue

        return False

    async def _navigate_to_next_page(self, page, retry_mgr) -> bool:
        """Try to navigate to the next page"""
        next_selectors = [
            self.settings.site.next_page_selector
        ] + self.settings.site.alternative_next_selectors

        for selector in next_selectors:
            try:
                next_btn = await retry_mgr.wait_for_selector_with_retry(
                    page, selector, 5000
                )

                if next_btn:
                    # Check if button is enabled
                    is_disabled = await next_btn.get_attribute("disabled")
                    if not is_disabled:
                        self.logger.debug(f"Clicking next page: {selector}")
                        await next_btn.click()
                        await page.wait_for_load_state("networkidle")
                        return True

            except Exception as e:
                self.logger.debug(f"Next page selector failed: {selector} - {e}")
                continue

        return False

    def _extract_doc_id_from_url(self, url: str) -> Optional[str]:
        """Extract document ID from URL"""
        import re

        match = re.search(r"docid=(\d+)", url)
        return match.group(1) if match else None

    async def _finalize_session(self):
        """Finalize scraping session"""
        self.logger.info("🏁 Finalizing scraping session")

        # Save final checkpoint
        self.storage.cleanup_session()

        # Log final metrics
        self.logger.log_final_summary()

        self.logger.info("✅ Scraping session completed successfully")

    async def _emergency_shutdown(self):
        """Emergency shutdown procedures"""
        self.logger.warning("⚠️ Performing emergency shutdown")

        try:
            if self.storage:
                self.storage.cleanup_session()
        except Exception as e:
            self.logger.error(f"Error during emergency shutdown: {e}")


def main():
    """Main entry point with CLI argument parsing"""
    parser = argparse.ArgumentParser(
        description="CURIA Legal Document Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                           # Run with default config
  python main.py --max-docs 100           # Limit to 100 documents
  python main.py --headless                # Force headless mode
  python main.py --config custom.toml     # Use custom config file
  python main.py --resume                 # Resume from checkpoint
        """,
    )

    parser.add_argument(
        "--config",
        "-c",
        default="config.toml",
        help="Configuration file path (default: config.toml)",
    )

    parser.add_argument(
        "--resume", "-r", action="store_true", help="Resume from previous checkpoint"
    )

    parser.add_argument(
        "--max-docs", "-m", type=int, help="Maximum number of documents to process"
    )

    parser.add_argument(
        "--headless", action="store_true", help="Force headless browser mode"
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Create and run scraper
    try:
        scraper = CuriaScraperEngine(args.config)

        # Apply CLI overrides
        if args.headless:
            scraper.settings.general.headless = True
        if args.verbose:
            scraper.settings.logging.level = "DEBUG"
            # Recreate logger with new level
            scraper.logger = setup_logger(scraper.settings)

        # Run the scraper
        asyncio.run(
            scraper.start_scraping(resume=args.resume, max_documents=args.max_docs)
        )

    except KeyboardInterrupt:
        print("\n🛑 Scraping interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
