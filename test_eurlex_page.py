#!/usr/bin/env python
"""Test EUR-Lex page access and document link detection"""

import asyncio
from playwright.async_api import async_playwright

async def test_eurlex():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        # Try to navigate to the URL
        url = 'https://eur-lex.europa.eu/search.html?DTA=2024&DTS_SUBDOM=EU_CASE_LAW&DTS_DOM=EU_LAW&CASE_LAW_SUMMARY=true&type=advanced&qid=1761588435862'
        print(f'Navigating to: {url}')

        try:
            await page.goto(url, timeout=30000)
            print('Navigation successful!')

            # Check if we can find the document links
            selector = 'a[title*="html CELEX"]'
            links = await page.query_selector_all(selector)
            print(f'Found {len(links)} document links with selector: {selector}')

            # Try alternative selectors
            alt_selectors = ['a[href*="legal-content/EN/TXT/HTML"]', 'a.piwik_download[href*="HTML"]']
            for alt_sel in alt_selectors:
                alt_links = await page.query_selector_all(alt_sel)
                print(f'Found {len(alt_links)} links with alternative selector: {alt_sel}')

            # Get page title and check content
            title = await page.title()
            print(f'Page title: {title}')

            # Try broader selectors to see what's actually on the page
            all_links = await page.query_selector_all('a')
            print(f'Total links on page: {len(all_links)}')

            # Check if there are any results at all
            result_items = await page.query_selector_all('.search-result, .result, [class*="result"]')
            print(f'Found {len(result_items)} potential result items')

            # Wait a bit to see the page
            await page.wait_for_timeout(5000)

        except Exception as e:
            print(f'Error: {e}')
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_eurlex())