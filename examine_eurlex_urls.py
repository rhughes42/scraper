#!/usr/bin/env python
"""Test to examine actual EUR-Lex URLs"""

import asyncio
from playwright.async_api import async_playwright

async def examine_eurlex_urls():
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
            print(f'Found {len(links)} document links')

            # Get the actual URLs
            for i, link in enumerate(links):
                href = await link.get_attribute("href")
                title = await link.get_attribute("title")
                text = await link.inner_text()
                print(f'  Link {i+1}:')
                print(f'    href: {href}')
                print(f'    title: {title}')
                print(f'    text: {text}')
                print()

        except Exception as e:
            print(f'Error: {e}')
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(examine_eurlex_urls())