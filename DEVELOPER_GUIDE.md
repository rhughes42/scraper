# Developer Guide: Adding New Scraper Applications

This guide walks you through creating a new scraper application for the generalized web scraper framework.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Step-by-Step Guide](#step-by-step-guide)
4. [Testing Your Application](#testing-your-application)
5. [Best Practices](#best-practices)
6. [Troubleshooting](#troubleshooting)

## Overview

The framework uses a plugin architecture where each website scraper is implemented as a separate "application". Each application consists of:

- **Configuration Class**: Defines site-specific settings
- **Parser Class**: Extracts data from HTML content
- **Default Config File**: Provides sensible defaults
- **Metadata**: Version and description information

## Prerequisites

- Python 3.8+
- Understanding of HTML/CSS selectors
- Basic knowledge of web scraping concepts
- Familiarity with the target website's structure

## Step-by-Step Guide

### 1. Create Application Directory

```bash
mkdir -p applications/mysite
cd applications/mysite
```

### 2. Create Package Metadata

Create `__init__.py`:

```python
"""
MyWebsite Application
====================

Site-specific implementation for scraping MyWebsite.
Replace this with a description of what this scraper does.
"""

__version__ = "1.0.0"
__description__ = "MyWebsite Document Scraper"
__author__ = "Your Name"
```

### 3. Create Configuration Class

Create `config.py`:

```python
"""
MyWebsite Configuration
======================

Configuration specific to MyWebsite scraper.
"""

import sys
from pathlib import Path

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scraper.core.base_config import BaseConfig
from pydantic import Field, validator
from typing import Dict, Any, List


class MysiteConfig(BaseConfig):
    """MyWebsite-specific configuration"""
    
    # Override base URL and start URL (required)
    base_url: str = Field(
        default="https://mywebsite.com",
        description="MyWebsite base URL"
    )
    start_url: str = Field(
        default="https://mywebsite.com/documents",
        description="MyWebsite document listing URL"
    )
    
    # Add site-specific settings
    preferred_format: str = Field(
        default="HTML",
        description="Preferred document format"
    )
    
    # CSS Selectors (customize for your site)
    document_link_selector: str = Field(
        default="a.document-link",
        description="CSS selector for document links"
    )
    next_page_selector: str = Field(
        default="a.next-page",
        description="CSS selector for next page button"
    )
    alternative_link_selectors: List[str] = Field(
        default=["a[href*='/doc/']", "a.doc-link"],
        description="Alternative selectors for document links"
    )
    alternative_next_selectors: List[str] = Field(
        default=["a[title*='Next']", "button.next"],
        description="Alternative selectors for next page"
    )
    document_content_selector: str = Field(
        default="div.content",
        description="CSS selector for document content"
    )
    
    # Validators (optional)
    @validator('preferred_format')
    def validate_format(cls, v):
        """Validate format is uppercase"""
        return v.upper()
    
    def validate_config(self) -> bool:
        """Validate site-specific configuration"""
        if "mywebsite.com" not in self.base_url:
            raise ValueError("base_url must be a MyWebsite domain")
        
        return True
    
    def get_selectors(self) -> Dict[str, Any]:
        """Get CSS selectors for MyWebsite"""
        return {
            "document_links": self.document_link_selector,
            "alternative_links": self.alternative_link_selectors,
            "next_page": self.next_page_selector,
            "alternative_next": self.alternative_next_selectors,
            "content": self.document_content_selector,
        }
```

### 4. Create Parser Class

Create `parser.py`:

```python
"""
MyWebsite Parser
===============

Parser for extracting data from MyWebsite documents.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scraper.core.base_parser import BaseParser, BaseDocumentMetadata
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import re


class MysiteParser(BaseParser):
    """Parser for MyWebsite documents"""
    
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
            processing_method: Method used (html, pdf, etc.)
            
        Returns:
            Dictionary containing extracted metadata
        """
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Extract basic metadata
        title = self._extract_title(soup)
        author = self._extract_author(soup)
        date = self._extract_date(soup)
        
        # Create metadata object
        metadata = BaseDocumentMetadata(
            doc_id=doc_id or self._extract_doc_id(url),
            url=url,
            title=title,
            content_length=len(html_content),
            processing_method=processing_method
        )
        
        # Add custom fields
        metadata.custom_fields = {
            "author": author,
            "date": date,
            "keywords": self._extract_keywords(soup),
            # Add more custom fields as needed
        }
        
        self.log_info(f"Parsed document: {title}", doc_id=metadata.doc_id)
        
        return metadata.to_dict()
    
    def extract_links(self, html_content: str, base_url: str) -> List[str]:
        """
        Extract document links from HTML content.
        
        Args:
            html_content: Raw HTML content
            base_url: Base URL for resolving relative links
            
        Returns:
            List of absolute URLs
        """
        soup = BeautifulSoup(html_content, 'lxml')
        links = []
        
        # Find all document links
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Filter based on pattern (customize for your site)
            if '/doc/' in href or 'document' in href.lower():
                url = self.normalize_url(href, base_url)
                links.append(url)
        
        self.log_debug(f"Extracted {len(links)} links")
        return links
    
    # Helper methods for extraction
    
    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract document title"""
        # Try multiple selectors
        title_selectors = [
            ('h1', {'class': 'title'}),
            ('title', {}),
            ('h1', {}),
        ]
        
        for tag, attrs in title_selectors:
            element = soup.find(tag, attrs)
            if element:
                return self.clean_text(element.get_text())
        
        return None
    
    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract document author"""
        author = soup.find('meta', {'name': 'author'})
        if author:
            return author.get('content', '')
        
        # Try other patterns
        author_div = soup.find('div', {'class': 'author'})
        if author_div:
            return self.clean_text(author_div.get_text())
        
        return None
    
    def _extract_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract document date"""
        date = soup.find('meta', {'name': 'date'})
        if date:
            return date.get('content', '')
        
        # Try other patterns
        date_span = soup.find('span', {'class': 'date'})
        if date_span:
            return self.clean_text(date_span.get_text())
        
        return None
    
    def _extract_keywords(self, soup: BeautifulSoup) -> List[str]:
        """Extract keywords from document"""
        keywords = soup.find('meta', {'name': 'keywords'})
        if keywords:
            content = keywords.get('content', '')
            return [k.strip() for k in content.split(',')]
        
        return []
    
    def _extract_doc_id(self, url: str) -> Optional[str]:
        """Extract document ID from URL"""
        # Customize pattern for your site
        match = re.search(r'/doc/(\d+)', url)
        if match:
            return match.group(1)
        
        # Fallback: use last part of URL
        return url.rstrip('/').split('/')[-1]


def create_parser(logger=None):
    """Factory function to create parser instance"""
    return MysiteParser(logger)
```

### 5. Create Default Configuration File

Create `config.toml`:

```toml
# MyWebsite Scraper Configuration
# ================================

# General Settings
output_dir = "./output"
checkpoint_file = "./checkpoint.json"
headless = true
throttle_delay_ms = 2000
concurrent_pages = 1
retry_attempts = 3
timeout_seconds = 30
max_documents = 100

# Site-Specific Settings
base_url = "https://mywebsite.com"
start_url = "https://mywebsite.com/documents"
preferred_format = "HTML"

# CSS Selectors
document_link_selector = "a.document-link"
next_page_selector = "a.next-page"
alternative_link_selectors = ["a[href*='/doc/']", "a.doc-link"]
alternative_next_selectors = ["a[title*='Next']", "button.next"]
document_content_selector = "div.content"
```

### 6. Create README (Optional)

Create `README.md`:

```markdown
# MyWebsite Scraper

Site-specific scraper for MyWebsite documents.

## Features

- Extracts document metadata (title, author, date)
- Downloads documents in preferred format
- Handles pagination automatically
- Respects rate limits

## Configuration

See `config.toml` for available settings.

## Usage

```bash
# Using CLI
python -m cli.main scrape mysite --max-docs 50

# Show configuration
python -m cli.main config show mysite
```

## Selectors

Update CSS selectors in `config.py` if the website structure changes.
```

## Testing Your Application

### 1. Basic Validation

```bash
# List applications (should show your app)
python -m cli.main list-apps

# Show configuration
python -m cli.main config show mysite

# Validate config file
python -m cli.main config validate mysite applications/mysite/config.toml
```

### 2. Test Scraping

Start with a small number of documents:

```bash
# Test with just 5 documents
python -m cli.main scrape mysite --max-docs 5 --verbose
```

### 3. Check Output

Verify that:
- Documents are downloaded to output directory
- Metadata is extracted correctly
- Errors are logged appropriately

### 4. Integration Testing

Create test file `applications/mysite/test_mysite.py`:

```python
import pytest
from applications.mysite.config import MysiteConfig
from applications.mysite.parser import MysiteParser


def test_config_validation():
    """Test configuration validation"""
    config = MysiteConfig()
    assert config.validate_config()


def test_parser():
    """Test parser with sample HTML"""
    parser = MysiteParser()
    
    html = """
    <html>
        <head><title>Test Document</title></head>
        <body>
            <h1 class="title">Test Title</h1>
            <div class="author">Test Author</div>
        </body>
    </html>
    """
    
    metadata = parser.parse_document(html, "https://test.com/doc/123")
    
    assert metadata['title'] == 'Test Title'
    assert 'author' in metadata['custom_fields']
```

Run tests:

```bash
pytest applications/mysite/test_mysite.py -v
```

## Best Practices

### 1. Selector Robustness

- Always provide alternative selectors
- Test selectors on multiple pages
- Handle missing elements gracefully

```python
# Good: Try multiple selectors
for selector in [primary, alternative1, alternative2]:
    element = soup.find(selector)
    if element:
        break

# Bad: Rely on single selector
element = soup.find(primary)  # May fail
```

### 2. Error Handling

```python
def parse_document(self, html_content, url, doc_id=None, processing_method="html"):
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        # ... extraction logic ...
    except Exception as e:
        self.log_error(f"Failed to parse document: {e}", url=url)
        # Return minimal metadata instead of failing
        return BaseDocumentMetadata(
            doc_id=doc_id,
            url=url,
            processing_method=processing_method
        ).to_dict()
```

### 3. Rate Limiting

Respect the target website:

```python
# In config
throttle_delay_ms = 2000  # At least 2 seconds between requests
concurrent_pages = 1      # Start with sequential processing
```

### 4. Logging

Use appropriate log levels:

```python
self.log_debug("Found 10 links")           # Verbose info
self.log_info("Processing page 5")         # Progress updates
self.log_warning("No title found")         # Potential issues
self.log_error("Failed to parse", exc_info=True)  # Errors
```

### 5. Data Validation

Validate extracted data:

```python
def _extract_date(self, soup):
    date_str = soup.find('span', {'class': 'date'}).get_text()
    
    # Validate date format
    try:
        from datetime import datetime
        datetime.fromisoformat(date_str)
        return date_str
    except ValueError:
        self.log_warning(f"Invalid date format: {date_str}")
        return None
```

## Troubleshooting

### Problem: Application Not Listed

**Solution**: Check that:
1. Application is in `applications/` directory
2. `__init__.py` exists with metadata
3. `config.py` exists

### Problem: Config Validation Fails

**Solution**: Check:
1. All required fields are present
2. Field types match Pydantic definitions
3. Custom validators pass

### Problem: Parser Returns Empty Metadata

**Solution**: Verify:
1. CSS selectors match the actual HTML structure
2. Alternative selectors are provided
3. Error handling doesn't silently fail

### Problem: Scraper Hangs or Times Out

**Solution**:
1. Increase `timeout_seconds` in config
2. Add more logging to identify where it hangs
3. Check network connectivity to target site
4. Verify selectors for pagination

## Advanced Topics

### Custom Scraper Class

For more control, create a custom scraper:

```python
# applications/mysite/scraper.py
from scraper.core.base_scraper import BaseScraper

class MysiteScraper(BaseScraper):
    async def scrape(self):
        # Custom scraping logic
        pass
    
    async def process_page(self, page, url):
        # Custom page processing
        pass
```

### Authentication

Add authentication to config:

```python
class MysiteConfig(BaseConfig):
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    api_key: Optional[str] = Field(default=None)
```

Handle in scraper:

```python
async def scrape(self):
    if self.config.username:
        await self._login()
    # ... continue scraping
```

### Dynamic Content

For JavaScript-heavy sites:

```python
async def process_page(self, page, url):
    await page.goto(url)
    
    # Wait for content to load
    await page.wait_for_selector('.document-content')
    await page.wait_for_timeout(2000)
    
    html = await page.content()
    return self.parser.parse_document(html, url)
```

## Resources

- [BeautifulSoup Documentation](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [Playwright Documentation](https://playwright.dev/python/)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [CSS Selectors Reference](https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_Selectors)

## Getting Help

1. Check existing applications in `applications/` for examples
2. Review base classes in `scraper/core/`
3. Run health checks: `python -m cli.main health-check`
4. Enable verbose logging: `--verbose` flag
