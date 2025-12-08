"""
Template Parser
==============

Parser for extracting data from template website documents.

INSTRUCTIONS:
1. Update _extract_* methods to match your site's HTML structure
2. Add new extraction methods as needed for your data
3. Test with real HTML from your target site
4. Handle missing data gracefully (return None or empty values)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scraper.core.base_parser import BaseParser, BaseDocumentMetadata
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import re


class TemplateParser(BaseParser):
    """
    Parser for template website documents.
    
    This class handles extracting structured data from HTML content.
    Customize the extraction methods to match your target site.
    """
    
    def parse_document(
        self,
        html_content: str,
        url: str,
        doc_id: Optional[str] = None,
        processing_method: str = "html"
    ) -> Dict[str, Any]:
        """
        Parse HTML content and extract metadata.
        
        This is the main method called by the scraper to extract data.
        
        Args:
            html_content: Raw HTML content of the document
            url: URL of the document
            doc_id: Optional document identifier
            processing_method: How the document was obtained (html, pdf, etc.)
            
        Returns:
            Dictionary containing extracted metadata
        """
        try:
            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Extract basic metadata using helper methods
            title = self._extract_title(soup)
            author = self._extract_author(soup)
            date = self._extract_date(soup)
            keywords = self._extract_keywords(soup)
            
            # Create base metadata object
            metadata = BaseDocumentMetadata(
                doc_id=doc_id or self._extract_doc_id(url, soup),
                url=url,
                title=title,
                content_length=len(html_content),
                processing_method=processing_method
            )
            
            # Add custom fields
            metadata.custom_fields = {
                "author": author,
                "date": date,
                "keywords": keywords,
                # Add more fields as needed
                "summary": self._extract_summary(soup),
                "category": self._extract_category(soup),
                "language": self._extract_language(soup),
            }
            
            # Log success
            self.log_info(f"Successfully parsed document: {title}", doc_id=metadata.doc_id)
            
            return metadata.to_dict()
            
        except Exception as e:
            # Log error but don't fail completely
            self.log_error(f"Error parsing document: {e}", url=url, exc_info=True)
            
            # Return minimal metadata
            return BaseDocumentMetadata(
                doc_id=doc_id,
                url=url,
                processing_method=processing_method
            ).to_dict()
    
    def extract_links(self, html_content: str, base_url: str) -> List[str]:
        """
        Extract document links from HTML content.
        
        Override this method to customize link extraction for your site.
        
        Args:
            html_content: Raw HTML content
            base_url: Base URL for resolving relative links
            
        Returns:
            List of absolute URLs to documents
        """
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            links = []
            
            # Find all links (customize selector as needed)
            for link in soup.find_all('a', href=True):
                href = link['href']
                
                # Filter links (customize this logic)
                if self._is_document_link(href):
                    url = self.normalize_url(href, base_url)
                    links.append(url)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_links = []
            for link in links:
                if link not in seen:
                    seen.add(link)
                    unique_links.append(link)
            
            self.log_debug(f"Extracted {len(unique_links)} unique links")
            return unique_links
            
        except Exception as e:
            self.log_error(f"Error extracting links: {e}", exc_info=True)
            return []
    
    # ========================================================================
    # HELPER METHODS - Customize these for your site
    # ========================================================================
    
    def _is_document_link(self, href: str) -> bool:
        """
        Determine if a link points to a document.
        
        Customize this logic based on your site's URL patterns.
        """
        # Example: Check if URL contains certain keywords
        document_indicators = ['/doc/', '/document/', '/article/', '/page/']
        return any(indicator in href.lower() for indicator in document_indicators)
    
    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract document title.
        
        Try multiple selectors in order of preference.
        """
        # List of selectors to try, in order of preference
        title_selectors = [
            ('h1', {'class': 'title'}),        # Primary title
            ('h1', {'class': 'document-title'}),
            ('div', {'class': 'title'}),
            ('title', {}),                      # HTML title tag
            ('h1', {}),                         # Any h1
        ]
        
        for tag, attrs in title_selectors:
            element = soup.find(tag, attrs)
            if element:
                title = self.clean_text(element.get_text())
                if title:  # Make sure it's not empty
                    self.log_debug(f"Found title with selector: {tag}{attrs}")
                    return title
        
        self.log_warning("No title found")
        return None
    
    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract document author."""
        # Try meta tag first
        author_meta = soup.find('meta', {'name': 'author'})
        if author_meta:
            return author_meta.get('content', '').strip()
        
        # Try common class names
        author_selectors = [
            ('span', {'class': 'author'}),
            ('div', {'class': 'author'}),
            ('p', {'class': 'author'}),
            ('span', {'class': 'by-author'}),
        ]
        
        for tag, attrs in author_selectors:
            element = soup.find(tag, attrs)
            if element:
                return self.clean_text(element.get_text())
        
        return None
    
    def _extract_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract document date."""
        # Try meta tag
        date_meta = soup.find('meta', {'name': 'date'})
        if date_meta:
            return date_meta.get('content', '').strip()
        
        # Try common patterns
        date_selectors = [
            ('time', {}),                    # HTML5 time element
            ('span', {'class': 'date'}),
            ('span', {'class': 'published'}),
            ('div', {'class': 'date'}),
        ]
        
        for tag, attrs in date_selectors:
            element = soup.find(tag, attrs)
            if element:
                # Check for datetime attribute
                date = element.get('datetime', '')
                if date:
                    return date.strip()
                # Otherwise get text
                return self.clean_text(element.get_text())
        
        return None
    
    def _extract_keywords(self, soup: BeautifulSoup) -> List[str]:
        """Extract keywords/tags."""
        # Try meta keywords
        keywords_meta = soup.find('meta', {'name': 'keywords'})
        if keywords_meta:
            content = keywords_meta.get('content', '')
            return [k.strip() for k in content.split(',') if k.strip()]
        
        # Try tag elements
        tags = soup.find_all(['span', 'a'], class_=re.compile(r'tag|keyword'))
        if tags:
            return [self.clean_text(tag.get_text()) for tag in tags]
        
        return []
    
    def _extract_summary(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract document summary or description."""
        # Try meta description
        desc_meta = soup.find('meta', {'name': 'description'})
        if desc_meta:
            return desc_meta.get('content', '').strip()
        
        # Try common summary elements
        summary = soup.find('div', {'class': re.compile(r'summary|abstract|description')})
        if summary:
            return self.clean_text(summary.get_text())
        
        # Try first paragraph as fallback
        first_p = soup.find('p')
        if first_p:
            text = self.clean_text(first_p.get_text())
            # Return first 200 characters
            return text[:200] + '...' if len(text) > 200 else text
        
        return None
    
    def _extract_category(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract document category."""
        # Try breadcrumbs
        breadcrumb = soup.find('nav', {'class': 'breadcrumb'})
        if breadcrumb:
            links = breadcrumb.find_all('a')
            if links:
                return self.clean_text(links[-1].get_text())
        
        # Try category meta
        category_meta = soup.find('meta', {'name': 'category'})
        if category_meta:
            return category_meta.get('content', '').strip()
        
        return None
    
    def _extract_language(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract document language."""
        # Try html lang attribute
        html_tag = soup.find('html')
        if html_tag and html_tag.get('lang'):
            return html_tag['lang'].upper()
        
        # Try meta tag
        lang_meta = soup.find('meta', {'name': 'language'})
        if lang_meta:
            return lang_meta.get('content', '').upper()
        
        return None
    
    def _extract_doc_id(self, url: str, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract document ID from URL or HTML.
        
        Try multiple methods to find a unique identifier.
        """
        # Method 1: Extract from URL pattern
        # Example: /doc/12345 or /document?id=12345
        patterns = [
            r'/doc/(\d+)',
            r'/document/(\d+)',
            r'[?&]id=(\d+)',
            r'[?&]docid=(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        # Method 2: Look for ID in HTML
        id_meta = soup.find('meta', {'name': 'document-id'})
        if id_meta:
            return id_meta.get('content', '').strip()
        
        # Method 3: Use last part of URL path
        path = url.rstrip('/').split('/')[-1]
        # Remove file extension
        path = path.split('.')[0]
        if path:
            return path
        
        return None


# ========================================================================
# FACTORY FUNCTION
# ========================================================================

def create_parser(logger=None):
    """
    Factory function to create parser instance.
    
    This is called by the scraper framework to instantiate your parser.
    
    Args:
        logger: Optional logger instance
        
    Returns:
        TemplateParser instance
    """
    return TemplateParser(logger)


# ========================================================================
# TESTING
# ========================================================================

if __name__ == "__main__":
    """
    Test the parser with sample HTML.
    
    Run: python applications/template/parser.py
    """
    import json
    
    # Sample HTML for testing
    sample_html = """
    <html lang="en">
    <head>
        <title>Sample Document Title</title>
        <meta name="author" content="John Doe">
        <meta name="date" content="2024-12-08">
        <meta name="keywords" content="test, sample, document">
        <meta name="description" content="This is a sample document for testing.">
    </head>
    <body>
        <h1 class="title">Sample Document Title</h1>
        <span class="author">John Doe</span>
        <span class="date">December 8, 2024</span>
        <div class="content">
            <p>This is the main content of the document.</p>
        </div>
        <div>
            <a href="/doc/123" class="document-link">Related Document</a>
        </div>
    </body>
    </html>
    """
    
    # Create parser
    parser = create_parser()
    
    # Test document parsing
    print("Testing document parsing...")
    metadata = parser.parse_document(
        sample_html,
        "https://example.com/doc/123",
        doc_id="123"
    )
    print(json.dumps(metadata, indent=2))
    
    # Test link extraction
    print("\nTesting link extraction...")
    links = parser.extract_links(sample_html, "https://example.com")
    print(f"Found {len(links)} links:")
    for link in links:
        print(f"  - {link}")
