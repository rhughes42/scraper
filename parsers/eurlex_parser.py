"""
EUR-Lex Document Parser Module
=============================

Advanced HTML parsing and data extraction specifically optimized
for EUR-Lex legal documents with intelligent content recognition.

Features:
- CELEX number extraction
- Multi-language support
- Document type classification (Cases, Regulations, Directives, etc.)
- Content quality assessment
- Enhanced metadata extraction for EU legal documents
"""

import re
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from dataclasses import dataclass, asdict
from pathlib import Path

from bs4 import BeautifulSoup, Tag
from utils.logging import ScraperLogger


@dataclass
class EurLexDocumentMetadata:
    """Structured container for EUR-Lex document metadata"""

    doc_id: Optional[str] = None
    url: Optional[str] = None
    language: Optional[str] = None
    celex_number: Optional[str] = None
    title: Optional[str] = None
    document_type: Optional[str] = None
    date_of_document: Optional[str] = None
    date_of_publication: Optional[str] = None
    court_formation: Optional[str] = None
    procedure_type: Optional[str] = None
    parties: Optional[List[str]] = None
    subject_matter: Optional[List[str]] = None
    keywords: Optional[List[str]] = None
    legal_basis: Optional[str] = None
    case_law_directory_code: Optional[str] = None
    html_length: int = 0
    content_quality_score: float = 0.0
    extracted_at: Optional[str] = None
    processing_method: Optional[str] = None

    def __post_init__(self):
        if self.parties is None:
            self.parties = []
        if self.subject_matter is None:
            self.subject_matter = []
        if self.keywords is None:
            self.keywords = []
        if self.extracted_at is None:
            self.extracted_at = datetime.now().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)


class EurLexPatterns:
    """Regular expression patterns for EUR-Lex document parsing"""

    # CELEX number patterns
    CELEX_PATTERN = re.compile(r'CELEX:(\d{5}[A-Z]{2}\d{4})', re.IGNORECASE)
    CELEX_EXTENDED_PATTERN = re.compile(r'CELEX:(\d+[A-Z]+\d+)', re.IGNORECASE)

    # Document type patterns
    CASE_LAW_PATTERN = re.compile(r'(\d{5}CC\d{4})', re.IGNORECASE)  # Court cases
    REGULATION_PATTERN = re.compile(r'(\d{5}R\d{4})', re.IGNORECASE)  # Regulations
    DIRECTIVE_PATTERN = re.compile(r'(\d{5}L\d{4})', re.IGNORECASE)  # Directives

    # Date patterns
    DATE_PATTERN = re.compile(r'(\d{1,2}[./]\d{1,2}[./]\d{4}|\d{4}-\d{2}-\d{2})')
    DATE_FULL_PATTERN = re.compile(r'(\d{1,2}\s+\w+\s+\d{4})', re.IGNORECASE)

    # Case-specific patterns
    CASE_NUMBER_PATTERN = re.compile(r'Case\s+([CT]-\d+/\d+)', re.IGNORECASE)
    PARTIES_PATTERN = re.compile(r'(.*)\s+v\s+(.*)', re.IGNORECASE)

    # Court formation patterns
    COURT_FORMATION_PATTERN = re.compile(
        r'(Court of Justice|General Court|Civil Service Tribunal|Grand Chamber|Full Court)',
        re.IGNORECASE
    )

    # Content quality indicators
    QUALITY_INDICATORS = [
        r'judgment',
        r'court of justice',
        r'general court',
        r'legal basis',
        r'whereas',
        r'article \d+',
        r'directive \d+',
        r'regulation \d+',
        r'celex',
        r'official journal'
    ]


class EurLexContentQualityAssessor:
    """Assess content quality for EUR-Lex documents"""

    def __init__(self):
        self.quality_patterns = [re.compile(pattern, re.IGNORECASE)
                               for pattern in EurLexPatterns.QUALITY_INDICATORS]

    def assess_quality(self, html_content: str, parsed_soup: BeautifulSoup) -> float:
        """
        Assess content quality based on legal document characteristics
        Returns score between 0.0 and 1.0
        """
        score = 0.0
        total_checks = 10

        try:
            # Check 1: HTML length (minimum threshold for legal documents)
            if len(html_content) > 5000:
                score += 0.1
            elif len(html_content) > 2000:
                score += 0.05

            # Check 2: Presence of legal terminology
            text_content = parsed_soup.get_text().lower()
            pattern_matches = sum(1 for pattern in self.quality_patterns
                                if pattern.search(text_content))
            score += min(pattern_matches * 0.1, 0.3)

            # Check 3: Document structure indicators
            if parsed_soup.find('title'):
                score += 0.1

            # Check 4: CELEX number presence
            if EurLexPatterns.CELEX_PATTERN.search(html_content):
                score += 0.1

            # Check 5: Metadata elements
            meta_elements = parsed_soup.find_all('meta')
            if len(meta_elements) > 5:
                score += 0.1

            # Check 6: Content paragraphs
            paragraphs = parsed_soup.find_all('p')
            if len(paragraphs) > 10:
                score += 0.1

            # Check 7: Article/section structure
            articles = parsed_soup.find_all(['article', 'section', 'div'],
                                          class_=re.compile(r'article|section', re.I))
            if articles:
                score += 0.1

            # Check 8: Legal document specific elements
            legal_elements = parsed_soup.find_all(['span', 'div'],
                                                 class_=re.compile(r'legal|court|judgment', re.I))
            if legal_elements:
                score += 0.1

            # Check 9: Date information
            if EurLexPatterns.DATE_PATTERN.search(text_content):
                score += 0.05

            # Check 10: Language indicators
            lang_attrs = [elem.get('lang') for elem in parsed_soup.find_all(attrs={'lang': True})]
            if lang_attrs:
                score += 0.05

        except Exception:
            # If any assessment fails, return minimum viable score
            score = 0.3

        return min(score, 1.0)


class EurLexDocumentParser:
    """Advanced EUR-Lex document parser with comprehensive metadata extraction"""

    def __init__(self, logger: ScraperLogger):
        self.logger = logger
        self.quality_assessor = EurLexContentQualityAssessor()

    def parse_document(
        self,
        html_content: str,
        url: str,
        doc_id: Optional[str] = None,
        processing_method: str = "html_extraction"
    ) -> EurLexDocumentMetadata:
        """
        Parse EUR-Lex document and extract comprehensive metadata

        Args:
            html_content: Raw HTML content
            url: Document URL
            doc_id: Optional document identifier
            processing_method: How the document was processed (pdf/html)

        Returns:
            EurLexDocumentMetadata: Structured metadata
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            metadata = EurLexDocumentMetadata(
                doc_id=doc_id,
                url=url,
                html_length=len(html_content),
                processing_method=processing_method
            )

            # Extract CELEX number (primary identifier)
            metadata.celex_number = self._extract_celex_number(html_content, url)

            # Extract title
            metadata.title = self._extract_title(soup)

            # Extract language
            metadata.language = self._extract_language(soup, url)

            # Extract document type
            metadata.document_type = self._extract_document_type(metadata.celex_number, soup)

            # Extract dates
            metadata.date_of_document, metadata.date_of_publication = self._extract_dates(soup)

            # Extract case-specific information
            if metadata.document_type and 'case' in metadata.document_type.lower():
                metadata.court_formation = self._extract_court_formation(soup)
                metadata.procedure_type = self._extract_procedure_type(soup)
                metadata.parties = self._extract_parties(soup, metadata.title)

            # Extract subject matter and keywords
            metadata.subject_matter = self._extract_subject_matter(soup)
            metadata.keywords = self._extract_keywords(soup)

            # Extract legal basis
            metadata.legal_basis = self._extract_legal_basis(soup)

            # Extract case law directory code
            metadata.case_law_directory_code = self._extract_case_law_directory(soup)

            # Assess content quality
            metadata.content_quality_score = self.quality_assessor.assess_quality(
                html_content, soup
            )

            self.logger.debug(
                f"Parsed EUR-Lex document",
                celex_number=metadata.celex_number,
                title=metadata.title[:50] + "..." if metadata.title and len(metadata.title) > 50 else metadata.title,
                quality_score=metadata.content_quality_score
            )

            return metadata

        except Exception as e:
            self.logger.error(f"Error parsing EUR-Lex document: {e}", exc_info=True)
            return EurLexDocumentMetadata(
                doc_id=doc_id,
                url=url,
                html_length=len(html_content),
                processing_method=processing_method,
                content_quality_score=0.0
            )

    def _extract_celex_number(self, html_content: str, url: str) -> Optional[str]:
        """Extract CELEX number from content or URL"""
        # Try URL first
        url_match = EurLexPatterns.CELEX_PATTERN.search(url)
        if url_match:
            return url_match.group(1)

        # Try HTML content
        content_match = EurLexPatterns.CELEX_PATTERN.search(html_content)
        if content_match:
            return content_match.group(1)

        # Try extended pattern
        extended_match = EurLexPatterns.CELEX_EXTENDED_PATTERN.search(html_content)
        if extended_match:
            return extended_match.group(1)

        return None

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract document title"""
        # Try multiple title sources
        title_selectors = [
            'title',
            'h1',
            '.document-title',
            '.title',
            '[class*="title"]',
            'meta[name="title"]',
            'meta[property="og:title"]'
        ]

        for selector in title_selectors:
            if selector.startswith('meta'):
                element = soup.find('meta', attrs={'name': 'title'} if 'name' in selector else {'property': 'og:title'})
                if element and element.get('content'):
                    content = element.get('content')
                    if isinstance(content, str):
                        title = content.strip()
                        if len(title) > 10:
                            return self._clean_title(title)
            else:
                element = soup.select_one(selector)
                if element:
                    title = element.get_text().strip()
                    if len(title) > 10:
                        return self._clean_title(title)

        return None

    def _clean_title(self, title: str) -> str:
        """Clean and normalize title text"""
        # Remove extra whitespace
        title = re.sub(r'\s+', ' ', title).strip()

        # Remove common prefixes/suffixes
        title = re.sub(r'^EUR-Lex\s*-\s*', '', title, flags=re.IGNORECASE)
        title = re.sub(r'\s*-\s*EUR-Lex$', '', title, flags=re.IGNORECASE)

        return title

    def _extract_language(self, soup: BeautifulSoup, url: str) -> Optional[str]:
        """Extract document language"""
        # Try URL first
        url_lang_match = re.search(r'/([A-Z]{2})/', url)
        if url_lang_match:
            return url_lang_match.group(1)

        # Try HTML lang attribute
        html_elem = soup.find('html')
        if html_elem and html_elem.get('lang'):
            lang_attr = html_elem.get('lang')
            if isinstance(lang_attr, str) and len(lang_attr) >= 2:
                return lang_attr[:2].upper()

        # Try meta tags
        lang_meta = soup.find('meta', attrs={'name': 'language'})
        if lang_meta and lang_meta.get('content'):
            content = lang_meta.get('content')
            if isinstance(content, str) and len(content) >= 2:
                return content[:2].upper()

        return 'EN'  # Default to English

    def _extract_document_type(self, celex_number: Optional[str], soup: BeautifulSoup) -> Optional[str]:
        """Extract document type from CELEX number or content"""
        if celex_number:
            # Decode CELEX number to determine type
            if 'CC' in celex_number:
                return 'Case Law - Court Decision'
            elif 'R' in celex_number:
                return 'Regulation'
            elif 'L' in celex_number:
                return 'Directive'
            elif 'C' in celex_number:
                return 'Communication'

        # Try to extract from content
        text_content = soup.get_text().lower()
        if 'judgment' in text_content or 'court' in text_content:
            return 'Case Law'
        elif 'regulation' in text_content:
            return 'Regulation'
        elif 'directive' in text_content:
            return 'Directive'

        return None

    def _extract_dates(self, soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
        """Extract document and publication dates"""
        date_of_document = None
        date_of_publication = None

        # Look for date metadata
        date_meta = soup.find('meta', attrs={'name': 'date'})
        if date_meta and date_meta.get('content'):
            content = date_meta.get('content')
            if isinstance(content, str):
                date_of_document = content

        # Look for dates in text content
        text_content = soup.get_text()
        date_matches = EurLexPatterns.DATE_PATTERN.findall(text_content)

        if date_matches:
            # Try to identify which dates are document vs publication
            for date_match in date_matches[:2]:  # Take first two dates found
                if not date_of_document:
                    date_of_document = date_match
                elif not date_of_publication:
                    date_of_publication = date_match

        return date_of_document, date_of_publication

    def _extract_court_formation(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract court formation for case law documents"""
        text_content = soup.get_text()
        court_match = EurLexPatterns.COURT_FORMATION_PATTERN.search(text_content)
        if court_match:
            return court_match.group(1)
        return None

    def _extract_procedure_type(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract procedure type"""
        text_content = soup.get_text().lower()

        if 'preliminary ruling' in text_content:
            return 'Preliminary Ruling'
        elif 'appeal' in text_content:
            return 'Appeal'
        elif 'action for annulment' in text_content:
            return 'Action for Annulment'
        elif 'infringement' in text_content:
            return 'Infringement Procedure'

        return None

    def _extract_parties(self, soup: BeautifulSoup, title: Optional[str]) -> List[str]:
        """Extract case parties"""
        parties = []

        # Try to extract from title first
        if title:
            parties_match = EurLexPatterns.PARTIES_PATTERN.search(title)
            if parties_match:
                parties = [parties_match.group(1).strip(), parties_match.group(2).strip()]

        # Look for party information in content
        if not parties:
            text_content = soup.get_text()
            # Look for common party patterns
            party_patterns = [
                r'Applicant[:\s]+([^.]+)',
                r'Defendant[:\s]+([^.]+)',
                r'Member State[:\s]+([^.]+)'
            ]

            for pattern in party_patterns:
                match = re.search(pattern, text_content, re.IGNORECASE)
                if match and match.group(1).strip() not in parties:
                    parties.append(match.group(1).strip())

        return parties

    def _extract_subject_matter(self, soup: BeautifulSoup) -> List[str]:
        """Extract subject matter/topics"""
        subjects = []

        # Look for subject matter in meta tags
        subject_meta = soup.find('meta', attrs={'name': 'subject'})
        if subject_meta and subject_meta.get('content'):
            subjects.append(subject_meta.get('content'))

        # Look for keywords in meta tags
        keywords_meta = soup.find('meta', attrs={'name': 'keywords'})
        if keywords_meta and keywords_meta.get('content'):
            content = keywords_meta.get('content')
            if isinstance(content, str):
                keywords = [k.strip() for k in content.split(',')]
                subjects.extend(keywords)

        return subjects

    def _extract_keywords(self, soup: BeautifulSoup) -> List[str]:
        """Extract keywords and legal terms"""
        keywords = []

        text_content = soup.get_text().lower()

        # Common EU legal keywords
        legal_keywords = [
            'fundamental rights', 'internal market', 'free movement',
            'competition law', 'state aid', 'preliminary ruling',
            'direct effect', 'supremacy', 'proportionality',
            'subsidiarity', 'legal basis', 'institutional balance'
        ]

        for keyword in legal_keywords:
            if keyword in text_content:
                keywords.append(keyword)

        return keywords

    def _extract_legal_basis(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract legal basis information"""
        text_content = soup.get_text()

        # Look for article references
        article_pattern = re.compile(r'Article\s+\d+[a-z]?\s+[A-Z]+', re.IGNORECASE)
        article_match = article_pattern.search(text_content)
        if article_match:
            return article_match.group(0)

        # Look for treaty references
        treaty_pattern = re.compile(r'Treaty\s+on\s+[^.]+', re.IGNORECASE)
        treaty_match = treaty_pattern.search(text_content)
        if treaty_match:
            return treaty_match.group(0)

        return None

    def _extract_case_law_directory(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract case law directory code if available"""
        # Look for directory codes in meta tags or content
        text_content = soup.get_text()

        # Common directory code patterns
        directory_pattern = re.compile(r'Directory\s+code[:\s]+([^\n.]+)', re.IGNORECASE)
        directory_match = directory_pattern.search(text_content)
        if directory_match:
            return directory_match.group(1).strip()

        return None


def create_eurlex_parser(logger: ScraperLogger) -> EurLexDocumentParser:
    """Factory function to create EUR-Lex parser instance"""
    return EurLexDocumentParser(logger)