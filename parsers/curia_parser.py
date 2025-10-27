"""
CURIA Document Parser Module
===========================

Advanced HTML parsing and data extraction specifically optimized
for CURIA legal documents with intelligent content recognition.

Features:
- Structured metadata extraction
- Multi-language support
- Document type classification
- Content quality assessment
- Entity recognition for legal terms
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
class DocumentMetadata:
    """Structured container for CURIA document metadata"""

    doc_id: Optional[str] = None
    url: Optional[str] = None
    language: Optional[str] = None
    case_number: Optional[str] = None
    title: Optional[str] = None
    date_of_judgment: Optional[str] = None
    court_formation: Optional[str] = None
    procedure_type: Optional[str] = None
    parties: Optional[List[str]] = None
    subject_matter: Optional[List[str]] = None
    keywords: Optional[List[str]] = None
    celex_number: Optional[str] = None
    ecli_identifier: Optional[str] = None
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


class CuriaPatterns:
    """Regex patterns for CURIA document parsing"""

    # Case number patterns
    CASE_PATTERNS = [
        r'Case\s+([A-Z]-\d+/\d+)',  # Case C-123/2023
        r'Joined\s+Cases\s+([A-Z]-\d+/\d+(?:\s+and\s+[A-Z]-\d+/\d+)*)',  # Joined cases
        r'Case\s+([A-Z]\s*\d+/\d+)',  # Case C 123/2023 (with space)
    ]

    # Date patterns
    DATE_PATTERNS = [
        r'\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b',
        r'\b(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{4})\b',
        r'\b(\d{4})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})\b',
    ]

    # Legal identifier patterns
    CELEX_PATTERN = r'CELEX:\s*([0-9]{5}[A-Z][A-Z0-9]{4})'
    ECLI_PATTERN = r'ECLI:EU:[A-Z]:\d{4}:\d+'

    # Party patterns
    PARTY_INDICATORS = [
        r'(?:applicant|plaintiff|claimant)(?:\(s\))?[:]\s*(.+?)(?:\n|,|\.|$)',
        r'(?:defendant|respondent)(?:\(s\))?[:]\s*(.+?)(?:\n|,|\.|$)',
        r'Member\s+State(?:\(s\))?[:]\s*(.+?)(?:\n|,|\.|$)',
    ]

    # Court formation patterns
    COURT_FORMATIONS = [
        'Grand Chamber',
        'First Chamber',
        'Second Chamber',
        'Third Chamber',
        'Fourth Chamber',
        'Fifth Chamber',
        'Sixth Chamber',
        'Seventh Chamber',
        'Eighth Chamber',
        'Ninth Chamber',
        'Tenth Chamber',
        'Full Court'
    ]

    # Procedure type patterns
    PROCEDURE_TYPES = [
        'Reference for a preliminary ruling',
        'Action for annulment',
        'Action for failure to fulfil obligations',
        'Appeal',
        'Application for interim measures',
        'Action for damages',
        'Staff case'
    ]


class ContentQualityAssessor:
    """Assess the quality and completeness of extracted content"""

    @staticmethod
    def calculate_quality_score(metadata: DocumentMetadata, html_content: str) -> float:
        """
        Calculate quality score based on completeness and content indicators

        Args:
            metadata: Extracted document metadata
            html_content: Raw HTML content

        Returns:
            float: Quality score from 0.0 to 1.0
        """
        score = 0.0
        max_score = 10.0

        # Basic metadata completeness (40% of score)
        if metadata.case_number:
            score += 1.0
        if metadata.title and len(metadata.title) > 10:
            score += 1.0
        if metadata.date_of_judgment:
            score += 1.0
        if metadata.parties:
            score += 1.0

        # Language and identifier presence (30% of score)
        if metadata.language:
            score += 0.5
        if metadata.celex_number:
            score += 1.0
        if metadata.ecli_identifier:
            score += 1.0
        if metadata.court_formation:
            score += 0.5

        # Content richness (30% of score)
        content_length = len(html_content)
        if content_length > 5000:
            score += 1.0
        elif content_length > 1000:
            score += 0.5

        # Look for judgment structure indicators
        judgment_indicators = [
            'operative part', 'reasoning', 'grounds', 'order',
            'judgment', 'decision', 'ruling', 'dispositif'
        ]

        content_lower = html_content.lower()
        indicator_count = sum(1 for indicator in judgment_indicators if indicator in content_lower)
        score += min(indicator_count * 0.25, 1.0)

        return min(score / max_score, 1.0)


class CuriaDocumentParser:
    """Advanced parser for CURIA legal documents"""

    def __init__(self, logger: ScraperLogger):
        self.logger = logger
        self.patterns = CuriaPatterns()
        self.quality_assessor = ContentQualityAssessor()

    def parse_document(
        self,
        html_content: str,
        url: str,
        doc_id: Optional[str] = None,
        processing_method: str = "html_parse"
    ) -> DocumentMetadata:
        """
        Parse CURIA document HTML and extract comprehensive metadata

        Args:
            html_content: Raw HTML content
            url: Document URL
            doc_id: Optional document ID
            processing_method: How the document was processed (pdf/html_parse)

        Returns:
            DocumentMetadata: Structured document information
        """
        with self.logger.log_processing_time("document_parsing", doc_id=doc_id):
            soup = BeautifulSoup(html_content, "html.parser")

            metadata = DocumentMetadata(
                doc_id=doc_id,
                url=url,
                html_length=len(html_content),
                processing_method=processing_method
            )

            # Extract basic identifiers
            metadata.language = self._extract_language(url, soup)
            metadata.case_number = self._extract_case_number(soup)
            metadata.title = self._extract_title(soup)
            metadata.date_of_judgment = self._extract_judgment_date(soup)

            # Extract legal identifiers
            metadata.celex_number = self._extract_celex_number(soup)
            metadata.ecli_identifier = self._extract_ecli_identifier(soup)

            # Extract court and procedure information
            metadata.court_formation = self._extract_court_formation(soup)
            metadata.procedure_type = self._extract_procedure_type(soup)

            # Extract parties and subject matter
            metadata.parties = self._extract_parties(soup)
            metadata.subject_matter = self._extract_subject_matter(soup)
            metadata.keywords = self._extract_keywords(soup)

            # Calculate quality score
            metadata.content_quality_score = self.quality_assessor.calculate_quality_score(
                metadata, html_content
            )

            self.logger.debug(
                f"Document parsed with quality score: {metadata.content_quality_score:.2f}",
                doc_id=doc_id,
                case_number=metadata.case_number,
                quality_score=metadata.content_quality_score
            )

            return metadata

    def _extract_language(self, url: str, soup: BeautifulSoup) -> Optional[str]:
        """Extract document language"""
        # Try URL parameter first
        lang_match = re.search(r'doclang=([A-Z]{2})', url)
        if lang_match:
            return lang_match.group(1)

        # Try HTML lang attribute
        html_tag = soup.find('html')
        if html_tag and html_tag.get('lang'):
            lang_attr = html_tag.get('lang')
            if isinstance(lang_attr, str):
                lang = lang_attr.upper()
                if len(lang) >= 2:
                    return lang[:2]

        return None

    def _extract_case_number(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract case number using multiple strategies"""
        text_content = soup.get_text()

        # Try each pattern
        for pattern in self.patterns.CASE_PATTERNS:
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            if matches:
                return matches[0]

        # Try specific selectors
        case_selectors = [
            'span.case-number',
            'div.case-number',
            '.document-reference',
            'h1', 'h2', 'h3'
        ]

        for selector in case_selectors:
            elements = soup.select(selector)
            for element in elements:
                if element.text:
                    for pattern in self.patterns.CASE_PATTERNS:
                        match = re.search(pattern, element.text, re.IGNORECASE)
                        if match:
                            return match.group(1)

        return None

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract document title"""
        # Try multiple title sources in order of preference
        title_selectors = [
            'title',
            'h1.document-title',
            'h1.judgment-title',
            'h1',
            'h2',
            '.document-title',
            '.main-title'
        ]

        for selector in title_selectors:
            element = soup.select_one(selector)
            if element and element.text.strip():
                title = element.text.strip()
                # Clean up common prefixes
                title = re.sub(r'^(CURIA\s*-\s*|InfoCuria\s*-\s*)', '', title, flags=re.IGNORECASE)
                title = re.sub(r'\s+', ' ', title)  # Normalize whitespace

                if len(title) > 10:  # Ensure substantial title
                    return title

        return None

    def _extract_judgment_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract judgment date"""
        text_content = soup.get_text()

        # Try date patterns
        for pattern in self.patterns.DATE_PATTERNS:
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            if matches:
                # Return first reasonable date match
                for match in matches:
                    if isinstance(match, tuple):
                        # Handle different pattern groups
                        if len(match) == 3:
                            day, month, year = match
                            # Try to format consistently
                            if month.isdigit():
                                return f"{day}/{month}/{year}"
                            else:
                                return f"{day} {month} {year}"
                    else:
                        return match

        # Try specific date selectors
        date_selectors = [
            '.judgment-date',
            '.date-judgment',
            'time[datetime]'
        ]

        for selector in date_selectors:
            element = soup.select_one(selector)
            if element:
                datetime_attr = element.get('datetime')
                if datetime_attr and isinstance(datetime_attr, str):
                    return datetime_attr
                elif element.text.strip():
                    return element.text.strip()

        return None

    def _extract_celex_number(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract CELEX number"""
        text_content = soup.get_text()
        match = re.search(self.patterns.CELEX_PATTERN, text_content)
        return match.group(1) if match else None

    def _extract_ecli_identifier(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract ECLI identifier"""
        text_content = soup.get_text()
        match = re.search(self.patterns.ECLI_PATTERN, text_content)
        return match.group(0) if match else None

    def _extract_court_formation(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract court formation information"""
        text_content = soup.get_text()

        for formation in self.patterns.COURT_FORMATIONS:
            if formation.lower() in text_content.lower():
                return formation

        return None

    def _extract_procedure_type(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract procedure type"""
        text_content = soup.get_text()

        for proc_type in self.patterns.PROCEDURE_TYPES:
            if proc_type.lower() in text_content.lower():
                return proc_type

        return None

    def _extract_parties(self, soup: BeautifulSoup) -> List[str]:
        """Extract party information"""
        parties = []
        text_content = soup.get_text()

        # Try regex patterns
        for pattern in self.patterns.PARTY_INDICATORS:
            matches = re.findall(pattern, text_content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                party = match.strip()
                if party and party not in parties:
                    parties.append(party)

        # Try structured extraction from tables
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)

                    if any(keyword in label for keyword in ['party', 'applicant', 'defendant', 'member state']):
                        if value and value not in parties:
                            parties.append(value)

        return parties[:10]  # Limit to avoid overly long lists

    def _extract_subject_matter(self, soup: BeautifulSoup) -> List[str]:
        """Extract subject matter/legal areas"""
        subjects = []

        # Look for subject matter sections
        subject_indicators = [
            'subject matter', 'legal basis', 'area of law',
            'subject-matter', 'classification', 'domain'
        ]

        for indicator in subject_indicators:
            elements = soup.find_all(text=re.compile(indicator, re.IGNORECASE))
            for element in elements:
                # Look for content after the indicator
                parent = element.parent
                if parent:
                    next_sibling = parent.find_next_sibling()
                    if next_sibling:
                        subject = next_sibling.get_text(strip=True)
                        if subject and len(subject) < 200:  # Reasonable length
                            subjects.append(subject)

        return subjects[:5]  # Limit to most relevant

    def _extract_keywords(self, soup: BeautifulSoup) -> List[str]:
        """Extract keywords and legal terms"""
        keywords = []

        # Look for keywords section
        keyword_elements = soup.find_all(text=re.compile(r'keywords?', re.IGNORECASE))
        for element in keyword_elements:
            parent = element.parent
            if parent:
                next_sibling = parent.find_next_sibling()
                if next_sibling:
                    keyword_text = next_sibling.get_text(strip=True)
                    # Split by common delimiters
                    words = re.split(r'[,;|\n]+', keyword_text)
                    for word in words:
                        word = word.strip()
                        if word and len(word) > 2:
                            keywords.append(word)

        return keywords[:20]  # Limit keyword list


def create_parser(logger: ScraperLogger) -> CuriaDocumentParser:
    """Factory function to create document parser"""
    return CuriaDocumentParser(logger)


if __name__ == "__main__":
    # Test parser
    from utils.logging import setup_logger
    from config.settings import get_settings

    settings = get_settings()
    logger = setup_logger(settings)
    parser = create_parser(logger)

    # Test with sample HTML
    sample_html = """
    <html>
        <head><title>Case C-123/2023 - Test vs. Example</title></head>
        <body>
            <h1>Judgment of 15 March 2024</h1>
            <p>Case C-123/2023</p>
            <p>CELEX: 62023CJ0123</p>
            <p>ECLI:EU:C:2024:123</p>
            <p>Grand Chamber</p>
        </body>
    </html>
    """

    metadata = parser.parse_document(
        sample_html,
        "https://curia.europa.eu/juris/document/document.jsf?docid=123&doclang=EN",
        "123"
    )

    print("Parsed metadata:")
    print(json.dumps(metadata.to_dict(), indent=2))