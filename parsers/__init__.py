"""Document parsing module for CURIA scraper"""

from .curia_parser import create_parser, CuriaDocumentParser, DocumentMetadata

__all__ = ["create_parser", "CuriaDocumentParser", "DocumentMetadata"]