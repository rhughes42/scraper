"""
Core Framework Components
========================

Core abstractions and interfaces for the generalized scraper framework.
"""

from .base_scraper import BaseScraper
from .base_parser import BaseParser
from .base_config import BaseConfig

__all__ = ["BaseScraper", "BaseParser", "BaseConfig"]
