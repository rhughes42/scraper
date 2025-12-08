# Generalized Web Scraper Framework

A powerful, modular web scraping framework that can be adapted to scrape any website. The framework provides a solid foundation with abstract base classes, while site-specific implementations are cleanly separated into application modules.

## 🚀 What's New in Version 3.0

### Major Improvements

- **Generalized Architecture**: Complete refactor to support any website through pluggable applications
- **CLI Tool**: New command-line interface for easy interaction and automation
- **Enhanced Utilities**: Advanced performance monitoring, rate limiting, and health checks
- **Better Organization**: Clear separation between core framework and site-specific implementations
- **Extensible Design**: Easy to add new website scrapers by extending base classes

## 📁 Project Structure

```
scraper/
├── scraper/                    # Core framework
│   └── core/                   # Abstract base classes
│       ├── base_scraper.py     # Base scraper interface
│       ├── base_parser.py      # Base parser interface
│       └── base_config.py      # Base configuration interface
│
├── applications/               # Site-specific implementations
│   ├── curia/                  # CURIA scraper
│   │   ├── config.py           # CURIA configuration
│   │   ├── config.toml         # Default CURIA settings
│   │   └── parser.py           # CURIA document parser
│   │
│   └── eurlex/                 # EUR-Lex scraper
│       ├── config.py           # EUR-Lex configuration
│       ├── config.toml         # Default EUR-Lex settings
│       └── parser.py           # EUR-Lex document parser
│
├── cli/                        # Command-line interface
│   └── main.py                 # CLI application
│
├── utilities/                  # Enhanced utilities
│   ├── performance.py          # Performance monitoring
│   ├── rate_limiter.py         # Rate limiting
│   └── health_check.py         # System health checks
│
├── browser/                    # Browser management (shared)
├── storage/                    # Storage management (shared)
├── utils/                      # Core utilities (shared)
├── config/                     # Configuration management (shared)
└── parsers/                    # Legacy parsers (for compatibility)
```

## ⚡ Quick Start

### 1. Installation

```bash
# Clone the repository
git clone https://github.com/rhughes42/scraper.git
cd scraper

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
```

### 2. Using the CLI Tool

The new CLI provides a unified interface for all scraping operations:

```bash
# List available applications
python -m cli.main list-apps

# Run health check
python -m cli.main health-check

# Scrape CURIA documents
python -m cli.main scrape curia --max-docs 50 --headless

# Scrape EUR-Lex documents
python -m cli.main scrape eurlex --max-docs 100 --verbose

# Show default configuration
python -m cli.main config show curia

# Validate a configuration file
python -m cli.main config validate curia ./my-config.toml
```

### 3. Using the Legacy Interface (Still Supported)

```bash
# Run with default config
python main.py

# Run with custom settings
python main.py --max-docs 100 --headless --verbose
```

## 🎯 CLI Commands

### Scrape Command

Start scraping a website using a specific application:

```bash
python -m cli.main scrape <application> [options]

Options:
  --config, -c PATH      Configuration file path
  --max-docs, -m N       Maximum number of documents
  --headless             Run browser in headless mode
  --verbose, -v          Enable verbose logging
  --resume, -r           Resume from checkpoint
  --output-dir, -o DIR   Output directory
```

### List Applications

See all available scraper applications:

```bash
python -m cli.main list-apps
```

### Health Check

Run system health checks to ensure everything is working:

```bash
python -m cli.main health-check [--output-dir DIR]
```

### Configuration Management

Manage and validate configurations:

```bash
# Show default configuration
python -m cli.main config show <application>

# Validate a configuration file
python -m cli.main config validate <application> <config-file>
```

## 🔧 Creating a New Application

To add support for a new website, follow these steps:

### 1. Create Application Directory

```bash
mkdir -p applications/mysite
touch applications/mysite/__init__.py
```

### 2. Create Configuration Class

Create `applications/mysite/config.py`:

```python
from scraper.core.base_config import BaseConfig
from pydantic import Field
from typing import Dict, Any, List

class MysiteConfig(BaseConfig):
    """Configuration for MyWebsite scraper"""
    
    # Override defaults
    base_url: str = Field(
        default="https://mywebsite.com",
        description="Base URL"
    )
    start_url: str = Field(
        default="https://mywebsite.com/documents",
        description="Starting URL"
    )
    
    # Add site-specific fields
    document_link_selector: str = Field(
        default="a.document-link",
        description="CSS selector for document links"
    )
    
    def validate_config(self) -> bool:
        # Add validation logic
        return True
    
    def get_selectors(self) -> Dict[str, Any]:
        return {
            "document_links": self.document_link_selector,
            # ... more selectors
        }
```

### 3. Create Parser Class

Create `applications/mysite/parser.py`:

```python
from scraper.core.base_parser import BaseParser, BaseDocumentMetadata
from typing import Dict, Any, List
from bs4 import BeautifulSoup

class MysiteParser(BaseParser):
    """Parser for MyWebsite documents"""
    
    def parse_document(
        self,
        html_content: str,
        url: str,
        doc_id: str = None,
        processing_method: str = "html"
    ) -> Dict[str, Any]:
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Extract metadata
        metadata = BaseDocumentMetadata(
            doc_id=doc_id,
            url=url,
            title=soup.find('title').text if soup.find('title') else None,
            # ... extract more fields
        )
        
        return metadata.to_dict()
    
    def extract_links(self, html_content: str, base_url: str) -> List[str]:
        soup = BeautifulSoup(html_content, 'lxml')
        links = []
        
        for link in soup.find_all('a', href=True):
            url = self.normalize_url(link['href'], base_url)
            links.append(url)
        
        return links
```

### 4. Add Default Configuration

Create `applications/mysite/config.toml`:

```toml
# MyWebsite Scraper Configuration

output_dir = "./output"
checkpoint_file = "./checkpoint.json"
headless = true
throttle_delay_ms = 2000
max_documents = 100

base_url = "https://mywebsite.com"
start_url = "https://mywebsite.com/documents"

document_link_selector = "a.document-link"
next_page_selector = "a.next-page"
```

### 5. Update Application Metadata

Edit `applications/mysite/__init__.py`:

```python
"""
MyWebsite Application
====================

Site-specific implementation for scraping MyWebsite.
"""

__version__ = "1.0.0"
__description__ = "MyWebsite Document Scraper"
```

### 6. Test Your Application

```bash
# Show configuration
python -m cli.main config show mysite

# Test scraping
python -m cli.main scrape mysite --max-docs 10 --verbose
```

## 📊 Enhanced Utilities

### Performance Monitoring

Track detailed performance metrics:

```python
from utilities.performance import PerformanceMonitor

monitor = PerformanceMonitor()

# Measure operations
with monitor.measure_operation("page_load"):
    # ... load page ...
    pass

# Record events
monitor.record_page()
monitor.record_document(method="pdf", file_size=1024000)

# Get reports
summary = monitor.get_summary()
detailed = monitor.get_detailed_report()
monitor.print_summary()
```

### Rate Limiting

Control request rates to avoid overwhelming servers:

```python
from utilities.rate_limiter import RateLimiter, AdaptiveRateLimiter

# Simple rate limiter (10 requests per 60 seconds)
limiter = RateLimiter(max_requests=10, time_window=60.0)

# Acquire token before making request
await limiter.acquire()
# ... make request ...

# Adaptive rate limiter adjusts based on responses
adaptive = AdaptiveRateLimiter(initial_rate=10, time_window=60.0)
await adaptive.acquire()
# ... make request ...

# Record result to adjust rate
adaptive.record_success()  # or
adaptive.record_failure(status_code=429)  # rate limited
```

### Health Checks

Verify system health before scraping:

```python
from utilities.health_check import HealthCheck

health = HealthCheck()

# Run all checks
is_healthy = await health.run_all_checks(
    output_dir="./output",
    test_urls=["https://mywebsite.com"]
)

# Print report
health.print_report()

# Get detailed results
report = health.get_report()
```

## 🏗️ Architecture

### Core Framework

The framework provides three main base classes:

1. **BaseScraper**: Defines the scraping workflow
2. **BaseParser**: Handles content extraction
3. **BaseConfig**: Manages configuration

Site-specific implementations extend these classes to add custom behavior.

### Shared Components

- **Browser Manager**: Handles Playwright browser instances
- **Storage Manager**: Manages file I/O and checkpoints
- **Logger**: Provides structured logging with metrics

### Application Plugins

Each application is self-contained with:
- Configuration class and defaults
- Parser implementation
- Optional custom logic

## 🔐 Best Practices

### Performance

- Use `concurrent_pages=1` for stability on new sites
- Increase `throttle_delay_ms` if getting rate limited
- Enable `headless=true` to reduce memory usage
- Monitor resource usage with `PerformanceMonitor`

### Reliability

- Always run health checks before long scraping sessions
- Use rate limiting to be respectful to target servers
- Implement proper error handling in custom parsers
- Test with small `max_documents` values first

### Development

- Extend base classes rather than modifying them
- Keep applications self-contained
- Use type hints for better IDE support
- Write tests for custom parsers

## 📝 Configuration

Configuration can be provided via:

1. **TOML files**: Default `config.toml` or custom file
2. **CLI arguments**: Override specific settings
3. **Environment variables**: Use `CURIA_*` prefix

Priority: CLI arguments > Environment variables > TOML file > Defaults

## 🤝 Contributing

1. Fork the repository
2. Create a new application in `applications/`
3. Follow the structure of existing applications
4. Test thoroughly with small datasets
5. Submit a pull request

## 📞 Support

- **Documentation**: See individual application READMEs
- **Issues**: GitHub Issues
- **Questions**: GitHub Discussions

## 📄 License

See LICENSE file for details.

---

**Version**: 3.0.0  
**Author**: Ryan Hughes <ryan@graphtechnologies.xyz>  
**Updated**: December 2024
