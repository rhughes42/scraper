# Generalized Web Scraper Framework

A powerful, modular web scraping framework that can be adapted to scrape any website. Built with modern Python async/await patterns and enterprise-grade architecture, the framework provides a solid foundation with abstract base classes while site-specific implementations are cleanly separated into application modules.

## 🚀 Features

### Core Capabilities

- **Multiple Site Support**: Built-in support for CURIA and EUR-Lex, with easy extensibility for any website
- **Plugin Architecture**: Add new website scrapers without modifying core code
- **CLI Tool**: Intuitive command-line interface for all operations
- **Automated Document Discovery**: Intelligent crawling and pagination handling
- **PDF Generation**: High-quality PDF generation directly from web pages
- **Metadata Extraction**: Comprehensive document metadata parsing and storage
- **Progress Tracking**: Session management with checkpoint and resume capability

### Performance & Reliability

- **Concurrent Processing**: Configurable parallel document processing
- **Intelligent Retry Logic**: Robust error handling with exponential backoff
- **Browser Pool Management**: Efficient resource utilization with Playwright
- **Rate Limiting**: Built-in rate limiting to respect server resources
- **Performance Monitoring**: Real-time metrics and detailed performance reports
- **Health Checks**: System validation before starting scraping operations

### Enterprise Features

- **Modular Architecture**: Clean separation between core framework and applications
- **Type Safety**: Full type hints and Pydantic validation throughout
- **Advanced Logging**: Structured JSON logging with performance metrics
- **Configuration Management**: Flexible TOML-based configuration with validation
- **Error Recovery**: Automatic checkpoint and resume functionality
- **Data Deduplication**: Intelligent duplicate detection

## 📁 Project Structure

<!-- markdownlint-disable MD040 -->
```
scraper/
├── scraper/                    # Core framework
│   └── core/                   # Abstract base classes
│       ├── base_scraper.py     # Base scraper interface
│       ├── base_parser.py      # Base parser interface
│       └── base_config.py      # Base configuration interface
│
├── applications/               # Site-specific implementations
│   ├── curia/                  # CURIA legal documents scraper
│   │   ├── config.py           # CURIA configuration
│   │   ├── config.toml         # Default CURIA settings
│   │   └── parser.py           # CURIA document parser
│   │
│   ├── eurlex/                 # EUR-Lex legal documents scraper
│   │   ├── config.py           # EUR-Lex configuration
│   │   ├── config.toml         # Default EUR-Lex settings
│   │   └── parser.py           # EUR-Lex document parser
│   │
│   └── template/               # Template for creating new applications
│       ├── README.md           # Detailed template guide
│       ├── config.py           # Example configuration
│       ├── config.toml         # Example settings
│       └── parser.py           # Example parser
│
├── cli/                        # Command-line interface
│   └── main.py                 # CLI application entry point
│
├── utilities/                  # Enhanced utilities
│   ├── performance.py          # Performance monitoring and metrics
│   ├── rate_limiter.py         # Rate limiting implementations
│   └── health_check.py         # System health checks
│
├── browser/                    # Browser management (shared)
│   └── manager.py              # Playwright browser pool management
│
├── storage/                    # Storage management (shared)
│   └── manager.py              # File I/O and checkpoint management
│
├── utils/                      # Core utilities (shared)
│   └── logging.py              # Structured logging infrastructure
│
├── config/                     # Configuration management (shared)
│   └── settings.py             # Base configuration system
│
├── main.py                     # Legacy entry point (still supported)
├── config.toml                 # Default configuration file
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## ⚡ Quick Start

### 1. Installation

```bash
# Clone the repository
git clone https://github.com/rhughes42/scraper.git
cd scraper

# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
```

### 2. Using the CLI Tool (Recommended)

The CLI provides a unified interface for all scraping operations:

```bash
# List available applications
python -m cli.main list-apps

# Run system health check
python -m cli.main health-check

# Scrape CURIA legal documents
python -m cli.main scrape curia --max-docs 50 --headless

# Scrape EUR-Lex documents
python -m cli.main scrape eurlex --max-docs 100 --verbose

# Show default configuration for an application
python -m cli.main config show curia

# Validate a custom configuration file
python -m cli.main config validate curia ./my-config.toml
```

### 3. Using the Legacy Interface (Still Supported)

For backward compatibility, the original interface still works:

```bash
# Basic usage with default CURIA configuration
python main.py

# Advanced usage with options
python main.py --max-docs 100 --headless --verbose

# Resume interrupted session
python main.py --resume

# Custom configuration
python main.py --config my-config.toml
```

## 🎛️ CLI Commands Reference

### Scrape Command

Start scraping a website using a specific application:

```bash
python -m cli.main scrape <application> [options]

Options:
  --config, -c PATH      Configuration file path
  --max-docs, -m N       Maximum number of documents to process
  --headless             Run browser in headless mode
  --verbose, -v          Enable verbose logging
  --resume, -r           Resume from checkpoint
  --output-dir, -o DIR   Output directory for files
```

**Examples:**
```bash
# Scrape 50 CURIA documents with custom config
python -m cli.main scrape curia --max-docs 50 --config custom.toml

# Resume a previous EUR-Lex scraping session
python -m cli.main scrape eurlex --resume --verbose
```

### List Applications

See all available scraper applications:

```bash
python -m cli.main list-apps
```

### Health Check

Run comprehensive system health checks:

```bash
python -m cli.main health-check [--output-dir DIR]
```

This validates:
- Python dependencies
- Playwright installation
- Network connectivity
- Disk space and memory
- Output directory permissions

### Configuration Management

Manage and validate configurations:

```bash
# Display default configuration for an application
python -m cli.main config show <application>

# Validate a configuration file
python -m cli.main config validate <application> <config-file>
```

### Legacy Command Line Options

When using `python main.py`, these options are available:

| Option | Description | Example |
|--------|-------------|---------|
| `--config, -c` | Configuration file path | `--config custom.toml` |
| `--resume, -r` | Resume from checkpoint | `--resume` |
| `--max-docs, -m` | Maximum documents to process | `--max-docs 100` |
| `--headless` | Force headless browser mode | `--headless` |
| `--verbose, -v` | Enable verbose logging | `--verbose` |

## 📊 Configuration

Configuration can be provided via:

1. **TOML files**: Default `config.toml` or custom file (application-specific)
2. **CLI arguments**: Override specific settings
3. **Environment variables**: Use application-specific prefixes (e.g., `CURIA_*`)

**Priority**: CLI arguments > Environment variables > TOML file > Defaults

### Example Configuration (config.toml)

```toml
# General scraping settings
[general]
max_documents = 100
preferred_language = "EN"
headless = true
concurrent_pages = 2
throttle_delay_ms = 3000

# Site-specific settings (example for CURIA)
[site]
base_url = "https://curia.europa.eu"
listing_url = "https://curia.europa.eu/juris/liste.jsf?language=en"
document_link_selector = "a.document-link"
next_page_selector = "a.next-page"

# Storage settings
[storage]
output_dir = "./output"
pdfs_subdir = "pdfs"
data_subdir = "data"
checkpoint_interval = 10

# Logging settings
[logging]
level = "INFO"
json_format = true
file_output = true
console_colors = true
```

### Common Configuration Options

#### General Settings

- `max_documents`: Maximum number of documents to process
- `preferred_language`: Document language preference (EN, FR, DE, etc.)
- `headless`: Run browser in headless mode (recommended for production)
- `concurrent_pages`: Number of concurrent browser pages (1 = most stable)
- `throttle_delay_ms`: Delay between page requests in milliseconds

#### Site Settings

- `base_url`: Website base URL
- `start_url`: Starting URL for document discovery
- `document_link_selector`: CSS selector for document links
- `next_page_selector`: CSS selector for pagination

#### Storage Settings

- `output_dir`: Base directory for all outputs
- `pdfs_subdir`: Subdirectory for PDF files
- `data_subdir`: Subdirectory for metadata and logs
- `checkpoint_interval`: How often to save progress (number of documents)

#### Logging Settings

- `level`: Log level (DEBUG, INFO, WARNING, ERROR)
- `json_format`: Use structured JSON logging
- `file_output`: Save logs to files
- `console_colors`: Colored console output

## 🏗️ Architecture

### Framework Overview

The framework uses a three-layer architecture:

1. **Core Framework** (`scraper/core/`): Abstract base classes defining interfaces
   - `BaseScraper`: Defines the scraping workflow and lifecycle
   - `BaseParser`: Handles content extraction and link discovery
   - `BaseConfig`: Manages configuration with validation

2. **Shared Components**: Reusable infrastructure
   - **Browser Manager**: Playwright browser pool management
   - **Storage Manager**: File I/O, checkpoints, and data persistence
   - **Logger**: Structured logging with performance metrics
   - **Utilities**: Performance monitoring, rate limiting, health checks

3. **Application Plugins** (`applications/`): Site-specific implementations
   - Each application extends base classes
   - Self-contained with configuration, parser, and defaults
   - Easy to add new applications without modifying core code

### Data Flow

```
Configuration → Health Check → Browser Setup → Application Init
                                       ↓
                           Discovery Phase (find document URLs)
                                       ↓
                          Processing Phase (parallel workers)
                                       ↓
                     PDF Generation ← → Metadata Extraction
                                       ↓
                     Storage + Checkpoint ← → Performance Monitoring
```

### Design Principles

- **Separation of Concerns**: Core framework separate from applications
- **Extensibility**: Easy to add new website scrapers
- **Type Safety**: Full type hints with Pydantic validation
- **Async/Await**: Modern Python async patterns throughout
- **Resilience**: Error handling, retries, and checkpointing
- **Observability**: Comprehensive logging and metrics

## 🔧 Creating a New Application

To add support for scraping a new website, follow these steps:

### 1. Create Application Directory

```bash
mkdir -p applications/mysite
touch applications/mysite/__init__.py
```

### 2. Define Configuration Class

Create `applications/mysite/config.py`:

```python
from scraper.core.base_config import BaseConfig
from pydantic import Field
from typing import Dict, Any

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
        """Validate configuration settings"""
        return True
    
    def get_selectors(self) -> Dict[str, Any]:
        """Return CSS selectors used for scraping"""
        return {
            "document_links": self.document_link_selector,
            # Add more selectors as needed
        }
```

### 3. Implement Parser Class

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
        """Extract metadata from a document"""
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Extract metadata fields
        metadata = BaseDocumentMetadata(
            doc_id=doc_id,
            url=url,
            title=soup.find('title').text if soup.find('title') else None,
            # Add more fields as needed
        )
        
        return metadata.to_dict()
    
    def extract_links(self, html_content: str, base_url: str) -> List[str]:
        """Extract document links from listing page"""
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

### 5. Test Your Application

```bash
# Display configuration
python -m cli.main config show mysite

# Test with small sample
python -m cli.main scrape mysite --max-docs 5 --verbose

# Run full scraping
python -m cli.main scrape mysite --max-docs 100 --headless
```

For detailed guidance, see the [Developer Guide](DEVELOPER_GUIDE.md) and the fully documented [template application](applications/template/).

## 📊 Enhanced Utilities

### Performance Monitoring

Track detailed performance metrics during scraping:

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

**Metrics tracked:**
- Pages and documents processed
- Processing time per document
- Throughput (documents/minute)
- System resources (CPU, memory)
- Operation durations

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

**Features:**
- Token bucket algorithm
- Adaptive rate adjustment
- Burst handling
- Backoff on rate limits

### Health Checks

Verify system health before starting scraping operations:

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

**Checks performed:**
- Python dependencies installed
- Playwright browsers available
- Network connectivity
- Disk space and memory
- Output directory permissions

## 📈 Performance Tuning

### Concurrency Settings

Choose based on your needs and system resources:

- **Single Page**: `concurrent_pages = 1` (most stable, recommended for new applications)
- **Moderate Load**: `concurrent_pages = 3-5` (balanced performance and stability)
- **High Performance**: `concurrent_pages = 8+` (requires monitoring, higher memory usage)

### Memory Optimization

- Enable `headless = true` for lower memory usage (recommended for production)
- Increase `throttle_delay_ms` to reduce memory pressure from rapid page loading
- Monitor browser pool size with performance monitoring tools
- Close browser contexts when not needed

### Network Optimization

- Adjust `throttle_delay_ms` based on server response times
- Use rate limiting to avoid overwhelming target servers
- Configure timeout values in browser settings for slow connections
- Implement retry logic with exponential backoff for transient failures

### Best Practices

1. **Start Small**: Test with `max_documents = 10` before scaling up
2. **Monitor Resources**: Use `PerformanceMonitor` to track system usage
3. **Run Health Checks**: Always validate system health before long scraping sessions
4. **Use Checkpoints**: Enable automatic checkpointing for long-running jobs
5. **Respect Servers**: Use appropriate delays and rate limiting

## 🛠️ Development

### Interactive Development

Use the Jupyter notebook for testing and development:

```bash
jupyter lab curia-scraper.ipynb
```

### Testing Individual Components

```python
# Test configuration
from applications.curia.config import CuriaConfig
config = CuriaConfig.from_toml("applications/curia/config.toml")

# Test browser management
from browser.manager import create_browser_manager
async with create_browser_manager(config, logger, "/tmp") as browser:
    page = await browser.create_page("test")
    await page.goto("https://example.com")

# Test parsing
from applications.curia.parser import CuriaParser
parser = CuriaParser(logger)
metadata = parser.parse_document(html_content, url, doc_id, "html")
```

### Creating Unit Tests

```python
import pytest
from applications.mysite.parser import MysiteParser

def test_parser_extract_title():
    parser = MysiteParser(logger=None)
    html = '<html><head><title>Test Document</title></head></html>'
    metadata = parser.parse_document(html, "https://test.com", "test-001")
    assert metadata['title'] == 'Test Document'
```

### Code Quality

```bash
# Format code
black .

# Lint code
flake8 .

# Type checking
mypy .

# Run tests
pytest
```

## 🐛 Troubleshooting

### Common Issues

<!-- markdownlint-disable MD036 -->
**Playwright Installation**

```bash
# Reinstall browsers
playwright install --force chromium
```

**Windows Subprocess Issues**

- Use the standalone script instead of Jupyter notebook
- Ensure proper PowerShell execution policy

**Memory Issues**

- Reduce `concurrent_pages`
- Enable `headless` mode
- Increase system virtual memory

**Network Timeouts**

- Increase timeout values in browser settings
- Add delays between requests
- Check firewall/proxy settings

### Debugging

Enable verbose logging:

```bash
python main.py --verbose
```

Check logs in the `data/logs/` directory for detailed error information.

## 🤝 Contributing

We welcome contributions! Here's how you can help:

### Adding New Site Scrapers

1. Create a new application in `applications/` following the template
2. Test thoroughly with small datasets first
3. Document any site-specific quirks or requirements
4. Submit a pull request with your application

### Improving the Framework

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes to core components
4. Add tests if applicable
5. Ensure all existing tests pass
6. Submit a pull request

### Reporting Issues

1. Check existing issues first
2. Include detailed reproduction steps
3. Provide configuration and system information
4. Include relevant log excerpts

### Documentation

- Fix typos or unclear explanations
- Add examples or use cases
- Improve code comments
- Create tutorials or guides

## 📚 Additional Resources

- **[Developer Guide](DEVELOPER_GUIDE.md)**: Comprehensive guide for creating new applications
- **[Template Application](applications/template/)**: Fully documented template with examples
- **[Next Steps](NEXT_STEPS.md)**: Roadmap and future enhancements

## 📞 Support

For issues and questions:

1. **Documentation**: Check this README and the Developer Guide
2. **Template**: Review the template application for examples
3. **Logs**: Check logs in `output/data/logs/` for error details
4. **Issues**: Open a GitHub issue with reproduction steps and system info
5. **Discussions**: Use GitHub Discussions for questions and ideas

---

**Version**: 3.0.0  
**Framework**: Generalized Web Scraper  
**Applications**: CURIA, EUR-Lex  
**Author**: Ryan Hughes <ryan@graphtechnologies.xyz>  
**Updated**: December 2024
