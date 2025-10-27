# CURIA Legal Document Scraper

A high-performance, modular web scraper for extracting legal documents from the Court of Justice of the European Union (CURIA) website. Built with modern Python async/await patterns and enterprise-grade architecture.

## 🚀 Features

### Core Functionality

- **Automated Document Discovery**: Intelligent crawling of CURIA listing pages
- **PDF Generation**: High-quality PDF generation directly from web pages
- **Metadata Extraction**: Comprehensive document metadata parsing
- **Content Analysis**: Quality assessment and language detection
- **Progress Tracking**: Session management with resume capability

### Performance & Reliability

- **Concurrent Processing**: Configurable parallel document processing
- **Intelligent Retry Logic**: Robust error handling with exponential backoff
- **Browser Pool Management**: Efficient resource utilization
- **Memory Optimization**: Streaming operations for large datasets
- **Atomic Operations**: Data integrity guarantees

### Enterprise Features

- **Modular Architecture**: Clean separation of concerns
- **Advanced Logging**: JSON structured logging with performance metrics
- **Configuration Management**: Type-safe settings with validation
- **Error Recovery**: Automatic checkpoint and resume functionality
- **Data Deduplication**: Intelligent duplicate detection

## Project Structure

<!-- markdownlint-disable MD040 -->
```
scraper/
├── main.py                    # Main orchestrator and CLI entry point
├── config/
│   ├── __init__.py
│   └── settings.py           # Configuration management with Pydantic
├── utils/
│   ├── __init__.py
│   └── logging.py           # Advanced logging infrastructure
├── browser/
│   ├── __init__.py
│   └── manager.py           # Browser pool and session management
├── parsers/
│   ├── __init__.py
│   └── curia_parser.py      # CURIA-specific document parsing
├── storage/
│   ├── __init__.py
│   └── manager.py           # Storage and checkpoint management
├── curia-scraper.ipynb      # Interactive Jupyter notebook
├── config.toml              # Default configuration file
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## ⚡ Quick Start

### 1. Installation

```bash
# Clone or download the project
cd scraper

# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
```

### 2. Configuration

Create or modify `config.toml`:

```toml
[general]
max_documents = 100
preferred_language = "EN"
headless = true
concurrent_pages = 2
throttle_delay_ms = 3000

[site]
base_url = "https://curia.europa.eu"
listing_url = "https://curia.europa.eu/juris/liste.jsf?language=en"

[logging]
level = "INFO"
json_format = true
file_output = true
```

### 3. Running the Scraper

```bash
# Basic usage
python main.py

# Advanced usage
python main.py --max-docs 100 --headless --verbose

# Resume interrupted session
python main.py --resume

# Custom configuration
python main.py --config my-config.toml
```

## 🎛️ Command Line Options

| Option | Description | Example |
|--------|-------------|---------|
| `--config, -c` | Configuration file path | `--config custom.toml` |
| `--resume, -r` | Resume from checkpoint | `--resume` |
| `--max-docs, -m` | Maximum documents to process | `--max-docs 100` |
| `--headless` | Force headless browser mode | `--headless` |
| `--verbose, -v` | Enable verbose logging | `--verbose` |

## 📊 Configuration Options

### General Settings

- `max_documents`: Maximum number of documents to process
- `preferred_language`: Document language preference (EN, FR, DE, etc.)
- `headless`: Run browser in headless mode
- `concurrent_pages`: Number of concurrent browser pages
- `throttle_delay_ms`: Delay between page requests

### Site Settings

- `base_url`: CURIA website base URL
- `listing_url`: Starting URL for document discovery
- `document_link_selector`: CSS selector for document links
- `next_page_selector`: CSS selector for pagination

### Storage Settings

- `output_dir`: Base directory for all outputs
- `pdfs_subdir`: Subdirectory for PDF files
- `data_subdir`: Subdirectory for metadata and logs
- `checkpoint_interval`: How often to save progress

### Logging Settings

- `level`: Log level (DEBUG, INFO, WARNING, ERROR)
- `json_format`: Use structured JSON logging
- `file_output`: Save logs to files
- `console_colors`: Colored console output

## 🏗️ Architecture

### Modular Design

The scraper is built with a clean modular architecture:

1. **Configuration Layer** (`config/`): Type-safe configuration management
2. **Logging Infrastructure** (`utils/`): Structured logging with metrics
3. **Browser Management** (`browser/`): Connection pooling and session handling
4. **Content Parsing** (`parsers/`): Document-specific parsing logic
5. **Storage Layer** (`storage/`): Persistent data management
6. **Orchestration** (`main.py`): High-level workflow coordination

### Data Flow

```
Config Loading → Browser Setup → Page Discovery → Document Processing → Storage
                                      ↓
                              PDF Generation ← → Metadata Extraction
                                      ↓
                              Progress Tracking ← → Error Handling
```

## 🔧 Advanced Usage

### Custom Parsers

Extend the parser system for different document types:

```python
from parsers.curia_parser import DocumentParser

class CustomParser(DocumentParser):
    def parse_document(self, html_content, url, doc_id, method):
        # Custom parsing logic
        return metadata
```

### Configuration Overrides

Runtime configuration modifications:

```python
from config.settings import get_settings

settings = get_settings("config.toml")
settings.general.max_documents = 1000
settings.general.concurrent_pages = 5
```

### Custom Storage Backends

Implement different storage strategies:

```python
from storage.manager import StorageManager

class S3StorageManager(StorageManager):
    def save_document_metadata(self, metadata, doc_index):
        # Upload to S3
        pass
```

## 📈 Performance Tuning

### Concurrency Settings

- **Single Page**: `concurrent_pages = 1` (most stable)
- **Moderate Load**: `concurrent_pages = 3-5` (balanced)
- **High Performance**: `concurrent_pages = 8+` (requires monitoring)

### Memory Optimization

- Use `headless = true` for lower memory usage
- Increase `throttle_delay_ms` to reduce server load
- Monitor browser pool size for memory leaks

### Network Optimization

- Adjust retry settings in browser manager
- Configure timeouts for slow connections
- Use connection pooling for multiple sessions

## 🛠️ Development

### Interactive Development

Use the Jupyter notebook for testing and development:

```bash
jupyter lab curia-scraper.ipynb
```

### Testing Individual Components

```python
# Test configuration
from config.settings import get_settings
settings = get_settings("config.toml")

# Test browser management
from browser.manager import create_browser_manager
async with create_browser_manager(settings, logger, "/tmp") as browser:
    page = await browser.create_page("test")
    await page.goto("https://example.com")

# Test parsing
from parsers.curia_parser import create_parser
parser = create_parser(logger)
metadata = parser.parse_document(html_content, url, doc_id, "html")
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

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📞 Support

For issues and questions:

1. Check the troubleshooting section
2. Review the logs for error details
3. Open an issue with reproduction steps
4. Include configuration and system information

---

**Version**: 2.0.0
**Author**: Ryan Hughes <ryan@graphtechnologies.xyz>
**Updated**: 2025
