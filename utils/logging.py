"""
Logging Configuration Module
===========================

Advanced logging setup with structured output, file rotation,
and performance monitoring for the CURIA scraper.

Features:
- Structured JSON logging for production
- File rotation with size limits
- Performance metrics tracking
- Error aggregation and reporting
- Debug mode with detailed tracing
"""

import logging
import logging.handlers
import json
import time
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from contextlib import contextmanager
from dataclasses import dataclass, field


@dataclass
class PerformanceMetrics:
    """Container for performance tracking data"""

    start_time: float = field(default_factory=time.time)
    pages_processed: int = 0
    documents_processed: int = 0
    pdfs_generated: int = 0
    html_fallbacks: int = 0
    errors: int = 0
    network_requests: int = 0
    total_bytes_downloaded: int = 0

    def get_duration(self) -> float:
        """Get total runtime in seconds"""
        return time.time() - self.start_time

    def get_docs_per_minute(self) -> float:
        """Calculate documents processed per minute"""
        duration_minutes = self.get_duration() / 60
        return self.documents_processed / duration_minutes if duration_minutes > 0 else 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary"""
        return {
            'duration_seconds': round(self.get_duration(), 2),
            'pages_processed': self.pages_processed,
            'documents_processed': self.documents_processed,
            'pdfs_generated': self.pdfs_generated,
            'html_fallbacks': self.html_fallbacks,
            'errors': self.errors,
            'network_requests': self.network_requests,
            'total_bytes_downloaded': self.total_bytes_downloaded,
            'docs_per_minute': round(self.get_docs_per_minute(), 2),
            'success_rate': round((self.documents_processed - self.errors) / max(self.documents_processed, 1) * 100, 2)
        }


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging"""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'message': record.getMessage(),
        }

        # Add extra fields if present
        if hasattr(record, 'doc_id'):
            log_entry['doc_id'] = getattr(record, 'doc_id')
        if hasattr(record, 'page_num'):
            log_entry['page_num'] = getattr(record, 'page_num')
        if hasattr(record, 'processing_time'):
            log_entry['processing_time'] = getattr(record, 'processing_time')
        if hasattr(record, 'url'):
            log_entry['url'] = getattr(record, 'url')

        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)

        return json.dumps(log_entry, ensure_ascii=False)


class ColoredFormatter(logging.Formatter):
    """Colored console formatter for better readability"""

    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'

    def format(self, record: logging.LogRecord) -> str:
        """Format with colors for console output"""
        color = self.COLORS.get(record.levelname, '')
        reset = self.RESET

        # Add color to level name
        original_levelname = record.levelname
        record.levelname = f"{color}{record.levelname}{reset}"

        # Format the message
        formatted = super().format(record)

        # Restore original level name
        record.levelname = original_levelname

        return formatted


class ScraperLogger:
    """Advanced logger for CURIA scraper with metrics tracking"""

    def __init__(self, settings):
        self.settings = settings
        self.metrics = PerformanceMetrics()
        self.logger = logging.getLogger("curia_scraper")
        self.setup_logging()

    def setup_logging(self):
        """Configure logging with multiple handlers"""
        # Clear existing handlers
        self.logger.handlers.clear()

        # Set logging level
        level = getattr(logging, self.settings.logging.level.upper(), logging.INFO)
        self.logger.setLevel(level)

        # Console handler with colors
        console_handler = logging.StreamHandler()
        console_formatter = ColoredFormatter(
            '[%(asctime)s] %(name)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(level)
        self.logger.addHandler(console_handler)

        # File handler with JSON formatting (if file path specified)
        if self.settings.logging.file_path:
            log_path = Path(self.settings.logging.file_path)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            # Rotating file handler
            file_handler = logging.handlers.RotatingFileHandler(
                log_path,
                maxBytes=self.settings.logging.max_file_size_mb * 1024 * 1024,
                backupCount=self.settings.logging.backup_count,
                encoding='utf-8'
            )

            file_formatter = JSONFormatter()
            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(logging.DEBUG)  # Log everything to file
            self.logger.addHandler(file_handler)

        # Error file handler for aggregating errors
        error_log_path = Path(self.settings.general.output_dir) / "errors.log"
        error_log_path.parent.mkdir(parents=True, exist_ok=True)

        error_handler = logging.FileHandler(error_log_path, encoding='utf-8')
        error_handler.setLevel(logging.ERROR)
        error_formatter = JSONFormatter()
        error_handler.setFormatter(error_formatter)
        self.logger.addHandler(error_handler)

    def info(self, message: str, **kwargs):
        """Log info message with optional extra fields"""
        # Separate logging kwargs from extra fields
        logging_kwargs = {k: v for k, v in kwargs.items() if k in ['exc_info', 'stack_info', 'stacklevel']}
        extra_kwargs = {k: v for k, v in kwargs.items() if k not in ['exc_info', 'stack_info', 'stacklevel']}
        self.logger.info(message, extra=extra_kwargs, **logging_kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message with optional extra fields"""
        # Separate logging kwargs from extra fields
        logging_kwargs = {k: v for k, v in kwargs.items() if k in ['exc_info', 'stack_info', 'stacklevel']}
        extra_kwargs = {k: v for k, v in kwargs.items() if k not in ['exc_info', 'stack_info', 'stacklevel']}
        self.logger.warning(message, extra=extra_kwargs, **logging_kwargs)

    def error(self, message: str, **kwargs):
        """Log error message and increment error counter"""
        self.metrics.errors += 1
        # Separate logging kwargs from extra fields
        logging_kwargs = {k: v for k, v in kwargs.items() if k in ['exc_info', 'stack_info', 'stacklevel']}
        extra_kwargs = {k: v for k, v in kwargs.items() if k not in ['exc_info', 'stack_info', 'stacklevel']}
        self.logger.error(message, extra=extra_kwargs, **logging_kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug message with optional extra fields"""
        # Separate logging kwargs from extra fields
        logging_kwargs = {k: v for k, v in kwargs.items() if k in ['exc_info', 'stack_info', 'stacklevel']}
        extra_kwargs = {k: v for k, v in kwargs.items() if k not in ['exc_info', 'stack_info', 'stacklevel']}
        self.logger.debug(message, extra=extra_kwargs, **logging_kwargs)

    @contextmanager
    def log_processing_time(self, operation: str, **extra_fields):
        """Context manager to log operation processing time"""
        start_time = time.time()
        try:
            yield
        finally:
            processing_time = time.time() - start_time
            self.info(
                f"Operation '{operation}' completed",
                processing_time=round(processing_time, 3),
                **extra_fields
            )

    def log_page_processed(self, page_num: int, doc_count: int):
        """Log page processing completion"""
        self.metrics.pages_processed += 1
        self.info(
            f"Page {page_num} processed",
            page_num=page_num,
            documents_found=doc_count,
            total_pages=self.metrics.pages_processed
        )

    def log_document_processed(self, doc_id: str, url: str, method: str, file_size: Optional[int] = None):
        """Log document processing completion"""
        self.metrics.documents_processed += 1

        if method == "pdf":
            self.metrics.pdfs_generated += 1
        elif method == "html":
            self.metrics.html_fallbacks += 1

        if file_size:
            self.metrics.total_bytes_downloaded += file_size

        self.info(
            f"Document processed: {doc_id}",
            doc_id=doc_id,
            url=url,
            method=method,
            file_size=file_size,
            total_processed=self.metrics.documents_processed
        )

    def log_network_request(self, url: str, status_code: int, response_size: int):
        """Log network request details"""
        self.metrics.network_requests += 1
        self.debug(
            f"Network request completed",
            url=url,
            status_code=status_code,
            response_size=response_size
        )

    def log_final_summary(self):
        """Log final processing summary with metrics"""
        metrics = self.metrics.to_dict()

        self.info("🎉 Scraping session completed", **metrics)

        # Also write summary to separate file
        summary_path = Path(self.settings.general.output_dir) / "scraping_summary.json"
        summary_data = {
            'session_end': datetime.now().isoformat(),
            'metrics': metrics,
            'configuration': {
                'listing_url': self.settings.site.listing_url,
                'preferred_language': self.settings.general.preferred_language,
                'headless_mode': self.settings.general.headless,
                'max_documents': self.settings.general.max_documents
            }
        }

        try:
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False)
            self.info(f"Summary saved to {summary_path}")
        except Exception as e:
            self.error(f"Failed to save summary: {e}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics"""
        return self.metrics.to_dict()


def setup_logger(settings) -> ScraperLogger:
    """Factory function to create configured logger"""
    return ScraperLogger(settings)


if __name__ == "__main__":
    # Test logging setup
    from config.settings import get_settings

    settings = get_settings()
    logger = setup_logger(settings)

    logger.info("Testing logger setup")
    logger.warning("This is a warning")
    logger.error("This is an error")

    with logger.log_processing_time("test_operation"):
        time.sleep(0.1)

    print("Metrics:", logger.get_metrics())