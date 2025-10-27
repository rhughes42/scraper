"""
Storage Management Module
========================

Efficient file storage, data persistence, and checkpoint management
for the CURIA scraper with data integrity and recovery features.

Features:
- Atomic file operations
- Checkpoint/resume functionality
- Data deduplication
- Compression support
- Backup and recovery
- Progress tracking
"""

import json
import gzip
import hashlib
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Union
from datetime import datetime
from dataclasses import dataclass, asdict
from contextlib import contextmanager

from parsers.curia_parser import DocumentMetadata
from parsers.eurlex_parser import EurLexDocumentMetadata
from utils.logging import ScraperLogger


@dataclass
class CheckpointData:
    """Container for checkpoint/resume data"""

    session_id: str
    start_time: str
    last_update: str
    processed_pages: int
    processed_documents: int
    processed_doc_ids: Set[str]
    current_page_url: Optional[str] = None
    errors_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        # Convert set to list for JSON compatibility
        data['processed_doc_ids'] = list(self.processed_doc_ids)
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CheckpointData':
        """Create instance from dictionary"""
        # Convert list back to set
        if 'processed_doc_ids' in data:
            data['processed_doc_ids'] = set(data['processed_doc_ids'])
        return cls(**data)


class AtomicFileWriter:
    """Atomic file writing to prevent corruption"""

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.temp_filepath = filepath.with_suffix(filepath.suffix + '.tmp')

    def __enter__(self):
        self.temp_filepath.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(self.temp_filepath, 'w', encoding='utf-8')
        return self.file

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()
        if exc_type is None:
            # Only move temp file to final location if no exception
            shutil.move(str(self.temp_filepath), str(self.filepath))
        else:
            # Clean up temp file on error
            if self.temp_filepath.exists():
                self.temp_filepath.unlink()


class DataDeduplicator:
    """Handle data deduplication and integrity checking"""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.hashes_file = output_dir / 'file_hashes.json'
        self.hashes: Dict[str, str] = self._load_hashes()

    def _load_hashes(self) -> Dict[str, str]:
        """Load existing file hashes"""
        if self.hashes_file.exists():
            try:
                with open(self.hashes_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_hashes(self):
        """Save file hashes to disk"""
        try:
            with AtomicFileWriter(self.hashes_file) as f:
                json.dump(self.hashes, f, indent=2)
        except Exception:
            pass  # Non-critical if saving fails

    def get_file_hash(self, filepath: Path) -> str:
        """Calculate file hash"""
        hasher = hashlib.sha256()
        try:
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
        except Exception:
            return ""
        return hasher.hexdigest()

    def is_duplicate(self, filepath: Path, content_hash: str) -> bool:
        """Check if file is a duplicate based on content"""
        filename = filepath.name
        if filename in self.hashes:
            return self.hashes[filename] == content_hash
        return False

    def register_file(self, filepath: Path, content_hash: str):
        """Register file with its hash"""
        self.hashes[filepath.name] = content_hash
        self._save_hashes()


class StorageManager:
    """Main storage manager with advanced features"""

    def __init__(self, settings, logger: ScraperLogger):
        self.settings = settings
        self.logger = logger
        self.output_dir = Path(settings.general.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self.deduplicator = DataDeduplicator(self.output_dir)
        self.checkpoint_file = Path(settings.general.checkpoint_file)
        self.checkpoint_data: Optional[CheckpointData] = None

        # Create subdirectories
        self.pdfs_dir = self.output_dir / 'pdfs'
        self.metadata_dir = self.output_dir / 'metadata'
        self.errors_dir = self.output_dir / 'errors'

        for directory in [self.pdfs_dir, self.metadata_dir, self.errors_dir]:
            directory.mkdir(exist_ok=True)

    def initialize_session(self, session_id: Optional[str] = None) -> str:
        """Initialize new scraping session or resume existing one"""
        if not session_id:
            session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Try to load existing checkpoint
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint_dict = json.load(f)
                self.checkpoint_data = CheckpointData.from_dict(checkpoint_dict)
                self.logger.info(
                    f"Resumed session from checkpoint",
                    session_id=self.checkpoint_data.session_id,
                    processed_docs=self.checkpoint_data.processed_documents,
                    processed_pages=self.checkpoint_data.processed_pages
                )
                return self.checkpoint_data.session_id
            except Exception as e:
                self.logger.warning(f"Could not load checkpoint: {e}")

        # Create new session
        self.checkpoint_data = CheckpointData(
            session_id=session_id,
            start_time=datetime.now().isoformat(),
            last_update=datetime.now().isoformat(),
            processed_pages=0,
            processed_documents=0,
            processed_doc_ids=set()
        )

        self.save_checkpoint()
        self.logger.info(f"Started new session: {session_id}")
        return session_id

    def save_checkpoint(self):
        """Save current progress to checkpoint file"""
        if self.checkpoint_data:
            self.checkpoint_data.last_update = datetime.now().isoformat()

            try:
                with AtomicFileWriter(self.checkpoint_file) as f:
                    json.dump(self.checkpoint_data.to_dict(), f, indent=2)

                self.logger.debug(
                    "Checkpoint saved",
                    processed_docs=self.checkpoint_data.processed_documents,
                    processed_pages=self.checkpoint_data.processed_pages
                )
            except Exception as e:
                self.logger.error(f"Failed to save checkpoint: {e}")

    def is_document_processed(self, doc_id: str) -> bool:
        """Check if document was already processed"""
        if self.checkpoint_data:
            return doc_id in self.checkpoint_data.processed_doc_ids
        return False

    def save_document_metadata(
        self,
        metadata: Union[DocumentMetadata, EurLexDocumentMetadata],
        doc_index: int,
        compress: bool = False
    ) -> Path:
        """
        Save document metadata to JSON file

        Args:
            metadata: Document metadata to save
            doc_index: Index number for filename
            compress: Whether to compress the file

        Returns:
            Path: Path to saved file
        """
        filename = f"doc_{doc_index:06d}.json"
        if compress:
            filename += ".gz"

        filepath = self.metadata_dir / filename

        try:
            metadata_dict = metadata.to_dict()

            # Calculate content hash for deduplication
            content_str = json.dumps(metadata_dict, sort_keys=True, ensure_ascii=False)
            content_hash = hashlib.sha256(content_str.encode()).hexdigest()

            # Check for duplicates
            if self.deduplicator.is_duplicate(filepath, content_hash):
                self.logger.debug(f"Skipping duplicate metadata: {filename}")
                return filepath

            # Save file
            if compress:
                with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                    json.dump(metadata_dict, f, ensure_ascii=False, indent=2)
            else:
                with AtomicFileWriter(filepath) as f:
                    json.dump(metadata_dict, f, ensure_ascii=False, indent=2)

            # Register file hash
            self.deduplicator.register_file(filepath, content_hash)

            # Update checkpoint
            if self.checkpoint_data and metadata.doc_id:
                self.checkpoint_data.processed_documents += 1
                self.checkpoint_data.processed_doc_ids.add(metadata.doc_id)

            file_size = filepath.stat().st_size
            self.logger.info(
                f"Saved metadata: {filename}",
                doc_id=metadata.doc_id,
                file_size=file_size,
                quality_score=metadata.content_quality_score
            )

            return filepath

        except Exception as e:
            self.logger.error(f"Failed to save metadata for doc {doc_index}: {e}")
            self.save_error_info(doc_index, metadata.url or "unknown", str(e))
            raise

    def save_pdf_info(self, doc_id: str, pdf_path: Path, doc_index: int) -> Dict[str, Any]:
        """
        Save PDF file information and create metadata entry

        Args:
            doc_id: Document ID
            pdf_path: Path to PDF file
            doc_index: Document index

        Returns:
            Dict: PDF file information
        """
        try:
            file_size = pdf_path.stat().st_size
            file_hash = self.deduplicator.get_file_hash(pdf_path)

            pdf_info = {
                "idx": doc_index,
                "doc_id": doc_id,
                "filename": pdf_path.name,
                "file_path": str(pdf_path),
                "file_size": file_size,
                "file_hash": file_hash,
                "processing_method": "pdf_generation",
                "saved_at": datetime.now().isoformat()
            }

            # Save PDF info as metadata
            info_path = self.metadata_dir / f"pdf_info_{doc_index:06d}.json"
            with AtomicFileWriter(info_path) as f:
                json.dump(pdf_info, f, indent=2, ensure_ascii=False)

            # Register PDF hash
            self.deduplicator.register_file(pdf_path, file_hash)

            self.logger.info(
                f"PDF saved: {pdf_path.name}",
                doc_id=doc_id,
                file_size=file_size,
                doc_index=doc_index
            )

            return pdf_info

        except Exception as e:
            self.logger.error(f"Failed to save PDF info for {doc_id}: {e}")
            return {}

    def save_error_info(self, doc_index: int, url: str, error_message: str):
        """Save error information for failed document processing"""
        error_info = {
            "doc_index": doc_index,
            "url": url,
            "error": error_message,
            "timestamp": datetime.now().isoformat()
        }

        error_file = self.errors_dir / f"error_{doc_index:06d}.json"

        try:
            with AtomicFileWriter(error_file) as f:
                json.dump(error_info, f, indent=2, ensure_ascii=False)

            self.logger.debug(f"Error info saved: {error_file.name}")

        except Exception as e:
            self.logger.error(f"Failed to save error info: {e}")

    def update_page_progress(self, page_num: int, current_url: str):
        """Update progress for page processing"""
        if self.checkpoint_data:
            self.checkpoint_data.processed_pages = page_num
            self.checkpoint_data.current_page_url = current_url

        # Save checkpoint every 5 pages
        if page_num % 5 == 0:
            self.save_checkpoint()

    def create_session_summary(self) -> Dict[str, Any]:
        """Create final session summary"""
        if not self.checkpoint_data:
            return {}

        summary = {
            "session_info": {
                "session_id": self.checkpoint_data.session_id,
                "start_time": self.checkpoint_data.start_time,
                "end_time": datetime.now().isoformat(),
                "duration_seconds": (
                    datetime.now() - datetime.fromisoformat(self.checkpoint_data.start_time)
                ).total_seconds()
            },
            "processing_stats": {
                "pages_processed": self.checkpoint_data.processed_pages,
                "documents_processed": self.checkpoint_data.processed_documents,
                "errors_count": self.checkpoint_data.errors_count,
                "unique_documents": len(self.checkpoint_data.processed_doc_ids)
            },
            "file_stats": {
                "metadata_files": len(list(self.metadata_dir.glob("doc_*.json*"))),
                "pdf_files": len(list(self.pdfs_dir.glob("*.pdf"))),
                "error_files": len(list(self.errors_dir.glob("error_*.json")))
            }
        }

        # Save summary
        summary_path = self.output_dir / "session_summary.json"
        try:
            with AtomicFileWriter(summary_path) as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Session summary saved: {summary_path}")
        except Exception as e:
            self.logger.error(f"Failed to save session summary: {e}")

        return summary

    def cleanup_session(self):
        """Clean up session resources"""
        # Save final checkpoint
        self.save_checkpoint()

        # Create summary
        self.create_session_summary()

        # Optionally remove checkpoint file (keep for resume capability)
        # self.checkpoint_file.unlink(missing_ok=True)


def create_storage_manager(settings, logger: ScraperLogger) -> StorageManager:
    """Factory function to create storage manager"""
    return StorageManager(settings, logger)


if __name__ == "__main__":
    # Test storage manager
    from config.settings import get_settings
    from utils.logging import setup_logger
    from parsers.curia_parser import DocumentMetadata

    settings = get_settings()
    logger = setup_logger(settings)
    storage = create_storage_manager(settings, logger)

    # Test session
    session_id = storage.initialize_session()
    print(f"Session ID: {session_id}")

    # Test metadata save
    test_metadata = DocumentMetadata(
        doc_id="test123",
        url="https://example.com",
        title="Test Document",
        case_number="C-123/2024"
    )

    saved_path = storage.save_document_metadata(test_metadata, 1)
    print(f"Saved to: {saved_path}")

    # Test summary
    summary = storage.create_session_summary()
    print("Summary:", json.dumps(summary, indent=2))