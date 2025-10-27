#!/usr/bin/env python
"""
Simple test to isolate the logging issue
"""

import sys
from pathlib import Path

# Add local modules to path
sys.path.insert(0, str(Path(__file__).parent))

def main():
    try:
        from config.settings import get_settings, ConfigManager
        from utils.logging import setup_logger

        print("Loading configuration...")
        scraper_config = ConfigManager("config.toml")
        settings = scraper_config.load_config()

        print("Setting up logger...")
        logger = setup_logger(settings)

        print("Testing basic logging...")
        logger.info("Basic info message")
        logger.debug("Debug message")
        logger.warning("Warning message")

        print("Testing error logging...")
        try:
            raise ValueError("Test error")
        except Exception as e:
            logger.error(f"Test error: {e}", exc_info=True)

        print("✅ Basic logging test passed!")

        # Test the problematic custom methods
        print("Testing custom logging methods...")
        logger.log_page_processed(1, 5)
        logger.log_document_processed("12345", "https://example.com", "pdf")

        print("✅ Custom logging methods passed!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()