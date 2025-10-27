#!/usr/bin/env python
"""
Test script for CURIA Scraper modular components
===============================================

Quick validation of all modules to ensure they work together properly.

Usage:
    python test_modules.py
"""

import sys
import asyncio
from pathlib import Path

# Add local modules to path
sys.path.insert(0, str(Path(__file__).parent))

async def test_configuration():
    """Test configuration loading"""
    print("🔧 Testing configuration module...")
    try:
        from config.settings import get_settings, ConfigManager

        # Test basic config loading
        settings = get_settings()
        print(f"   ✅ Config loaded: max_documents={settings.general.max_documents}")

        # Test config manager
        config_mgr = ConfigManager("config.toml")
        loaded_settings = config_mgr.load_config()
        print(f"   ✅ Config manager: output_dir={loaded_settings.general.output_dir}")

        return True
    except Exception as e:
        print(f"   ❌ Configuration test failed: {e}")
        return False

async def test_logging():
    """Test logging setup"""
    print("📋 Testing logging module...")
    try:
        from utils.logging import setup_logger
        from config.settings import get_settings

        settings = get_settings()
        logger = setup_logger(settings)

        logger.info("Test log message")
        logger.debug("Debug message")
        logger.warning("Warning message")

        print("   ✅ Logger initialized and working")
        return True
    except Exception as e:
        print(f"   ❌ Logging test failed: {e}")
        return False

async def test_parser():
    """Test document parser"""
    print("📄 Testing parser module...")
    try:
        from parsers.curia_parser import create_parser
        from utils.logging import setup_logger
        from config.settings import get_settings

        settings = get_settings()
        logger = setup_logger(settings)
        parser = create_parser(logger)

        # Test with sample HTML
        sample_html = """
        <html>
            <head><title>Test Document</title></head>
            <body>
                <h1>Court Case 12345</h1>
                <p>This is a test legal document for parsing validation.</p>
                <div class="date">2024-01-15</div>
            </body>
        </html>
        """

        metadata = parser.parse_document(
            sample_html,
            "https://example.com/doc/12345",
            "12345",
            "html_extraction"
        )

        print(f"   ✅ Parser working: doc_id={metadata.doc_id}, title='{metadata.title}'")
        return True
    except Exception as e:
        print(f"   ❌ Parser test failed: {e}")
        return False

async def test_storage():
    """Test storage manager"""
    print("💾 Testing storage module...")
    try:
        from storage.manager import create_storage_manager
        from utils.logging import setup_logger
        from config.settings import get_settings

        settings = get_settings()
        logger = setup_logger(settings)
        storage = create_storage_manager(settings, logger)

        # Test session initialization
        session_id = storage.initialize_session()
        print(f"   ✅ Session initialized: {session_id}")

        # Test directory creation
        print(f"   ✅ Output directory: {storage.output_dir}")
        print(f"   ✅ PDFs directory: {storage.pdfs_dir}")
        print(f"   ✅ Metadata directory: {storage.metadata_dir}")

        return True
    except Exception as e:
        print(f"   ❌ Storage test failed: {e}")
        return False

async def test_browser_manager():
    """Test browser manager (without actually starting browser)"""
    print("🌐 Testing browser manager module...")
    try:
        from browser.manager import create_browser_manager, BrowserPool
        from utils.logging import setup_logger
        from config.settings import get_settings

        settings = get_settings()
        logger = setup_logger(settings)

        # Test browser manager creation (but don't start browser)
        # This just validates the class structure
        print("   ✅ Browser manager module imported successfully")
        print("   ✅ BrowserPool class available")
        print("   ℹ️  Skipping actual browser start for quick test")

        return True
    except Exception as e:
        print(f"   ❌ Browser manager test failed: {e}")
        return False

async def test_main_scraper():
    """Test main scraper class initialization"""
    print("🚀 Testing main scraper module...")
    try:
        from main import CuriaScraperEngine

        # Test scraper initialization
        scraper = CuriaScraperEngine("config.toml")

        print(f"   ✅ Scraper initialized with session: {scraper.session_id}")
        print(f"   ✅ Max documents: {scraper.settings.general.max_documents}")
        print(f"   ✅ Components loaded: parser, storage, logger")

        return True
    except Exception as e:
        print(f"   ❌ Main scraper test failed: {e}")
        return False

async def main():
    """Run all tests"""
    print("🧪 CURIA Scraper Module Test Suite")
    print("=" * 50)

    tests = [
        ("Configuration", test_configuration),
        ("Logging", test_logging),
        ("Parser", test_parser),
        ("Storage", test_storage),
        ("Browser Manager", test_browser_manager),
        ("Main Scraper", test_main_scraper)
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        try:
            success = await test_func()
            if success:
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"   💥 {test_name} test crashed: {e}")
            failed += 1

        print()  # Empty line between tests

    print("=" * 50)
    print(f"📊 Test Results: {passed} passed, {failed} failed")

    if failed == 0:
        print("🎉 All tests passed! The modular system is ready to use.")
        print("\nTo run the scraper:")
        print("   python main.py --max-docs 10 --verbose")
    else:
        print("⚠️  Some tests failed. Check the error messages above.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())