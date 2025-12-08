#!/usr/bin/env python
"""
Generalized Web Scraper CLI
===========================

Command-line interface for the generalized web scraping framework.

This tool provides a unified interface for scraping different websites
using site-specific application implementations.

Usage:
    scraper-cli scrape curia --max-docs 100
    scraper-cli scrape eurlex --headless
    scraper-cli list-apps
    scraper-cli health-check
    scraper-cli config show curia
"""

import sys
import asyncio
import argparse
from pathlib import Path
from typing import Optional, List
import json

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utilities.health_check import HealthCheck
from utilities.performance import PerformanceMonitor


class ScraperCLI:
    """Main CLI application"""
    
    def __init__(self):
        self.parser = self._create_parser()
        self.available_apps = self._discover_applications()
    
    def _create_parser(self) -> argparse.ArgumentParser:
        """Create the argument parser"""
        parser = argparse.ArgumentParser(
            prog="scraper-cli",
            description="Generalized Web Scraper - Scrape any website with ease",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Scrape CURIA with default settings
  scraper-cli scrape curia

  # Scrape EUR-Lex with custom limits
  scraper-cli scrape eurlex --max-docs 50 --headless

  # List available applications
  scraper-cli list-apps

  # Run health check
  scraper-cli health-check

  # Show configuration for an application
  scraper-cli config show curia

  # Validate configuration file
  scraper-cli config validate curia ./my-config.toml

For more information, visit: https://github.com/rhughes42/scraper
            """
        )
        
        subparsers = parser.add_subparsers(dest="command", help="Available commands")
        
        # Scrape command
        scrape_parser = subparsers.add_parser(
            "scrape",
            help="Start scraping a website"
        )
        scrape_parser.add_argument(
            "application",
            help="Application to use (e.g., curia, eurlex)"
        )
        scrape_parser.add_argument(
            "--config", "-c",
            help="Path to configuration file"
        )
        scrape_parser.add_argument(
            "--max-docs", "-m",
            type=int,
            help="Maximum number of documents to scrape"
        )
        scrape_parser.add_argument(
            "--headless",
            action="store_true",
            help="Run browser in headless mode"
        )
        scrape_parser.add_argument(
            "--verbose", "-v",
            action="store_true",
            help="Enable verbose logging"
        )
        scrape_parser.add_argument(
            "--resume", "-r",
            action="store_true",
            help="Resume from previous checkpoint"
        )
        scrape_parser.add_argument(
            "--output-dir", "-o",
            help="Output directory for scraped data"
        )
        
        # List applications command
        subparsers.add_parser(
            "list-apps",
            help="List available scraper applications"
        )
        
        # Health check command
        health_parser = subparsers.add_parser(
            "health-check",
            help="Run system health checks"
        )
        health_parser.add_argument(
            "--output-dir",
            default="./output",
            help="Output directory to check"
        )
        
        # Config command
        config_parser = subparsers.add_parser(
            "config",
            help="Configuration management"
        )
        config_subparsers = config_parser.add_subparsers(dest="config_action")
        
        # Config show
        show_parser = config_subparsers.add_parser(
            "show",
            help="Show default configuration for an application"
        )
        show_parser.add_argument(
            "application",
            help="Application name"
        )
        
        # Config validate
        validate_parser = config_subparsers.add_parser(
            "validate",
            help="Validate a configuration file"
        )
        validate_parser.add_argument(
            "application",
            help="Application name"
        )
        validate_parser.add_argument(
            "config_file",
            help="Configuration file to validate"
        )
        
        # Version command
        subparsers.add_parser(
            "version",
            help="Show version information"
        )
        
        return parser
    
    def _discover_applications(self) -> List[str]:
        """Discover available application implementations"""
        apps_dir = Path(__file__).parent.parent / "applications"
        if not apps_dir.exists():
            return []
        
        apps = []
        for item in apps_dir.iterdir():
            if item.is_dir() and not item.name.startswith("_"):
                # Check if it has required files
                if (item / "config.py").exists():
                    apps.append(item.name)
        
        return sorted(apps)
    
    async def run(self, args=None):
        """Run the CLI with given arguments"""
        parsed_args = self.parser.parse_args(args)
        
        if not parsed_args.command:
            self.parser.print_help()
            return 0
        
        # Route to appropriate handler
        if parsed_args.command == "scrape":
            return await self.handle_scrape(parsed_args)
        elif parsed_args.command == "list-apps":
            return self.handle_list_apps()
        elif parsed_args.command == "health-check":
            return await self.handle_health_check(parsed_args)
        elif parsed_args.command == "config":
            return self.handle_config(parsed_args)
        elif parsed_args.command == "version":
            return self.handle_version()
        else:
            self.parser.print_help()
            return 1
    
    async def handle_scrape(self, args) -> int:
        """Handle the scrape command"""
        app_name = args.application.lower()
        
        if app_name not in self.available_apps:
            print(f"❌ Error: Application '{app_name}' not found.")
            print(f"Available applications: {', '.join(self.available_apps)}")
            print("\nRun 'scraper-cli list-apps' to see all available applications.")
            return 1
        
        print(f"🚀 Starting {app_name} scraper...")
        
        try:
            # Import the original main.py to use existing functionality
            # For now, delegate to the existing main.py with appropriate arguments
            from main import CuriaScraperEngine
            
            # Build config path
            config_file = args.config or f"applications/{app_name}/config.toml"
            if not Path(config_file).exists():
                config_file = "config.toml"  # Fall back to default
            
            # Create scraper
            scraper = CuriaScraperEngine(config_file)
            
            # Apply CLI overrides
            if args.headless:
                scraper.settings.general.headless = True
            if args.verbose:
                scraper.settings.logging.level = "DEBUG"
                from utils.logging import setup_logger
                scraper.logger = setup_logger(scraper.settings)
            if args.output_dir:
                scraper.settings.general.output_dir = args.output_dir
            
            # Run scraper
            await scraper.start_scraping(
                resume=args.resume,
                max_documents=args.max_docs
            )
            
            print("✅ Scraping completed successfully!")
            return 0
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return 1
    
    def handle_list_apps(self) -> int:
        """Handle the list-apps command"""
        print("\n📚 Available Scraper Applications:")
        print("=" * 60)
        
        if not self.available_apps:
            print("No applications found.")
            print("\nApplications should be in the 'applications/' directory.")
            return 1
        
        for app in self.available_apps:
            # Try to load app info
            try:
                app_module = __import__(
                    f"applications.{app}",
                    fromlist=["__description__", "__version__"]
                )
                desc = getattr(app_module, "__description__", "No description")
                version = getattr(app_module, "__version__", "Unknown")
                
                print(f"\n  • {app} (v{version})")
                print(f"    {desc}")
            except Exception:
                print(f"\n  • {app}")
                print(f"    (Could not load metadata)")
        
        print("\n" + "=" * 60)
        print(f"\nTotal: {len(self.available_apps)} application(s)")
        print("\nUsage: scraper-cli scrape <application> [options]")
        print("Example: scraper-cli scrape curia --max-docs 100\n")
        
        return 0
    
    async def handle_health_check(self, args) -> int:
        """Handle the health-check command"""
        print("🏥 Running system health checks...\n")
        
        health = HealthCheck()
        
        # Run all checks
        is_healthy = await health.run_all_checks(
            output_dir=args.output_dir
        )
        
        # Print report
        health.print_report()
        
        return 0 if is_healthy else 1
    
    def handle_config(self, args) -> int:
        """Handle config commands"""
        if not args.config_action:
            print("❌ Error: Please specify a config action (show or validate)")
            return 1
        
        if args.config_action == "show":
            return self.handle_config_show(args)
        elif args.config_action == "validate":
            return self.handle_config_validate(args)
        
        return 1
    
    def handle_config_show(self, args) -> int:
        """Show default configuration"""
        app_name = args.application.lower()
        
        if app_name not in self.available_apps:
            print(f"❌ Error: Application '{app_name}' not found.")
            return 1
        
        try:
            # Import config module
            config_module = __import__(
                f"applications.{app_name}.config",
                fromlist=["CuriaConfig", "EurlexConfig"]
            )
            
            # Get config class
            config_class = None
            for attr_name in dir(config_module):
                attr = getattr(config_module, attr_name)
                if (isinstance(attr, type) and 
                    hasattr(attr, '__bases__') and
                    'BaseConfig' in [b.__name__ for b in attr.__bases__]):
                    config_class = attr
                    break
            
            if not config_class:
                print(f"❌ Error: Could not find config class for {app_name}")
                return 1
            
            # Create instance with defaults
            config = config_class()
            
            # Display as JSON
            print(f"\n📄 Default Configuration for '{app_name}':")
            print("=" * 60)
            # Use model_dump for Pydantic v2 compatibility
            config_dict = config.model_dump() if hasattr(config, 'model_dump') else config.dict()
            print(json.dumps(config_dict, indent=2))
            print("=" * 60 + "\n")
            
            return 0
            
        except Exception as e:
            print(f"❌ Error loading configuration: {e}")
            return 1
    
    def handle_config_validate(self, args) -> int:
        """Validate a configuration file"""
        app_name = args.application.lower()
        config_file = Path(args.config_file)
        
        if not config_file.exists():
            print(f"❌ Error: Configuration file not found: {config_file}")
            return 1
        
        print(f"🔍 Validating configuration for '{app_name}'...\n")
        
        try:
            import toml
            
            # Load config file
            config_data = toml.load(config_file)
            
            # Import config class
            config_module = __import__(
                f"applications.{app_name}.config",
                fromlist=["CuriaConfig", "EurlexConfig"]
            )
            
            # Get config class (similar to show)
            config_class = None
            for attr_name in dir(config_module):
                attr = getattr(config_module, attr_name)
                if (isinstance(attr, type) and 
                    hasattr(attr, '__bases__') and
                    'BaseConfig' in [b.__name__ for b in attr.__bases__]):
                    config_class = attr
                    break
            
            if not config_class:
                print(f"❌ Error: Could not find config class for {app_name}")
                return 1
            
            # Validate by instantiating
            config = config_class(**config_data)
            config.validate_config()
            
            print("✅ Configuration is valid!")
            print(f"\nLoaded settings:")
            print(f"  Base URL: {config.base_url}")
            print(f"  Start URL: {config.start_url}")
            print(f"  Max documents: {config.max_documents}")
            print(f"  Output directory: {config.output_dir}")
            
            return 0
            
        except Exception as e:
            print(f"❌ Validation failed: {e}")
            return 1
    
    def handle_version(self) -> int:
        """Show version information"""
        try:
            from scraper import __version__, __author__
            print(f"\nGeneralized Web Scraper")
            print(f"Version: {__version__}")
            print(f"Author: {__author__}")
            print(f"\nAvailable applications: {len(self.available_apps)}")
            print(f"Python: {sys.version.split()[0]}\n")
        except Exception:
            print("\nGeneralized Web Scraper")
            print("Version: Unknown\n")
        
        return 0


def main(args=None):
    """Main entry point"""
    cli = ScraperCLI()
    
    try:
        exit_code = asyncio.run(cli.run(args))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n🛑 Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
