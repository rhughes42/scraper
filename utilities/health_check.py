"""
Health Check Utilities
======================

Tools for monitoring system health and performing diagnostics.
"""

import psutil
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path


class HealthCheck:
    """
    Perform health checks on the scraping system.
    
    Monitors system resources, connectivity, and component status.
    """
    
    def __init__(self):
        self.checks: Dict[str, bool] = {}
        self.warnings: List[str] = []
        self.errors: List[str] = []
    
    def check_system_resources(
        self,
        min_memory_mb: float = 512,
        min_disk_gb: float = 1.0
    ) -> bool:
        """
        Check if system has sufficient resources.
        
        Args:
            min_memory_mb: Minimum available memory in MB
            min_disk_gb: Minimum available disk space in GB
            
        Returns:
            True if resources are sufficient
        """
        try:
            # Check memory
            memory = psutil.virtual_memory()
            available_mb = memory.available / 1024 / 1024
            
            if available_mb < min_memory_mb:
                self.warnings.append(
                    f"Low memory: {available_mb:.0f}MB available "
                    f"(minimum: {min_memory_mb}MB)"
                )
                return False
            
            # Check disk space
            disk = psutil.disk_usage('/')
            available_gb = disk.free / 1024 / 1024 / 1024
            
            if available_gb < min_disk_gb:
                self.warnings.append(
                    f"Low disk space: {available_gb:.1f}GB available "
                    f"(minimum: {min_disk_gb}GB)"
                )
                return False
            
            self.checks['system_resources'] = True
            return True
            
        except Exception as e:
            self.errors.append(f"Failed to check system resources: {e}")
            return False
    
    def check_output_directory(self, output_dir: str) -> bool:
        """
        Check if output directory exists and is writable.
        
        Args:
            output_dir: Path to output directory
            
        Returns:
            True if directory is accessible
        """
        try:
            path = Path(output_dir)
            
            # Create if doesn't exist
            path.mkdir(parents=True, exist_ok=True)
            
            # Test write access
            test_file = path / ".health_check_test"
            test_file.touch()
            test_file.unlink()
            
            self.checks['output_directory'] = True
            return True
            
        except Exception as e:
            self.errors.append(f"Output directory not accessible: {e}")
            return False
    
    async def check_network_connectivity(
        self,
        test_urls: Optional[List[str]] = None
    ) -> bool:
        """
        Check network connectivity to target sites.
        
        Args:
            test_urls: List of URLs to test (optional)
            
        Returns:
            True if connectivity is good
        """
        try:
            import aiohttp
            
            test_urls = test_urls or ["https://www.google.com"]
            
            async with aiohttp.ClientSession() as session:
                for url in test_urls:
                    try:
                        async with session.get(
                            url,
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as response:
                            if response.status >= 500:
                                self.warnings.append(
                                    f"Server error for {url}: {response.status}"
                                )
                    except asyncio.TimeoutError:
                        self.warnings.append(f"Timeout connecting to {url}")
                    except Exception as e:
                        self.warnings.append(f"Failed to connect to {url}: {e}")
            
            self.checks['network_connectivity'] = len(self.warnings) == 0
            return self.checks['network_connectivity']
            
        except ImportError:
            self.warnings.append("aiohttp not available for network checks")
            return True  # Don't fail if aiohttp not available
        except Exception as e:
            self.errors.append(f"Network connectivity check failed: {e}")
            return False
    
    def check_dependencies(self) -> bool:
        """
        Check if required dependencies are installed.
        
        Returns:
            True if all dependencies are available
        """
        required_packages = [
            'playwright',
            'beautifulsoup4',
            'pydantic',
            'toml',
        ]
        
        missing = []
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                missing.append(package)
        
        if missing:
            self.errors.append(
                f"Missing required packages: {', '.join(missing)}"
            )
            return False
        
        self.checks['dependencies'] = True
        return True
    
    def check_browser_installation(self) -> bool:
        """
        Check if Playwright browsers are installed.
        
        Returns:
            True if browsers are available
        """
        try:
            from playwright.sync_api import sync_playwright
            
            with sync_playwright() as p:
                try:
                    # Try to launch chromium
                    browser = p.chromium.launch(headless=True)
                    browser.close()
                    self.checks['browser_installation'] = True
                    return True
                except Exception as e:
                    self.errors.append(
                        f"Playwright browser not installed. "
                        f"Run 'playwright install chromium'. Error: {e}"
                    )
                    return False
                    
        except ImportError:
            self.errors.append("Playwright not available")
            return False
        except Exception as e:
            self.errors.append(f"Browser check failed: {e}")
            return False
    
    async def run_all_checks(
        self,
        output_dir: str = "./output",
        test_urls: Optional[List[str]] = None
    ) -> bool:
        """
        Run all health checks.
        
        Args:
            output_dir: Output directory to check
            test_urls: URLs to test connectivity
            
        Returns:
            True if all checks pass
        """
        self.checks.clear()
        self.warnings.clear()
        self.errors.clear()
        
        # Run checks
        checks = [
            self.check_dependencies(),
            self.check_system_resources(),
            self.check_output_directory(output_dir),
        ]
        
        # Network check (async)
        network_ok = await self.check_network_connectivity(test_urls)
        checks.append(network_ok)
        
        # Browser check (may be slow)
        # browser_ok = self.check_browser_installation()
        # checks.append(browser_ok)
        
        return all(checks)
    
    def get_report(self) -> Dict[str, Any]:
        """
        Get health check report.
        
        Returns:
            Dictionary with check results
        """
        return {
            'timestamp': datetime.now().isoformat(),
            'checks': self.checks,
            'warnings': self.warnings,
            'errors': self.errors,
            'overall_status': 'healthy' if not self.errors else 'unhealthy',
        }
    
    def print_report(self):
        """Print a human-readable health report"""
        print("\n" + "=" * 60)
        print("HEALTH CHECK REPORT")
        print("=" * 60)
        
        # Print checks
        for check, passed in self.checks.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{status} - {check.replace('_', ' ').title()}")
        
        # Print warnings
        if self.warnings:
            print("\n⚠️  WARNINGS:")
            for warning in self.warnings:
                print(f"  - {warning}")
        
        # Print errors
        if self.errors:
            print("\n❌ ERRORS:")
            for error in self.errors:
                print(f"  - {error}")
        
        # Overall status
        print("\n" + "-" * 60)
        if not self.errors:
            print("Overall Status: ✅ HEALTHY")
        else:
            print("Overall Status: ❌ UNHEALTHY")
        print("=" * 60 + "\n")
