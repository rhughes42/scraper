# Template Scraper Application

This is a template for creating new scraper applications for the generalized web scraper framework.

## Purpose

Use this template as a starting point when adding support for a new website. It includes:

- Fully commented configuration class
- Complete parser implementation with examples
- Default configuration file
- Testing examples
- Best practices and tips

## How to Use This Template

### 1. Copy the Template

```bash
# Copy the template to a new application
cp -r applications/template applications/mysite

# Update the application name in files
cd applications/mysite
```

### 2. Customize the Files

Edit the following files:

- **`__init__.py`**: Update description and metadata
- **`config.py`**: Set URLs, selectors, and validation rules
- **`parser.py`**: Implement extraction methods for your site
- **`config.toml`**: Set default configuration values

### 3. Test Your Parser

Test the parser with sample HTML:

```bash
# Run the parser test
python applications/mysite/parser.py
```

### 4. Test Your Configuration

Validate the configuration:

```bash
# Run config test
python applications/mysite/config.py

# Or use the CLI
python -m cli.main config show mysite
python -m cli.main config validate mysite applications/mysite/config.toml
```

### 5. Test Scraping

Start with a small number of documents:

```bash
python -m cli.main scrape mysite --max-docs 5 --verbose
```

## File Descriptions

### `__init__.py`

Package metadata file. Update:
- `__description__`: What the scraper does
- `__version__`: Version number (start with 1.0.0)
- `__author__`: Your name and email

### `config.py`

Configuration class that extends `BaseConfig`. Contains:
- URL settings (base_url, start_url)
- CSS selectors for finding elements
- Site-specific settings
- Validation logic
- Fully commented examples

**Key sections to update:**
- `base_url` and `start_url` (lines 33-42)
- All CSS selectors (lines 77-116)
- `validate_config()` method (lines 147-170)

### `parser.py`

Parser class that extends `BaseParser`. Contains:
- `parse_document()`: Main parsing method
- `extract_links()`: Find document links on pages
- Helper methods for extracting specific fields
- Testing code at the bottom

**Key sections to update:**
- `_extract_title()`, `_extract_author()`, etc. (lines 141-319)
- `_is_document_link()` (lines 128-137)
- `_extract_doc_id()` (lines 321-353)

### `config.toml`

Default configuration in TOML format. Update:
- All URLs and selectors
- Throttle settings
- Max documents for testing

## Development Workflow

1. **Inspect the Target Site**
   - Open the site in a browser
   - Use DevTools to find CSS selectors
   - Note URL patterns

2. **Update Configuration**
   - Set base_url and start_url
   - Find and test CSS selectors
   - Add any custom settings needed

3. **Implement Parser**
   - Start with _extract_title()
   - Add other extraction methods
   - Test with real HTML

4. **Test Incrementally**
   - Test parser with sample HTML
   - Test config validation
   - Test with 1-2 documents
   - Gradually increase document count

5. **Handle Edge Cases**
   - Missing elements
   - Different page structures
   - Pagination variations
   - Error conditions

## Tips

### Finding CSS Selectors

1. **Chrome DevTools Method:**
   ```
   Right-click element → Inspect → Right-click in Elements tab
   → Copy → Copy selector
   ```

2. **Test in Console:**
   ```javascript
   document.querySelector("your-selector")
   document.querySelectorAll("your-selector")
   ```

3. **Common Patterns:**
   - Class: `.class-name` or `div.class-name`
   - ID: `#element-id`
   - Attribute: `a[href*='document']`
   - Multiple: `.class1, .class2` (tries both)

### Writing Robust Selectors

✅ **Good:**
```python
# Try multiple selectors
for selector in [primary, fallback1, fallback2]:
    element = soup.find(selector)
    if element:
        break
```

❌ **Bad:**
```python
# Single selector, fails if structure changes
element = soup.find(primary)
```

### Handling Missing Data

✅ **Good:**
```python
def _extract_title(self, soup):
    element = soup.find('h1', {'class': 'title'})
    if element:
        return self.clean_text(element.get_text())
    return None  # Graceful fallback
```

❌ **Bad:**
```python
def _extract_title(self, soup):
    return soup.find('h1').get_text()  # Crashes if no h1
```

### Testing

Always test your parser before running on the full site:

```python
# In parser.py
if __name__ == "__main__":
    sample_html = """<html>...</html>"""
    parser = create_parser()
    result = parser.parse_document(sample_html, "http://test.com/doc/1")
    print(json.dumps(result, indent=2))
```

## Common Issues

### Issue: "Application not found"

**Solution:** Make sure:
- Application is in `applications/` directory
- `__init__.py` exists
- `config.py` exists

### Issue: "No links found"

**Solution:**
- Check `document_link_selector` matches actual HTML
- Add alternative selectors
- Enable verbose logging: `--verbose`

### Issue: "Scraper hangs"

**Solution:**
- Increase `timeout_seconds`
- Check `next_page_selector` is correct
- Test pagination manually

### Issue: "Empty metadata"

**Solution:**
- Test parser with real HTML
- Check all selectors are correct
- Add logging to see what's being found

## Next Steps

After creating your application:

1. **Test thoroughly** with small datasets
2. **Handle errors gracefully** in your parser
3. **Document any quirks** of the target site
4. **Add unit tests** (optional but recommended)
5. **Share with the community** via pull request

## Resources

- [Full Developer Guide](../../DEVELOPER_GUIDE.md)
- [BeautifulSoup Docs](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [CSS Selectors Reference](https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_Selectors)
- [Playwright Python Docs](https://playwright.dev/python/)

## Questions?

- Check the [Developer Guide](../../DEVELOPER_GUIDE.md)
- Review existing applications (curia, eurlex)
- Open an issue on GitHub
