"""Tests for text extraction and cleaning."""

import pytest
from pathlib import Path
import tempfile

from backend.text_clean import TextExtractor


class TestTextExtractor:
    """Test TextExtractor class."""
    
    @pytest.fixture
    def extractor(self):
        """Create a text extractor instance."""
        return TextExtractor()
    
    def test_extract_from_plain_text(self, extractor):
        """Test extracting from plain text file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("This is a test filing.\n\nIt has multiple paragraphs.\n")
            temp_path = Path(f.name)
        
        try:
            text = extractor.extract_from_file(temp_path)
            assert "This is a test filing" in text
            assert "multiple paragraphs" in text
        finally:
            temp_path.unlink()
    
    def test_extract_from_html(self, extractor):
        """Test extracting text from HTML file."""
        html_content = """
        <html>
        <head><title>Test Filing</title></head>
        <body>
            <p>This is paragraph one.</p>
            <p>This is paragraph two.</p>
            <script>var x = 1;</script>
        </body>
        </html>
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
            f.write(html_content)
            temp_path = Path(f.name)
        
        try:
            text = extractor.extract_from_file(temp_path)
            assert "This is paragraph one" in text
            assert "This is paragraph two" in text
            assert "var x = 1" not in text  # Scripts should be removed
        finally:
            temp_path.unlink()
    
    def test_clean_plain_text_removes_excessive_whitespace(self, extractor):
        """Test that cleaning removes excessive whitespace."""
        dirty_text = "This   has    multiple     spaces.\n\n\n\nAnd   many   newlines."
        clean_text = extractor._clean_plain_text(dirty_text)
        
        assert "  " not in clean_text  # No double spaces
        assert "\n\n\n" not in clean_text  # No triple newlines
    
    def test_clean_plain_text_removes_page_numbers(self, extractor):
        """Test that page numbers are removed."""
        text_with_pages = "Content here. Page 1 of 100. More content."
        clean_text = extractor._clean_plain_text(text_with_pages)
        
        assert "Page 1 of 100" not in clean_text
        assert "Content here" in clean_text
        assert "More content" in clean_text
    
    def test_is_html_detects_html(self, extractor):
        """Test HTML detection."""
        html = "<html><body>Content</body></html>"
        assert extractor._is_html(html) is True
    
    def test_is_html_detects_plain_text(self, extractor):
        """Test that plain text is not detected as HTML."""
        plain = "This is just plain text with no tags."
        assert extractor._is_html(plain) is False
    
    def test_extract_from_file_nonexistent_raises_error(self, extractor):
        """Test that missing file raises FileNotFoundError."""
        fake_path = Path("/nonexistent/file.txt")
        with pytest.raises(FileNotFoundError):
            extractor.extract_from_file(fake_path)
    
    def test_extract_from_filing_with_none_raises_error(self, extractor):
        """Test that None path raises ValueError."""
        with pytest.raises(ValueError, match="Filing path is None"):
            extractor.extract_from_filing(None)
    
    def test_extract_from_filing_with_valid_path(self, extractor):
        """Test extracting from a valid filing path."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("Filing content here.")
            temp_path = Path(f.name)
        
        try:
            text = extractor.extract_from_filing(temp_path)
            assert "Filing content here" in text
        finally:
            temp_path.unlink()
    
    def test_clean_plain_text_preserves_structure(self, extractor):
        """Test that cleaning preserves paragraph structure."""
        text = "Paragraph one.\n\nParagraph two.\n\nParagraph three."
        clean_text = extractor._clean_plain_text(text)
        
        # Should preserve double newlines (paragraph breaks)
        assert "\n\n" in clean_text
        assert "Paragraph one" in clean_text
        assert "Paragraph two" in clean_text
    
    def test_clean_plain_text_empty_after_cleaning_raises_error(self, extractor):
        """Test that empty text after cleaning raises ValueError."""
        # Text that becomes empty after cleaning
        empty_text = "   \n\n\n   "
        with pytest.raises(ValueError, match="Extracted text is empty"):
            extractor._clean_plain_text(empty_text)



