"""
Tests for phone number standardization utilities.
"""

import pytest
from app.utils.phone import standardize_phone, detect_phone_column, validate_phone


class TestStandardizePhone:
    """Test the standardize_phone function with various formats and options."""
    
    def test_e164_format_us_number(self):
        """Test E.164 format with US numbers."""
        # Various US formats should all produce +14155551234
        assert standardize_phone("(415) 555-1234", default_country_code="1") == "+14155551234"
        assert standardize_phone("415.555.1234", default_country_code="1") == "+14155551234"
        assert standardize_phone("415-555-1234", default_country_code="1") == "+14155551234"
        assert standardize_phone("4155551234", default_country_code="1") == "+14155551234"
        assert standardize_phone("+1 415 555 1234") == "+14155551234"
    
    def test_e164_format_international(self):
        """Test E.164 format with international numbers."""
        assert standardize_phone("+44 20 7946 1234") == "+442079461234"
        assert standardize_phone("+81 3 1234 5678") == "+81312345678"
        assert standardize_phone("+33 1 42 86 82 00") == "+33142868200"
    
    def test_international_format(self):
        """Test international format output."""
        result = standardize_phone(
            "(415) 555-1234",
            default_country_code="1",
            output_format="international"
        )
        assert result == "+1 (415) 555-1234"
        
        result = standardize_phone(
            "+44 20 7946 1234",
            output_format="international"
        )
        assert result == "+44 20 7946 1234"
    
    def test_national_format(self):
        """Test national format output."""
        result = standardize_phone(
            "4155551234",
            default_country_code="1",
            output_format="national"
        )
        assert result == "(415) 555-1234"
    
    def test_digits_only_format(self):
        """Test digits-only format."""
        assert standardize_phone("(415) 555-1234", output_format="digits_only") == "4155551234"
        assert standardize_phone("+1-415-555-1234", output_format="digits_only") == "14155551234"
    
    def test_preserve_extension(self):
        """Test extension preservation."""
        result = standardize_phone(
            "415-555-1234 x123",
            default_country_code="1",
            preserve_extension=True
        )
        assert result == "+14155551234x123"
        
        result = standardize_phone(
            "415-555-1234 ext 456",
            default_country_code="1",
            preserve_extension=True
        )
        assert result == "+14155551234x456"
    
    def test_no_country_code_without_default(self):
        """Test that numbers without country code and no default remain local."""
        result = standardize_phone("415-555-1234", output_format="e164")
        # Should return just the digits without + prefix
        assert result == "4155551234"
    
    def test_invalid_phone_numbers(self):
        """Test handling of invalid phone numbers."""
        # Too few digits
        assert standardize_phone("123", min_digits=7) is None
        
        # Too many digits
        assert standardize_phone("12345678901234567890", max_digits=15) is None
        
        # Empty/None
        assert standardize_phone(None) is None
        assert standardize_phone("") is None
        assert standardize_phone("   ") is None
    
    def test_leading_zeros(self):
        """Test leading zero handling."""
        # With strip_leading_zeros=True (default)
        result = standardize_phone("0415551234", default_country_code="1")
        assert result == "+1415551234"
        
        # With strip_leading_zeros=False
        result = standardize_phone(
            "0415551234",
            default_country_code="1",
            strip_leading_zeros=False
        )
        assert result == "+10415551234"
    
    def test_auto_detect_country_code(self):
        """Test automatic country code detection."""
        # US number (11 digits starting with 1)
        assert standardize_phone("14155551234") == "+14155551234"
        
        # UK number (12+ digits starting with 44)
        assert standardize_phone("442079461234") == "+442079461234"
        
        # France number (11+ digits starting with 33)
        assert standardize_phone("33142868200") == "+33142868200"
    
    def test_various_separators(self):
        """Test phone numbers with various separators."""
        phone_formats = [
            "(415) 555-1234",
            "415.555.1234",
            "415-555-1234",
            "415 555 1234",
            "415/555/1234",
        ]
        
        for phone in phone_formats:
            result = standardize_phone(phone, default_country_code="1")
            assert result == "+14155551234", f"Failed for format: {phone}"


class TestDetectPhoneColumn:
    """Test phone column detection."""
    
    def test_detect_us_phone_column(self):
        """Test detection of US phone number columns."""
        values = [
            "(415) 555-1234",
            "415-555-5678",
            "415.555.9012",
            "(650) 555-3456",
        ]
        assert detect_phone_column(values) is True
    
    def test_detect_international_phone_column(self):
        """Test detection of international phone numbers."""
        values = [
            "+1 415 555 1234",
            "+44 20 7946 1234",
            "+81 3 1234 5678",
            "+33 1 42 86 82 00",
        ]
        assert detect_phone_column(values) is True
    
    def test_detect_non_phone_column(self):
        """Test that non-phone columns are not detected as phones."""
        values = ["John", "Jane", "Bob", "Alice"]
        assert detect_phone_column(values) is False
        
        values = ["123", "456", "789"]  # Too short
        assert detect_phone_column(values) is False
    
    def test_detect_mixed_column(self):
        """Test detection with mixed valid/invalid values."""
        # More than 50% valid should be detected
        values = [
            "(415) 555-1234",
            "415-555-5678",
            "not a phone",
            "(650) 555-3456",
        ]
        assert detect_phone_column(values) is True
        
        # Less than 50% valid should not be detected
        values = [
            "(415) 555-1234",
            "not a phone",
            "also not a phone",
            "still not a phone",
        ]
        assert detect_phone_column(values) is False
    
    def test_detect_empty_column(self):
        """Test detection with empty values."""
        assert detect_phone_column([]) is False
        assert detect_phone_column([None, None, None]) is False


class TestValidatePhone:
    """Test phone number validation."""
    
    def test_valid_phone_numbers(self):
        """Test validation of valid phone numbers."""
        assert validate_phone("(415) 555-1234") is True
        assert validate_phone("+1 415 555 1234") is True
        assert validate_phone("4155551234") is True
    
    def test_invalid_phone_numbers(self):
        """Test validation of invalid phone numbers."""
        assert validate_phone("123") is False  # Too short
        assert validate_phone("12345678901234567890") is False  # Too long
        assert validate_phone(None) is False
        assert validate_phone("") is False
        assert validate_phone("not a phone") is False
    
    def test_custom_digit_limits(self):
        """Test validation with custom min/max digits."""
        assert validate_phone("12345", min_digits=5, max_digits=10) is True
        assert validate_phone("123", min_digits=5, max_digits=10) is False
        assert validate_phone("12345678901", min_digits=5, max_digits=10) is False


class TestPhoneTransformationIntegration:
    """Test phone standardization in the context of data transformations."""
    
    def test_column_transformation(self):
        """Test standardize_phone as a column transformation."""
        from app.domain.imports.mapper import _apply_column_transformations
        
        record = {"phone": "(415) 555-1234", "name": "John"}
        transformation = {
            "type": "standardize_phone",
            "source_column": "phone",
            "target_column": "phone",
            "default_country_code": "1",
            "output_format": "e164"
        }
        
        result = _apply_column_transformations(record, [transformation])
        assert result["phone"] == "+14155551234"
        assert result["name"] == "John"
    
    def test_row_transformation(self):
        """Test standardize_phone as a row transformation."""
        from app.domain.imports.preprocessor import _apply_standardize_phone
        
        records = [
            {"phone": "(415) 555-1234", "name": "Alice"},
            {"phone": "650-555-5678", "name": "Bob"},
        ]
        
        transformation = {
            "source_column": "phone",
            "target_column": "phone_standardized",
            "default_country_code": "1",
            "output_format": "e164"
        }
        
        result, errors = _apply_standardize_phone(records, transformation, row_offset=0)
        
        assert len(result) == 2
        assert result[0]["phone_standardized"] == "+14155551234"
        assert result[1]["phone_standardized"] == "+16505555678"
        assert len(errors) == 0
    
    def test_multiple_output_formats(self):
        """Test different output formats in transformations."""
        from app.domain.imports.mapper import _apply_column_transformations
        
        record = {"phone": "(415) 555-1234"}
        
        # E.164 format
        transformation = {
            "type": "standardize_phone",
            "source_column": "phone",
            "target_column": "phone_e164",
            "default_country_code": "1",
            "output_format": "e164"
        }
        result = _apply_column_transformations(record, [transformation])
        assert result["phone_e164"] == "+14155551234"
        
        # International format
        transformation["output_format"] = "international"
        transformation["target_column"] = "phone_intl"
        result = _apply_column_transformations(record, [transformation])
        assert result["phone_intl"] == "+1 (415) 555-1234"
        
        # Digits only
        transformation["output_format"] = "digits_only"
        transformation["target_column"] = "phone_digits"
        result = _apply_column_transformations(record, [transformation])
        assert result["phone_digits"] == "4155551234"

    def test_multiple_phone_columns_in_place(self):
        """Test that multiple phone columns can be standardized in-place."""
        from app.domain.imports.mapper import _apply_column_transformations

        record = {
            "Primary Phone": "(415) 555-1234",
            "Mobile Phone": "650-555-5678",
            "Work Phone": "+1 415 555 9012",
            "name": "John Doe"
        }

        transformations = [
            {
                "type": "standardize_phone",
                "source_column": "Primary Phone",
                "target_column": "Primary Phone",  # In-place
                "default_country_code": "1",
                "output_format": "e164"
            },
            {
                "type": "standardize_phone",
                "source_column": "Mobile Phone",
                "target_column": "Mobile Phone",  # In-place
                "default_country_code": "1",
                "output_format": "e164"
            },
            {
                "type": "standardize_phone",
                "source_column": "Work Phone",
                "target_column": "Work Phone",  # In-place
                "default_country_code": "1",
                "output_format": "e164"
            }
        ]

        result = _apply_column_transformations(record, transformations)

        # Verify each phone column was standardized in-place
        assert result["Primary Phone"] == "+14155551234"
        assert result["Mobile Phone"] == "+16505555678"
        assert result["Work Phone"] == "+14155559012"
        # Non-phone column should remain unchanged
        assert result["name"] == "John Doe"

    def test_multiple_phone_columns_mixed_targets(self):
        """Test multiple phone columns with mixed in-place and new column targets."""
        from app.domain.imports.mapper import _apply_column_transformations

        record = {
            "Primary Phone": "(415) 555-1234",
            "Mobile Phone": "650-555-5678",
            "name": "Jane Smith"
        }

        transformations = [
            {
                "type": "standardize_phone",
                "source_column": "Primary Phone",
                "target_column": "Primary Phone",  # In-place
                "default_country_code": "1",
                "output_format": "e164"
            },
            {
                "type": "standardize_phone",
                "source_column": "Mobile Phone",
                "target_column": "mobile_phone_e164",  # New column
                "default_country_code": "1",
                "output_format": "e164"
            }
        ]

        result = _apply_column_transformations(record, transformations)

        # Primary phone standardized in-place
        assert result["Primary Phone"] == "+14155551234"
        # Mobile phone standardized to new column
        assert result["mobile_phone_e164"] == "+16505555678"
        # Original mobile phone column should still exist
        assert result["Mobile Phone"] == "650-555-5678"
        # Non-phone column unchanged
        assert result["name"] == "Jane Smith"
