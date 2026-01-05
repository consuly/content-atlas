"""
Tests for preset regex validators.

This module tests all preset validation patterns including:
- Contact & Communication (email, phone)
- Identifiers & Codes (UUID, SSN, postal codes)
- Web & Network (URLs, domains, IPs)
- Financial (credit cards, currency)
- Data Formats (dates, times, colors, slugs)
- Custom Business IDs (alphanumeric, SKUs)
"""

import pytest
from app.domain.imports.validators import (
    get_preset_pattern,
    get_preset_description,
    validate_with_preset,
    list_available_presets,
    PRESET_PATTERNS,
)


class TestPresetPatternLookup:
    """Test helper functions for preset patterns."""
    
    def test_get_preset_pattern_exists(self):
        """Should return pattern for existing preset."""
        pattern = get_preset_pattern("email")
        assert pattern is not None
        assert isinstance(pattern, str)
    
    def test_get_preset_pattern_missing(self):
        """Should return None for non-existent preset."""
        pattern = get_preset_pattern("nonexistent")
        assert pattern is None
    
    def test_get_preset_description_exists(self):
        """Should return description for existing preset."""
        desc = get_preset_description("email")
        assert desc is not None
        assert "email" in desc.lower()
    
    def test_get_preset_description_missing(self):
        """Should return None for non-existent preset."""
        desc = get_preset_description("nonexistent")
        assert desc is None
    
    def test_list_available_presets(self):
        """Should return all available presets."""
        presets = list_available_presets()
        assert len(presets) == len(PRESET_PATTERNS)
        assert "email" in presets
        assert "phone" in presets


class TestEmailValidators:
    """Test email validation patterns."""
    
    @pytest.mark.parametrize("email", [
        "user@example.com",
        "test.user@example.com",
        "user+tag@example.co.uk",
        "user_name@example-domain.com",
        "123@example.com",
    ])
    def test_email_valid(self, email):
        """Should accept valid email addresses."""
        is_valid, error = validate_with_preset(email, "email")
        assert is_valid, f"Email '{email}' should be valid: {error}"
    
    @pytest.mark.parametrize("invalid_email", [
        "notanemail",
        "@example.com",
        "user@",
        "user @example.com",
        "user@example",
        "Researching...",
    ])
    def test_email_invalid(self, invalid_email):
        """Should reject invalid email addresses."""
        is_valid, error = validate_with_preset(invalid_email, "email")
        assert not is_valid
        assert error is not None


class TestPhoneValidators:
    """Test phone number validation patterns."""
    
    @pytest.mark.parametrize("phone", [
        "+14155551234",
        "415-555-1234",
        "(415) 555-1234",
        "415.555.1234",
        "4155551234",
        "+1 (415) 555-1234",
    ])
    def test_phone_loose_valid(self, phone):
        """Should accept various phone formats (loose matching)."""
        is_valid, error = validate_with_preset(phone, "phone")
        assert is_valid, f"Phone '{phone}' should be valid: {error}"
    
    @pytest.mark.parametrize("phone", [
        "415-555-1234",
        "(415) 555-1234",
        "415.555.1234",
        "4155551234",
        "+1-415-555-1234",
    ])
    def test_phone_us_valid(self, phone):
        """Should accept US phone formats."""
        is_valid, error = validate_with_preset(phone, "phone_us")
        assert is_valid, f"US phone '{phone}' should be valid: {error}"
    
    @pytest.mark.parametrize("phone", [
        "+14155551234",
        "+442071234567",
        "+33123456789",
    ])
    def test_phone_international_valid(self, phone):
        """Should accept E.164 international format."""
        is_valid, error = validate_with_preset(phone, "phone_international")
        assert is_valid, f"International phone '{phone}' should be valid: {error}"
    
    @pytest.mark.parametrize("invalid_phone", [
        "123",  # Too short
        "abc",
        "TBD",
    ])
    def test_phone_invalid(self, invalid_phone):
        """Should reject invalid phone numbers."""
        is_valid, error = validate_with_preset(invalid_phone, "phone")
        assert not is_valid


class TestIdentifierValidators:
    """Test identifier and code validators."""
    
    @pytest.mark.parametrize("uuid", [
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "123e4567-e89b-12d3-a456-426614174000",
    ])
    def test_uuid_valid(self, uuid):
        """Should accept valid UUIDs."""
        is_valid, error = validate_with_preset(uuid, "uuid")
        assert is_valid, f"UUID '{uuid}' should be valid: {error}"
    
    @pytest.mark.parametrize("ssn", [
        "123-45-6789",
        "987-65-4321",
    ])
    def test_ssn_valid(self, ssn):
        """Should accept valid SSN format."""
        is_valid, error = validate_with_preset(ssn, "ssn")
        assert is_valid, f"SSN '{ssn}' should be valid: {error}"
    
    @pytest.mark.parametrize("ein", [
        "12-3456789",
        "98-7654321",
    ])
    def test_ein_valid(self, ein):
        """Should accept valid EIN format."""
        is_valid, error = validate_with_preset(ein, "ein")
        assert is_valid, f"EIN '{ein}' should be valid: {error}"
    
    @pytest.mark.parametrize("zip_code", [
        "12345",
        "12345-6789",
        "90210",
    ])
    def test_postal_code_us_valid(self, zip_code):
        """Should accept valid US ZIP codes."""
        is_valid, error = validate_with_preset(zip_code, "postal_code_us")
        assert is_valid, f"ZIP '{zip_code}' should be valid: {error}"
    
    @pytest.mark.parametrize("postal", [
        "K1A 0B1",
        "K1A0B1",
        "M5W 1E6",
        "M5W-1E6",
    ])
    def test_postal_code_ca_valid(self, postal):
        """Should accept valid Canadian postal codes."""
        is_valid, error = validate_with_preset(postal, "postal_code_ca")
        assert is_valid, f"Postal '{postal}' should be valid: {error}"


class TestWebNetworkValidators:
    """Test web and network validators."""
    
    @pytest.mark.parametrize("url", [
        "https://example.com",
        "http://example.com/path",
        "https://example.com/path?query=value",
        "http://sub.example.com:8080/path",
    ])
    def test_url_valid(self, url):
        """Should accept valid URLs."""
        is_valid, error = validate_with_preset(url, "url")
        assert is_valid, f"URL '{url}' should be valid: {error}"
    
    @pytest.mark.parametrize("domain", [
        "example.com",
        "sub.example.com",
        "example.co.uk",
        "test-domain.com",
    ])
    def test_domain_valid(self, domain):
        """Should accept valid domain names."""
        is_valid, error = validate_with_preset(domain, "domain")
        assert is_valid, f"Domain '{domain}' should be valid: {error}"
    
    @pytest.mark.parametrize("ip", [
        "192.168.1.1",
        "10.0.0.1",
        "8.8.8.8",
        "255.255.255.255",
    ])
    def test_ipv4_valid(self, ip):
        """Should accept valid IPv4 addresses."""
        is_valid, error = validate_with_preset(ip, "ipv4")
        assert is_valid, f"IPv4 '{ip}' should be valid: {error}"
    
    @pytest.mark.parametrize("ip", [
        "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        "::1",
        "2001:db8::8a2e:370:7334",
    ])
    def test_ipv6_valid(self, ip):
        """Should accept valid IPv6 addresses."""
        is_valid, error = validate_with_preset(ip, "ipv6")
        assert is_valid, f"IPv6 '{ip}' should be valid: {error}"


class TestFinancialValidators:
    """Test financial validators."""
    
    @pytest.mark.parametrize("card", [
        "4111111111111111",
        "4111-1111-1111-1111",
        "4111 1111 1111 1111",
        "5500 0000 0000 0004",
    ])
    def test_credit_card_valid(self, card):
        """Should accept valid credit card formats."""
        is_valid, error = validate_with_preset(card, "credit_card")
        assert is_valid, f"Card '{card}' should be valid: {error}"
    
    @pytest.mark.parametrize("amount", [
        "$1,234.56",
        "1234.56",
        "$1234.56",
        "1,234.56",
        "1234",
    ])
    def test_currency_usd_valid(self, amount):
        """Should accept valid USD amounts."""
        is_valid, error = validate_with_preset(amount, "currency_usd")
        assert is_valid, f"Amount '{amount}' should be valid: {error}"
    
    @pytest.mark.parametrize("iban", [
        "GB82WEST12345698765432",
        "DE89370400440532013000",
        "FR1420041010050500013M02606",
    ])
    def test_iban_valid(self, iban):
        """Should accept valid IBAN formats."""
        is_valid, error = validate_with_preset(iban, "iban")
        assert is_valid, f"IBAN '{iban}' should be valid: {error}"


class TestDataFormatValidators:
    """Test data format validators."""
    
    @pytest.mark.parametrize("date", [
        "2024-01-15",
        "2024-12-31",
        "2023-06-30",
    ])
    def test_date_iso_valid(self, date):
        """Should accept valid ISO dates."""
        is_valid, error = validate_with_preset(date, "date_iso")
        assert is_valid, f"Date '{date}' should be valid: {error}"
    
    @pytest.mark.parametrize("date", [
        "01/15/2024",
        "12/31/2024",
        "06/30/2023",
    ])
    def test_date_us_valid(self, date):
        """Should accept valid US dates."""
        is_valid, error = validate_with_preset(date, "date_us")
        assert is_valid, f"Date '{date}' should be valid: {error}"
    
    @pytest.mark.parametrize("time", [
        "23:59:59",
        "00:00:00",
        "12:30",
        "09:15:30",
    ])
    def test_time_24h_valid(self, time):
        """Should accept valid 24-hour times."""
        is_valid, error = validate_with_preset(time, "time_24h")
        assert is_valid, f"Time '{time}' should be valid: {error}"
    
    @pytest.mark.parametrize("color", [
        "#FF5733",
        "#000",
        "#fff",
        "#123ABC",
    ])
    def test_hex_color_valid(self, color):
        """Should accept valid hex colors."""
        is_valid, error = validate_with_preset(color, "hex_color")
        assert is_valid, f"Color '{color}' should be valid: {error}"
    
    @pytest.mark.parametrize("slug", [
        "my-article-title",
        "hello-world",
        "product-name-123",
    ])
    def test_slug_valid(self, slug):
        """Should accept valid URL slugs."""
        is_valid, error = validate_with_preset(slug, "slug")
        assert is_valid, f"Slug '{slug}' should be valid: {error}"


class TestBusinessIDValidators:
    """Test custom business ID validators."""
    
    @pytest.mark.parametrize("id_val", [
        "ABC123",
        "TTT12345",
        "ID0001",
        "XYZ789ABC",
    ])
    def test_alphanumeric_id_valid(self, id_val):
        """Should accept alphanumeric IDs."""
        is_valid, error = validate_with_preset(id_val, "alphanumeric_id")
        assert is_valid, f"ID '{id_val}' should be valid: {error}"
    
    @pytest.mark.parametrize("id_val", [
        "ABC-123",
        "TTT_12345",
        "ID-0001-XL",
    ])
    def test_alphanumeric_id_invalid(self, id_val):
        """Should reject IDs with special characters (use SKU instead)."""
        is_valid, error = validate_with_preset(id_val, "alphanumeric_id")
        assert not is_valid
    
    @pytest.mark.parametrize("sku", [
        "SKU-12345",
        "PROD_ABC123",
        "ITEM-XYZ-789",
        "SKU12345XL",
    ])
    def test_sku_valid(self, sku):
        """Should accept SKU formats."""
        is_valid, error = validate_with_preset(sku, "sku")
        assert is_valid, f"SKU '{sku}' should be valid: {error}"


class TestNullHandling:
    """Test null and empty value handling."""
    
    def test_null_value_allowed(self):
        """Should accept None when allow_null is True."""
        is_valid, error = validate_with_preset(None, "email", allow_null=True)
        assert is_valid
        assert error is None
    
    def test_null_value_not_allowed(self):
        """Should reject None when allow_null is False."""
        is_valid, error = validate_with_preset(None, "email", allow_null=False)
        assert not is_valid
        assert "required" in error.lower()
    
    def test_empty_string_allowed(self):
        """Should accept empty string when allow_null is True."""
        is_valid, error = validate_with_preset("", "email", allow_null=True)
        assert is_valid
    
    def test_empty_string_not_allowed(self):
        """Should reject empty string when allow_null is False."""
        is_valid, error = validate_with_preset("", "email", allow_null=False)
        assert not is_valid
    
    def test_whitespace_treated_as_empty(self):
        """Should treat whitespace-only strings as empty."""
        is_valid, error = validate_with_preset("   ", "email", allow_null=False)
        assert not is_valid


class TestInvalidPresetHandling:
    """Test handling of unknown preset names."""
    
    def test_unknown_preset_name(self):
        """Should return error for unknown preset."""
        is_valid, error = validate_with_preset("test", "unknown_validator")
        assert not is_valid
        assert "unknown" in error.lower()


class TestIntegrationWithMapper:
    """Test that preset validators work with the mapper validation flow."""
    
    def test_validation_rule_with_preset(self):
        """Should work with ValidationRule schema."""
        from app.api.schemas.shared import ValidationRule
        
        # Create a validation rule
        rule = ValidationRule(
            column="email",
            validator="email",
            allow_null=False
        )
        
        assert rule.validator == "email"
        assert rule.column == "email"
        assert not rule.allow_null
    
    def test_all_presets_in_literal(self):
        """Should verify all presets are in the Literal type."""
        from app.api.schemas.shared import ValidationRule
        import typing
        
        # Get the Literal type from ValidationRule
        validator_field = ValidationRule.__annotations__['validator']
        
        # Extract literal values (this is somewhat hacky but works for testing)
        # In Python 3.8+, we can check Literal args
        if hasattr(validator_field, '__args__'):
            literal_values = validator_field.__args__
            
            # Check that key presets are included
            assert "email" in literal_values
            assert "phone" in literal_values
            assert "uuid" in literal_values
            assert "url" in literal_values
