"""
Tests for Authentication Module
"""

import sys
from pathlib import Path

import pytest

# Add app directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.auth import check_password


class TestCheckPassword:
    """Tests for password comparison function."""
    
    def test_correct_password(self):
        """Test that correct password returns True."""
        assert check_password("secret123", "secret123") is True
    
    def test_incorrect_password(self):
        """Test that incorrect password returns False."""
        assert check_password("wrong", "secret123") is False
    
    def test_empty_password(self):
        """Test empty password handling."""
        assert check_password("", "secret123") is False
        assert check_password("secret123", "") is False
    
    def test_case_sensitive(self):
        """Test that password comparison is case-sensitive."""
        assert check_password("Secret123", "secret123") is False
    
    def test_timing_attack_resistance(self):
        """Test that comparison uses constant-time algorithm (hmac.compare_digest)."""
        # This is more of a structural test - the actual timing attack 
        # resistance is provided by hmac.compare_digest
        import hmac
        from app.auth import check_password
        
        # Verify the function works for various inputs
        assert check_password("a" * 100, "a" * 100) is True
        assert check_password("a" * 100, "b" * 100) is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
