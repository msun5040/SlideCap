"""
Hashing service for PHI-safe identifier storage.
Uses SHA-256 with a salt stored on the network drive.
"""
import hashlib
import os
from pathlib import Path


class SlideHasher:
    """
    Creates consistent, non-reversible hashes of accession numbers.
    
    The salt is stored on the network drive, so:
    - Anyone with DB access but no network access can't reverse hashes
    - Anyone with network access already has access to the filenames anyway
    """
    
    def __init__(self, salt_path: Path):
        self.salt_path = Path(salt_path)
        self.salt = self._load_or_create_salt()
    
    def _load_or_create_salt(self) -> str:
        """Load existing salt or create a new one."""
        if self.salt_path.exists():
            with open(self.salt_path, 'r') as f:
                return f.read().strip()
        else:
            # Create a new random salt
            salt = hashlib.sha256(os.urandom(32)).hexdigest()
            
            # Ensure directory exists
            self.salt_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save salt
            with open(self.salt_path, 'w') as f:
                f.write(salt)
            
            print(f"Created new salt at {self.salt_path}")
            return salt
    
    def hash_accession(self, accession: str) -> str:
        """
        Create a hash of an accession number (or any identifier).
        
        Args:
            accession: The identifier to hash (e.g., "S24-12345")
            
        Returns:
            64-character hex string (SHA-256)
        """
        salted = f"{self.salt}:{accession}"
        return hashlib.sha256(salted.encode()).hexdigest()
    
    def hash_slide_stem(self, stem: str) -> str:
        """
        Create a hash of a full slide filename stem.
        
        Args:
            stem: Filename without extension (e.g., "S24-12345_A1_HE_7f3a2b")
            
        Returns:
            64-character hex string (SHA-256)
        """
        return self.hash_accession(stem)  # Same algorithm, different semantic


# Quick test
if __name__ == "__main__":
    from config import settings
    
    hasher = SlideHasher(settings.salt_path)
    
    test_accession = "S24-12345"
    hash1 = hasher.hash_accession(test_accession)
    hash2 = hasher.hash_accession(test_accession)
    
    print(f"Accession: {test_accession}")
    print(f"Hash: {hash1}")
    print(f"Consistent: {hash1 == hash2}")
