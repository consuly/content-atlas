"""
Migration script to add file_hash column to uploaded_files table.
Run this once to update the existing database schema.
"""
from app.db.session import get_engine
from sqlalchemy import text

def migrate_add_file_hash():
    """Add file_hash column to uploaded_files table."""
    engine = get_engine()
    
    migration_sql = """
    -- Add file_hash column if it doesn't exist
    DO $$ 
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'uploaded_files' 
            AND column_name = 'file_hash'
        ) THEN
            ALTER TABLE uploaded_files ADD COLUMN file_hash VARCHAR(64);
            CREATE INDEX idx_uploaded_files_file_hash ON uploaded_files(file_hash);
            PRINT 'Added file_hash column and index';
        ELSE
            PRINT 'file_hash column already exists';
        END IF;
    END $$;
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(migration_sql))
            conn.commit()
            print("✓ Migration completed successfully")
            print("✓ Added file_hash column to uploaded_files table")
            print("✓ Created index on file_hash column")
    except Exception as e:
        print(f"✗ Migration failed: {e}")
        raise

if __name__ == "__main__":
    print("Running migration: Add file_hash column to uploaded_files table")
    print("=" * 80)
    migrate_add_file_hash()
    print("=" * 80)
    print("Migration complete!")
