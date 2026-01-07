"""
Debug script to check the current state of data_validation_errors in uploaded_files.
"""
from sqlalchemy import text
from app.db.session import get_engine

def check_validation_errors():
    """
    Check current state of data_validation_errors for all mapped files.
    """
    engine = get_engine()
    
    with engine.connect() as conn:
        # Get all mapped files and their validation error counts
        result = conn.execute(text("""
            SELECT 
                uf.id,
                uf.file_name,
                uf.status,
                uf.mapped_table_name,
                uf.data_validation_errors,
                uf.duplicates_found,
                uf.mapping_errors
            FROM uploaded_files uf
            ORDER BY uf.upload_date DESC
        """))
        
        files = result.fetchall()
        print(f"Found {len(files)} total files\n")
        
        for row in files:
            file_id = str(row[0])
            file_name = row[1]
            status = row[2]
            table_name = row[3]
            validation_errors = row[4]
            duplicates = row[5]
            mapping_errors = row[6]
            
            print(f"File: {file_name}")
            print(f"  ID: {file_id}")
            print(f"  Status: {status}")
            print(f"  Table: {table_name}")
            print(f"  Validation Errors: {validation_errors} (type: {type(validation_errors).__name__})")
            print(f"  Duplicates: {duplicates} (type: {type(duplicates).__name__})")
            print(f"  Mapping Errors: {mapping_errors} (type: {type(mapping_errors).__name__})")
            print()


if __name__ == "__main__":
    try:
        check_validation_errors()
    except Exception as exc:
        print(f"❌ Check failed: {exc}")
        import traceback
        traceback.print_exc()
