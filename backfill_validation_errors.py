"""
Backfill script to populate data_validation_errors in uploaded_files table
from the import_history table for files that have been mapped but are missing this value.
"""
import sys
from sqlalchemy import text
from app.db.session import get_engine

def backfill_validation_errors():
    """
    Sync data_validation_errors from import_history to uploaded_files for mapped files.
    Also handles archive/container files (ZIP, Excel workbooks) which don't map to tables directly.
    """
    engine = get_engine()
    
    with engine.begin() as conn:
        # Find mapped files with NULL data_validation_errors, duplicates_found, or mapping_errors
        result = conn.execute(text("""
            SELECT 
                uf.id,
                uf.file_name,
                uf.mapped_table_name,
                uf.file_hash,
                uf.data_validation_errors,
                uf.duplicates_found,
                uf.mapping_errors
            FROM uploaded_files uf
            WHERE uf.status = 'mapped'
            AND (
                uf.data_validation_errors IS NULL
                OR uf.duplicates_found IS NULL
                OR uf.mapping_errors IS NULL
            )
        """))
        
        files_to_update = result.fetchall()
        print(f"Found {len(files_to_update)} mapped files with NULL data_validation_errors")
        
        updated_count = 0
        not_found_count = 0
        
        for row in files_to_update:
            file_id = str(row[0])
            file_name = row[1]
            table_name = row[2]
            file_hash = row[3]
            
            # Try to find matching import_history record
            # First try by file_hash if available
            history_row = None
            if file_hash and table_name:
                history_result = conn.execute(text("""
                    SELECT data_validation_errors
                    FROM import_history
                    WHERE table_name = :table_name
                    AND file_hash = :file_hash
                    ORDER BY import_timestamp DESC
                    LIMIT 1
                """), {"table_name": table_name, "file_hash": file_hash})
                history_row = history_result.fetchone()
            
            # If not found, try by file_name and table_name
            if not history_row and table_name:
                history_result = conn.execute(text("""
                    SELECT data_validation_errors
                    FROM import_history
                    WHERE table_name = :table_name
                    AND file_name = :file_name
                    ORDER BY import_timestamp DESC
                    LIMIT 1
                """), {"table_name": table_name, "file_name": file_name})
                history_row = history_result.fetchone()
            
            if history_row:
                validation_errors = history_row[0] if history_row[0] is not None else 0
                
                # Update the uploaded_files record - also set other NULL columns to 0
                conn.execute(text("""
                    UPDATE uploaded_files
                    SET data_validation_errors = COALESCE(data_validation_errors, :validation_errors),
                        duplicates_found = COALESCE(duplicates_found, 0),
                        mapping_errors = COALESCE(mapping_errors, 0)
                    WHERE id = :file_id
                """), {
                    "validation_errors": validation_errors,
                    "file_id": file_id
                })
                
                updated_count += 1
                print(f"✓ Updated {file_name}: set data_validation_errors = {validation_errors}")
            else:
                # No history found (e.g., archive/container files), default all to 0
                conn.execute(text("""
                    UPDATE uploaded_files
                    SET data_validation_errors = COALESCE(data_validation_errors, 0),
                        duplicates_found = COALESCE(duplicates_found, 0),
                        mapping_errors = COALESCE(mapping_errors, 0)
                    WHERE id = :file_id
                """), {"file_id": file_id})
                
                not_found_count += 1
                print(f"⚠ No history found for {file_name} (likely archive/container), defaulting all to 0")
        
        print(f"\nBackfill complete:")
        print(f"  - {updated_count} files updated from import_history")
        print(f"  - {not_found_count} files defaulted to 0 (no history found)")
        print(f"  - Total: {updated_count + not_found_count} files processed")


if __name__ == "__main__":
    try:
        backfill_validation_errors()
        print("\n✅ Backfill completed successfully")
        sys.exit(0)
    except Exception as exc:
        print(f"\n❌ Backfill failed: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
