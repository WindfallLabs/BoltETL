import hashlib
import os
from datetime import datetime

import duckdb
from rich.console import Console


DATA = r"C:\Workspace\tmpdb\Data\raw"

IGNORED = {
    ".cpg",
    ".dbf",
    ".prj",
    ".sbn",
    ".sbx",
    ".shx",
    ".pdf",
    ".html",
    ".xml",
}

console = Console()


class FileTracker:
    def __init__(self, db_path="file_tracker.duckdb"):
        """Initialize the file tracker with a SQLite database."""
        self.db_path = db_path
        self.setup_database()

    def setup_database(self):
        """Create the SQLite database and table if they don't exist."""
        with duckdb.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    file_path TEXT PRIMARY KEY,
                    sha256_hash TEXT NOT NULL,
                    modified_date TEXT NOT NULL
                )
            """)

    def calculate_file_hash(self, file_path):
        """Calculate SHA256 hash of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def get_current_files(self):
        """Get current files and their information, excluding .gdb folders."""
        current_files = {}
        for root, dirs, files in os.walk(DATA):
            # Skip .gdb directories
            dirs[:] = [d for d in dirs if not d.endswith('.gdb')]
            
            for file in files:
                if any(file.endswith(ext) for ext in IGNORED):
                    continue
                if file.startswith("_"):
                    continue
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, DATA)
                
                try:
                    current_files[rel_path] = {
                        'hash': self.calculate_file_hash(full_path),
                        'modified': datetime.fromtimestamp(
                            os.path.getmtime(full_path)
                        ).isoformat()
                    }
                except (PermissionError, FileNotFoundError):
                    continue
                
        return current_files

    def get_tracked_files(self):
        """Get all tracked files from the database."""
        with duckdb.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT file_path, sha256_hash, modified_date FROM files"
            )
            return {
                row[0]: {'hash': row[1], 'modified': row[2]} 
                for row in cursor.fetchall()
            }

    def update_database(self, current_files):
        """Update the database with the current state of files."""
        with duckdb.connect(self.db_path) as conn:
            # Clear existing entries
            conn.execute("DELETE FROM files")
            
            # Insert current files
            conn.executemany(
                "INSERT INTO files (file_path, sha256_hash, modified_date) VALUES (?, ?, ?)",
                [(path, info['hash'], info['modified']) 
                 for path, info in current_files.items()]
            )

    def status(self):
        """Check the status of files compared to the last tracked state."""
        current_files = self.get_current_files()
        tracked_files = self.get_tracked_files()
        
        changes = {
            'new_files': [],
            'modified': [],
            'removed': []
        }
        
        # Check for new and modified files
        for file_path, current_info in current_files.items():
            if file_path not in tracked_files:
                changes['new_files'].append(file_path)
            elif current_info['hash'] != tracked_files[file_path]['hash']:
                changes['modified'].append(file_path)
        
        # Check for removed files
        for file_path in tracked_files:
            if file_path not in current_files:
                changes['removed'].append(file_path)
        
        # Print changes
        if changes['new_files']:
            status = "new file:"
            #console.print("\nNew files:")
            for file in sorted(changes['new_files']):
                #console.print(f"    [red]{file}[/]")
                console.print(f"        [red]{status.ljust(11)} {file}[/]")
        
        if changes['modified']:
            status = "modified:"
            #console.print("\nModified files:")
            for file in sorted(changes['modified']):
                #console.print(f"    [red]{file}[/]")
                console.print(f"        [red]{status.ljust(11)} {file}[/]")
        
        if changes['removed']:
            status = "removed:"
            #console.print("\nRemoved files:")
            for file in sorted(changes['removed']):
                #console.print(f"    [red]{file}[/]")
                console.print(f"        [red]{status.ljust(11)} {file}[/]")
        
        if not any(changes.values()):
            console.print("\n[green]No changes detected.[/]")
        
        return changes

    def commit_changes(self):
        """Update the database with the current state of files."""
        current_files = self.get_current_files()
        #self.update_database(current_files)

        with duckdb.connect(self.db_path) as conn:
            # Clear existing entries
            conn.execute("DELETE FROM files")
            
            # Insert current files
            conn.executemany(
                "INSERT INTO files (file_path, sha256_hash, modified_date) VALUES (?, ?, ?)",
                [(path, info['hash'], info['modified']) 
                 for path, info in current_files.items()]
            )

        # TODO: 158 files updated is misleading...
        console.print(f"[green]Changes committed:[/] {len(current_files)}")


if __name__ == "__main__":
    tracker = FileTracker()
    # For now, we'll just comment these out to test
    #tracker.status()
    tracker.commit_changes()
    #...
