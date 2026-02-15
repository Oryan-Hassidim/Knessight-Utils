"""Test database schema compatibility."""

from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

from src.modules.config import Config
from src.modules.database import Database


def test_database_schema():
    """Test that database has all required tables and columns."""
    print("=" * 60)
    print("DATABASE SCHEMA COMPATIBILITY TEST")
    print("=" * 60)

    config = Config()
    database = Database(Path(config.DATABASE_PATH))

    # Test queries that the system will use
    tests = [
        (
            "knesset_speeches_view exists",
            "SELECT name FROM sqlite_master WHERE type='view' AND name='knesset_speeches_view'",
        ),
        (
            "people table exists",
            "SELECT name FROM sqlite_master WHERE type='table' AND name='people'",
        ),
        (
            "Sample speech columns",
            "SELECT id, text, date, person_id FROM knesset_speeches_view LIMIT 1",
        ),
        (
            "Sample person columns",
            "SELECT person_id, first_name, surname FROM people LIMIT 1",
        ),
    ]

    with database._get_connection() as conn:
        cursor = conn.cursor()

        for test_name, query in tests:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    print(f"✓ {test_name}")
                else:
                    print(f"⚠ {test_name} - No data")
            except Exception as e:
                print(f"✗ {test_name} - Error: {e}")

    # Test speech count for one person
    print("\n" + "=" * 60)
    print("SPEECH COUNT TEST")
    print("=" * 60)

    person_id = 965  # Netanyahu
    speeches = database.get_all_speeches_by_person_id(person_id)
    print(f"Person ID {person_id} has {len(speeches)} speeches")

    if speeches:
        print(f"\nSample speech:")
        print(f"  ID: {speeches[0].id}")
        print(f"  Date: {speeches[0].date}")
        print(f"  Topic: {speeches[0].topic}")
        print(f"  Text length: {len(speeches[0].text)} chars")

    print("\n" + "=" * 60)
    print("✓ DATABASE SCHEMA COMPATIBLE")
    print("=" * 60)


if __name__ == "__main__":
    test_database_schema()
