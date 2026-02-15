"""Test database connection and MK name extraction."""

from pathlib import Path
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

from src.modules.config import Config
from src.modules.database import Database
from src.modules.disambiguation import Disambiguation


def test_database_connection():
    """Test database connection."""
    print("=" * 60)
    print("TEST 1: Database Connection")
    print("=" * 60)

    config = Config()
    db_path = config.DATABASE_PATH

    print(f"Database path from .env: {db_path}")
    print(f"Database path exists: {Path(db_path).exists()}")

    try:
        database = Database(Path(db_path))
        print("✓ Database connection successful!")
        print(f"  Database file: {database.db_path}")
        return database
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return None


def test_people_search(database):
    """Test searching for people in database."""
    print("\n" + "=" * 60)
    print("TEST 2: People Search")
    print("=" * 60)

    test_names = ["יצחק רבין", "נתניהו", "פרס"]

    for name in test_names:
        print(f"\nSearching for: {name}")
        try:
            results = database.search_people_by_name(name)
            print(f"  Found {len(results)} matches:")
            for person in results[:3]:  # Show first 3
                print(f"    - {person['name']} (person_id: {person['person_id']})")
        except Exception as e:
            print(f"  Error: {e}")


def test_speeches_by_person(database):
    """Test retrieving speeches by person_id."""
    print("\n" + "=" * 60)
    print("TEST 3: Retrieve Speeches")
    print("=" * 60)

    # Try first person
    try:
        results = database.search_people_by_name("רבין")
        if results:
            person_id = results[0]["person_id"]
            person_name = results[0]["name"]
            print(f"\nRetrieving speeches for: {person_name} (person_id: {person_id})")

            speeches = database.get_all_speeches_by_person_id(person_id)
            print(f"  Total speeches: {len(speeches)}")

            if speeches:
                print(f"  First speech preview:")
                speech = speeches[0]
                print(f"    Date: {speech.date}")
                print(f"    Topic: {speech.topic}")
                print(f"    Text: {speech.text[:100]}...")
    except Exception as e:
        print(f"  Error: {e}")


def test_mk_disambiguation():
    """Test MK name disambiguation."""
    print("\n" + "=" * 60)
    print("TEST 4: MK Name Disambiguation")
    print("=" * 60)

    config = Config()
    database = Database(Path(config.DATABASE_PATH))
    disambiguation = Disambiguation(database)

    # Load MK names from input file
    input_file = Path("input/mks.txt")

    if not input_file.exists():
        print(f"✗ Input file not found: {input_file}")
        return

    print(f"\nLoading MK names from: {input_file}")
    mk_names = disambiguation.load_mk_list_from_file(input_file)
    print(f"Found {len(mk_names)} MK names to resolve\n")

    try:
        resolved = disambiguation.resolve_mk_names(mk_names)
        print(f"\n✓ Successfully resolved {len(resolved)}/{len(mk_names)} MK names")

        print("\nResolved mappings:")
        for name, person_id in resolved.items():
            print(f"  {name} → person_id {person_id}")

    except Exception as e:
        print(f"✗ Error during disambiguation: {e}")


def main():
    """Run all tests."""
    print("\n🧪 TESTING DATABASE AND MK EXTRACTION")
    print("=" * 60)

    # Test 1: Database connection
    database = test_database_connection()

    if database:
        # Test 2: Search for people
        test_people_search(database)

        # Test 3: Get speeches
        test_speeches_by_person(database)

    # Test 4: Full disambiguation workflow
    test_mk_disambiguation()

    print("\n" + "=" * 60)
    print("✓ ALL TESTS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
