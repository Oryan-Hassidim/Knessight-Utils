"""Database access layer for Knesset speeches."""

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from contextlib import contextmanager


@dataclass
class Speech:
    """Represents a speech from knesset_speeches_view."""

    id: int
    name: str
    text: str
    knesset: int
    session_number: Optional[int]
    date: str
    person_id: int
    topic: Optional[str] = None
    topic_extra: Optional[str] = None
    chair: int = 0
    qa: Optional[int] = None


class Database:
    """Read-only SQLite database access for Knesset data."""

    def __init__(self, db_path: Path):
        """Initialize database connection.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found at {db_path}")

        # Test connection
        self._test_connection()

    def _test_connection(self):
        """Test database connection and verify schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Check if knesset_speeches_view exists
            cursor.execute(
                """
                SELECT name FROM sqlite_master 
                WHERE type='view' AND name='knesset_speeches_view'
            """
            )
            if not cursor.fetchone():
                raise ValueError("Database missing knesset_speeches_view")

    @contextmanager
    def _get_connection(self):
        """Get read-only database connection.

        Yields:
            SQLite connection
        """
        # Open in read-only mode
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def get_all_speeches_by_person_id(self, person_id: int) -> List[Speech]:
        """Retrieve all speeches by a specific person.

        Args:
            person_id: The person_id from people table

        Returns:
            List of Speech objects
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 
                    k.id,
                    k.name,
                    k.text,
                    k.knesset,
                    k.session_number,
                    k.date,
                    k.person_id,
                    k.topic,
                    k.topic_extra,
                    k.chair,
                    k.qa
                FROM knesset_speeches_view k
                WHERE k.person_id = ?
                ORDER BY k.date, k.id
            """,
                (person_id,),
            )

            speeches = []
            for row in cursor.fetchall():
                speeches.append(
                    Speech(
                        id=row["id"],
                        name=row["name"],
                        text=row["text"] or "",
                        knesset=row["knesset"],
                        session_number=row["session_number"],
                        date=row["date"],
                        person_id=row["person_id"],
                        topic=row["topic"],
                        topic_extra=row["topic_extra"],
                        chair=row["chair"],
                        qa=row["qa"],
                    )
                )

            return speeches

    def get_speeches_by_ids(self, speech_ids: List[int]) -> Dict[int, str]:
        """Retrieve speech texts by specific IDs.

        Args:
            speech_ids: List of speech IDs to retrieve

        Returns:
            Dictionary mapping speech_id to speech text
        """
        if not speech_ids:
            return {}

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Build placeholders for IN clause
            placeholders = ",".join("?" * len(speech_ids))

            cursor.execute(
                f"""
                SELECT id, text
                FROM knesset_speeches_view
                WHERE id IN ({placeholders})
            """,
                speech_ids,
            )

            return {row["id"]: row["text"] or "" for row in cursor.fetchall()}

    def get_speech_metadata(self, speech_id: int) -> Optional[Dict]:
        """Get metadata for a specific speech.

        Args:
            speech_id: The speech ID

        Returns:
            Dictionary with speech metadata (id, date, topic, person_id)
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, date, topic, person_id
                FROM knesset_speeches_view
                WHERE id = ?
            """,
                (speech_id,),
            )

            row = cursor.fetchone()
            if row:
                return {
                    "id": row["id"],
                    "date": row["date"],
                    "topic": row["topic"],
                    "person_id": row["person_id"],
                }
            return None

    def get_person_metadata(self, person_id: int) -> Optional[Dict]:
        """Get metadata for a specific person (MK).

        Args:
            person_id: The person_id from people table

        Returns:
            Dictionary with person details
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT DISTINCT
                    person_id,
                    first_name,
                    surname,
                    gender,
                    faction,
                    party_name,
                    dob,
                    city
                FROM people
                WHERE person_id = ?
                LIMIT 1
            """,
                (person_id,),
            )

            row = cursor.fetchone()
            if row:
                return {
                    "id": row["person_id"],
                    "knessetSiteId": row["person_id"],
                    "first_name": row["first_name"],
                    "surname": row["surname"],
                    "name": f"{row['first_name']} {row['surname']}",
                    "gender": row["gender"],
                    "faction": row["faction"],
                    "party_name": row["party_name"],
                    "dob": row["dob"],
                    "city": row["city"],
                }
            return None

    def search_people_by_name(self, name: str) -> List[Dict]:
        """Search for people by name (fuzzy matching at SQL level).

        Args:
            name: Name to search for

        Returns:
            List of matching people with their details
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Split name into parts for flexible matching
            name_parts = name.strip().split()

            # Build LIKE conditions for flexible matching
            conditions = []
            params = []
            for part in name_parts:
                conditions.append("(first_name LIKE ? OR surname LIKE ?)")
                params.extend([f"%{part}%", f"%{part}%"])

            query = f"""
                SELECT DISTINCT
                    person_id,
                    first_name,
                    surname,
                    faction,
                    party_name
                FROM people
                WHERE {' OR '.join(conditions)}
                ORDER BY person_id
            """

            cursor.execute(query, params)

            results = []
            for row in cursor.fetchall():
                results.append(
                    {
                        "person_id": row["person_id"],
                        "first_name": row["first_name"],
                        "surname": row["surname"],
                        "name": f"{row['first_name']} {row['surname']}",
                        "faction": row["faction"],
                        "party_name": row["party_name"],
                    }
                )

            return results

    def get_all_person_ids(self) -> List[int]:
        """Get all unique person_ids from people table.

        Returns:
            List of person_ids
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT person_id FROM people ORDER BY person_id")
            return [row["person_id"] for row in cursor.fetchall()]
