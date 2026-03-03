"""
Database layer for MoEngage metrics
SQLite storage with UPSERT logic for metrics, campaigns, and pull history
"""
import json
import logging
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

from config import DATABASE_PATH

logger = logging.getLogger(__name__)


class MoEngageDatabase:
    """SQLite database for MoEngage metrics"""

    def __init__(self, db_path: str = DATABASE_PATH):
        """Initialize database connection and create tables"""
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        """Create tables if they don't exist"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Segment metrics table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS segment_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    segment_type TEXT NOT NULL,
                    country TEXT NOT NULL,
                    user_count INTEGER,
                    segment_id TEXT,
                    created_at TEXT NOT NULL,
                    period_start TEXT,
                    period_end TEXT,
                    raw_json TEXT,
                    UNIQUE(segment_type, country, period_start, period_end)
                )
            """
            )

            # Campaign metrics table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS campaign_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id TEXT NOT NULL UNIQUE,
                    campaign_name TEXT,
                    country TEXT,
                    channel TEXT,
                    campaign_type TEXT,
                    sent INTEGER DEFAULT 0,
                    delivered INTEGER DEFAULT 0,
                    open INTEGER DEFAULT 0,
                    click INTEGER DEFAULT 0,
                    unsubscribe INTEGER DEFAULT 0,
                    bounced INTEGER DEFAULT 0,
                    failed INTEGER DEFAULT 0,
                    created_date TEXT,
                    period_start TEXT,
                    period_end TEXT,
                    created_at TEXT NOT NULL,
                    raw_json TEXT
                )
            """
            )

            # Transactional campaign mappings
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS transactional_campaigns (
                    campaign_id TEXT PRIMARY KEY,
                    campaign_name TEXT,
                    added_at TEXT NOT NULL
                )
            """
            )

            # Data pull history
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pull_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pull_id TEXT UNIQUE,
                    period_start TEXT NOT NULL,
                    period_end TEXT NOT NULL,
                    segments_fetched INTEGER,
                    campaigns_fetched INTEGER,
                    status TEXT,
                    error_message TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    raw_response TEXT
                )
            """
            )

            conn.commit()
            logger.info(f"Database initialized: {self.db_path}")

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ========================================================================
    # SEGMENT METRICS
    # ========================================================================

    def upsert_segment_metric(
        self,
        segment_type: str,
        country: str,
        user_count: Optional[int],
        segment_id: Optional[str],
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        raw_json: Optional[str] = None,
    ) -> int:
        """
        Insert or update segment metric
        Returns: row id
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.utcnow().isoformat()

            cursor.execute(
                """
                INSERT INTO segment_metrics
                (segment_type, country, user_count, segment_id, created_at,
                 period_start, period_end, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(segment_type, country, period_start, period_end)
                DO UPDATE SET
                    user_count = excluded.user_count,
                    segment_id = excluded.segment_id,
                    created_at = excluded.created_at,
                    raw_json = excluded.raw_json
            """,
                (
                    segment_type,
                    country,
                    user_count,
                    segment_id,
                    now,
                    period_start,
                    period_end,
                    raw_json,
                ),
            )

            conn.commit()
            logger.debug(f"Upserted segment metric: {segment_type} {country}")
            return cursor.lastrowid

    def get_segment_metric(
        self,
        segment_type: str,
        country: str,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
    ) -> Optional[Dict]:
        """Get segment metric"""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT * FROM segment_metrics
                WHERE segment_type = ? AND country = ?
                AND period_start = ? AND period_end = ?
                ORDER BY created_at DESC LIMIT 1
            """,
                (segment_type, country, period_start, period_end),
            )

            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_segment_metrics(
        self, period_start: Optional[str] = None, period_end: Optional[str] = None
    ) -> List[Dict]:
        """Get all segment metrics for a period"""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            if period_start and period_end:
                cursor.execute(
                    """
                    SELECT * FROM segment_metrics
                    WHERE period_start = ? AND period_end = ?
                    ORDER BY country, segment_type
                """,
                    (period_start, period_end),
                )
            else:
                cursor.execute(
                    """
                    SELECT * FROM segment_metrics
                    ORDER BY country, segment_type, created_at DESC
                """
                )

            return [dict(row) for row in cursor.fetchall()]

    # ========================================================================
    # CAMPAIGN METRICS
    # ========================================================================

    def upsert_campaign_metric(
        self,
        campaign_id: str,
        campaign_name: Optional[str],
        country: Optional[str],
        channel: Optional[str],
        campaign_type: Optional[str],
        sent: int = 0,
        delivered: int = 0,
        open: int = 0,
        click: int = 0,
        unsubscribe: int = 0,
        bounced: int = 0,
        failed: int = 0,
        created_date: Optional[str] = None,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        raw_json: Optional[str] = None,
    ) -> int:
        """
        Insert or update campaign metric
        Returns: row id
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.utcnow().isoformat()

            cursor.execute(
                """
                INSERT INTO campaign_metrics
                (campaign_id, campaign_name, country, channel, campaign_type,
                 sent, delivered, open, click, unsubscribe, bounced, failed,
                 created_date, period_start, period_end, created_at, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(campaign_id)
                DO UPDATE SET
                    sent = excluded.sent,
                    delivered = excluded.delivered,
                    open = excluded.open,
                    click = excluded.click,
                    unsubscribe = excluded.unsubscribe,
                    bounced = excluded.bounced,
                    failed = excluded.failed,
                    created_at = excluded.created_at,
                    raw_json = excluded.raw_json
            """,
                (
                    campaign_id,
                    campaign_name,
                    country,
                    channel,
                    campaign_type,
                    sent,
                    delivered,
                    open,
                    click,
                    unsubscribe,
                    bounced,
                    failed,
                    created_date,
                    period_start,
                    period_end,
                    now,
                    raw_json,
                ),
            )

            conn.commit()
            logger.debug(f"Upserted campaign metric: {campaign_id}")
            return cursor.lastrowid

    def get_campaign_metric(self, campaign_id: str) -> Optional[Dict]:
        """Get campaign metric by ID"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM campaign_metrics WHERE campaign_id = ?",
                          (campaign_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_campaign_metrics(
        self, period_start: Optional[str] = None, period_end: Optional[str] = None
    ) -> List[Dict]:
        """Get all campaign metrics"""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            if period_start and period_end:
                cursor.execute(
                    """
                    SELECT * FROM campaign_metrics
                    WHERE period_start = ? AND period_end = ?
                    ORDER BY country, channel, campaign_type, campaign_name
                """,
                    (period_start, period_end),
                )
            else:
                cursor.execute(
                    """
                    SELECT * FROM campaign_metrics
                    ORDER BY country, channel, campaign_type, campaign_name
                """
                )

            return [dict(row) for row in cursor.fetchall()]

    def get_campaigns_by_category(
        self,
        country: str,
        channel: str,
        campaign_type: str,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
    ) -> List[Dict]:
        """Get campaigns filtered by category"""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            if period_start and period_end:
                cursor.execute(
                    """
                    SELECT * FROM campaign_metrics
                    WHERE country = ? AND channel = ? AND campaign_type = ?
                    AND period_start = ? AND period_end = ?
                    ORDER BY campaign_name
                """,
                    (country, channel, campaign_type, period_start, period_end),
                )
            else:
                cursor.execute(
                    """
                    SELECT * FROM campaign_metrics
                    WHERE country = ? AND channel = ? AND campaign_type = ?
                    ORDER BY campaign_name
                """,
                    (country, channel, campaign_type),
                )

            return [dict(row) for row in cursor.fetchall()]

    # ========================================================================
    # TRANSACTIONAL CAMPAIGNS
    # ========================================================================

    def add_transactional_campaign(
        self, campaign_id: str, campaign_name: str
    ) -> None:
        """Add a campaign ID to transactional campaigns list"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.utcnow().isoformat()

            cursor.execute(
                """
                INSERT OR REPLACE INTO transactional_campaigns
                (campaign_id, campaign_name, added_at)
                VALUES (?, ?, ?)
            """,
                (campaign_id, campaign_name, now),
            )

            conn.commit()
            logger.info(f"Added transactional campaign: {campaign_id}")

    def remove_transactional_campaign(self, campaign_id: str) -> None:
        """Remove a campaign ID from transactional campaigns list"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM transactional_campaigns WHERE campaign_id = ?",
                (campaign_id,),
            )
            conn.commit()
            logger.info(f"Removed transactional campaign: {campaign_id}")

    def get_transactional_campaigns(self) -> List[str]:
        """Get list of transactional campaign IDs"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT campaign_id FROM transactional_campaigns")
            return [row[0] for row in cursor.fetchall()]

    # ========================================================================
    # PULL HISTORY
    # ========================================================================

    def record_pull_started(
        self, pull_id: str, period_start: str, period_end: str
    ) -> None:
        """Record start of a data pull"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.utcnow().isoformat()

            cursor.execute(
                """
                INSERT INTO pull_history
                (pull_id, period_start, period_end, status, started_at)
                VALUES (?, ?, ?, ?, ?)
            """,
                (pull_id, period_start, period_end, "STARTED", now),
            )

            conn.commit()

    def record_pull_completed(
        self,
        pull_id: str,
        segments_fetched: int,
        campaigns_fetched: int,
        status: str = "COMPLETED",
        error_message: Optional[str] = None,
    ) -> None:
        """Record completion of a data pull"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.utcnow().isoformat()

            cursor.execute(
                """
                UPDATE pull_history
                SET segments_fetched = ?, campaigns_fetched = ?,
                    status = ?, error_message = ?, completed_at = ?
                WHERE pull_id = ?
            """,
                (segments_fetched, campaigns_fetched, status, error_message, now,
                 pull_id),
            )

            conn.commit()
            logger.info(f"Recorded pull completion: {pull_id}")

    def get_pull_history(self, limit: int = 10) -> List[Dict]:
        """Get recent pull history"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM pull_history
                ORDER BY started_at DESC
                LIMIT ?
            """,
                (limit,),
            )
            return [dict(row) for row in cursor.fetchall()]
