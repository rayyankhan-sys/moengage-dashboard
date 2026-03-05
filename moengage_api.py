"""
MoEngage API Client - HARDENED VERSION
Handles Segmentation, Campaign Meta, and Stats APIs with comprehensive error handling,
circuit breaker pattern, response validation, and extensive retry logic.
"""
import base64
import json
import logging
import time
import uuid
import random
import string
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from collections import defaultdict

import requests
from requests.exceptions import RequestException, Timeout, ConnectionError, JSONDecodeError

from config import (
    API_BASE,
    WORKSPACE_ID,
    DATA_API_KEY,
    CAMPAIGN_API_KEY,
    APP_KEY_ID,
    SEGMENTATION_API_ENDPOINT,
    CAMPAIGN_META_API_ENDPOINT,
    CAMPAIGN_STATS_API_ENDPOINT,
    SEGMENT_API_RATE_LIMIT,
    SEGMENT_POLL_TIMEOUT,
    SEGMENT_POLL_INTERVAL,
    CAMPAIGN_META_LIMIT,
    STATS_API_BATCH_SIZE,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MoEngageAPIError(Exception):
    """Base exception for MoEngage API errors"""
    pass


class MoEngageSegmentationError(MoEngageAPIError):
    """Segmentation API specific error"""
    pass


class MoEngageCampaignError(MoEngageAPIError):
    """Campaign API specific error"""
    pass


class CircuitBreaker:
    """Circuit Breaker pattern to prevent cascading failures"""

    def __init__(self, failure_threshold: int = 3, recovery_timeout: int = 300):
        """
        Args:
            failure_threshold: Number of consecutive failures to trip the circuit
            recovery_timeout: Seconds before attempting recovery
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.is_open = False

    def record_success(self):
        """Reset on success"""
        self.failure_count = 0
        self.is_open = False
        self.last_failure_time = None

    def record_failure(self):
        """Record a failure and possibly trip the circuit"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.is_open = True

    def is_available(self) -> bool:
        """Check if the circuit allows requests"""
        if not self.is_open:
            return True

        # Check if recovery timeout has passed
        if self.last_failure_time and time.time() - self.last_failure_time > self.recovery_timeout:
            logger.info("Circuit breaker attempting recovery")
            self.is_open = False
            self.failure_count = 0
            return True

        return False

    def get_status(self) -> str:
        """Get human-readable status"""
        if self.is_open:
            remaining = self.recovery_timeout - (time.time() - self.last_failure_time)
            return f"OPEN (recovery in {remaining:.0f}s)"
        return "CLOSED"


class MoEngageAPIClient:
    """
    MoEngage API Client for Segmentation, Campaign Meta, and Stats APIs
    Includes circuit breaker pattern, response validation, and deep error handling
    """

    def __init__(self):
        """Initialize API client with credentials and circuit breakers"""
        self.workspace_id = WORKSPACE_ID
        self.data_api_key = DATA_API_KEY
        self.campaign_api_key = CAMPAIGN_API_KEY
        self.app_key_id = APP_KEY_ID

        # Configure session with connection pooling
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=5,
            pool_maxsize=5,
            max_retries=0  # We handle retries manually
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        self.last_segment_request_time = 0

        # Circuit breakers for each endpoint
        self.circuit_breakers = {
            'segmentation': CircuitBreaker(),
            'campaign_meta': CircuitBreaker(),
            'campaign_stats': CircuitBreaker(),
        }

    def _get_basic_auth_header(self, api_key: str) -> str:
        """
        Generate Basic Auth header
        Base64(WORKSPACE_ID:API_KEY)
        """
        credentials = f"{self.workspace_id}:{api_key}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"

    def _apply_rate_limit(self, rate_limit_seconds: float) -> None:
        """Apply rate limiting between API calls"""
        elapsed = time.time() - self.last_segment_request_time
        if elapsed < rate_limit_seconds:
            sleep_time = rate_limit_seconds - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_segment_request_time = time.time()

    def _generate_request_id(self) -> str:
        """Generate unique request ID for tracking"""
        return f"req_{uuid.uuid4().hex[:8]}_{int(time.time())}"

    def _validate_response_shape(self, response_data: Dict, endpoint_type: str, request_id: str) -> bool:
        """
        Validate response has expected structure
        Returns True if valid, raises MoEngageAPIError if invalid
        """
        try:
            if endpoint_type == "segmentation":
                # MoEngage v3 returns segment data nested under "data" key
                # e.g. {"data": {"id": "abc123", "name": "...", ...}}
                # Also accept top-level "id" or "segment_id" for backwards compat
                # Also pass through error responses for graceful handling upstream
                data_obj = response_data.get("data", {}) if isinstance(response_data.get("data"), dict) else {}
                error_obj = response_data.get("error", {}) if isinstance(response_data.get("error"), dict) else {}
                has_id = (
                    "id" in response_data
                    or "segment_id" in response_data
                    or "id" in data_obj
                    or "segment_id" in data_obj
                )
                has_existing = (
                    "existing_cs_id" in error_obj
                    or "existing_cs_id" in response_data
                )
                has_error = bool(error_obj) or "error" in response_data
                # Let it through if it has an ID, existing segment ref, or error
                # Errors will be handled in create_segment()
                if not has_id and not has_existing and not has_error:
                    truncated = str(response_data)[:500]
                    logger.warning(
                        f"[{request_id}] Segmentation response has no direct 'id' key "
                        f"(will attempt extraction upstream). Response: {truncated}"
                    )
                # Always pass through â never raise for segmentation

            elif endpoint_type == "campaign_meta":
                # API may return {"campaigns": [...]} OR a raw list [...]
                if isinstance(response_data, list):
                    # Raw list response - valid, will be wrapped upstream
                    logger.info(
                        f"[{request_id}] campaign_meta returned raw list with {len(response_data)} items"
                    )
                elif isinstance(response_data, dict):
                    if "campaigns" not in response_data:
                    truncated = str(response_data)[:500]
                    raise MoEngageAPIError(
                        f"[{request_id}] Invalid campaign_meta response: no 'campaigns' key. "
                        f"Response: {truncated}"
                    )
                if not isinstance(response_data["campaigns"], list):
                        raise MoEngageAPIError(
                            f"[{request_id}] Invalid campaign_meta response: 'campaigns' is not a list"
                        )
                else:
                    raise MoEngageAPIError(
                        f"[{request_id}] Invalid campaign_meta response type: {type(response_data)}"
                    )

            elif endpoint_type == "campaign_stats":
                # Must have "data" key (dict)
                if "data" not in response_data:
                    truncated = str(response_data)[:500]
                    raise MoEngageAPIError(
                        f"[{request_id}] Invalid campaign_stats response: no 'data' key. "
                        f"Response: {truncated}"
                    )
                if not isinstance(response_data["data"], dict):
                    raise MoEngageAPIError(
                        f"[{request_id}] Invalid campaign_stats response: 'data' is not a dict"
                    )

            return True

        except MoEngageAPIError:
            raise
        except Exception as e:
            raise MoEngageAPIError(f"[{request_id}] Error validating response shape: {e}")

    def _make_request(
        self,
        method: str,
        url: str,
        auth_header: str,
        json_data: Dict = None,
        timeout: int = 30,
        max_retries: int = 3,
        endpoint_type: str = "general",
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic, escalating timeouts, and circuit breaker
        """
        request_id = self._generate_request_id()

        # Check circuit breaker
        if endpoint_type in self.circuit_breakers:
            breaker = self.circuit_breakers[endpoint_type]
            if not breaker.is_available():
                raise MoEngageAPIError(
                    f"[{request_id}] Circuit breaker OPEN for {endpoint_type} endpoint. "
                    f"Status: {breaker.get_status()}"
                )

        headers = {
            "Authorization": auth_header,
            "MOE-APPKEY": self.app_key_id,
            "Content-Type": "application/json",
        }

        retries = 0
        timeout_schedule = [30, 60, 90]  # Timeout escalation

        while retries < max_retries:
            current_timeout = timeout_schedule[min(retries, len(timeout_schedule) - 1)]

            try:
                logger.debug(f"[{request_id}] {method} {url} (attempt {retries + 1}/{max_retries}, timeout={current_timeout}s)")

                if method == "GET":
                    response = self.session.get(url, headers=headers, timeout=current_timeout)
                elif method == "POST":
                    response = self.session.post(
                        url, headers=headers, json=json_data, timeout=current_timeout
                    )
                elif method == "DELETE":
                    response = self.session.delete(url, headers=headers, timeout=current_timeout)
                else:
                    raise ValueError(f"[{request_id}] Unsupported HTTP method: {method}")

                # Check for empty response body
                if response.status_code == 200 and not response.text:
                    raise MoEngageAPIError(f"[{request_id}] Received 200 OK but empty response body")

                # Try to parse JSON
                try:
                    response_data = response.json()
                except JSONDecodeError as e:
                    logger.error(f"[{request_id}] Failed to decode JSON. Raw response: {response.text[:500]}")
                    raise MoEngageAPIError(
                        f"[{request_id}] Response is not valid JSON: {str(e)}"
                    )

                # Validate response shape
                try:
                    self._validate_response_shape(response_data, endpoint_type, request_id)
                except MoEngageAPIError:
                    raise

                # For segmentation endpoints, return the parsed data even if status != 200
                # because error responses contain useful info (existing_cs_id, etc.)
                if endpoint_type == "segmentation" and response_data:
                    if endpoint_type in self.circuit_breakers:
                        self.circuit_breakers[endpoint_type].record_success()
                    logger.debug(f"[{request_id}] {method} {url} -> {response.status_code}")
                    return response_data

                response.raise_for_status()

                # Success - reset circuit breaker
                if endpoint_type in self.circuit_breakers:
                    self.circuit_breakers[endpoint_type].record_success()

                logger.debug(f"[{request_id}] {method} {url} -> {response.status_code}")
                return response_data

            except Timeout:
                retries += 1
                if endpoint_type in self.circuit_breakers:
                    self.circuit_breakers[endpoint_type].record_failure()

                if retries < max_retries:
                    wait_time = 2 ** retries
                    logger.warning(
                        f"[{request_id}] Request timeout (attempt {retries}/{max_retries}). "
                        f"Retrying in {wait_time}s with timeout={timeout_schedule[min(retries, len(timeout_schedule) - 1)]}s"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"[{request_id}] Request timeout after {max_retries} retries")
                    raise MoEngageAPIError(f"[{request_id}] Request timeout to {url}")

            except ConnectionError as e:
                retries += 1
                if endpoint_type in self.circuit_breakers:
                    self.circuit_breakers[endpoint_type].record_failure()

                if retries < max_retries:
                    wait_time = 2 ** retries
                    logger.warning(
                        f"[{request_id}] Connection error (attempt {retries}/{max_retries}). "
                        f"Retrying in {wait_time}s: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"[{request_id}] Connection error after {max_retries} retries: {e}")
                    raise MoEngageAPIError(f"[{request_id}] Connection error to {url}: {e}")

            except requests.HTTPError as e:
                response = e.response
                status_code = response.status_code

                # 409 Conflict for segments = name already exists (not a fatal error)
                if status_code == 409:
                    logger.info(f"[{request_id}] 409 Conflict: Segment name may already exist")
                    if endpoint_type in self.circuit_breakers:
                        self.circuit_breakers[endpoint_type].record_success()
                    return {"conflict": True, "status_code": 409}

                # Rate limit with Retry-After header support
                elif status_code == 429:
                    if endpoint_type in self.circuit_breakers:
                        self.circuit_breakers[endpoint_type].record_failure()

                    # Check for Retry-After header
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        try:
                            wait_time = int(retry_after)
                            logger.warning(f"[{request_id}] Rate limited. Retry-After: {wait_time}s")
                        except ValueError:
                            wait_time = 2 ** (retries + 1)
                    else:
                        wait_time = 2 ** (retries + 1)

                    retries += 1
                    if retries < max_retries:
                        logger.warning(f"[{request_id}] Retrying in {wait_time}s (attempt {retries}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"[{request_id}] Rate limited after {max_retries} retries")
                        raise MoEngageAPIError(f"[{request_id}] Rate limited: {response.text[:500]}")

                # Server errors - retry with exponential backoff
                elif status_code >= 500:
                    if endpoint_type in self.circuit_breakers:
                        self.circuit_breakers[endpoint_type].record_failure()

                    retries += 1
                    if retries < max_retries:
                        wait_time = 2 ** retries
                        logger.warning(
                            f"[{request_id}] Server error {status_code} (attempt {retries}/{max_retries}). "
                            f"Retrying in {wait_time}s"
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(f"[{request_id}] Server error {status_code} after {max_retries} retries")
                        raise MoEngageAPIError(
                            f"[{request_id}] Server error {status_code}: {response.text[:500]}"
                        )

                # Client errors - don't retry
                else:
                    if endpoint_type in self.circuit_breakers:
                        self.circuit_breakers[endpoint_type].record_success()

                    logger.error(
                        f"[{request_id}] Client error {status_code}: {response.text[:500]}"
                    )
                    raise MoEngageAPIError(
                        f"[{request_id}] HTTP {status_code}: {response.text[:500]}"
                    )

            except RequestException as e:
                retries += 1
                if endpoint_type in self.circuit_breakers:
                    self.circuit_breakers[endpoint_type].record_failure()

                if retries < max_retries:
                    wait_time = 2 ** retries
                    logger.warning(f"[{request_id}] Request error (attempt {retries}/{max_retries}). Retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"[{request_id}] Request failed after {max_retries} retries: {e}")
                    raise MoEngageAPIError(f"[{request_id}] Request failed: {e}")

        raise MoEngageAPIError(f"[{request_id}] Failed after {max_retries} retries")

    # ========================================================================
    # SEGMENTATION API
    # ========================================================================

    def _generate_segment_suffix(self) -> str:
        """Generate random suffix for segment name collision handling"""
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

    def create_segment(self, segment_payload: Dict[str, Any]) -> Tuple[str, Dict]:
        """
        Create a segment via Segmentation API
        Includes collision handling with retry on 409
        Returns: (segment_id, response)
        """
        auth_header = self._get_basic_auth_header(self.data_api_key)
        self._apply_rate_limit(SEGMENT_API_RATE_LIMIT)

        url = SEGMENTATION_API_ENDPOINT
        original_name = segment_payload.get('name', 'unknown')

        # Try up to 3 times with suffix appending on 409
        for attempt in range(3):
            payload = segment_payload.copy()

            if attempt > 0:
                suffix = self._generate_segment_suffix()
                payload['name'] = f"{original_name}_{suffix}"
                logger.info(f"Segment name collision, retrying with suffix: {payload['name']}")
            else:
                logger.info(f"Creating segment: {original_name}")

            try:
                response = self._make_request(
                    "POST", url, auth_header, payload, endpoint_type="segmentation"
                )

                # Check for MoEngage error responses that passed validation
                error_data = response.get("error", {}) if isinstance(response.get("error"), dict) else {}
                if error_data.get("code") == "Internal Server Error":
                    logger.warning(f"MoEngage Internal Server Error for {payload['name']} (attempt {attempt + 1}/3)")
                    if attempt < 2:
                        time.sleep(2 ** (attempt + 1))
                        continue
                    raise MoEngageAPIError(f"MoEngage Internal Server Error after 3 attempts for {payload['name']}")

                # Check for "Resource not created" with existing_cs_id
                if error_data.get("code") == "Resource not created" and error_data.get("existing_cs_id"):
                    existing_id = error_data["existing_cs_id"]
                    logger.info(f"Segment already exists with same filters, reusing: {existing_id}")
                    return existing_id, response

                # Check for conflict (409)
                if response.get("conflict"):
                    logger.warning(f"Segment creation returned 409 Conflict (attempt {attempt + 1}/3)")
                    if attempt < 2:
                        continue  # Try again with suffix
                    return None, response

                # Check for "already exists" error with existing_cs_id
                error_data = response.get("error", {})
                if isinstance(error_data, dict) and error_data.get("existing_cs_id"):
                    existing_id = error_data["existing_cs_id"]
                    logger.info(f"Segment already exists with same filters, reusing: {existing_id}")
                    return existing_id, response
                if response.get("existing_cs_id"):
                    existing_id = response["existing_cs_id"]
                    logger.info(f"Segment already exists with same filters, reusing: {existing_id}")
                    return existing_id, response

                # Extract segment_id from response
                # MoEngage v3 nests under "data" key: {"data": {"id": "abc123"}}
                data_obj = response.get("data", {}) if isinstance(response.get("data"), dict) else {}
                segment_id = (
                    response.get("id")
                    or response.get("segment_id")
                    or data_obj.get("id")
                    or data_obj.get("segment_id")
                )
                if not segment_id:
                    logger.warning(f"No segment ID in response: {str(response)[:500]}")
                    return None, response

                logger.info(f"Segment created: {segment_id} (name: {payload['name']})")
                return segment_id, response

            except MoEngageAPIError as e:
                error_str = str(e)
                # Check if the error message contains existing_cs_id
                if "existing_cs_id" in error_str:
                    # Parse the existing_cs_id from the error string
                    import re
                    match = re.search(r"'existing_cs_id':\s*'([^']+)'", error_str)
                    if match:
                        existing_id = match.group(1)
                        logger.info(f"Segment already exists (from error), reusing: {existing_id}")
                        return existing_id, {"reused": True}
                if attempt == 2:
                    raise
                logger.warning(f"Error creating segment (attempt {attempt + 1}/3): {e}")
                continue

        return None, {}

    def get_segment_count(self, segment_id: str) -> Optional[int]:
        """
        Retrieve segment count from GET segment endpoint.
        Tries multiple possible field names since the MoEngage API docs
        don't explicitly document which field contains the user count.
        Returns None if not yet computed.
        """
        auth_header = self._get_basic_auth_header(self.data_api_key)
        self._apply_rate_limit(SEGMENT_API_RATE_LIMIT)

        url = f"{SEGMENTATION_API_ENDPOINT}/{segment_id}"
        logger.debug(f"Polling segment count for {segment_id}")

        try:
            response = self._make_request("GET", url, auth_header, endpoint_type="segmentation")

            # Log the FULL response keys for debugging
            logger.info(f"Segment {segment_id} GET response top-level keys: {list(response.keys())}")

            # MoEngage v3 may nest under "data" key
            data_obj = response.get("data", {}) if isinstance(response.get("data"), dict) else {}
            if data_obj:
                logger.info(f"Segment {segment_id} data keys: {list(data_obj.keys())}")

            # Try multiple possible field names for the user count
            count_fields = [
                'user_count', 'count', 'size', 'total_count', 'total_users',
                'users_count', 'reachable_users', 'segment_count', 'userCount',
                'totalCount', 'segmentSize', 'users', 'total', 'audience_size',
            ]

            for field in count_fields:
                val = response.get(field)
                if val is not None:
                    logger.info(f"Segment {segment_id} found count in top-level '{field}': {val}")
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        logger.warning(f"Segment {segment_id} field '{field}' not int-convertible: {val}")

                val = data_obj.get(field)
                if val is not None:
                    logger.info(f"Segment {segment_id} found count in data.'{field}': {val}")
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        logger.warning(f"Segment {segment_id} data.'{field}' not int-convertible: {val}")

            # Check for any numeric fields that could be counts
            for key, val in data_obj.items():
                if isinstance(val, (int, float)) and val > 0 and key not in ('created_time', 'updated_time'):
                    logger.info(f"Segment {segment_id} found numeric field data.'{key}': {val}")

            # Log full response for debugging if no count found
            truncated = str(response)[:800]
            logger.warning(
                f"Segment {segment_id} - no count field found in response. "
                f"Response: {truncated}"
            )
            return None

        except MoEngageAPIError as e:
            logger.warning(f"Error fetching segment {segment_id}: {e}")
            return None

    def poll_segment_count(
        self, segment_id: str, timeout: int = SEGMENT_POLL_TIMEOUT
    ) -> Optional[int]:
        """
        Poll for segment count with timeout
        Returns user_count or None if timeout
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            user_count = self.get_segment_count(segment_id)
            if user_count is not None:
                return user_count

            elapsed = time.time() - start_time
            logger.info(
                f"Segment {segment_id} still computing... ({elapsed:.1f}s/{timeout}s)"
            )
            time.sleep(SEGMENT_POLL_INTERVAL)

        logger.warning(
            f"Segment {segment_id} did not compute within {timeout}s timeout. "
            f"Continuing without count."
        )
        return None

    def delete_segment(self, segment_id: str) -> bool:
        """
        Delete a segment
        Returns True if successful
        """
        auth_header = self._get_basic_auth_header(self.data_api_key)
        self._apply_rate_limit(SEGMENT_API_RATE_LIMIT)

        url = f"{SEGMENTATION_API_ENDPOINT}/{segment_id}"
        logger.info(f"Deleting segment: {segment_id}")

        try:
            response = self._make_request("DELETE", url, auth_header, endpoint_type="segmentation")
            logger.info(f"Segment {segment_id} deleted")
            return True
        except MoEngageAPIError as e:
            logger.error(f"Failed to delete segment {segment_id}: {e}")
            return False

    def query_user_count(
        self, segment_payload: Dict[str, Any]
    ) -> Optional[int]:
        """
        Create a segment, poll for count, then delete it
        Returns user_count or None if timed out
        """
        segment_id, response = self.create_segment(segment_payload)

        if not segment_id:
            logger.error("Failed to create segment for query")
            return None

        user_count = self.poll_segment_count(segment_id)
        self.delete_segment(segment_id)

        return user_count

    # ========================================================================
    # CAMPAIGN META API
    # ========================================================================

    def list_campaigns(
        self, from_date: str, to_date: str, page: int = 1, limit: int = CAMPAIGN_META_LIMIT
    ) -> Tuple[List[Dict], int]:
        """
        Fetch campaigns for a date range with pagination
        Returns: (campaigns_list, total_count)
        """
        auth_header = self._get_basic_auth_header(self.campaign_api_key)

        url = CAMPAIGN_META_API_ENDPOINT
        request_id = self._generate_request_id()

        payload = {
            "request_id": request_id,
            "page": page,
            "limit": limit,
            "campaign_fields": {
                "created_date": {
                    "from_date": from_date,
                    "to_date": to_date,
                }
            },
        }

        logger.info(
            f"[{request_id}] Fetching campaigns from {from_date} to {to_date} (page {page})"
        )

        response = self._make_request("POST", url, auth_header, payload, endpoint_type="campaign_meta")

        # Handle both dict {"campaigns": [...], "total_count": N} and raw list [...] responses
        if isinstance(response, list):
            campaigns = response
            total_count = len(response)  # Best estimate when API returns raw list
        else:
            campaigns = response.get("campaigns", [])
            total_count = response.get("total_count", 0)

        logger.info(f"[{request_id}] Retrieved {len(campaigns)} campaigns (total: {total_count})")
        return campaigns, total_count

    def list_all_campaigns(self, from_date: str, to_date: str) -> List[Dict]:
        """
        Fetch ALL campaigns for a date range with automatic pagination
        Returns: list of all campaigns
        """
        all_campaigns = []
        page = 1

        while True:
            campaigns, total_count = self.list_campaigns(
                from_date, to_date, page=page
            )

            if not campaigns:
                break

            all_campaigns.extend(campaigns)

            # Check if we've fetched all campaigns
            if len(all_campaigns) >= total_count:
                break

            page += 1

        logger.info(f"Fetched all {len(all_campaigns)} campaigns")
        return all_campaigns

    # ========================================================================
    # STATS API
    # ========================================================================

    def fetch_campaign_stats(
        self,
        campaign_ids: List[str],
        start_date: str,
        end_date: str,
        attribution_type: str = "VIEW_THROUGH",
        metric_type: str = "TOTAL",
    ) -> Dict[str, Any]:
        """
        Fetch stats for a batch of campaigns (max 10 per request)
        Returns: parsed stats dict
        """
        if len(campaign_ids) > STATS_API_BATCH_SIZE:
            raise ValueError(
                f"Max {STATS_API_BATCH_SIZE} campaign IDs per request"
            )

        auth_header = self._get_basic_auth_header(self.campaign_api_key)
        request_id = self._generate_request_id()

        url = CAMPAIGN_STATS_API_ENDPOINT
        payload = {
            "request_id": request_id,
            "campaign_ids": campaign_ids,
            "start_date": start_date,
            "end_date": end_date,
            "attribution_type": attribution_type,
            "metric_type": metric_type,
        }

        logger.info(
            f"[{request_id}] Fetching stats for {len(campaign_ids)} campaigns ({start_date} to {end_date})"
        )

        response = self._make_request("POST", url, auth_header, payload, endpoint_type="campaign_stats")
        return response

    def fetch_all_campaign_stats(
        self,
        campaign_ids: List[str],
        start_date: str,
        end_date: str,
        attribution_type: str = "VIEW_THROUGH",
        metric_type: str = "TOTAL",
    ) -> Dict[str, Any]:
        """
        Fetch stats for all campaigns with automatic batching
        Returns: aggregated stats dict
        """
        all_stats = {}

        # Batch campaign IDs
        for i in range(0, len(campaign_ids), STATS_API_BATCH_SIZE):
            batch = campaign_ids[i : i + STATS_API_BATCH_SIZE]
            logger.info(
                f"Fetching stats batch {i // STATS_API_BATCH_SIZE + 1} ({len(batch)} campaigns)"
            )

            stats = self.fetch_campaign_stats(
                batch, start_date, end_date, attribution_type, metric_type
            )

            # Merge stats
            if "data" in stats:
                all_stats.update(stats.get("data", {}))

        logger.info(f"Fetched stats for {len(all_stats)} campaigns")
        return {"data": all_stats}

    def parse_campaign_stats(
        self, campaign_id: str, stats_response: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Parse nested stats response for a single campaign
        Response path: data.{campaign_id}[0].platforms.ALL_PLATFORMS.locales.all_locale.variations.all_variations.performance_stats

        With detailed error tracking at each nesting level.
        """
        try:
            # Level 1: Get data key
            data = stats_response.get("data")
            if data is None:
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: root (key 'data' not found)")
                return None

            # Level 2: Get campaign_id data
            campaign_data = data.get(campaign_id)
            if campaign_data is None:
                logger.warning(f"No stats data for campaign {campaign_id}")
                return None

            # Level 3: Check array
            if not isinstance(campaign_data, list) or len(campaign_data) == 0:
                logger.warning(f"Stats parse failed for campaign {campaign_id} at level: campaign_data (empty array)")
                return None

            first_elem = campaign_data[0]

            # Level 4: platforms
            platforms = first_elem.get("platforms")
            if platforms is None:
                available = list(first_elem.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: root (key 'platforms' not found. Available keys: {available})")
                return None

            # Level 5: ALL_PLATFORMS
            all_platforms = platforms.get("ALL_PLATFORMS")
            if all_platforms is None:
                available = list(platforms.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: platforms.ALL_PLATFORMS (key 'ALL_PLATFORMS' not found. Available keys: {available})")
                return None

            # Level 6: locales
            locales = all_platforms.get("locales")
            if locales is None:
                available = list(all_platforms.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: platforms.ALL_PLATFORMS.locales (key 'locales' not found. Available keys: {available})")
                return None

            # Level 7: all_locale
            all_locale = locales.get("all_locale")
            if all_locale is None:
                available = list(locales.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: platforms.ALL_PLATFORMS.locales.all_locale (key 'all_locale' not found. Available keys: {available})")
                return None

            # Level 8: variations
            variations = all_locale.get("variations")
            if variations is None:
                available = list(all_locale.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: platforms.ALL_PLATFORMS.locales.all_locale.variations (key 'variations' not found. Available keys: {available})")
                return None

            # Level 9: all_variations
            all_variations = variations.get("all_variations")
            if all_variations is None:
                available = list(variations.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: platforms.ALL_PLATFORMS.locales.all_locale.variations.all_variations (key 'all_variations' not found. Available keys: {available})")
                return None

            # Level 10: performance_stats
            perf_stats = all_variations.get("performance_stats")
            if perf_stats is None:
                available = list(all_variations.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: platforms.ALL_PLATFORMS.locales.all_locale.variations.all_variations.performance_stats (key 'performance_stats' not found. Available keys: {available})")
                return None

            # Extract metrics
            stats = {
                "sent": perf_stats.get("sent", 0),
                "delivered": perf_stats.get("delivered", 0),
                "open": perf_stats.get("open", 0),  # Unique Opens
                "click": perf_stats.get("click", 0),  # Unique Clicks
                "unsubscribe": perf_stats.get("unsubscribe", 0),
                "bounced": perf_stats.get("bounced", 0),
                "failed": perf_stats.get("failed", 0),
            }

            logger.debug(f"Parsed stats for {campaign_id}: {stats}")
            return stats

        except (KeyError, IndexError, TypeError, AttributeError) as e:
            logger.error(f"Error parsing stats for {campaign_id}: {e}")
            return None
"""
MoEngage API Client - HARDENED VERSION
Handles Segmentation, Campaign Meta, and Stats APIs with comprehensive error handling,
circuit breaker pattern, response validation, and extensive retry logic.
"""
import base64
import json
import logging
import time
import uuid
import random
import string
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from collections import defaultdict

import requests
from requests.exceptions import RequestException, Timeout, ConnectionError, JSONDecodeError

from config import (
    API_BASE,
    WORKSPACE_ID,
    DATA_API_KEY,
    CAMPAIGN_API_KEY,
    APP_KEY_ID,
    SEGMENTATION_API_ENDPOINT,
    CAMPAIGN_META_API_ENDPOINT,
    CAMPAIGN_STATS_API_ENDPOINT,
    SEGMENT_API_RATE_LIMIT,
    SEGMENT_POLL_TIMEOUT,
    SEGMENT_POLL_INTERVAL,
    CAMPAIGN_META_LIMIT,
    STATS_API_BATCH_SIZE,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MoEngageAPIError(Exception):
    """Base exception for MoEngage API errors"""
    pass


class MoEngageSegmentationError(MoEngageAPIError):
    """Segmentation API specific error"""
    pass


class MoEngageCampaignError(MoEngageAPIError):
    """Campaign API specific error"""
    pass


class CircuitBreaker:
    """Circuit Breaker pattern to prevent cascading failures"""

    def __init__(self, failure_threshold: int = 3, recovery_timeout: int = 300):
        """
        Args:
            failure_threshold: Number of consecutive failures to trip the circuit
            recovery_timeout: Seconds before attempting recovery
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.is_open = False

    def record_success(self):
        """Reset on success"""
        self.failure_count = 0
        self.is_open = False
        self.last_failure_time = None

    def record_failure(self):
        """Record a failure and possibly trip the circuit"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.is_open = True

    def is_available(self) -> bool:
        """Check if the circuit allows requests"""
        if not self.is_open:
            return True

        # Check if recovery timeout has passed
        if self.last_failure_time and time.time() - self.last_failure_time > self.recovery_timeout:
            logger.info("Circuit breaker attempting recovery")
            self.is_open = False
            self.failure_count = 0
            return True

        return False

    def get_status(self) -> str:
        """Get human-readable status"""
        if self.is_open:
            remaining = self.recovery_timeout - (time.time() - self.last_failure_time)
            return f"OPEN (recovery in {remaining:.0f}s)"
        return "CLOSED"


class MoEngageAPIClient:
    """
    MoEngage API Client for Segmentation, Campaign Meta, and Stats APIs
    Includes circuit breaker pattern, response validation, and deep error handling
    """

    def __init__(self):
        """Initialize API client with credentials and circuit breakers"""
        self.workspace_id = WORKSPACE_ID
        self.data_api_key = DATA_API_KEY
        self.campaign_api_key = CAMPAIGN_API_KEY
        self.app_key_id = APP_KEY_ID

        # Configure session with connection pooling
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=5,
            pool_maxsize=5,
            max_retries=0  # We handle retries manually
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        self.last_segment_request_time = 0

        # Circuit breakers for each endpoint
        self.circuit_breakers = {
            'segmentation': CircuitBreaker(),
            'campaign_meta': CircuitBreaker(),
            'campaign_stats': CircuitBreaker(),
        }

    def _get_basic_auth_header(self, api_key: str) -> str:
        """
        Generate Basic Auth header
        Base64(WORKSPACE_ID:API_KEY)
        """
        credentials = f"{self.workspace_id}:{api_key}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"

    def _apply_rate_limit(self, rate_limit_seconds: float) -> None:
        """Apply rate limiting between API calls"""
        elapsed = time.time() - self.last_segment_request_time
        if elapsed < rate_limit_seconds:
            sleep_time = rate_limit_seconds - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_segment_request_time = time.time()

    def _generate_request_id(self) -> str:
        """Generate unique request ID for tracking"""
        return f"req_{uuid.uuid4().hex[:8]}_{int(time.time())}"

    def _validate_response_shape(self, response_data: Dict, endpoint_type: str, request_id: str) -> bool:
        """
        Validate response has expected structure
        Returns True if valid, raises MoEngageAPIError if invalid
        """
        try:
            if endpoint_type == "segmentation":
                # MoEngage v3 returns segment data nested under "data" key
                # e.g. {"data": {"id": "abc123", "name": "...", ...}}
                # Also accept top-level "id" or "segment_id" for backwards compat
                # Also pass through error responses for graceful handling upstream
                data_obj = response_data.get("data", {}) if isinstance(response_data.get("data"), dict) else {}
                error_obj = response_data.get("error", {}) if isinstance(response_data.get("error"), dict) else {}
                has_id = (
                    "id" in response_data
                    or "segment_id" in response_data
                    or "id" in data_obj
                    or "segment_id" in data_obj
                )
                has_existing = (
                    "existing_cs_id" in error_obj
                    or "existing_cs_id" in response_data
                )
                has_error = bool(error_obj) or "error" in response_data
                # Let it through if it has an ID, existing segment ref, or error
                # Errors will be handled in create_segment()
                if not has_id and not has_existing and not has_error:
                    truncated = str(response_data)[:500]
                    logger.warning(
                        f"[{request_id}] Segmentation response has no direct 'id' key "
                        f"(will attempt extraction upstream). Response: {truncated}"
                    )
                # Always pass through — never raise for segmentation

            elif endpoint_type == "campaign_meta":
                # Must have "campaigns" key (list)
                if "campaigns" not in response_data:
                    truncated = str(response_data)[:500]
                    raise MoEngageAPIError(
                        f"[{request_id}] Invalid campaign_meta response: no 'campaigns' key. "
                        f"Response: {truncated}"
                    )
                if not isinstance(response_data["campaigns"], list):
                    raise MoEngageAPIError(
                        f"[{request_id}] Invalid campaign_meta response: 'campaigns' is not a list"
                    )

            elif endpoint_type == "campaign_stats":
                # Must have "data" key (dict)
                if "data" not in response_data:
                    truncated = str(response_data)[:500]
                    raise MoEngageAPIError(
                        f"[{request_id}] Invalid campaign_stats response: no 'data' key. "
                        f"Response: {truncated}"
                    )
                if not isinstance(response_data["data"], dict):
                    raise MoEngageAPIError(
                        f"[{request_id}] Invalid campaign_stats response: 'data' is not a dict"
                    )

            return True

        except MoEngageAPIError:
            raise
        except Exception as e:
            raise MoEngageAPIError(f"[{request_id}] Error validating response shape: {e}")

    def _make_request(
        self,
        method: str,
        url: str,
        auth_header: str,
        json_data: Dict = None,
        timeout: int = 30,
        max_retries: int = 3,
        endpoint_type: str = "general",
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic, escalating timeouts, and circuit breaker
        """
        request_id = self._generate_request_id()

        # Check circuit breaker
        if endpoint_type in self.circuit_breakers:
            breaker = self.circuit_breakers[endpoint_type]
            if not breaker.is_available():
                raise MoEngageAPIError(
                    f"[{request_id}] Circuit breaker OPEN for {endpoint_type} endpoint. "
                    f"Status: {breaker.get_status()}"
                )

        headers = {
            "Authorization": auth_header,
            "MOE-APPKEY": self.app_key_id,
            "Content-Type": "application/json",
        }

        retries = 0
        timeout_schedule = [30, 60, 90]  # Timeout escalation

        while retries < max_retries:
            current_timeout = timeout_schedule[min(retries, len(timeout_schedule) - 1)]

            try:
                logger.debug(f"[{request_id}] {method} {url} (attempt {retries + 1}/{max_retries}, timeout={current_timeout}s)")

                if method == "GET":
                    response = self.session.get(url, headers=headers, timeout=current_timeout)
                elif method == "POST":
                    response = self.session.post(
                        url, headers=headers, json=json_data, timeout=current_timeout
                    )
                elif method == "DELETE":
                    response = self.session.delete(url, headers=headers, timeout=current_timeout)
                else:
                    raise ValueError(f"[{request_id}] Unsupported HTTP method: {method}")

                # Check for empty response body
                if response.status_code == 200 and not response.text:
                    raise MoEngageAPIError(f"[{request_id}] Received 200 OK but empty response body")

                # Try to parse JSON
                try:
                    response_data = response.json()
                except JSONDecodeError as e:
                    logger.error(f"[{request_id}] Failed to decode JSON. Raw response: {response.text[:500]}")
                    raise MoEngageAPIError(
                        f"[{request_id}] Response is not valid JSON: {str(e)}"
                    )

                # Validate response shape
                try:
                    self._validate_response_shape(response_data, endpoint_type, request_id)
                except MoEngageAPIError:
                    raise

                # For segmentation endpoints, return the parsed data even if status != 200
                # because error responses contain useful info (existing_cs_id, etc.)
                if endpoint_type == "segmentation" and response_data:
                    if endpoint_type in self.circuit_breakers:
                        self.circuit_breakers[endpoint_type].record_success()
                    logger.debug(f"[{request_id}] {method} {url} -> {response.status_code}")
                    return response_data

                response.raise_for_status()

                # Success - reset circuit breaker
                if endpoint_type in self.circuit_breakers:
                    self.circuit_breakers[endpoint_type].record_success()

                logger.debug(f"[{request_id}] {method} {url} -> {response.status_code}")
                return response_data

            except Timeout:
                retries += 1
                if endpoint_type in self.circuit_breakers:
                    self.circuit_breakers[endpoint_type].record_failure()

                if retries < max_retries:
                    wait_time = 2 ** retries
                    logger.warning(
                        f"[{request_id}] Request timeout (attempt {retries}/{max_retries}). "
                        f"Retrying in {wait_time}s with timeout={timeout_schedule[min(retries, len(timeout_schedule) - 1)]}s"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"[{request_id}] Request timeout after {max_retries} retries")
                    raise MoEngageAPIError(f"[{request_id}] Request timeout to {url}")

            except ConnectionError as e:
                retries += 1
                if endpoint_type in self.circuit_breakers:
                    self.circuit_breakers[endpoint_type].record_failure()

                if retries < max_retries:
                    wait_time = 2 ** retries
                    logger.warning(
                        f"[{request_id}] Connection error (attempt {retries}/{max_retries}). "
                        f"Retrying in {wait_time}s: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"[{request_id}] Connection error after {max_retries} retries: {e}")
                    raise MoEngageAPIError(f"[{request_id}] Connection error to {url}: {e}")

            except requests.HTTPError as e:
                response = e.response
                status_code = response.status_code

                # 409 Conflict for segments = name already exists (not a fatal error)
                if status_code == 409:
                    logger.info(f"[{request_id}] 409 Conflict: Segment name may already exist")
                    if endpoint_type in self.circuit_breakers:
                        self.circuit_breakers[endpoint_type].record_success()
                    return {"conflict": True, "status_code": 409}

                # Rate limit with Retry-After header support
                elif status_code == 429:
                    if endpoint_type in self.circuit_breakers:
                        self.circuit_breakers[endpoint_type].record_failure()

                    # Check for Retry-After header
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        try:
                            wait_time = int(retry_after)
                            logger.warning(f"[{request_id}] Rate limited. Retry-After: {wait_time}s")
                        except ValueError:
                            wait_time = 2 ** (retries + 1)
                    else:
                        wait_time = 2 ** (retries + 1)

                    retries += 1
                    if retries < max_retries:
                        logger.warning(f"[{request_id}] Retrying in {wait_time}s (attempt {retries}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"[{request_id}] Rate limited after {max_retries} retries")
                        raise MoEngageAPIError(f"[{request_id}] Rate limited: {response.text[:500]}")

                # Server errors - retry with exponential backoff
                elif status_code >= 500:
                    if endpoint_type in self.circuit_breakers:
                        self.circuit_breakers[endpoint_type].record_failure()

                    retries += 1
                    if retries < max_retries:
                        wait_time = 2 ** retries
                        logger.warning(
                            f"[{request_id}] Server error {status_code} (attempt {retries}/{max_retries}). "
                            f"Retrying in {wait_time}s"
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(f"[{request_id}] Server error {status_code} after {max_retries} retries")
                        raise MoEngageAPIError(
                            f"[{request_id}] Server error {status_code}: {response.text[:500]}"
                        )

                # Client errors - don't retry
                else:
                    if endpoint_type in self.circuit_breakers:
                        self.circuit_breakers[endpoint_type].record_success()

                    logger.error(
                        f"[{request_id}] Client error {status_code}: {response.text[:500]}"
                    )
                    raise MoEngageAPIError(
                        f"[{request_id}] HTTP {status_code}: {response.text[:500]}"
                    )

            except RequestException as e:
                retries += 1
                if endpoint_type in self.circuit_breakers:
                    self.circuit_breakers[endpoint_type].record_failure()

                if retries < max_retries:
                    wait_time = 2 ** retries
                    logger.warning(f"[{request_id}] Request error (attempt {retries}/{max_retries}). Retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"[{request_id}] Request failed after {max_retries} retries: {e}")
                    raise MoEngageAPIError(f"[{request_id}] Request failed: {e}")

        raise MoEngageAPIError(f"[{request_id}] Failed after {max_retries} retries")

    # ========================================================================
    # SEGMENTATION API
    # ========================================================================

    def _generate_segment_suffix(self) -> str:
        """Generate random suffix for segment name collision handling"""
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

    def create_segment(self, segment_payload: Dict[str, Any]) -> Tuple[str, Dict]:
        """
        Create a segment via Segmentation API
        Includes collision handling with retry on 409
        Returns: (segment_id, response)
        """
        auth_header = self._get_basic_auth_header(self.data_api_key)
        self._apply_rate_limit(SEGMENT_API_RATE_LIMIT)

        url = SEGMENTATION_API_ENDPOINT
        original_name = segment_payload.get('name', 'unknown')

        # Try up to 3 times with suffix appending on 409
        for attempt in range(3):
            payload = segment_payload.copy()

            if attempt > 0:
                suffix = self._generate_segment_suffix()
                payload['name'] = f"{original_name}_{suffix}"
                logger.info(f"Segment name collision, retrying with suffix: {payload['name']}")
            else:
                logger.info(f"Creating segment: {original_name}")

            try:
                response = self._make_request(
                    "POST", url, auth_header, payload, endpoint_type="segmentation"
                )

                # Check for MoEngage error responses that passed validation
                error_data = response.get("error", {}) if isinstance(response.get("error"), dict) else {}
                if error_data.get("code") == "Internal Server Error":
                    logger.warning(f"MoEngage Internal Server Error for {payload['name']} (attempt {attempt + 1}/3)")
                    if attempt < 2:
                        time.sleep(2 ** (attempt + 1))
                        continue
                    raise MoEngageAPIError(f"MoEngage Internal Server Error after 3 attempts for {payload['name']}")

                # Check for "Resource not created" with existing_cs_id
                if error_data.get("code") == "Resource not created" and error_data.get("existing_cs_id"):
                    existing_id = error_data["existing_cs_id"]
                    logger.info(f"Segment already exists with same filters, reusing: {existing_id}")
                    return existing_id, response

                # Check for conflict (409)
                if response.get("conflict"):
                    logger.warning(f"Segment creation returned 409 Conflict (attempt {attempt + 1}/3)")
                    if attempt < 2:
                        continue  # Try again with suffix
                    return None, response

                # Check for "already exists" error with existing_cs_id
                error_data = response.get("error", {})
                if isinstance(error_data, dict) and error_data.get("existing_cs_id"):
                    existing_id = error_data["existing_cs_id"]
                    logger.info(f"Segment already exists with same filters, reusing: {existing_id}")
                    return existing_id, response
                if response.get("existing_cs_id"):
                    existing_id = response["existing_cs_id"]
                    logger.info(f"Segment already exists with same filters, reusing: {existing_id}")
                    return existing_id, response

                # Extract segment_id from response
                # MoEngage v3 nests under "data" key: {"data": {"id": "abc123"}}
                data_obj = response.get("data", {}) if isinstance(response.get("data"), dict) else {}
                segment_id = (
                    response.get("id")
                    or response.get("segment_id")
                    or data_obj.get("id")
                    or data_obj.get("segment_id")
                )
                if not segment_id:
                    logger.warning(f"No segment ID in response: {str(response)[:500]}")
                    return None, response

                logger.info(f"Segment created: {segment_id} (name: {payload['name']})")
                return segment_id, response

            except MoEngageAPIError as e:
                error_str = str(e)
                # Check if the error message contains existing_cs_id
                if "existing_cs_id" in error_str:
                    # Parse the existing_cs_id from the error string
                    import re
                    match = re.search(r"'existing_cs_id':\s*'([^']+)'", error_str)
                    if match:
                        existing_id = match.group(1)
                        logger.info(f"Segment already exists (from error), reusing: {existing_id}")
                        return existing_id, {"reused": True}
                if attempt == 2:
                    raise
                logger.warning(f"Error creating segment (attempt {attempt + 1}/3): {e}")
                continue

        return None, {}

    def get_segment_count(self, segment_id: str) -> Optional[int]:
        """
        Retrieve segment count from GET segment endpoint.
        Tries multiple possible field names since the MoEngage API docs
        don't explicitly document which field contains the user count.
        Returns None if not yet computed.
        """
        auth_header = self._get_basic_auth_header(self.data_api_key)
        self._apply_rate_limit(SEGMENT_API_RATE_LIMIT)

        url = f"{SEGMENTATION_API_ENDPOINT}/{segment_id}"
        logger.debug(f"Polling segment count for {segment_id}")

        try:
            response = self._make_request("GET", url, auth_header, endpoint_type="segmentation")

            # Log the FULL response keys for debugging
            logger.info(f"Segment {segment_id} GET response top-level keys: {list(response.keys())}")

            # MoEngage v3 may nest under "data" key
            data_obj = response.get("data", {}) if isinstance(response.get("data"), dict) else {}
            if data_obj:
                logger.info(f"Segment {segment_id} data keys: {list(data_obj.keys())}")

            # Try multiple possible field names for the user count
            count_fields = [
                'user_count', 'count', 'size', 'total_count', 'total_users',
                'users_count', 'reachable_users', 'segment_count', 'userCount',
                'totalCount', 'segmentSize', 'users', 'total', 'audience_size',
            ]

            for field in count_fields:
                val = response.get(field)
                if val is not None:
                    logger.info(f"Segment {segment_id} found count in top-level '{field}': {val}")
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        logger.warning(f"Segment {segment_id} field '{field}' not int-convertible: {val}")

                val = data_obj.get(field)
                if val is not None:
                    logger.info(f"Segment {segment_id} found count in data.'{field}': {val}")
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        logger.warning(f"Segment {segment_id} data.'{field}' not int-convertible: {val}")

            # Check for any numeric fields that could be counts
            for key, val in data_obj.items():
                if isinstance(val, (int, float)) and val > 0 and key not in ('created_time', 'updated_time'):
                    logger.info(f"Segment {segment_id} found numeric field data.'{key}': {val}")

            # Log full response for debugging if no count found
            truncated = str(response)[:800]
            logger.warning(
                f"Segment {segment_id} - no count field found in response. "
                f"Response: {truncated}"
            )
            return None

        except MoEngageAPIError as e:
            logger.warning(f"Error fetching segment {segment_id}: {e}")
            return None

    def poll_segment_count(
        self, segment_id: str, timeout: int = SEGMENT_POLL_TIMEOUT
    ) -> Optional[int]:
        """
        Poll for segment count with timeout
        Returns user_count or None if timeout
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            user_count = self.get_segment_count(segment_id)
            if user_count is not None:
                return user_count

            elapsed = time.time() - start_time
            logger.info(
                f"Segment {segment_id} still computing... ({elapsed:.1f}s/{timeout}s)"
            )
            time.sleep(SEGMENT_POLL_INTERVAL)

        logger.warning(
            f"Segment {segment_id} did not compute within {timeout}s timeout. "
            f"Continuing without count."
        )
        return None

    def delete_segment(self, segment_id: str) -> bool:
        """
        Delete a segment
        Returns True if successful
        """
        auth_header = self._get_basic_auth_header(self.data_api_key)
        self._apply_rate_limit(SEGMENT_API_RATE_LIMIT)

        url = f"{SEGMENTATION_API_ENDPOINT}/{segment_id}"
        logger.info(f"Deleting segment: {segment_id}")

        try:
            response = self._make_request("DELETE", url, auth_header, endpoint_type="segmentation")
            logger.info(f"Segment {segment_id} deleted")
            return True
        except MoEngageAPIError as e:
            logger.error(f"Failed to delete segment {segment_id}: {e}")
            return False

    def query_user_count(
        self, segment_payload: Dict[str, Any]
    ) -> Optional[int]:
        """
        Create a segment, poll for count, then delete it
        Returns user_count or None if timed out
        """
        segment_id, response = self.create_segment(segment_payload)

        if not segment_id:
            logger.error("Failed to create segment for query")
            return None

        user_count = self.poll_segment_count(segment_id)
        self.delete_segment(segment_id)

        return user_count

    # ========================================================================
    # CAMPAIGN META API
    # ========================================================================

    def list_campaigns(
        self, from_date: str, to_date: str, page: int = 1, limit: int = CAMPAIGN_META_LIMIT
    ) -> Tuple[List[Dict], int]:
        """
        Fetch campaigns for a date range with pagination
        Returns: (campaigns_list, total_count)
        """
        auth_header = self._get_basic_auth_header(self.campaign_api_key)

        url = CAMPAIGN_META_API_ENDPOINT
        request_id = self._generate_request_id()

        payload = {
            "request_id": request_id,
            "page": page,
            "limit": limit,
            "campaign_fields": {
                "created_date": {
                    "from_date": from_date,
                    "to_date": to_date,
                }
            },
        }

        logger.info(
            f"[{request_id}] Fetching campaigns from {from_date} to {to_date} (page {page})"
        )

        response = self._make_request("POST", url, auth_header, payload, endpoint_type="campaign_meta")

        campaigns = response.get("campaigns", [])
        total_count = response.get("total_count", 0)

        logger.info(f"[{request_id}] Retrieved {len(campaigns)} campaigns (total: {total_count})")
        return campaigns, total_count

    def list_all_campaigns(self, from_date: str, to_date: str) -> List[Dict]:
        """
        Fetch ALL campaigns for a date range with automatic pagination
        Returns: list of all campaigns
        """
        all_campaigns = []
        page = 1

        while True:
            campaigns, total_count = self.list_campaigns(
                from_date, to_date, page=page
            )

            if not campaigns:
                break

            all_campaigns.extend(campaigns)

            # Check if we've fetched all campaigns
            if len(all_campaigns) >= total_count:
                break

            page += 1

        logger.info(f"Fetched all {len(all_campaigns)} campaigns")
        return all_campaigns

    # ========================================================================
    # STATS API
    # ========================================================================

    def fetch_campaign_stats(
        self,
        campaign_ids: List[str],
        start_date: str,
        end_date: str,
        attribution_type: str = "VIEW_THROUGH",
        metric_type: str = "TOTAL",
    ) -> Dict[str, Any]:
        """
        Fetch stats for a batch of campaigns (max 10 per request)
        Returns: parsed stats dict
        """
        if len(campaign_ids) > STATS_API_BATCH_SIZE:
            raise ValueError(
                f"Max {STATS_API_BATCH_SIZE} campaign IDs per request"
            )

        auth_header = self._get_basic_auth_header(self.campaign_api_key)
        request_id = self._generate_request_id()

        url = CAMPAIGN_STATS_API_ENDPOINT
        payload = {
            "request_id": request_id,
            "campaign_ids": campaign_ids,
            "start_date": start_date,
            "end_date": end_date,
            "attribution_type": attribution_type,
            "metric_type": metric_type,
        }

        logger.info(
            f"[{request_id}] Fetching stats for {len(campaign_ids)} campaigns ({start_date} to {end_date})"
        )

        response = self._make_request("POST", url, auth_header, payload, endpoint_type="campaign_stats")
        return response

    def fetch_all_campaign_stats(
        self,
        campaign_ids: List[str],
        start_date: str,
        end_date: str,
        attribution_type: str = "VIEW_THROUGH",
        metric_type: str = "TOTAL",
    ) -> Dict[str, Any]:
        """
        Fetch stats for all campaigns with automatic batching
        Returns: aggregated stats dict
        """
        all_stats = {}

        # Batch campaign IDs
        for i in range(0, len(campaign_ids), STATS_API_BATCH_SIZE):
            batch = campaign_ids[i : i + STATS_API_BATCH_SIZE]
            logger.info(
                f"Fetching stats batch {i // STATS_API_BATCH_SIZE + 1} ({len(batch)} campaigns)"
            )

            stats = self.fetch_campaign_stats(
                batch, start_date, end_date, attribution_type, metric_type
            )

            # Merge stats
            if "data" in stats:
                all_stats.update(stats.get("data", {}))

        logger.info(f"Fetched stats for {len(all_stats)} campaigns")
        return {"data": all_stats}

    def parse_campaign_stats(
        self, campaign_id: str, stats_response: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Parse nested stats response for a single campaign
        Response path: data.{campaign_id}[0].platforms.ALL_PLATFORMS.locales.all_locale.variations.all_variations.performance_stats

        With detailed error tracking at each nesting level.
        """
        try:
            # Level 1: Get data key
            data = stats_response.get("data")
            if data is None:
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: root (key 'data' not found)")
                return None

            # Level 2: Get campaign_id data
            campaign_data = data.get(campaign_id)
            if campaign_data is None:
                logger.warning(f"No stats data for campaign {campaign_id}")
                return None

            # Level 3: Check array
            if not isinstance(campaign_data, list) or len(campaign_data) == 0:
                logger.warning(f"Stats parse failed for campaign {campaign_id} at level: campaign_data (empty array)")
                return None

            first_elem = campaign_data[0]

            # Level 4: platforms
            platforms = first_elem.get("platforms")
            if platforms is None:
                available = list(first_elem.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: root (key 'platforms' not found. Available keys: {available})")
                return None

            # Level 5: ALL_PLATFORMS
            all_platforms = platforms.get("ALL_PLATFORMS")
            if all_platforms is None:
                available = list(platforms.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: platforms.ALL_PLATFORMS (key 'ALL_PLATFORMS' not found. Available keys: {available})")
                return None

            # Level 6: locales
            locales = all_platforms.get("locales")
            if locales is None:
                available = list(all_platforms.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: platforms.ALL_PLATFORMS.locales (key 'locales' not found. Available keys: {available})")
                return None

            # Level 7: all_locale
            all_locale = locales.get("all_locale")
            if all_locale is None:
                available = list(locales.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: platforms.ALL_PLATFORMS.locales.all_locale (key 'all_locale' not found. Available keys: {available})")
                return None

            # Level 8: variations
            variations = all_locale.get("variations")
            if variations is None:
                available = list(all_locale.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: platforms.ALL_PLATFORMS.locales.all_locale.variations (key 'variations' not found. Available keys: {available})")
                return None

            # Level 9: all_variations
            all_variations = variations.get("all_variations")
            if all_variations is None:
                available = list(variations.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: platforms.ALL_PLATFORMS.locales.all_locale.variations.all_variations (key 'all_variations' not found. Available keys: {available})")
                return None

            # Level 10: performance_stats
            perf_stats = all_variations.get("performance_stats")
            if perf_stats is None:
                available = list(all_variations.keys())
                logger.error(f"Stats parse failed for campaign {campaign_id} at level: platforms.ALL_PLATFORMS.locales.all_locale.variations.all_variations.performance_stats (key 'performance_stats' not found. Available keys: {available})")
                return None

            # Extract metrics
            stats = {
                "sent": perf_stats.get("sent", 0),
                "delivered": perf_stats.get("delivered", 0),
                "open": perf_stats.get("open", 0),  # Unique Opens
                "click": perf_stats.get("click", 0),  # Unique Clicks
                "unsubscribe": perf_stats.get("unsubscribe", 0),
                "bounced": perf_stats.get("bounced", 0),
                "failed": perf_stats.get("failed", 0),
            }

            logger.debug(f"Parsed stats for {campaign_id}: {stats}")
            return stats

        except (KeyError, IndexError, TypeError, AttributeError) as e:
            logger.error(f"Error parsing stats for {campaign_id}: {e}")
            return None
