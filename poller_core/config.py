import os
from dotenv import load_dotenv

load_dotenv()  # reads .env in project root

# -------- Config --------
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_HOST = "https://chauffeur-app-api.blacklane.com"  # Platform 1 (mobile)
MOBILE_AUTH_BASE = os.getenv("BL_MOBILE_AUTH_BASE", "https://login-chauffeur.blacklane.com")
ATHENA_BASE = "https://athena.blacklane.com"          # Platform 2 (Portal)
PARTNER_API_BASE = "https://partner-portal-api.blacklane.com"
PORTAL_CLIENT_ID = os.getenv("BL_PORTAL_CLIENT_ID", "7qL5jGGai6MqBCatVeoihQx5dKEhrNCh")
MOBILE_CLIENT_ID = os.getenv("BL_MOBILE_CLIENT_ID", "")
PORTAL_PAGE_SIZE = 50

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL_S", "0.3"))
P2_POLL_INTERVAL_S       = float(os.getenv("P2_POLL_INTERVAL_S",       "2.0"))  # calm-phase interval (safe)
P2_BURST_INTERVAL_S      = float(os.getenv("P2_BURST_INTERVAL_S",      "0.8"))  # burst-phase interval (aggressive)
P2_BURST_ACTIVE_DURATION = float(os.getenv("P2_BURST_ACTIVE_DURATION",  "300")) # seconds of fast polling per cycle (default 5 min)
P2_BURST_CALM_DURATION   = float(os.getenv("P2_BURST_CALM_DURATION",    "60"))  # seconds of slow polling per cycle (default 1 min)
MAX_WORKERS = max(1, int(os.getenv("MAX_WORKERS", "10")))
RIDES_REFRESH_INTERVAL_S = int(os.getenv("RIDES_REFRESH_INTERVAL_S", "3600"))
HTTP_POOL_SIZE = max(4, int(os.getenv("HTTP_POOL_SIZE", "32")))

# Filter cache (seconds)
FILTERS_CACHE_TTL_S = int(os.getenv("FILTERS_CACHE_TTL_S", "15"))

# Burst polling (seconds)
BURST_POLL_INTERVAL_S = float(os.getenv("BURST_POLL_INTERVAL_S", "0.3"))
BURST_DURATION_S = float(os.getenv("BURST_DURATION_S", "0"))

# HTTP timeouts (seconds)
P1_POLL_TIMEOUT_S = int(os.getenv("P1_POLL_TIMEOUT_S", "8"))
P1_RESERVE_TIMEOUT_S = int(os.getenv("P1_RESERVE_TIMEOUT_S", "8"))
P2_POLL_TIMEOUT_S = int(os.getenv("P2_POLL_TIMEOUT_S", "8"))
P2_RESERVE_TIMEOUT_S = int(os.getenv("P2_RESERVE_TIMEOUT_S", "8"))
P1_REFRESH_SKEW_S = int(os.getenv("P1_REFRESH_SKEW_S", "90"))
P1_STRIP_VOLATILE_HEADERS = os.getenv("P1_STRIP_VOLATILE_HEADERS", "1") == "1"
P1_FORCE_FRESH_REQUEST_IDS = os.getenv("P1_FORCE_FRESH_REQUEST_IDS", "1") == "1"

# Toggle mock data for development (default: real polling)
USE_MOCK_P1 = False    
USE_MOCK_P2 = False
ENABLE_P1 = True
ENABLE_P2 = os.getenv("ENABLE_P2", "1") == "1"
ALWAYS_POLL_REAL_ORDERS = True  # always poll real /rides (both platforms when available)
# When enabled, accepted offers will be actually reserved via API calls (P1/P2).
AUTO_RESERVE_ENABLED = True
TRACE_USER_POLL = os.getenv("TRACE_USER_POLL", "0") == "1"

# Diagnostics
DEBUG_PRINT_OFFERS = os.getenv("DEBUG_PRINT_OFFERS", "0") == "1"
CF_DEBUG = os.getenv("CF_DEBUG", "0") == "1"
ATHENA_PRINT_DEBUG = os.getenv("ATHENA_PRINT_DEBUG", "0") == "1"
DEBUG_ENDS = os.getenv("DEBUG_ENDS", "0") == "1"
APPLY_GAP_TO_BUSY_INTERVALS = False  # gap will NOT extend busy intervals
LOG_OFFERS_PAYLOAD = os.getenv("LOG_OFFERS_PAYLOAD", "0") == "1"
LOG_RAW_API_RESPONSES = os.getenv("LOG_RAW_API_RESPONSES", "0") == "1"
MAX_LOGGED_OFFERS = int(os.getenv("MAX_LOGGED_OFFERS", "3"))
FAST_ACCEPT_MODE = os.getenv("FAST_ACCEPT_MODE", "0") == "1"
FAST_ACCEPT_NOTIFY_REJECTED = os.getenv("FAST_ACCEPT_NOTIFY_REJECTED", "1") == "1"

# --- Rides visibility ---
DUMP_RIDES_IN_LOGS = os.getenv("DUMP_RIDES_IN_LOGS", "0") == "1"
DUMP_RIDES_IN_TELEGRAM = os.getenv("DUMP_RIDES_IN_TELEGRAM", "0") == "1"
MAX_RIDES_SHOWN = int(os.getenv("MAX_RIDES_SHOWN", "20"))

# Athena token/etag helpers
ATHENA_RELOGIN_SKEW_S = int(os.getenv("ATHENA_RELOGIN_SKEW_S", "3600"))
OFFER_MEMORY_DEDUPE = os.getenv("OFFER_MEMORY_DEDUPE", "0") == "1"
ATHENA_USE_OFFERS_ETAG = os.getenv("ATHENA_USE_OFFERS_ETAG", "0") == "1"
METRICS_LOG_EVERY_CYCLES = max(1, int(os.getenv("METRICS_LOG_EVERY_CYCLES", "20")))
