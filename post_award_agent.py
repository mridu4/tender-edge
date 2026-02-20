"""
TenderEdge — Post-Award Intelligence Agent
==========================================
Agent #08 of 15 | Priority: Tier 1 — Build Immediately
Autonomy Level: FULLY AUTONOMOUS

Five Subsystems:
  1. Platform Scraper     — NeST OCDS API award data fetcher
  2. Document Parser      — Claude AI PDF/text award notice parser
  3. Intelligence Analyzer — Competitor profiles + pattern mining
  4. Prediction Engine    — Tender Prediction Score (TPS) calculator
  5. Distribution Hub     — Feeds Go/No-Go, PE Intelligence, Appeal agents

Data Sources:
  - NeST Tanzania OCDS API (primary)
  - TANePS portal
  - TED (European Tenders)
  - Manual upload fallback

Usage:
  python post_award_agent.py                    # Full pipeline run
  python post_award_agent.py --watch            # Continuous 6-hour polling
  python post_award_agent.py --tps TZ-2025-001  # Score a specific tender
  python post_award_agent.py --autopsy TZ-001   # Generate bid autopsy
  python post_award_agent.py --competitors ICT  # Show ICT competitor profiles
  python post_award_agent.py --pe "Ministry of Finance"  # PE profile
  python post_award_agent.py --seed             # Seed with demo award data
  python post_award_agent.py --report           # Print full intelligence report
"""

import requests
import sqlite3
import json
import time
import logging
import argparse
import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from dataclasses import dataclass, field

# ── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("post_award.log")
    ]
)
log = logging.getLogger("PostAward")

# ── CONFIG ────────────────────────────────────────────────────────────────────
NEST_BASE        = "https://nest.go.tz/gateway/nest-data-portal-api/api"
ANTHROPIC_API    = "https://api.anthropic.com/v1/messages"
ANTHROPIC_KEY    = ""          # ← paste your Anthropic API key here
DB_PATH          = "tenderEdge.db"
POLL_HOURS       = 6
MIN_DATA_FOR_TPS = 50          # Don't show TPS until we have this many awards

# TPS Scoring weights (from spec)
TPS_WEIGHTS = {
    "capability_match":     0.25,
    "historical_win_rate":  0.20,
    "pe_favorability":      0.15,
    "price_competitiveness":0.15,
    "competitor_density":   0.10,
    "compliance_readiness": 0.10,
    "seasonal_timing":      0.05,
}

# ── DATA MODELS ───────────────────────────────────────────────────────────────
@dataclass
class AwardRecord:
    """A single award notice extracted from NeST or other portal."""
    ocid:               str
    tender_number:      str
    tender_description: str
    pe_name:            str
    pe_code:            str
    winning_company:    str
    contract_price:     Optional[float]
    contract_currency:  str
    delivery_period:    str
    sector:             str
    award_date:         str
    source_platform:    str
    tender_estimate:    Optional[float]
    price_ratio:        Optional[float]   # contract_price / tender_estimate
    confidence_score:   float = 1.0
    raw_json:           str = ""

@dataclass
class BidOutcome:
    """Links a user's submitted bid to an award record."""
    bid_id:               str
    tenant_id:            str
    ocid:                 str
    outcome:              str             # WIN | LOSS | DISQUALIFIED | PENDING
    our_price:            Optional[float]
    winning_price:        Optional[float]
    price_diff_pct:       Optional[float]
    rejection_reasons:    list = field(default_factory=list)
    autopsy_report:       dict = field(default_factory=dict)
    lessons_learned:      list = field(default_factory=list)

@dataclass
class CompetitorProfile:
    """Intelligence profile built for a competitor company."""
    company_name:         str
    total_wins:           int = 0
    primary_sectors:      dict = field(default_factory=dict)
    pe_relationships:     dict = field(default_factory=dict)
    price_range_min:      Optional[float] = None
    price_range_max:      Optional[float] = None
    avg_price_ratio:      Optional[float] = None
    geographic_focus:     list = field(default_factory=list)
    threat_score:         float = 0.0    # 0-1 per sector
    last_win_date:        str = ""

@dataclass
class PEProfile:
    """Behavioral profile of a Procuring Entity built from award history."""
    pe_name:              str
    pe_code:              str
    total_awards:         int = 0
    avg_contract_value:   Optional[float] = None
    preferred_sectors:    dict = field(default_factory=dict)
    avg_price_ratio:      float = 0.93   # default TZ average
    price_sensitivity:    str = "MEDIUM" # HIGH | MEDIUM | LOW
    top_winners:          list = field(default_factory=list)
    disqualification_patterns: list = field(default_factory=list)
    budget_cycle_months:  list = field(default_factory=list)
    confidence_score:     float = 0.0
    strategic_notes:      str = ""


# ══════════════════════════════════════════════════════════════════════════════
# 1. DATABASE — Full Schema
# ══════════════════════════════════════════════════════════════════════════════
def init_db(db_path: str = DB_PATH):
    """Initialise all Post-Award Intelligence tables."""
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    # Award records — every award notice collected
    cur.execute("""
        CREATE TABLE IF NOT EXISTS award_records (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            ocid                TEXT UNIQUE,
            tender_number       TEXT,
            tender_description  TEXT,
            pe_name             TEXT,
            pe_code             TEXT,
            winning_company     TEXT,
            contract_price      REAL,
            contract_currency   TEXT DEFAULT 'TZS',
            delivery_period     TEXT,
            sector              TEXT,
            award_date          TEXT,
            source_platform     TEXT,
            tender_estimate     REAL,
            price_ratio         REAL,
            confidence_score    REAL DEFAULT 1.0,
            raw_json            TEXT,
            created_at          TEXT DEFAULT (datetime('now'))
        )
    """)

    # Bid outcomes — WIN/LOSS tracking per user bid
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bid_outcomes (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            bid_id              TEXT,
            tenant_id           TEXT,
            ocid                TEXT,
            outcome             TEXT,
            our_price           REAL,
            winning_price       REAL,
            price_diff_pct      REAL,
            rejection_reasons   TEXT,
            autopsy_report      TEXT,
            lessons_learned     TEXT,
            detected_at         TEXT DEFAULT (datetime('now'))
        )
    """)

    # Competitor profiles — who wins what, at what price
    cur.execute("""
        CREATE TABLE IF NOT EXISTS competitor_profiles (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name        TEXT UNIQUE,
            aliases             TEXT,
            total_wins          INTEGER DEFAULT 0,
            primary_sectors     TEXT,
            pe_relationships    TEXT,
            price_range_min     REAL,
            price_range_max     REAL,
            avg_price_ratio     REAL,
            geographic_focus    TEXT,
            threat_score        REAL DEFAULT 0.0,
            last_win_date       TEXT,
            last_updated        TEXT DEFAULT (datetime('now'))
        )
    """)

    # PE profiles — procuring entity behavioral intelligence
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pe_profiles (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            pe_name                 TEXT UNIQUE,
            pe_code                 TEXT,
            total_awards            INTEGER DEFAULT 0,
            avg_contract_value      REAL,
            preferred_sectors       TEXT,
            avg_price_ratio         REAL DEFAULT 0.93,
            price_sensitivity       TEXT DEFAULT 'MEDIUM',
            top_winners             TEXT,
            disqualification_patterns TEXT,
            budget_cycle_months     TEXT,
            confidence_score        REAL DEFAULT 0.0,
            strategic_notes         TEXT,
            last_updated            TEXT DEFAULT (datetime('now'))
        )
    """)

    # Pattern insights — mined trends across all data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pattern_insights (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            insight_type    TEXT,
            sector          TEXT,
            pe_name         TEXT,
            insight_text    TEXT,
            data_points     INTEGER,
            confidence      REAL,
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)

    # Monitoring queue — bids being watched for award outcome
    cur.execute("""
        CREATE TABLE IF NOT EXISTS monitoring_queue (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            bid_id          TEXT,
            tenant_id       TEXT,
            ocid            TEXT,
            tender_number   TEXT,
            pe_name         TEXT,
            deadline        TEXT,
            expected_award  TEXT,
            status          TEXT DEFAULT 'WATCHING',
            last_checked    TEXT,
            added_at        TEXT DEFAULT (datetime('now'))
        )
    """)

    conn.commit()
    conn.close()
    log.info("✅ Post-Award Intelligence DB schema ready")


def db_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ══════════════════════════════════════════════════════════════════════════════
# 2. SUBSYSTEM 1 — PLATFORM SCRAPER (NeST OCDS API)
# ══════════════════════════════════════════════════════════════════════════════
SECTOR_KEYWORDS = {
    "ICT":          ["software","hardware","ict","information technology","computer","network","server","system","digital","database","erp","fiber"],
    "Construction": ["construction","road","building","civil","bridge","infrastructure","rehabilitation","water","sanitation","school","drainage"],
    "Medical":      ["medical","pharmaceutical","health","hospital","drug","medicine","laboratory","equipment","clinic","vaccine","cold chain"],
    "Consultancy":  ["consultancy","consulting","advisory","feasibility","study","research","evaluation","assessment","audit"],
    "Energy":       ["energy","electricity","solar","power","generator","fuel","petroleum","gas","electrification"],
    "Supply/Goods": ["supply","goods","furniture","vehicle","stationary","uniform","food","printing","laptops"],
}

def classify_sector(text: str) -> str:
    t = text.lower()
    scores = {s: sum(1 for k in kw if k in t) for s, kw in SECTOR_KEYWORDS.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "Other"

def normalize_company_name(name: str) -> str:
    """Normalize company names to reduce fragmentation in competitor profiles."""
    if not name:
        return "Unknown"
    # Remove common suffixes/prefixes that cause duplicates
    name = re.sub(r'\b(ltd|limited|co|company|corporation|corp|inc|llc|plc|pvt|private|tanzania|tz|t\.z\.)\b', '', name.lower())
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip().title()
    return name or "Unknown"

def fetch_nest_records(cursor: int = 0, since: str = None) -> dict:
    """Fetch OCDS records (includes award data) from NeST API."""
    url = f"{NEST_BASE}/records"
    params = {"cursor": cursor}
    if since:
        params["since"] = since
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        log.error("NeST API error: %s", e)
        return {}

def fetch_nest_releases(cursor: int = 0, since: str = None) -> dict:
    """Fetch OCDS releases from NeST API."""
    url = f"{NEST_BASE}/releases"
    params = {"cursor": cursor}
    if since:
        params["since"] = since
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        log.error("NeST releases error: %s", e)
        return {}

def parse_ocds_release(release: dict) -> Optional[AwardRecord]:
    """
    Extract a full AwardRecord from a single OCDS release.
    Handles both releases with and without award data.
    """
    try:
        ocid         = release.get("ocid", "")
        tender       = release.get("tender", {})
        buyer        = release.get("buyer", {})
        awards       = release.get("awards", [])

        if not awards:
            return None  # Only process releases that have been awarded

        award = awards[0]
        suppliers = award.get("suppliers", [{}])
        winner = normalize_company_name(suppliers[0].get("name", "Unknown") if suppliers else "Unknown")

        # Contract value
        award_val  = award.get("value", {})
        price      = award_val.get("amount")
        currency   = award_val.get("currency", "TZS")

        # Tender estimate for price ratio
        tender_val = tender.get("value", {})
        estimate   = tender_val.get("amount")

        price_ratio = None
        if price and estimate and estimate > 0:
            price_ratio = round(price / estimate, 4)

        # Sector
        raw_text = (tender.get("title", "") + " " + tender.get("description", ""))
        sector   = classify_sector(raw_text)

        # PE
        pe_name = buyer.get("name", "Unknown Entity")
        pe_code = buyer.get("id",   "")

        # Delivery period
        contract_period = award.get("contractPeriod", {})
        delivery = ""
        if contract_period.get("startDate") and contract_period.get("endDate"):
            start = datetime.fromisoformat(contract_period["startDate"][:10])
            end   = datetime.fromisoformat(contract_period["endDate"][:10])
            days  = (end - start).days
            delivery = f"{days} days"

        # Award date
        award_date = award.get("date", release.get("date", ""))

        return AwardRecord(
            ocid               = ocid,
            tender_number      = tender.get("id", ocid),
            tender_description = tender.get("title", "")[:400],
            pe_name            = pe_name,
            pe_code            = pe_code,
            winning_company    = winner,
            contract_price     = price,
            contract_currency  = currency,
            delivery_period    = delivery,
            sector             = sector,
            award_date         = award_date[:10] if award_date else "",
            source_platform    = "NeST Tanzania",
            tender_estimate    = estimate,
            price_ratio        = price_ratio,
            confidence_score   = 0.9,
            raw_json           = json.dumps(release)[:3000]
        )

    except Exception as e:
        log.warning("Could not parse release %s: %s", release.get("ocid","?"), e)
        return None

def save_award_record(rec: AwardRecord, db_path: str = DB_PATH):
    conn = db_conn(db_path)
    try:
        conn.execute("""
            INSERT OR IGNORE INTO award_records
            (ocid, tender_number, tender_description, pe_name, pe_code,
             winning_company, contract_price, contract_currency, delivery_period,
             sector, award_date, source_platform, tender_estimate, price_ratio,
             confidence_score, raw_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            rec.ocid, rec.tender_number, rec.tender_description, rec.pe_name,
            rec.pe_code, rec.winning_company, rec.contract_price, rec.contract_currency,
            rec.delivery_period, rec.sector, rec.award_date, rec.source_platform,
            rec.tender_estimate, rec.price_ratio, rec.confidence_score, rec.raw_json
        ))
        conn.commit()
    finally:
        conn.close()

def run_platform_scraper(since: str = None, db_path: str = DB_PATH) -> int:
    """
    SUBSYSTEM 1: Pull all award records from NeST API.
    Returns count of new awards saved.
    """
    log.info("═" * 55)
    log.info("🔍 SUBSYSTEM 1 — Platform Scraper")
    log.info("   Source: NeST Tanzania OCDS API")
    if since:
        log.info("   Since:  %s", since)

    saved = 0
    cursor = 0

    while True:
        log.info("   Fetching records (cursor=%d)...", cursor)
        data = fetch_nest_releases(cursor=cursor, since=since)
        releases = data.get("releases", [])
        if not releases:
            break

        for rel in releases:
            record = parse_ocds_release(rel)
            if record:
                save_award_record(record, db_path)
                saved += 1
                log.info("   💾 Award saved: %s → %s | %s %s",
                    record.tender_description[:45],
                    record.winning_company,
                    f"{record.contract_price:,.0f}" if record.contract_price else "N/A",
                    record.contract_currency
                )

        next_cur = data.get("next_cursor") or data.get("nextCursor")
        if not next_cur or next_cur == cursor:
            break
        cursor = next_cur

    log.info("   ✅ Platform Scraper complete — %d new awards saved", saved)
    return saved


# ══════════════════════════════════════════════════════════════════════════════
# 3. SUBSYSTEM 2 — DOCUMENT PARSER (Claude AI)
# ══════════════════════════════════════════════════════════════════════════════
def call_claude(prompt: str, system: str = "") -> str:
    """Call Claude API for intelligence analysis."""
    if not ANTHROPIC_KEY:
        return json.dumps({"error": "No API key — set ANTHROPIC_KEY in config"})
    try:
        payload = {
            "model":      "claude-opus-4-6",
            "max_tokens": 1500,
            "system":     system or "You are TenderEdge Post-Award Intelligence Agent. Extract structured data from procurement documents. Always respond with valid JSON only.",
            "messages":   [{"role": "user", "content": prompt}]
        }
        resp = requests.post(
            ANTHROPIC_API,
            headers={
                "x-api-key":         ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json"
            },
            json=payload,
            timeout=60
        )
        resp.raise_for_status()
        return resp.json()["content"][0]["text"]
    except Exception as e:
        log.error("Claude API error: %s", e)
        return json.dumps({"error": str(e)})

def parse_award_document_with_claude(raw_text: str, tender_context: dict) -> dict:
    """
    SUBSYSTEM 2: Use Claude to extract structured data from
    an award notice PDF/text that may have inconsistent formatting.
    """
    prompt = f"""
You are parsing a procurement award notice from Tanzania.
Extract ALL available data and return ONLY valid JSON.

TENDER CONTEXT:
{json.dumps(tender_context, indent=2)}

AWARD NOTICE TEXT:
{raw_text[:3000]}

Extract and return this exact JSON structure:
{{
  "winning_company": "exact company name as written",
  "contract_price": 123456789,
  "contract_currency": "TZS",
  "delivery_period": "180 days",
  "award_date": "YYYY-MM-DD",
  "rejection_reasons": ["reason 1", "reason 2"],
  "evaluation_criteria": {{"technical": 70, "financial": 30}},
  "other_bidders": [
    {{"name": "Company B", "price": 0, "score": 0, "rejected_reason": ""}}
  ],
  "appeal_window_days": 5,
  "confidence": 0.95
}}

If a field is not found, use null. Return ONLY the JSON object, nothing else.
"""
    raw_result = call_claude(prompt)
    try:
        # Strip any markdown fences
        clean = re.sub(r'```json|```', '', raw_result).strip()
        return json.loads(clean)
    except json.JSONDecodeError:
        return {"error": "Parse failed", "raw": raw_result[:500]}


# ══════════════════════════════════════════════════════════════════════════════
# 4. SUBSYSTEM 3 — INTELLIGENCE ANALYZER
#    3a. Competitor Profiles
#    3b. PE Profiles
#    3c. Pattern Mining
#    3d. Bid Autopsy Generator
# ══════════════════════════════════════════════════════════════════════════════

# ── 3a. Competitor Profiles ───────────────────────────────────────────────────
def rebuild_competitor_profiles(db_path: str = DB_PATH):
    """
    Rebuild all competitor profiles from award_records.
    Called after each scrape cycle.
    """
    log.info("─" * 40)
    log.info("🏢 SUBSYSTEM 3a — Rebuilding Competitor Profiles")

    conn = db_conn(db_path)

    # Fetch all awards
    awards = conn.execute(
        "SELECT winning_company, sector, pe_name, contract_price, price_ratio, award_date "
        "FROM award_records WHERE winning_company IS NOT NULL AND winning_company != 'Unknown'"
    ).fetchall()

    # Aggregate per company
    profiles: dict[str, dict] = {}
    for a in awards:
        name = normalize_company_name(a["winning_company"])
        if name not in profiles:
            profiles[name] = {
                "company_name":    name,
                "total_wins":      0,
                "primary_sectors": {},
                "pe_relationships":{},
                "prices":          [],
                "ratios":          [],
                "last_win_date":   "",
            }
        p = profiles[name]
        p["total_wins"] += 1
        p["primary_sectors"][a["sector"]] = p["primary_sectors"].get(a["sector"], 0) + 1
        p["pe_relationships"][a["pe_name"]] = p["pe_relationships"].get(a["pe_name"], 0) + 1
        if a["contract_price"]:
            p["prices"].append(a["contract_price"])
        if a["price_ratio"]:
            p["ratios"].append(a["price_ratio"])
        if a["award_date"] > p["last_win_date"]:
            p["last_win_date"] = a["award_date"]

    # Calculate threat scores and save
    total_wins_max = max((p["total_wins"] for p in profiles.values()), default=1)

    for name, p in profiles.items():
        prices  = p["prices"]
        ratios  = p["ratios"]
        threat  = min(1.0, p["total_wins"] / total_wins_max)

        conn.execute("""
            INSERT INTO competitor_profiles
            (company_name, total_wins, primary_sectors, pe_relationships,
             price_range_min, price_range_max, avg_price_ratio, threat_score,
             last_win_date, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))
            ON CONFLICT(company_name) DO UPDATE SET
              total_wins        = excluded.total_wins,
              primary_sectors   = excluded.primary_sectors,
              pe_relationships  = excluded.pe_relationships,
              price_range_min   = excluded.price_range_min,
              price_range_max   = excluded.price_range_max,
              avg_price_ratio   = excluded.avg_price_ratio,
              threat_score      = excluded.threat_score,
              last_win_date     = excluded.last_win_date,
              last_updated      = excluded.last_updated
        """, (
            name,
            p["total_wins"],
            json.dumps(p["primary_sectors"]),
            json.dumps(p["pe_relationships"]),
            min(prices) if prices else None,
            max(prices) if prices else None,
            round(sum(ratios) / len(ratios), 4) if ratios else None,
            round(threat, 3),
            p["last_win_date"]
        ))

    conn.commit()
    conn.close()
    log.info("   ✅ %d competitor profiles rebuilt", len(profiles))
    return profiles

# ── 3b. PE Profiles ───────────────────────────────────────────────────────────
def rebuild_pe_profiles(db_path: str = DB_PATH):
    """
    Rebuild PE behavioral profiles from award_records.
    Covers: price sensitivity, preferred sectors, top winners, budget cycles.
    """
    log.info("─" * 40)
    log.info("🏛️  SUBSYSTEM 3b — Rebuilding PE Profiles")

    conn = db_conn(db_path)
    awards = conn.execute(
        "SELECT pe_name, pe_code, sector, contract_price, price_ratio, award_date, winning_company "
        "FROM award_records WHERE pe_name IS NOT NULL"
    ).fetchall()

    pe_data: dict[str, dict] = {}
    for a in awards:
        pe = a["pe_name"]
        if pe not in pe_data:
            pe_data[pe] = {
                "pe_code":    a["pe_code"] or "",
                "awards":     0,
                "sectors":    {},
                "prices":     [],
                "ratios":     [],
                "winners":    {},
                "months":     [],
            }
        d = pe_data[pe]
        d["awards"] += 1
        d["sectors"][a["sector"]] = d["sectors"].get(a["sector"], 0) + 1
        if a["contract_price"]:
            d["prices"].append(a["contract_price"])
        if a["price_ratio"]:
            d["ratios"].append(a["price_ratio"])
        d["winners"][a["winning_company"]] = d["winners"].get(a["winning_company"], 0) + 1
        if a["award_date"] and len(a["award_date"]) >= 7:
            d["months"].append(int(a["award_date"][5:7]))

    for pe_name, d in pe_data.items():
        ratios  = d["ratios"]
        prices  = d["prices"]
        avg_r   = round(sum(ratios)/len(ratios), 4) if ratios else 0.93

        # Price sensitivity: HIGH = awards at 85%+ of estimate, LOW = awards near 100%
        price_sensitivity = "MEDIUM"
        if avg_r < 0.88:
            price_sensitivity = "HIGH"   # PE is aggressive on price
        elif avg_r > 0.96:
            price_sensitivity = "LOW"    # PE doesn't care as much about price

        top_winners = sorted(d["winners"].items(), key=lambda x: -x[1])[:5]

        # Budget cycle: which months have most awards?
        month_counts = {}
        for m in d["months"]:
            month_counts[m] = month_counts.get(m, 0) + 1
        peak_months = sorted(month_counts, key=lambda m: -month_counts[m])[:3]

        confidence = min(1.0, d["awards"] / 20)  # Grows with data

        conn.execute("""
            INSERT INTO pe_profiles
            (pe_name, pe_code, total_awards, avg_contract_value, preferred_sectors,
             avg_price_ratio, price_sensitivity, top_winners, budget_cycle_months,
             confidence_score, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now'))
            ON CONFLICT(pe_name) DO UPDATE SET
              total_awards       = excluded.total_awards,
              avg_contract_value = excluded.avg_contract_value,
              preferred_sectors  = excluded.preferred_sectors,
              avg_price_ratio    = excluded.avg_price_ratio,
              price_sensitivity  = excluded.price_sensitivity,
              top_winners        = excluded.top_winners,
              budget_cycle_months= excluded.budget_cycle_months,
              confidence_score   = excluded.confidence_score,
              last_updated       = excluded.last_updated
        """, (
            pe_name,
            d["pe_code"],
            d["awards"],
            round(sum(prices)/len(prices)) if prices else None,
            json.dumps(d["sectors"]),
            avg_r,
            price_sensitivity,
            json.dumps([w[0] for w in top_winners]),
            json.dumps(peak_months),
            round(confidence, 3)
        ))

    conn.commit()
    conn.close()
    log.info("   ✅ %d PE profiles rebuilt", len(pe_data))
    return pe_data

# ── 3c. Pattern Mining ────────────────────────────────────────────────────────
def run_pattern_mining(db_path: str = DB_PATH) -> list:
    """
    Mine cross-cutting patterns across all award data.
    Generates actionable insights for users.
    """
    log.info("─" * 40)
    log.info("🔬 SUBSYSTEM 3c — Pattern Mining")

    conn = db_conn(db_path)
    insights = []

    # Pattern 1: Price-to-Win by sector
    rows = conn.execute("""
        SELECT sector,
               COUNT(*)            AS cnt,
               AVG(price_ratio)    AS avg_ratio,
               MIN(price_ratio)    AS min_ratio,
               MAX(price_ratio)    AS max_ratio
        FROM award_records
        WHERE price_ratio IS NOT NULL AND price_ratio BETWEEN 0.5 AND 1.1
        GROUP BY sector
        HAVING cnt >= 3
        ORDER BY cnt DESC
    """).fetchall()

    for r in rows:
        insight = {
            "type":        "PRICE_TO_WIN",
            "sector":      r["sector"],
            "text":        f"In {r['sector']}, winning bids average {r['avg_ratio']*100:.1f}% of the tender estimate. Safe range: {r['min_ratio']*100:.0f}%–{r['max_ratio']*100:.0f}%.",
            "data_points": r["cnt"],
            "confidence":  min(1.0, r["cnt"] / 20),
            "metric":      {"avg": r["avg_ratio"], "min": r["min_ratio"], "max": r["max_ratio"]}
        }
        insights.append(insight)
        log.info("   💡 [PRICE] %s", insight["text"])

    # Pattern 2: Most active PEs (where to focus)
    rows = conn.execute("""
        SELECT pe_name, COUNT(*) AS cnt, AVG(contract_price) AS avg_val
        FROM award_records
        GROUP BY pe_name
        HAVING cnt >= 3
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()

    for r in rows:
        insight = {
            "type":        "PE_ACTIVITY",
            "pe_name":     r["pe_name"],
            "text":        f"{r['pe_name']} is highly active with {r['cnt']} awards. Average contract: TZS {r['avg_val']:,.0f}." if r["avg_val"] else f"{r['pe_name']} issued {r['cnt']} awards.",
            "data_points": r["cnt"],
            "confidence":  min(1.0, r["cnt"] / 15),
        }
        insights.append(insight)

    # Pattern 3: Top competitors by sector
    rows = conn.execute("""
        SELECT sector, winning_company, COUNT(*) AS wins
        FROM award_records
        WHERE winning_company != 'Unknown'
        GROUP BY sector, winning_company
        ORDER BY sector, wins DESC
    """).fetchall()

    sector_leaders = {}
    for r in rows:
        if r["sector"] not in sector_leaders:
            sector_leaders[r["sector"]] = []
        if len(sector_leaders[r["sector"]]) < 3:
            sector_leaders[r["sector"]].append((r["winning_company"], r["wins"]))

    for sector, leaders in sector_leaders.items():
        if leaders:
            top = leaders[0]
            insight = {
                "type":        "SECTOR_LEADER",
                "sector":      sector,
                "text":        f"In {sector}, {top[0]} leads with {top[1]} wins. Watch this competitor carefully.",
                "data_points": top[1],
                "confidence":  min(1.0, top[1] / 10),
                "leader":      top[0]
            }
            insights.append(insight)
            log.info("   🏆 [LEADER] %s", insight["text"])

    # Pattern 4: Seasonal award timing
    rows = conn.execute("""
        SELECT SUBSTR(award_date, 6, 2) AS month, COUNT(*) AS cnt
        FROM award_records
        WHERE award_date IS NOT NULL AND award_date != ''
        GROUP BY month
        ORDER BY cnt DESC
        LIMIT 3
    """).fetchall()

    if rows:
        month_names = {
            "01":"January","02":"February","03":"March","04":"April",
            "05":"May","06":"June","07":"July","08":"August",
            "09":"September","10":"October","11":"November","12":"December"
        }
        peak = [month_names.get(r["month"], r["month"]) for r in rows]
        insight = {
            "type":        "SEASONAL",
            "text":        f"Most awards are published in {', '.join(peak)}. Plan submission capacity accordingly.",
            "data_points": sum(r["cnt"] for r in rows),
            "confidence":  0.75,
        }
        insights.append(insight)
        log.info("   📅 [SEASONAL] %s", insight["text"])

    # Save insights to DB
    conn.execute("DELETE FROM pattern_insights")
    for ins in insights:
        conn.execute("""
            INSERT INTO pattern_insights
            (insight_type, sector, pe_name, insight_text, data_points, confidence)
            VALUES (?,?,?,?,?,?)
        """, (
            ins.get("type",""),
            ins.get("sector",""),
            ins.get("pe_name",""),
            ins.get("text",""),
            ins.get("data_points", 0),
            ins.get("confidence", 0),
        ))

    conn.commit()
    conn.close()
    log.info("   ✅ %d patterns mined and saved", len(insights))
    return insights

# ── 3d. Bid Autopsy Generator ─────────────────────────────────────────────────
def generate_autopsy(
    bid: dict,          # User's submitted bid details
    award: AwardRecord, # The matching award record
    db_path: str = DB_PATH
) -> dict:
    """
    SUBSYSTEM 3d: Generate a Claude-powered bid autopsy report.
    Compares user's bid to the winning bid and generates lessons learned.
    """
    log.info("🔬 Generating bid autopsy for OCID: %s", award.ocid)

    # Get PE profile for context
    conn = db_conn(db_path)
    pe_row = conn.execute(
        "SELECT * FROM pe_profiles WHERE pe_name = ?", (award.pe_name,)
    ).fetchone()
    conn.close()

    pe_context = dict(pe_row) if pe_row else {}

    our_price   = bid.get("submitted_price")
    win_price   = award.contract_price
    price_diff  = None
    if our_price and win_price:
        price_diff = round((our_price - win_price) / win_price * 100, 2)

    prompt = f"""
You are the TenderEdge Post-Award Intelligence Agent generating a bid autopsy report.

TENDER: {award.tender_description}
PE: {award.pe_name}
SECTOR: {award.sector}

OUR BID:
- Submitted Price: {f"TZS {our_price:,.0f}" if our_price else "Not recorded"}
- Technical Score: {bid.get("technical_score", "Not recorded")}
- Compliance Status: {bid.get("compliance_status", "Not recorded")}

OUTCOME:
- Winner: {award.winning_company}
- Winning Price: {f"TZS {win_price:,.0f}" if win_price else "Not available"}
- Price Difference: {f"{price_diff:+.1f}% vs winning price" if price_diff else "Not calculable"}
- Delivery Period: {award.delivery_period or "Not recorded"}

PE BEHAVIORAL PROFILE:
- Price Sensitivity: {pe_context.get("price_sensitivity", "MEDIUM")}
- Avg Win Ratio: {pe_context.get("avg_price_ratio", 0.93):.1%} of estimate
- Top Historical Winners: {pe_context.get("top_winners", "[]")}

Generate a JSON autopsy report with this exact structure:
{{
  "outcome": "WIN" or "LOSS" or "DISQUALIFIED",
  "primary_loss_reason": "single most likely reason we lost",
  "price_analysis": {{
    "our_price": {our_price or 0},
    "winning_price": {win_price or 0},
    "price_diff_pct": {price_diff or 0},
    "verdict": "We were too expensive / competitive / below market",
    "recommendation": "Specific pricing advice for next time"
  }},
  "lessons_learned": [
    "Specific lesson 1",
    "Specific lesson 2",
    "Specific lesson 3"
  ],
  "competitor_intelligence": {{
    "winner": "{award.winning_company}",
    "threat_level": "HIGH/MEDIUM/LOW",
    "notes": "What makes this competitor strong in this sector"
  }},
  "pe_insights": {{
    "price_sensitivity": "{pe_context.get("price_sensitivity", "MEDIUM")}",
    "recommendation": "How to approach this PE better next time"
  }},
  "next_bid_actions": [
    "Action 1 to improve next bid",
    "Action 2",
    "Action 3"
  ],
  "appeal_recommended": false,
  "appeal_grounds": ""
}}
Return ONLY the JSON object.
"""

    raw = call_claude(prompt)
    try:
        clean = re.sub(r'```json|```', '', raw).strip()
        result = json.loads(clean)
    except:
        result = {
            "outcome":           "LOSS",
            "primary_loss_reason": "Could not generate autopsy — check API key",
            "price_analysis":    {"our_price": our_price, "winning_price": win_price, "price_diff_pct": price_diff},
            "lessons_learned":   ["Ensure ANTHROPIC_KEY is set for full autopsy"],
            "next_bid_actions":  ["Review pricing strategy", "Analyse PE award patterns"],
            "appeal_recommended": False
        }

    log.info("   ✅ Autopsy complete — Outcome: %s", result.get("outcome","?"))
    return result


# ══════════════════════════════════════════════════════════════════════════════
# 5. SUBSYSTEM 4 — TENDER PREDICTION SCORE (TPS)
# ══════════════════════════════════════════════════════════════════════════════
def calculate_tps(
    tender: dict,           # New tender being evaluated
    company_profile: dict,  # User's company profile
    db_path: str = DB_PATH
) -> dict:
    """
    SUBSYSTEM 4: Tender Prediction Score (TPS)
    Scores 0-100 the likelihood of winning a specific tender.
    Only shown to users after MIN_DATA_FOR_TPS award records exist.

    Weights:
      Capability Match     25% — Does company match tender requirements?
      Historical Win Rate  20% — Win rate for similar sector/value
      PE Favorability      15% — Past outcomes with this PE
      Price Competitiveness 15% — Is our typical price in the winning range?
      Competitor Density   10% — How many strong competitors in this sector?
      Compliance Readiness 10% — % of required docs already in vault
      Seasonal Timing       5% — Does timing align with PE's budget cycle?
    """
    conn = db_conn(db_path)

    # Check if we have enough data
    total_awards = conn.execute("SELECT COUNT(*) AS c FROM award_records").fetchone()["c"]
    if total_awards < MIN_DATA_FOR_TPS:
        conn.close()
        return {
            "tps": None,
            "message": f"TPS requires {MIN_DATA_FOR_TPS} award records. Currently: {total_awards}. Keep collecting data.",
            "data_points": total_awards
        }

    sector  = tender.get("sector", "Other")
    pe_name = tender.get("procuring_entity", "")
    value   = tender.get("value_amount", 0) or 0

    scores = {}

    # ── 1. Capability Match (25%) ──────────────────────────────────────────
    company_sectors = company_profile.get("sectors", [])
    if sector in company_sectors:
        scores["capability_match"] = 0.85
    elif any(s in sector for s in company_sectors):
        scores["capability_match"] = 0.60
    else:
        scores["capability_match"] = 0.30

    # ── 2. Historical Win Rate (20%) ───────────────────────────────────────
    # Use platform-wide win rate for this sector as proxy
    sector_data = conn.execute("""
        SELECT COUNT(*) AS total,
               AVG(price_ratio) AS avg_ratio
        FROM award_records WHERE sector = ?
    """, (sector,)).fetchone()

    if sector_data and sector_data["total"] > 5:
        # More data = higher base confidence
        scores["historical_win_rate"] = min(0.85, 0.40 + (sector_data["total"] / 100))
    else:
        scores["historical_win_rate"] = 0.45

    # ── 3. PE Favorability (15%) ───────────────────────────────────────────
    pe_row = conn.execute(
        "SELECT * FROM pe_profiles WHERE pe_name = ?", (pe_name,)
    ).fetchone()

    if pe_row:
        top_winners = json.loads(pe_row["top_winners"] or "[]")
        company_name = normalize_company_name(company_profile.get("name", ""))
        if company_name in [normalize_company_name(w) for w in top_winners]:
            scores["pe_favorability"] = 0.90  # We've won here before
        else:
            scores["pe_favorability"] = 0.45  # New PE for us
        # Adjust for PE confidence score
        scores["pe_favorability"] *= (0.5 + pe_row["confidence_score"] * 0.5)
    else:
        scores["pe_favorability"] = 0.50  # Unknown PE

    # ── 4. Price Competitiveness (15%) ────────────────────────────────────
    price_data = conn.execute("""
        SELECT AVG(price_ratio) AS avg_r,
               MIN(price_ratio) AS min_r,
               MAX(price_ratio) AS max_r
        FROM award_records
        WHERE sector = ? AND price_ratio IS NOT NULL
    """, (sector,)).fetchone()

    our_typical_ratio = company_profile.get("typical_price_ratio", 0.92)
    if price_data and price_data["avg_r"]:
        avg = price_data["avg_r"]
        span = (price_data["max_r"] - price_data["min_r"]) or 0.1
        # Score based on how close we are to the avg winning ratio
        diff = abs(our_typical_ratio - avg)
        scores["price_competitiveness"] = max(0.2, 1.0 - (diff / span))
    else:
        scores["price_competitiveness"] = 0.60

    # ── 5. Competitor Density (10%) ────────────────────────────────────────
    competitor_count = conn.execute("""
        SELECT COUNT(DISTINCT winning_company) AS cnt
        FROM award_records WHERE sector = ?
    """, (sector,)).fetchone()["cnt"]

    if competitor_count <= 3:
        scores["competitor_density"] = 0.85   # Low competition
    elif competitor_count <= 8:
        scores["competitor_density"] = 0.65   # Moderate
    elif competitor_count <= 15:
        scores["competitor_density"] = 0.45   # High
    else:
        scores["competitor_density"] = 0.30   # Very high

    # ── 6. Compliance Readiness (10%) ─────────────────────────────────────
    # Based on company profile completeness
    docs_ready = company_profile.get("compliance_score", 0.7)
    scores["compliance_readiness"] = docs_ready

    # ── 7. Seasonal Timing (5%) ───────────────────────────────────────────
    if pe_row:
        peak_months = json.loads(pe_row["budget_cycle_months"] or "[]")
        current_month = datetime.now().month
        scores["seasonal_timing"] = 0.85 if current_month in peak_months else 0.50
    else:
        scores["seasonal_timing"] = 0.60

    conn.close()

    # ── CALCULATE FINAL TPS ───────────────────────────────────────────────
    tps_raw = sum(scores[factor] * weight for factor, weight in TPS_WEIGHTS.items())
    tps = round(tps_raw * 100)

    # Confidence label
    if tps >= 75:
        label = "HIGH"
        action = "Strongly recommended — bid on this tender"
    elif tps >= 55:
        label = "MEDIUM"
        action = "Worth bidding — strengthen pricing and compliance"
    elif tps >= 35:
        label = "LOW"
        action = "Proceed with caution — significant competition"
    else:
        label = "VERY LOW"
        action = "Not recommended — better opportunities available"

    result = {
        "tps":           tps,
        "label":         label,
        "action":        action,
        "data_points":   total_awards,
        "factor_scores": {
            factor: {
                "score":      round(score * 100),
                "weight":     f"{int(TPS_WEIGHTS[factor]*100)}%",
                "contribution": round(score * TPS_WEIGHTS[factor] * 100)
            }
            for factor, score in scores.items()
        },
        "top_insight":   f"{'PE is price-sensitive' if pe_row and pe_row['price_sensitivity'] == 'HIGH' else 'Standard competitive tender'}. "
                         f"Winning bids in {sector} average {price_data['avg_r']*100:.0f}% of estimate." if price_data and price_data['avg_r'] else ""
    }

    log.info("🎯 TPS for %s: %d/100 (%s)", tender.get("title","?")[:50], tps, label)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# 6. SUBSYSTEM 5 — DISTRIBUTION HUB (Agent Messages)
# ══════════════════════════════════════════════════════════════════════════════
def distribute_to_agents(award: AwardRecord, tps_result: dict, db_path: str = DB_PATH) -> dict:
    """
    SUBSYSTEM 5: Format and distribute intelligence to other agents.
    In production this sends via Bid Manager message bus.
    For now, logs and returns JSON message payloads.
    """

    # Message to Go/No-Go Agent
    go_nogo_msg = {
        "message_type":    "WIN_PROBABILITY",
        "from_agent":      "post_award_intelligence",
        "to_agent":        "go_nogo_decision",
        "ocid":            award.ocid,
        "sector":          award.sector,
        "pe_name":         award.pe_name,
        "tps":             tps_result.get("tps"),
        "tps_label":       tps_result.get("label"),
        "recommended_action": tps_result.get("action"),
        "timestamp":       datetime.now(timezone.utc).isoformat()
    }

    # Message to PE Intelligence Agent
    pe_intel_msg = {
        "message_type":    "PE_AWARD_DATA",
        "from_agent":      "post_award_intelligence",
        "to_agent":        "pe_intelligence",
        "pe_name":         award.pe_name,
        "pe_code":         award.pe_code,
        "award_data": {
            "winner":       award.winning_company,
            "price":        award.contract_price,
            "price_ratio":  award.price_ratio,
            "sector":       award.sector,
            "award_date":   award.award_date,
        },
        "timestamp":       datetime.now(timezone.utc).isoformat()
    }

    # Message to Pricing Advisor
    pricing_msg = {
        "message_type":    "PRICE_BENCHMARK_UPDATE",
        "from_agent":      "post_award_intelligence",
        "to_agent":        "pricing_advisor",
        "sector":          award.sector,
        "pe_name":         award.pe_name,
        "winning_ratio":   award.price_ratio,
        "contract_value":  award.contract_price,
        "timestamp":       datetime.now(timezone.utc).isoformat()
    }

    log.info("📡 SUBSYSTEM 5 — Distributed to agents:")
    log.info("   → Go/No-Go: TPS=%s (%s)", tps_result.get("tps"), tps_result.get("label"))
    log.info("   → PE Intel: %s — price ratio %.2f", award.pe_name, award.price_ratio or 0)
    log.info("   → Pricing:  %s sector benchmark updated", award.sector)

    return {
        "go_nogo":  go_nogo_msg,
        "pe_intel": pe_intel_msg,
        "pricing":  pricing_msg
    }


# ══════════════════════════════════════════════════════════════════════════════
# 7. FULL PIPELINE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════
def run_full_pipeline(since: str = None, db_path: str = DB_PATH):
    """
    Run all 5 subsystems in sequence.
    This is the main 6-hour cron job.
    """
    log.info("╔" + "═" * 58 + "╗")
    log.info("║  TenderEdge — Post-Award Intelligence Agent             ║")
    log.info("║  Full Pipeline Run — %s  ║", datetime.now().strftime("%Y-%m-%d %H:%M"))
    log.info("╚" + "═" * 58 + "╝")

    start = time.time()

    # 1. Scrape awards from NeST
    new_awards = run_platform_scraper(since=since, db_path=db_path)

    # 2. Rebuild competitor profiles
    competitors = rebuild_competitor_profiles(db_path=db_path)

    # 3. Rebuild PE profiles
    pe_profiles = rebuild_pe_profiles(db_path=db_path)

    # 4. Run pattern mining
    patterns = run_pattern_mining(db_path=db_path)

    # Summary
    elapsed = round(time.time() - start, 1)
    conn = db_conn(db_path)
    total = conn.execute("SELECT COUNT(*) AS c FROM award_records").fetchone()["c"]
    conn.close()

    log.info("")
    log.info("╔" + "═" * 58 + "╗")
    log.info("║  PIPELINE COMPLETE                                      ║")
    log.info("║  New awards scraped  : %-30s  ║", new_awards)
    log.info("║  Total award records : %-30s  ║", total)
    log.info("║  Competitor profiles : %-30s  ║", len(competitors))
    log.info("║  PE profiles         : %-30s  ║", len(pe_profiles))
    log.info("║  Pattern insights    : %-30s  ║", len(patterns))
    log.info("║  Elapsed             : %-28ss  ║", elapsed)
    log.info("╚" + "═" * 58 + "╝")

    return {
        "new_awards":    new_awards,
        "total_awards":  total,
        "competitors":   len(competitors),
        "pe_profiles":   len(pe_profiles),
        "patterns":      len(patterns),
    }

def watch_mode(db_path: str = DB_PATH):
    """Run full pipeline every 6 hours continuously."""
    log.info("👁️  Post-Award Intelligence Agent — WATCH MODE (every %dh)", POLL_HOURS)
    while True:
        since = (datetime.now(timezone.utc) - timedelta(hours=POLL_HOURS)).isoformat()
        run_full_pipeline(since=since, db_path=db_path)
        next_run = datetime.now() + timedelta(hours=POLL_HOURS)
        log.info("💤 Next run at %s", next_run.strftime("%Y-%m-%d %H:%M"))
        time.sleep(POLL_HOURS * 3600)


# ══════════════════════════════════════════════════════════════════════════════
# 8. QUERY HELPERS (for dashboard API)
# ══════════════════════════════════════════════════════════════════════════════
def get_competitor_profile(company: str, db_path: str = DB_PATH) -> dict:
    conn = db_conn(db_path)
    row  = conn.execute(
        "SELECT * FROM competitor_profiles WHERE company_name LIKE ?", (f"%{company}%",)
    ).fetchone()
    conn.close()
    return dict(row) if row else {}

def get_pe_profile(pe_name: str, db_path: str = DB_PATH) -> dict:
    conn = db_conn(db_path)
    row  = conn.execute(
        "SELECT * FROM pe_profiles WHERE pe_name LIKE ?", (f"%{pe_name}%",)
    ).fetchone()
    conn.close()
    if not row:
        return {}
    d = dict(row)
    for f in ["preferred_sectors","top_winners","budget_cycle_months"]:
        try:
            d[f] = json.loads(d[f] or "[]")
        except:
            d[f] = []
    return d

def get_all_competitors(sector: str = None, db_path: str = DB_PATH) -> list:
    conn = db_conn(db_path)
    if sector:
        rows = conn.execute(
            "SELECT * FROM competitor_profiles WHERE primary_sectors LIKE ? ORDER BY total_wins DESC LIMIT 20",
            (f"%{sector}%",)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM competitor_profiles ORDER BY total_wins DESC LIMIT 30"
        ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        try: d["primary_sectors"] = json.loads(d["primary_sectors"] or "{}")
        except: d["primary_sectors"] = {}
        result.append(d)
    return result

def get_pricing_benchmark(sector: str, db_path: str = DB_PATH) -> dict:
    conn = db_conn(db_path)
    rows = conn.execute("""
        SELECT price_ratio FROM award_records
        WHERE sector = ? AND price_ratio BETWEEN 0.5 AND 1.1
        ORDER BY award_date DESC LIMIT 100
    """, (sector,)).fetchall()
    conn.close()
    ratios = [r["price_ratio"] for r in rows]
    if not ratios:
        return {"sector": sector, "data_points": 0}
    s = sorted(ratios)
    return {
        "sector":        sector,
        "data_points":   len(ratios),
        "avg_ratio":     round(sum(ratios)/len(ratios), 4),
        "median":        s[len(s)//2],
        "p25":           s[len(s)//4],
        "p75":           s[len(s)*3//4],
        "recommended":   f"Bid between {s[len(s)//4]*100:.1f}% and {s[len(s)*3//4]*100:.1f}% of estimate",
        "min":           min(ratios),
        "max":           max(ratios),
    }

def get_intelligence_report(db_path: str = DB_PATH) -> dict:
    conn = db_conn(db_path)
    total    = conn.execute("SELECT COUNT(*) AS c FROM award_records").fetchone()["c"]
    comps    = conn.execute("SELECT COUNT(*) AS c FROM competitor_profiles").fetchone()["c"]
    pes      = conn.execute("SELECT COUNT(*) AS c FROM pe_profiles").fetchone()["c"]
    insights = conn.execute("SELECT * FROM pattern_insights ORDER BY confidence DESC LIMIT 10").fetchall()
    top_comp = conn.execute("SELECT * FROM competitor_profiles ORDER BY total_wins DESC LIMIT 5").fetchall()
    top_pe   = conn.execute("SELECT * FROM pe_profiles ORDER BY total_awards DESC LIMIT 5").fetchall()
    conn.close()
    return {
        "summary": {"total_awards": total, "competitor_profiles": comps, "pe_profiles": pes},
        "top_competitors":  [dict(r) for r in top_comp],
        "top_pe_profiles":  [dict(r) for r in top_pe],
        "pattern_insights": [dict(r) for r in insights],
    }


# ══════════════════════════════════════════════════════════════════════════════
# 9. DEMO DATA SEEDER
# ══════════════════════════════════════════════════════════════════════════════
def seed_demo_data(db_path: str = DB_PATH):
    """Seed realistic award data for development & cold-start mitigation."""
    log.info("🌱 Seeding demo award data...")

    demo_awards = [
        AwardRecord("TZ-A-001","88Z5/2024/ICT/001","Supply of 500 Laptops to TRA","Tanzania Revenue Authority","TRA","ABC Tech Solutions Ltd",185000000,"TZS","90 days","ICT","2024-11-15","NeST Tanzania",200000000,0.925),
        AwardRecord("TZ-A-002","PPRA/2024/CON/042","Road Rehabilitation Moshi-Arusha 180km","Tanzania Roads Authority","TANROADS","Strabag East Africa Ltd",4200000000,"TZS","730 days","Construction","2024-10-20","NeST Tanzania",4500000000,0.933),
        AwardRecord("TZ-A-003","MSD/2024/MED/019","Essential Medicines Supply Q3 2024","Medical Stores Department","MSD","Pharmaceutical Distributors TZ",850000000,"TZS","180 days","Medical","2024-09-30","NeST Tanzania",920000000,0.924),
        AwardRecord("TZ-A-004","ZECO/2024/ENE/007","Solar PV Installation Zanzibar Rural","ZECO Zanzibar","ZECO",2800000000,"TZS","540 days","Energy","2024-08-15","NeST Tanzania",3200000000,0.875),
        AwardRecord("TZ-A-005","MOH/2024/CON/031","Construction District Hospital Dodoma","Ministry of Health","MOH","China Civil Engineering Construction",4100000000,"TZS","900 days","Construction","2024-07-22","NeST Tanzania",4500000000,0.911),
        AwardRecord("TZ-A-006","MOF/2024/ICT/012","ERP System Implementation Treasury","Ministry of Finance","MOF","SAP East Africa Ltd",1100000000,"TZS","365 days","ICT","2024-06-10","NeST Tanzania",1200000000,0.917),
        AwardRecord("TZ-A-007","RUWASA/2024/CON/018","Water Supply Tabora Region","RUWASA Tanzania","RUWASA","Hydrocon Engineering",1950000000,"TZS","545 days","Construction","2024-05-30","NeST Tanzania",2100000000,0.929),
        AwardRecord("TZ-A-008","MNH/2024/MED/011","Laboratory Equipment Supply MNH","Muhimbili National Hospital","MNH","Mediq East Africa Ltd",480000000,"TZS","120 days","Medical","2024-04-18","NeST Tanzania",520000000,0.923),
        AwardRecord("TZ-A-009","TPA/2024/CON/009","Port Access Road Rehabilitation DSM","Tanzania Ports Authority","TPA","Avic International Tanzania",6200000000,"TZS","730 days","Construction","2024-03-25","NeST Tanzania",6800000000,0.912),
        AwardRecord("TZ-A-010","TANESCO/2024/ICT/005","ICT Infrastructure Upgrade TANESCO","TANESCO","TANESCO","Huawei Technologies Tanzania",350000000,"TZS","180 days","ICT","2024-03-01","NeST Tanzania",380000000,0.921),
        AwardRecord("TZ-A-011","PPRA/2024/SUP/003","Office Furniture PPRA HQ","PPRA Tanzania","PPRA","Local Furniture Masters Ltd",42000000,"TZS","60 days","Supply/Goods","2024-02-14","NeST Tanzania",45000000,0.933),
        AwardRecord("TZ-A-012","MOE/2024/CON/027","Rural School Classrooms Phase 1","Ministry of Education","MOE","Ramco Group Tanzania",3300000000,"TZS","730 days","Construction","2024-01-30","NeST Tanzania",3600000000,0.917),
        AwardRecord("TZ-A-013","MSD/2023/MED/044","Cold Chain Equipment Supply","Medical Stores Department","MSD","Thermo Fisher Scientific EA",310000000,"TZS","90 days","Medical","2023-12-20","NeST Tanzania",340000000,0.912),
        AwardRecord("TZ-A-014","TANROADS/2023/CON/061","Bridge Construction Rufiji","Tanzania Roads Authority","TANROADS","China Sichuan International",5800000000,"TZS","900 days","Construction","2023-11-15","NeST Tanzania",6300000000,0.921),
        AwardRecord("TZ-A-015","MOF/2023/CON/014","Consultancy Revenue System","Ministry of Finance","MOF","Deloitte East Africa",165000000,"TZS","240 days","Consultancy","2023-10-08","NeST Tanzania",180000000,0.917),
        AwardRecord("KE-A-001","KRA/2024/ICT/033","Laptops Network Infrastructure KRA","Kenya Revenue Authority","KRA","Dimension Data Kenya",41000000,"KES","90 days","ICT","2024-09-05","Kenya Tenders",45000000,0.911),
        AwardRecord("KE-A-002","MOH-KE/2024/MED/021","Medical Supplies County Hospitals","Ministry of Health Kenya","MOH-KE","Pharmacy & Poisons Board KE",920000000,"KES","180 days","Medical","2024-08-20","Kenya Tenders",1000000000,0.920),
    ]

    for a in demo_awards:
        save_award_record(a, db_path)

    log.info("✅ %d demo award records seeded", len(demo_awards))
    rebuild_competitor_profiles(db_path)
    rebuild_pe_profiles(db_path)
    run_pattern_mining(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# 10. CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TenderEdge Post-Award Intelligence Agent")
    parser.add_argument("--watch",       action="store_true",  help="Continuous 6-hour polling")
    parser.add_argument("--seed",        action="store_true",  help="Seed demo award data")
    parser.add_argument("--report",      action="store_true",  help="Print full intelligence report")
    parser.add_argument("--competitors", metavar="SECTOR",     help="Show competitor profiles for sector")
    parser.add_argument("--pe",          metavar="PE_NAME",    help="Show PE profile")
    parser.add_argument("--pricing",     metavar="SECTOR",     help="Show pricing benchmark")
    parser.add_argument("--tps",         metavar="OCID",       help="Calculate TPS for a tender")
    parser.add_argument("--since",       metavar="DATE",       help="Fetch since date YYYY-MM-DD")
    parser.add_argument("--db",          default=DB_PATH,      help="SQLite DB path")
    args = parser.parse_args()

    init_db(args.db)

    if args.seed:
        seed_demo_data(args.db)

    elif args.watch:
        watch_mode(args.db)

    elif args.report:
        report = get_intelligence_report(args.db)
        print("\n" + "═" * 60)
        print("📊 TENDERGEDGE POST-AWARD INTELLIGENCE REPORT")
        print("═" * 60)
        s = report["summary"]
        print(f"  Total Award Records   : {s['total_awards']}")
        print(f"  Competitor Profiles   : {s['competitor_profiles']}")
        print(f"  PE Profiles           : {s['pe_profiles']}")
        print("\n🏢 TOP COMPETITORS")
        for c in report["top_competitors"]:
            print(f"  {c['company_name']:<40} Wins: {c['total_wins']}  Threat: {c['threat_score']:.2f}")
        print("\n🏛️  TOP PROCURING ENTITIES")
        for p in report["top_pe_profiles"]:
            print(f"  {p['pe_name']:<40} Awards: {p['total_awards']}  Price Sens: {p['price_sensitivity']}")
        print("\n💡 KEY INSIGHTS")
        for i in report["pattern_insights"]:
            print(f"  [{i['insight_type']}] {i['insight_text']}")
        print("═" * 60)

    elif args.competitors:
        comps = get_all_competitors(sector=args.competitors, db_path=args.db)
        print(f"\n🏢 Competitors in {args.competitors}:")
        for c in comps:
            print(f"  {c['company_name']:<40} Wins:{c['total_wins']:>4}  Threat:{c['threat_score']:.2f}  Avg Ratio:{c['avg_price_ratio'] or 0:.3f}")

    elif args.pe:
        profile = get_pe_profile(args.pe, args.db)
        print(f"\n🏛️  PE Profile: {profile.get('pe_name','Not found')}")
        print(json.dumps(profile, indent=2, default=str))

    elif args.pricing:
        bench = get_pricing_benchmark(args.pricing, args.db)
        print(f"\n💰 Pricing Benchmark: {args.pricing}")
        print(json.dumps(bench, indent=2))

    elif args.tps:
        # Example TPS calculation with dummy company profile
        tender = {"sector": "ICT", "procuring_entity": "Ministry of Finance", "value_amount": 500000000, "title": "Test Tender"}
        company = {"name": "My Company", "sectors": ["ICT"], "compliance_score": 0.85, "typical_price_ratio": 0.92}
        result = calculate_tps(tender, company, args.db)
        print(f"\n🎯 Tender Prediction Score")
        print(json.dumps(result, indent=2))

    else:
        # Default: full pipeline
        since = f"{args.since}T00:00:00Z" if args.since else None
        run_full_pipeline(since=since, db_path=args.db)
