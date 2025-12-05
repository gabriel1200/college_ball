# College Basketball Data Pipeline

A robust Python-based scraping system for college basketball game data, featuring dual data sources (ESPN API and NCAA.com) with intelligent caching and historical backfill capabilities.

## 🎯 Overview

This pipeline automatically collects comprehensive college basketball data including:
- Game schedules and results
- Play-by-play data
- Team and player statistics
- Real-time game status tracking


## 📂 Repository Structure

### Primary Pipeline (ESPN API)

#### `live_scrape.py`
**Purpose**: Daily driver for real-time game tracking  
**What it does**:
- Fetches today's game schedule
- Determines game status (Pre-game, In-Progress, Final)
- Updates master records with latest data
- Includes "Smart Backfill" for legacy data
- Skips re-scraping finalized games

**When to use**: Run daily to track current games

#### `season_loop.py`
**Purpose**: Historical data backfiller  
**What it does**:
- Iterates through a date range (e.g., season start to yesterday)
- Populates missing historical data
- Checks for existing data to avoid duplicates
- Cross references and adds to a (overridable) log of already fully scraped game dates for speed purposes

**When to use**: Initial setup or filling data gaps

### Backup Pipeline (NCAA.com)

#### `backup_start.py`
**Purpose**: Schedule generator for backup pipeline  
**What it does**:
- Scrapes NCAA.com for game schedules
- Creates `backup_schedule.csv` for specified date range

**When to use**: Before running backup scrapers

#### `backup_daily.py`
**Purpose**: Live backup scraper  
**What it does**:
- Reads schedule file and filters for today's games
- Scrapes live data into `data/live` directory

**When to use**: Daily backup when ESPN API is unavailable

#### `backup_loop.py`
**Purpose**: Bulk historical backup scraper  
**What it does**:
- Reads schedule file and scrapes all historical games
- Outputs to `data/raw/usa_ncaam_backup`

**When to use**: Historical backup when ESPN API is unavailable

---

## ⚙️ Setup & Requirements

### Prerequisites

- **Python 3.9+**
- **Required packages**:
  ```bash
  pip install pandas requests
  ```

### Directory Structure

Scripts automatically generate organized directories:

```
data/
├── raw/
│   ├── usa_ncaam/{year}/          # ESPN data
│   └── usa_ncaam_backup/{year}/   # NCAA backup data
└── live/{year}/                    # NCAA live data
```

Each year folder contains:
- `/games` - Master schedule/status list
- `/teams` - Master team list
- `/players` - Master player list
- `/play_by_play` - Individual game CSVs
- `/team_stats` - Individual game CSVs
- `/player_stats` - Individual game CSVs

---

## 🚀 Usage

### 1. Daily Live Scraping (Recommended)

Track today's games and update master files:

```bash
python live_scrape2.py
```

**Behavior**:
- Uses Central Time (`zoneinfo`) for accurate scheduling
- Skips games that haven't started
- Only updates active or recently finished games

**Schedule**: Run once daily (recommended: after midnight CT)

---

### 2. Historical / Season Backfill

Populate data from season start to current date:

**Steps**:
1. Open `season_loop.py`
2. Adjust date range if needed:
   ```python
   START_DATE = "2024-11-04"  # Season start
   END_DATE = "2025-11-25"    # Current date
   ```
3. Run:
   ```bash
   python season_loop.py
   ```

**Use case**: Initial setup, recovering missing data, or backfilling after downtime

---

### 3. Backup System (NCAA.com)

Use when ESPN API is unavailable.

#### Step A: Generate Schedule

```bash
python backup_start.py
```
Creates `backup_schedule.csv` with game listings.

#### Step B: Scrape Data

**For today's games (live)**:
```bash
python backup_daily.py
```

**For historical bulk scraping**:
```bash
python backup_loop.py
```

---

## 🧠 Logic & Key Features

### Master Files System
Maintains relational CSV databases (`games.csv`, `teams.csv`, `players.csv`) that:
- Act as centralized records
- Check for existing entries before inserting
- Prevent duplicate data

### Smart Caching

**`season_loop.py`**:
- Checks if game ID already processed (PBP file exists)
- Skips redundant API calls for existing data

**`live_scrape2.py`**:
- Checks game status in master file
- Skips re-scraping games marked as "Final"
- Updates only active/in-progress games

### Error Handling
- Retry logic with exponential backoff
- Handles API rate limits gracefully
- Connection timeout protection

### Data Normalization
Consistent CSV format across both pipelines:
- Standard columns: `game_id`, `team_id`, `player_id`, `home_away`
- Uniform data types and formatting
- Easy cross-pipeline analysis

---

## 🔧 Configuration

### Season Year Logic

Automatically determines season based on current date:
- **Rule**: If current month > August, Season Year = Current Year + 1
- **Example**: November 2025 → 2026 Season

### API Rate Limiting

Built-in delays to respect API limits:
- `live_scrape2.py`: 0.5s between requests
- `season_loop.py`: 1.5s between requests
- Adjust in script if needed for your use case

### Customization

**Time Zone**: `live_scrape2.py` uses Central Time. Modify if your games are in different zones.

**Date Ranges**: Edit `START_DATE` and `END_DATE` in `season_loop.py` for custom historical ranges.

---

## 📊 Output Format

All data is stored as CSV files with consistent schemas:

**Games**: `game_id`, `date`, `home_team`, `away_team`, `status`, `score`  
**Teams**: `team_id`, `name`, `conference`, `record`  
**Players**: `player_id`, `name`, `team_id`, `position`  
**Play-by-Play**: `game_id`, `time`, `team`, `action`, `score`  
**Stats**: `game_id`, `team_id`/`player_id`, various stat columns

---
