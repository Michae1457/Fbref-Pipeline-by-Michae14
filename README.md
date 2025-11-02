# FBref Scraping Pipeline

A comprehensive pipeline for scraping soccer data from FBref (https://fbref.com) and storing it in a DuckDB database. The pipeline is modular, organized by data type, and designed to handle various competition types including domestic leagues, international club tournaments, and national team competitions.

## Overview

This pipeline extracts structured data from FBref including:
- Competition information (domestic leagues, international cups, national tournaments)
- Season data for each competition
- Score tables and standings
- Fixture schedules
- Match reports and statistics

All data is stored in a DuckDB database (`database/fbref_database.db`) for efficient querying and analysis.

## Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

## Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd "FBref Pipeline copy"
   ```

2. **Create and activate a virtual environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Optional: Download pre-populated cache (recommended)**
   - Download the cache files from Google Drive (link in `pipeline/cli.py`)
   - Place the cache files in the `cache/` directory
   - This will significantly speed up initial runs by avoiding redundant web requests
   - Note: Cache files will be created automatically if not provided, but starting from scratch will require many web requests

The pipeline will automatically create the following directories when first run:
- `database/` - DuckDB database files
- `cache/` - Cached HTML pages (SQLite databases)
- `log/` - Log files

For all available commands and detailed usage instructions, see `pipeline/cli.py`.

## Pipeline Structure

The pipeline is organized into modular components, each responsible for scraping and processing a specific type of data:

### Core Modules

#### 1. **Competition Pipeline** (`pipeline/competition/`)
   - Scrapes competition metadata from FBref
   - Handles three competition types:
     - **Domestic Leagues**: First-tier domestic leagues (e.g., Premier League, La Liga)
     - **International Club Cups**: Club tournaments (e.g., Champions League, Europa League)
     - **National Team Competitions**: International tournaments (e.g., World Cup, Euros)
   - Stores competition data in `competition_club` and `competition_nation` tables

#### 2. **Season Pipeline** (`pipeline/season/`)
   - Scrapes season data for each competition
   - Extracts season links and metadata
   - Handles different season structures for domestic vs. tournament competitions
   - Stores season data in `season`, `season_club_tournament`, and `season_nation_tournament` tables

#### 3. **Score Table Pipeline** (`pipeline/stats/score_table/`)
   - Scrapes league standings and score tables for domestic leagues
   - Extracts team positions, points, wins, losses, draws
   - Stores data in `score_table` table

#### 4. **Tournament Score Table Pipelines** (`pipeline/stats/score_table_tournament/`)
   - **Club Tournament** (`club/`): Scrapes historical tournament results for club competitions
   - **Nation Tournament** (`nation/`): Scrapes historical tournament results for national team competitions
   - Stores data in `score_table_club_tournament` and `score_table_nation_tournament` tables

#### 5. **Fixture Pipeline** (`pipeline/fixture/`)
   - Scrapes fixture schedules for competitions
   - Extracts match dates, times, teams, and fixture links
   - Supports filtering by years back and refresh options for current seasons
   - Stores fixture data in `fixture` table

#### 6. **Match Pipeline** (`pipeline/match/`)
   - Scrapes detailed match report data from fixture links
   - Extracts comprehensive match statistics and details
   - Processes matches in batches with error handling
   - Stores match data in `match` table

### Utility Modules (`pipeline/utils/`)

#### **Database Manager** (`database.py`)
   - Centralized database operations using DuckDB
   - Manages table creation, data insertion, and queries
   - Handles database connections and transactions

#### **Universal Scraper** (`scrape.py`)
   - Shared web scraping functionality
   - Implements caching mechanism to avoid redundant requests
   - Handles HTTP requests with proper headers and error handling
   - Rate limiting to be respectful to FBref servers

#### **Cache Manager** (`cache.py`)
   - Manages HTML caching to reduce redundant web requests
   - Stores cached pages in `cache/` directory organized by pipeline

#### **Logger** (`logging.py`)
   - Centralized logging configuration
   - Provides consistent logging across all pipeline modules

#### **Query Builder** (`query.py`)
   - Contains SQL query templates for database operations
   - Centralizes all database query logic

#### **Mapping Utilities** (`mapping.py`)
   - Mapping dictionaries for data transformation
   - Handles competition type mappings and data conversions

## Data Flow

The pipeline follows a hierarchical data flow:

```
1. Competition Pipeline
   └── Scrapes competition metadata
       └── Stores: competition_club, competition_nation

2. Season Pipeline
   └── Uses competition data
       └── Scrapes season information for each competition
           └── Stores: season, season_club_tournament, season_nation_tournament

3. Score Table Pipelines
   └── Uses season data
       └── Scrapes standings/score tables
           └── Stores: score_table, score_table_club_tournament, score_table_nation_tournament

4. Fixture Pipeline
   └── Uses season data
       └── Scrapes fixture schedules
           └── Stores: fixture

5. Match Pipeline
   └── Uses fixture data
       └── Scrapes detailed match reports
           └── Stores: match
```

## Directory Structure

```
├── pipeline/                    # Main pipeline code
│   ├── cli.py                   # Command-line interface (commands and tutorials)
│   ├── competition/             # Competition scraping module
│   │   ├── main.py             # CompetitionPipeline class
│   │   ├── parse.py            # Competition data parsing
│   │   └── scrape/             # Competition-specific scrapers
│   ├── season/                  # Season scraping module
│   │   ├── main.py             # SeasonPipeline class
│   │   └── parse_*.py          # Season parsers for different competition types
│   ├── stats/                   # Statistics scraping modules
│   │   ├── score_table/         # League standings
│   │   └── score_table_tournament/  # Tournament history
│   ├── fixture/                 # Fixture scraping module
│   ├── match/                   # Match report scraping module
│   └── utils/                   # Shared utilities
│       ├── database.py          # Database operations
│       ├── scrape.py            # Web scraping utilities
│       ├── cache.py             # Caching system
│       ├── logging.py           # Logging configuration
│       ├── query.py             # SQL queries
│       └── mapping.py           # Data mappings
├── data/                        # Data processing and export utilities
│   ├── dataset.py               # Data processing functions
│   ├── query.py                 # Data query utilities
│   ├── player.py                # Player data utilities
│   └── stats/                   # Exported statistics CSVs
├── database/                    # DuckDB database files
├── cache/                       # Cached HTML pages
├── log/                         # Log files
├── timelines/                   # Fixture timeline JSON files
└── venv/                        # Virtual environment (not tracked in git)
```

## Database Schema

The pipeline creates and manages the following DuckDB tables:

- `competition_club` - Club competitions (domestic leagues and international cups)
- `competition_nation` - National team competitions
- `season` - Seasons for domestic leagues
- `season_club_tournament` - Seasons for club tournaments
- `season_nation_tournament` - Seasons for national tournaments
- `score_table` - League standings for domestic leagues
- `score_table_club_tournament` - Historical results for club tournaments
- `score_table_nation_tournament` - Historical results for national tournaments
- `fixture` - Fixture schedules
- `match` - Detailed match reports

## Usage

For commands and usage instructions, see `pipeline/cli.py` which contains all command-line interface documentation and tutorials.

