# Big Data Yelp Analysis: Requirement 02 - Data Enrichment and Exploration

## Overview

Building upon the foundational analysis from Requirement 01, this phase extends the investigation through advanced pattern detection, anomaly identification, and multi-dimensional correlation studies. This requirement focuses on uncovering hidden patterns in the Yelp ecosystem, detecting fraudulent or manipulative behaviors, and exploring cross-sectional relationships between business characteristics and user engagement across different geographic and temporal dimensions.

## Analysis Projects

### 1. The Weather-Mood Hypothesis

_Investigates the relationship between weather conditions and customer sentiment/ratings._

**Objective**: Determine whether weather patterns influence customer review sentiment and business ratings.

**Methods**:

- Correlated weather data with review sentiment and ratings across different locations
- Analyzed seasonal rating variations and weather-driven review patterns
- Identified optimal weather conditions for specific restaurant types and cuisines

**Key Outputs**:

- Sample weather date previews and trend visualizations
- Overall review sentiment by weather condition
- Average ratings across different weather scenarios
- Best and worst performing weather conditions for reviews

**Deliverables**:

- `notebook/01_weather-mood-analysis.zpln`
- `src code/01_The Weather-Mood Hypothesis/weather_mood_analysis.py`
- visualizations in `results/01_The Weather-Mood Hypothesis/`

---

### 2. The Cursed Storefronts & Multi-Dimensional Attribution

_Identifies locations that rapidly cycle through multiple business tenants, suggesting unfavorable conditions._

**Objective**: Detect "cursed" storefronts—addresses that have hosted numerous failed businesses.

**Methods**:

- Aggregated business history by address (street location)
- Identified locations with 3+ tenant turnovers where all current businesses are closed
- Analyzed location DNA: attributes, categories, and characteristics of failed venues
- Performed failure breakdown analysis to identify patterns in why businesses fail at specific locations

**Key Findings**:

- Top cursed storefronts in PA, FL, and LA regions
- Identified locations with 10+ business failures (e.g., 3131 Walnut St, Philadelphia)
- Common failure patterns: service/food quality, cost/value misalignment, parking/access issues, visibility problems

**Deliverables**:

- `notebook/02_cursed-analysis.zpln`
- `src code/` (integrated with curse analysis logic)
- Ranked lists of problematic storefronts by failure count

---

### 3. The Review Manipulation Syndicate

_Advanced fraud detection identifying patterns of suspicious review behavior and coordinated manipulation._

**Objective**: Uncover coordinated fraud rings, ghost accounts, and systematic review manipulation schemes.

**Methods**:

**Step 1: Spike Detection** - Identifies abnormal review submission patterns

- Detects temporal spikes in review submissions
- Analyzes burst patterns that deviate from normal business activity
- Visualizations: BAR, LINE, PIE, SCATTER, TABLE charts for spike analysis

**Step 2A: Ghost Account Profiling** - Identifies suspicious user accounts

- Profiles users with suspicious patterns (new accounts, high review rates, low engagement)
- Detects accounts that may be fake or part of manipulation schemes
- Analyzes account age vs. activity metrics

**Step 2B: NLP Fraud Vocabulary** - Analyzes suspicious language patterns

- Identifies repetitive or formulaic language in reviews
- Detects copy-paste patterns and boilerplate text
- Analyzes vocabulary inconsistencies that suggest fraud

**Step 3A: Spatial Teleportation** - Identifies geographically impossible check-in patterns

- Detects users checking in at impossible distances within short time frames
- Identifies coordinated multi-location activity impossible for legitimate users
- Maps suspicious movement patterns

**Step 3B: Syndicate Network** - Maps coordination relationships between suspicous accounts

- Builds network graph of related fake accounts
- Identifies clusters of coordinated fraudsters
- Analyzes relationship patterns and timing alignments

**Step 4A: Long-term Rating Consequences** - Analyzes impact of manipulation campaigns

- Tracks rating changes before and after suspected manipulation
- Analyzes business closure rates post-manipulation
- Measures long-term reputational damage

**Step 4B: Closure Rate Analysis** - Measures business failure rates

- Analyzes closure rates of manipulated vs. legitimate businesses
- Identifies temporal patterns in business failure post-fraud

**Deliverables**:

- `notebook/03_Review_Manipulation.zpln`
- `src code/03_The Review Manipulation Syndicate/` (7 step scripts)
- Multi-format visualizations (Area, Bar, Line, Pie, Scatter, Table charts) for each step in `results/03_The Review Manipulation Syndicate/`

---

### 4. The Open-World Data Safari

_Comprehensive cross-dimensional analysis exploring relationships between different data sources and business characteristics._

**Objective**: Conduct exploratory analysis across multiple data dimensions to uncover hidden correlations and patterns.

**Methods**:

**Step 1: Data Loading & Integration** - Consolidates and validates multi-source data

- Loads and integrates business, review, user, and check-in data
- Validates data quality and completeness
- Prepares unified dataset for analysis

**Step 2: City-Starbucks Matching** - Correlates Starbucks presence with business ecosystem

- Analyzes Starbucks locations and local business density
- Studies correlation between chain presence and local business diversity
- Identifies market dynamics in Starbucks-dominated vs. independent markets

**Step 3: Consolidated City Analysis** - Holistic city-level business metrics

- Aggregates city-level business, review, and check-in statistics
- Compares performance metrics across different cities
- Identifies city characteristics influencing business success

**Step 4: Correlation Tier Analysis** - Multi-dimensional correlation studies

- Analyzes correlations between business features (ratings, categories, attributes)
- Identifies feature combinations that drive success
- Tiered analysis by business similarity and market conditions

**Step 5: Final Report Summary** - Synthesizes findings across all dimensions

- Integrates insights from all analysis steps
- Provides actionable recommendations
- Generates comprehensive visualization dashboard

**Deliverables**:

- `notebook/04_the open world safari.zpln`
- `src code/04_The Open-World Data Safari/` (5 step scripts)
- Multi-format visualizations in `results/04_The Open-World Data Safari/`

---

## Repository Structure

- **notebook/**: Contains Zeppelin notebooks (.zpln) executing PySpark-based analyses
  - `01_weather-mood-analysis.zpln` - Weather sentiment correlation
  - `02_cursed-analysis.zpln` - Cursed storefront detection
  - `03_Review_Manipulation.zpln` - Fraud detection pipeline
  - `04_the open world safari.zpln` - Cross-dimensional exploration

- **src code/**: Contains modular Python scripts for each analysis project
  - `01_The Weather-Mood Hypothesis/` - Weather analysis scripts
  - `03_The Review Manipulation Syndicate/` - 7-step fraud detection pipeline
  - `04_The Open-World Data Safari/` - 5-step cross-dimensional analysis
- **results/**: Contains generated outputs organized by project
  - `01_The Weather-Mood Hypothesis/` - Weather-sentiment visualizations
  - `03_The Review Manipulation Syndicate/` - Fraud detection outputs (7 subdirectories)
  - `04_The Open-World Data Safari/` - Cross-dimensional analysis charts

---

## Key Insights & Findings

### Data Quality & Integrity

- Identified systematic fraud patterns and review manipulation schemes
- Detected suspicious geographic and temporal user behavior
- Quantified prevalence of fraudulent activity in the Yelp ecosystem

### Business Lifecycle Patterns

- Mapped location-specific factors influencing business success/failure
- Identified "cursed" storefronts with recurrent business failures
- Analyzed long-term consequences of fraud on legitimate businesses

### External Factors

- Demonstrated weather-sentiment correlations for service businesses
- Identified seasonal patterns in customer engagement
- Analyzed geographic market dynamics and competition effects

---

## Technical Stack

- **Processing**: PySpark (distributed big data processing)
- **Notebooks**: Apache Zeppelin (interactive analysis environment)
- **Languages**: Python (PySpark, data processing)
- **Visualization**: Zeppelin native charting (Bar, Line, Pie, Scatter, Area, Table charts)
- **Data Source**: HDFS (Hadoop Distributed File System)

---

## How to Use

1. Access notebooks through Apache Zeppelin at `notebook/` directory
2. Execute analyses sequentially or individually as needed
3. Review visualizations in `results/` directory for insights
4. Reference source code in `src code/` for reproducibility and modifications

---

## Connection to Requirement 01

This analysis leverages the foundational insights from **Requirement_01_Data_Analysis_and_Visualization**, which established baseline metrics for:

- Business landscape and categories
- User demographics and behavior
- Review characteristics and patterns
- Rating distributions
- Geographic check-in patterns

Requirement 02 extends this foundation by focusing on anomaly detection, fraud identification, and exploratory pattern discovery.
