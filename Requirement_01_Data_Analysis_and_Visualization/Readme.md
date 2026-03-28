# Big Data Yelp Analysis

## Overview

This project is a comprehensive big data analysis of the Yelp dataset. The workflow is divided into multiple analytical stages, covering business landscapes, user behavior, review semantics, rating trends, and check-in patterns.

## Project Progress & Step-by-Step Analysis

Here is the step-by-step breakdown of what has been accomplished in this project:

### 1. Business Analysis

_Explored the landscape of businesses on Yelp._

- Identified the most common merchants and top cities/associated states.
- Calculated average ratings for the most common merchants.
- Extracted and categorized businesses, identifying top categories and restaurant types.
- Analyzed the correlation between restaurant types, review counts, and rating distributions.
- Investigated "turnaround" merchants, category synergy pairs, and polarizing businesses.

### 2. User Analysis

_Investigated Yelp user behavior, engagement, and demographics._

- Tracked user growth by analyzing users joining per year.
- Identified top reviewers by total count and top users by number of fans.
- Computed the ratio of Elite vs. Regular users and analyzed their impact on the platform.
- Evaluated the proportion of "silent" users (users with no or few reviews).
- Explored yearly user statistics, early adopters ("tastemakers"), adventurous eaters, and the evolution of user ratings.

### 3. Review Analysis

_Analyzed textual data and engagement metrics of user reviews._

- Measured total reviews per year and aggregated useful, funny, and cool engagement counts.
- Ranked users based on their review volume per year.
- Performed NLP analysis to identify the top positive/negative words and generated sentiment-based word clouds.
- Built word association graphs and extracted top bigrams linked to low ratings.
- Analyzed the correlation between review length and rating, identifying "mixed signal" reviews.

### 4. Rating Analysis

_Deep dive into the quantitative ratings given to businesses._

- Modeled the overall rating distribution and weekly rating frequencies.
- Highlighted the top businesses consistently receiving five-star reviews.
- Identified the top cities boasting the highest average ratings.
- Analyzed rating differentials across different cuisines.
- Compared weekend vs. weekday rating patterns, particularly for nightlife businesses.

### 5. Check-in Analysis

_Examined geographical and temporal patterns in user check-ins._

- Analyzed check-in trends by year and peak check-in times by the hour.
- Identified the most popular cities based solely on check-in volume.
- Ranked individual businesses by their aggregate check-ins.
- Calculated the Month-over-Month (MoM) growth rates for top-performing restaurants.
- Analyzed check-in seasonality broken down by cuisine type (e.g., peak seasons for specific foods).

### 6. Comprehensive Analysis

_Combining multiple dimensions for holistic, cross-sectional insights._

- Identified the top 5 merchants per city based on an aggregation of comprehensive metrics.

## Repository Structure

- **notebook/**: Contains the core Zeppelin notebooks (.zpln), structured sequentially, containing the PySpark execution code.
- **src code/**: Contains isolated Python scripts (.py) grouped by analysis phases.
- **results/**: Contains the generated output, including aggregated data results (.csv) and visual analytics/charts (.png), neatly organized into sub-folders matching the analysis phases.

---

## Next Steps: Requirement 2

This analysis serves as the foundation for **Requirement_02_Data_Enrichment_and_Exploration**, which builds upon these foundational insights with advanced pattern detection, anomaly identification, and cross-dimensional correlation studies.
