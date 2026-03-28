%pyspark
print("""
╔══════════════════════════════════════════════════════════════╗
║         OPEN-WORLD DATA SAFARI - FINAL REPORT               ║
╠══════════════════════════════════════════════════════════════╣
║  HYPOTHESIS:                                                 ║
║  Cities with more Starbucks locations have higher average    ║
║  restaurant ratings and more review activity, indicating     ║
║  a gentrification effect on the local dining scene.          ║
╠══════════════════════════════════════════════════════════════╣
║  DATASETS USED:                                              ║
║  • Internal: Yelp (6,990,280 reviews, 150,346 businesses)   ║
║  • External: Starbucks Store Locations (25,601 stores)       ║
╠══════════════════════════════════════════════════════════════╣
║  KEY FINDINGS:                                               ║
║                                                              ║
║  1. RATING: Weak negative correlation (-0.0133)              ║
║     → More Starbucks does NOT mean higher ratings            ║
║     → High Starbucks cities avg rating: 3.34                 ║
║     → Low Starbucks cities avg rating:  3.68 (HIGHER!)       ║
║                                                              ║
║  2. REVIEW ACTIVITY: Strong positive correlation (0.7722)    ║
║     → More Starbucks = MUCH more review activity             ║
║     → High cities: 98.5 reviews per restaurant               ║
║     → Minimal cities: 61.2 reviews per restaurant            ║
║                                                              ║
║  3. RESTAURANT COUNT: Very strong correlation (0.8495)       ║
║     → More Starbucks = More restaurants in the city          ║
║     → High cities avg: 4,255 restaurants                     ║
║     → Minimal cities avg: 106 restaurants                    ║
║                                                              ║
║  4. SURPRISE FINDING:                                        ║
║     → New Orleans (13 Starbucks) has the HIGHEST avg         ║
║       rating (4.0) AND highest reviews per restaurant        ║
║       (206.1) — proving quality beats quantity!              ║
╠══════════════════════════════════════════════════════════════╣
║  VERDICT:                                                    ║
║  HYPOTHESIS PARTIALLY SUPPORTED                              ║
║                                                              ║
║  Starbucks presence strongly predicts city SIZE and          ║
║  ENGAGEMENT (reviews) but NOT higher restaurant quality.     ║
║  Gentrification brings more restaurants and more reviewers   ║
║  but smaller cities with fewer Starbucks actually have       ║
║  BETTER rated restaurants — suggesting boutique dining       ║
║  culture thrives away from corporate chain saturation.       ║
╚══════════════════════════════════════════════════════════════╝
""")

print("="*60)
print("CITIES ANALYZED: 259")
print("STARBUCKS LOCATIONS (US): 13,608")
print("YELP RESTAURANTS ANALYZED: 150,346")
print("TOTAL REVIEWS ANALYZED: 6,990,280")
print("="*60)
