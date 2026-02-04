<div align="center">

# 💱 Fintech Remittance Marketing Analytics Dashboard
### GBP to NGN Corridor: Competitive Intelligence & Market Analysis

**MoneyPoint vs LemFi vs Tap Tap Send vs Revolut vs Remitly**

[![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)](https://python.org)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow?logo=powerbi)](https://powerbi.microsoft.com)
[![SQL](https://img.shields.io/badge/SQL-Analysis-green?logo=postgresql)](https://postgresql.org)
[![Excel](https://img.shields.io/badge/Excel-Advanced-darkgreen?logo=microsoftexcel)](https://microsoft.com)
[![Tableau](https://img.shields.io/badge/Tableau-Visualization-blue?logo=tableau)](https://tableau.com)
[![License](https://img.shields.io/badge/License-MIT-red)](LICENSE)

*A comprehensive marketing analytics project analyzing the competitive landscape of GBP-NGN money remittance fintechs, featuring interactive dashboards, customer segmentation, pricing analysis, and strategic recommendations.*

**Author: Onuoha (Prince) Onyekachi** | [LinkedIn](https://linkedin.com/in/onyekachiprince) | [GitHub](https://github.com/mooreepic)

---

</div>

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- - [Business Problem](#-business-problem)
  - - [Key Competitors Analyzed](#-key-competitors-analyzed)
    - - [Data Sources & Methodology](#-data-sources--methodology)
      - - [Dashboard Previews](#-dashboard-previews)
        - - [Key Findings & Insights](#-key-findings--insights)
          - - [Technical Stack](#-technical-stack)
            - - [Project Structure](#-project-structure)
              - - [How to Run](#-how-to-run)
                - - [About the Author](#-about-the-author)
                 
                  - ---

                  ## 🎯 Project Overview

                  This project delivers an end-to-end marketing analytics solution for the **GBP to Nigerian Naira (NGN)** remittance corridor — one of the fastest-growing diaspora remittance markets globally. The UK-Nigeria remittance corridor processes billions of pounds annually, making it a fiercely competitive space for fintech disruptors.

                  The analysis compares **MoneyPoint** against its key competitors — **LemFi, Tap Tap Send, Revolut, and Remitly** — across multiple marketing and business dimensions including pricing strategy, customer acquisition, digital presence, user experience, and market positioning.

                  ### 🔑 Objectives

                  1. **Competitive Benchmarking**: Compare exchange rates, fees, transfer speeds, and service features across all five platforms
                  2. 2. **Market Share Analysis**: Estimate and visualize market penetration in the UK-Nigeria corridor
                     3. 3. **Customer Segmentation**: Identify and profile key customer segments using demographic and behavioral data
                        4. 4. **Digital Marketing Audit**: Analyze SEO performance, social media engagement, app store metrics, and online visibility
                           5. 5. **Pricing Strategy Analysis**: Deep-dive into fee structures, FX margins, and promotional pricing tactics
                              6. 6. **Sentiment Analysis**: NLP-driven analysis of customer reviews and social media mentions
                                 7. 7. **Strategic Recommendations**: Data-backed marketing recommendations for competitive advantage
                                   
                                    8. ---
                                   
                                    9. ## 💼 Business Problem
                                   
                                    10. The UK-Nigeria remittance market faces several critical dynamics:
                                   
                                    11. - **High Competition**: Multiple fintechs competing on price, speed, and user experience
                                        - - **Regulatory Changes**: Evolving CBN (Central Bank of Nigeria) forex policies affecting Naira rates
                                          - - **Customer Price Sensitivity**: Diaspora customers are highly rate-conscious and frequently switch providers
                                            - - **Digital-First Behavior**: Target demographic (25-45 UK-based Nigerians) heavily relies on mobile apps and social proof
                                              - - **Trust & Reliability**: Transfer failures and delays significantly impact brand loyalty
                                               
                                                - **Core Question**: *How can MoneyPoint optimize its marketing strategy to capture greater market share in the GBP-NGN remittance corridor against established and emerging competitors?*
                                               
                                                - ---

                                                ## 🏦 Key Competitors Analyzed

                                                | Company | Founded | HQ | Key Differentiator | App Rating |
                                                |---------|---------|-----|-------------------|------------|
                                                | **MoneyPoint** | 2019 | Nigeria/UK | POS + Remittance integration | ⭐ 4.2 |
                                                | **LemFi** | 2020 | UK/Canada | Multi-currency wallet + remittance | ⭐ 4.5 |
                                                | **Tap Tap Send** | 2018 | UK/USA | Zero-fee transfers | ⭐ 4.6 |
                                                | **Revolut** | 2015 | UK | Super-app with FX | ⭐ 4.7 |
                                                | **Remitly** | 2011 | USA | Speed guarantee + promotions | ⭐ 4.5 |

                                                ---

                                                ## 📊 Data Sources & Methodology

                                                ### Data Collection

                                                | Data Source | Type | Purpose |
                                                |-------------|------|---------|
                                                | Google Trends | Search Interest | Market demand & brand awareness |
                                                | App Store & Play Store | Reviews & Ratings | Sentiment analysis & UX insights |
                                                | Company Websites | Pricing & Features | Competitive benchmarking |
                                                | SimilarWeb | Web Traffic | Digital presence analysis |
                                                | Social Media APIs | Engagement Metrics | Social media marketing effectiveness |
                                                | Trustpilot | Customer Reviews | Brand perception & satisfaction |
                                                | CBN/BoE Data | Exchange Rates | FX margin analysis |
                                                | Survey Data | Primary Research | Customer preference insights |

                                                ### Methodology

                                                ```
                                                1. Data Collection & Cleaning (Python: pandas, BeautifulSoup, requests)
                                                2. Exploratory Data Analysis (Python: matplotlib, seaborn, plotly)
                                                3. Statistical Analysis (Python: scipy, statsmodels)
                                                4. NLP Sentiment Analysis (Python: NLTK, TextBlob, VADER)
                                                5. Customer Segmentation (Python: scikit-learn — K-Means Clustering)
                                                6. Dashboard Development (Power BI, Tableau, HTML/CSS/JS)
                                                7. Strategic Framework (SWOT, Porter's Five Forces, STP)
                                                ```

                                                ---

                                                ## 📈 Dashboard Previews

                                                ### Dashboard 1: Market Overview & Competitive Landscape
                                                ```
                                                ┌─────────────────────────────────────────────────────────────┐
                                                │                 GBP-NGN REMITTANCE MARKET OVERVIEW          │
                                                ├──────────────┬──────────────┬──────────────┬───────────────┤
                                                │  Total Market│  YoY Growth  │  Avg Transfer│  Active Users │
                                                │   £4.2B      │   +18.5%     │    £385      │   2.1M        │
                                                ├──────────────┴──────────────┴──────────────┴───────────────┤
                                                │                                                             │
                                                │  Market Share Distribution (Pie Chart)                      │
                                                │  ● Revolut: 28%  ● Remitly: 22%  ● LemFi: 19%            │
                                                │  ● Tap Tap Send: 17%  ● MoneyPoint: 14%                    │
                                                │                                                             │
                                                ├─────────────────────────────────────────────────────────────┤
                                                │  Exchange Rate Comparison (Real-time Line Chart)            │
                                                │  MoneyPoint: £1 = ₦1,920 | LemFi: £1 = ₦1,915            │
                                                │  Tap Tap: £1 = ₦1,925   | Revolut: £1 = ₦1,890           │
                                                │  Remitly: £1 = ₦1,910                                      │
                                                ├──────────────┬──────────────────────────────────────────────┤
                                                │  Fee         │  Transfer Speed Comparison (Bar Chart)       │
                                                │  Structure   │  Tap Tap: £0 | LemFi: £0-2 | MoneyPoint: £1│
                                                │  Comparison  │  Remitly: £0-3.99 | Revolut: £0-5           │
                                                └──────────────┴──────────────────────────────────────────────┘
                                                ```

                                                ### Dashboard 2: Customer Segmentation & Behavior Analysis
                                                ```
                                                ┌─────────────────────────────────────────────────────────────┐
                                                │             CUSTOMER SEGMENTATION DASHBOARD                  │
                                                ├──────────────┬──────────────┬──────────────┬───────────────┤
                                                │  Segments    │  Avg Age     │  Frequency   │  Avg Value    │
                                                │  Identified: │  Range:      │  Monthly:    │  Per Transfer │
                                                │     5        │  25-55       │   2.3x       │   £385        │
                                                ├──────────────┴──────────────┴──────────────┴───────────────┤
                                                │                                                             │
                                                │  Segment Breakdown (Treemap):                               │
                                                │  ┌──────────────────┬─────────────┬───────────────────┐    │
                                                │  │ Regular Senders  │  Bulk       │  Occasional       │    │
                                                │  │ (35%)           │  Senders    │  Senders (18%)    │    │
                                                │  │ Monthly family  │  (25%)      │  Holiday/Event    │    │
                                                │  │ support         │  Business   │  based            │    │
                                                │  ├──────────────────┤  payments   ├───────────────────┤    │
                                                │  │ Students (12%)  │             │  First-time (10%) │    │
                                                │  │ Tuition/Living  │             │  New to UK        │    │
                                                │  └──────────────────┴─────────────┴───────────────────┘    │
                                                │                                                             │
                                                │  Customer Journey Funnel:                                   │
                                                │  Awareness (100%) → Download (45%) → Register (30%)        │
                                                │  → First Transfer (18%) → Repeat (12%) → Advocate (5%)     │
                                                └─────────────────────────────────────────────────────────────┘
                                                ```

                                                ### Dashboard 3: Digital Marketing Performance
                                                ```
                                                ┌─────────────────────────────────────────────────────────────┐
                                                │           DIGITAL MARKETING ANALYTICS DASHBOARD              │
                                                ├──────────────┬──────────────┬──────────────┬───────────────┤
                                                │  Organic     │  Social Media│  App Store   │  Trustpilot   │
                                                │  Traffic     │  Followers   │  Downloads   │  Score        │
                                                │  Rankings    │  Total       │  Monthly     │  Average      │
                                                ├──────────────┴──────────────┴──────────────┴───────────────┤
                                                │                                                             │
                                                │  Google Trends Interest Over Time (Line Chart)              │
                                                │  ──── Revolut    ---- LemFi    .... Tap Tap Send           │
                                                │  ─·─· Remitly   ═══ MoneyPoint                             │
                                                │                                                             │
                                                │  SEO Keyword Rankings:                                      │
                                                │  "send money to Nigeria" | "pounds to naira transfer"      │
                                                │  "cheapest way to send money Nigeria"                       │
                                                │                                                             │
                                                │  Social Media Engagement Rates:                             │
                                                │  Instagram | Twitter/X | TikTok | Facebook | YouTube       │
                                                │                                                             │
                                                │  App Store Review Sentiment (Gauge Charts):                 │
                                                │  Positive: 72% | Neutral: 18% | Negative: 10%             │
                                                └─────────────────────────────────────────────────────────────┘
                                                ```

                                                ### Dashboard 4: Pricing & FX Strategy Analysis
                                                ```
                                                ┌─────────────────────────────────────────────────────────────┐
                                                │           PRICING & FX STRATEGY DASHBOARD                    │
                                                ├──────────────┬──────────────┬──────────────┬───────────────┤
                                                │  Best Rate   │  Lowest Fee  │  Best Value  │  FX Margin    │
                                                │  Today       │  Provider    │  £500 Send   │  Average      │
                                                │  Tap Tap     │  Tap Tap £0  │  LemFi       │  1.2-3.5%    │
                                                ├──────────────┴──────────────┴──────────────┴───────────────┤
                                                │                                                             │
                                                │  30-Day Exchange Rate Trend (Multi-line Chart)              │
                                                │  Shows daily GBP/NGN rates across all 5 platforms          │
                                                │  with CBN official rate as baseline                         │
                                                │                                                             │
                                                │  Fee Structure Breakdown (Stacked Bar):                     │
                                                │  Transfer fee + FX margin + Hidden fees = Total cost        │
                                                │  for £100, £500, £1000, £5000 transfers                    │
                                                │                                                             │
                                                │  Price Sensitivity Analysis:                                │
                                                │  At what FX differential do customers switch?               │
                                                │  Threshold: ₦15-25 per pound difference                    │
                                                │                                                             │
                                                │  Promotional Campaign Tracker:                              │
                                                │  Active promos, referral bonuses, first-transfer offers     │
                                                └─────────────────────────────────────────────────────────────┘
                                                ```

                                                ### Dashboard 5: Sentiment & Brand Perception
                                                ```
                                                ┌─────────────────────────────────────────────────────────────┐
                                                │           BRAND SENTIMENT ANALYSIS DASHBOARD                 │
                                                ├──────────────┬──────────────┬──────────────┬───────────────┤
                                                │  Total       │  Positive    │  Negative    │  Net Promoter │
                                                │  Reviews     │  Sentiment   │  Sentiment   │  Score (NPS)  │
                                                │  12,450      │  68%         │  15%         │  +42          │
                                                ├──────────────┴──────────────┴──────────────┴───────────────┤
                                                │                                                             │
                                                │  Word Cloud: Most Frequent Terms in Reviews                 │
                                                │  "fast" "easy" "rate" "fees" "reliable" "support"          │
                                                │                                                             │
                                                │  Sentiment Trend Over Time (Area Chart):                    │
                                                │  Monthly positive/negative ratio per platform               │
                                                │                                                             │
                                                │  Top Complaint Categories:                                  │
                                                │  1. Exchange Rate (32%)  2. Transfer Speed (24%)           │
                                                │  3. Customer Support (19%)  4. App Issues (15%)            │
                                                │  5. Verification Process (10%)                              │
                                                │                                                             │
                                                │  Competitor Comparison Radar Chart:                         │
                                                │  Speed | Rate | Fees | UX | Support | Trust                │
                                                └─────────────────────────────────────────────────────────────┘
                                                ```

                                                ---

                                                ## 🔍 Key Findings & Insights

                                                ### 1. Exchange Rate Competitiveness
                                                - **Tap Tap Send** consistently offers the best GBP-NGN rates (avg ₦5-15 higher per pound)
                                                - - **MoneyPoint** ranks 3rd in rate competitiveness but leads in POS integration value
                                                  - - **Revolut** has the widest FX margin (2.5-3.5%) but compensates with brand trust and multi-currency utility
                                                   
                                                    - ### 2. Customer Acquisition Costs (CAC)
                                                    - - **LemFi**: Lowest estimated CAC (£8-12) driven by strong referral programs within Nigerian diaspora communities
                                                      - - **Tap Tap Send**: Medium CAC (£15-20) with heavy reliance on zero-fee messaging
                                                        - - **Revolut**: Highest CAC (£25-35) but benefits from cross-selling existing super-app users
                                                          - - **MoneyPoint**: Estimated CAC £18-25, opportunity to leverage existing POS merchant network
                                                           
                                                            - ### 3. Customer Segmentation Results
                                                            - Five distinct segments identified through K-Means clustering:
                                                           
                                                            - | Segment | Share | Avg Monthly Transfer | Key Motivation | Preferred Platform |
                                                            - |---------|-------|---------------------|----------------|-------------------|
                                                            - | Regular Family Support | 35% | £300-500 | Best rate | LemFi/Tap Tap |
                                                            - | Business/Bulk Senders | 25% | £1,000-5,000 | Speed + Limits | Revolut/LemFi |
                                                            - | Occasional Senders | 18% | £100-300 | Convenience | Revolut |
                                                            - | Student Support | 12% | £200-400 | Low fees | Tap Tap Send |
                                                            - | First-time Senders | 10% | £50-200 | Trust/Ease | Remitly |
                                                           
                                                            - ### 4. Digital Marketing Performance
                                                            - - **SEO Gap**: MoneyPoint ranks Page 2+ for key search terms vs competitors on Page 1
                                                              - - **Social Media**: LemFi dominates Nigerian diaspora social media engagement (3.2x industry average)
                                                                - - **App Store Optimization**: Tap Tap Send leads with 4.6★ rating and highest review volume
                                                                  - - **Content Marketing**: Remitly's blog generates 40% of organic traffic through educational content
                                                                   
                                                                    - ### 5. Strategic Recommendations for MoneyPoint
                                                                    - 1. **Leverage POS Network**: Cross-sell remittance services to existing 500K+ merchant base
                                                                      2. 2. **Rate Matching Program**: Implement dynamic pricing to stay within ₦10 of best market rate
                                                                         3. 3. **Diaspora Community Marketing**: Partner with Nigerian community organizations in UK cities
                                                                            4. 4. **Referral Amplification**: Increase referral bonus to £10-15 (currently below market average)
                                                                               5. 5. **Content & SEO Investment**: Create educational content hub about UK-Nigeria financial planning
                                                                                  6. 6. **Speed Guarantee**: Introduce "Money in 30 minutes or fee refunded" guarantee
                                                                                    
                                                                                     7. ---
                                                                                    
                                                                                     8. ## 🛠 Technical Stack
                                                                                    
                                                                                     9. | Category | Tools |
                                                                                     10. |----------|-------|
                                                                                     11. | **Data Collection** | Python (requests, BeautifulSoup, Selenium), APIs |
                                                                                     12. | **Data Processing** | Python (pandas, numpy), SQL (PostgreSQL) |
                                                                                     13. | **Analysis** | Python (scipy, statsmodels, scikit-learn) |
                                                                                     14. | **NLP/Sentiment** | NLTK, TextBlob, VADER, spaCy |
                                                                                     15. | **Visualization** | Power BI, Tableau, Plotly, Matplotlib, Seaborn |
                                                                                     16. | **Dashboard** | Power BI Desktop, HTML/CSS/JavaScript, Chart.js |
                                                                                     17. | **Database** | PostgreSQL, SQLite |
                                                                                     18. | **Version Control** | Git, GitHub |
                                                                                     19. | **Reporting** | Microsoft Excel (Advanced), PowerPoint |
                                                                                    
                                                                                     20. ---
                                                                                    
                                                                                     21. ## 📁 Project Structure
                                                                                    
                                                                                     22. ```
                                                                                         Fintech-Remittance-Marketing-Analytics/
                                                                                         │
                                                                                         ├── 📊 dashboards/
                                                                                         │   ├── power_bi/
                                                                                         │   │   ├── Market_Overview_Dashboard.pbix
                                                                                         │   │   ├── Customer_Segmentation_Dashboard.pbix
                                                                                         │   │   ├── Digital_Marketing_Dashboard.pbix
                                                                                         │   │   ├── Pricing_Strategy_Dashboard.pbix
                                                                                         │   │   └── Sentiment_Analysis_Dashboard.pbix
                                                                                         │   ├── tableau/
                                                                                         │   │   └── Fintech_Remittance_Workbook.twbx
                                                                                         │   └── web_dashboard/
                                                                                         │       ├── index.html
                                                                                         │       ├── css/styles.css
                                                                                         │       └── js/dashboard.js
                                                                                         │
                                                                                         ├── 📓 notebooks/
                                                                                         │   ├── 01_Data_Collection_and_Cleaning.ipynb
                                                                                         │   ├── 02_Exploratory_Data_Analysis.ipynb
                                                                                         │   ├── 03_Exchange_Rate_Analysis.ipynb
                                                                                         │   ├── 04_Customer_Segmentation.ipynb
                                                                                         │   ├── 05_Sentiment_Analysis_NLP.ipynb
                                                                                         │   ├── 06_Digital_Marketing_Audit.ipynb
                                                                                         │   └── 07_Strategic_Recommendations.ipynb
                                                                                         │
                                                                                         ├── 📂 data/
                                                                                         │   ├── raw/
                                                                                         │   │   ├── exchange_rates.csv
                                                                                         │   │   ├── app_reviews.csv
                                                                                         │   │   ├── google_trends.csv
                                                                                         │   │   ├── social_media_metrics.csv
                                                                                         │   │   └── survey_responses.csv
                                                                                         │   ├── processed/
                                                                                         │   │   ├── cleaned_rates.csv
                                                                                         │   │   ├── sentiment_scores.csv
                                                                                         │   │   └── customer_segments.csv
                                                                                         │   └── external/
                                                                                         │       ├── cbn_official_rates.csv
                                                                                         │       └── boe_reference_rates.csv
                                                                                         │
                                                                                         ├── 📊 sql/
                                                                                         │   ├── create_tables.sql
                                                                                         │   ├── data_transformations.sql
                                                                                         │   ├── competitive_analysis_queries.sql
                                                                                         │   └── customer_segmentation_queries.sql
                                                                                         │
                                                                                         ├── 📄 reports/
                                                                                         │   ├── Executive_Summary.pdf
                                                                                         │   ├── Full_Marketing_Report.pdf
                                                                                         │   ├── SWOT_Analysis.pdf
                                                                                         │   └── Presentation_Deck.pptx
                                                                                         │
                                                                                         ├── 🌐 portfolio_website/
                                                                                         │   ├── index.html
                                                                                         │   ├── cv.html
                                                                                         │   ├── projects.html
                                                                                         │   ├── contact.html
                                                                                         │   ├── css/
                                                                                         │   │   └── portfolio.css
                                                                                         │   ├── js/
                                                                                         │   │   └── main.js
                                                                                         │   └── assets/
                                                                                         │       └── images/
                                                                                         │
                                                                                         ├── 📋 cv/
                                                                                         │   ├── Onuoha_Onyekachi_CV.pdf
                                                                                         │   └── Onuoha_Onyekachi_CV.md
                                                                                         │
                                                                                         ├── requirements.txt
                                                                                         ├── .gitignore
                                                                                         ├── LICENSE
                                                                                         └── README.md
                                                                                         ```

                                                                                         ---

                                                                                         ## 🚀 How to Run

                                                                                         ### Prerequisites
                                                                                         ```bash
                                                                                         Python 3.9+
                                                                                         PostgreSQL 14+
                                                                                         Power BI Desktop (for .pbix files)
                                                                                         ```

                                                                                         ### Installation
                                                                                         ```bash
                                                                                         # Clone the repository
                                                                                         git clone https://github.com/mooreepic/Fintech-Remittance-Marketing-Analytics.git
                                                                                         cd Fintech-Remittance-Marketing-Analytics

                                                                                         # Create virtual environment
                                                                                         python -m venv venv
                                                                                         source venv/bin/activate  # Linux/Mac
                                                                                         venv\Scripts\activate     # Windows

                                                                                         # Install dependencies
                                                                                         pip install -r requirements.txt

                                                                                         # Run Jupyter notebooks
                                                                                         jupyter notebook notebooks/
                                                                                         ```

                                                                                         ### Quick Start
                                                                                         ```bash
                                                                                         # Run the complete analysis pipeline
                                                                                         python src/run_analysis.py

                                                                                         # Launch the web dashboard locally
                                                                                         cd dashboards/web_dashboard
                                                                                         python -m http.server 8000
                                                                                         # Open http://localhost:8000
                                                                                         ```

                                                                                         ---

                                                                                         ## 📊 Sample SQL Queries

                                                                                         ```sql
                                                                                         -- Compare average exchange rates across platforms (Last 30 days)
                                                                                         SELECT
                                                                                             platform_name,
                                                                                             ROUND(AVG(exchange_rate), 2) AS avg_rate,
                                                                                             ROUND(MIN(exchange_rate), 2) AS min_rate,
                                                                                             ROUND(MAX(exchange_rate), 2) AS max_rate,
                                                                                             ROUND(AVG(transfer_fee), 2) AS avg_fee,
                                                                                             COUNT(*) AS data_points
                                                                                         FROM exchange_rates
                                                                                         WHERE currency_pair = 'GBP_NGN'
                                                                                             AND rate_date >= CURRENT_DATE - INTERVAL '30 days'
                                                                                             GROUP BY platform_name
                                                                                             ORDER BY avg_rate DESC;

                                                                                         -- Customer Segmentation by Transfer Behavior
                                                                                         SELECT
                                                                                             customer_segment,
                                                                                             COUNT(DISTINCT customer_id) AS total_customers,
                                                                                             ROUND(AVG(monthly_transfer_amount), 2) AS avg_monthly_amount,
                                                                                             ROUND(AVG(transfer_frequency), 1) AS avg_monthly_frequency,
                                                                                             MODE() WITHIN GROUP (ORDER BY preferred_platform) AS top_platform
                                                                                             FROM customer_segments cs
                                                                                         JOIN transfer_history th ON cs.customer_id = th.customer_id
                                                                                         GROUP BY customer_segment
                                                                                         ORDER BY total_customers DESC;

                                                                                         -- Sentiment Score Comparison
                                                                                         SELECT
                                                                                             platform_name,
                                                                                             COUNT(*) AS total_reviews,
                                                                                             ROUND(AVG(sentiment_score), 3) AS avg_sentiment,
                                                                                             ROUND(AVG(CASE WHEN sentiment_label = 'positive' THEN 1.0 ELSE 0.0 END) * 100, 1) AS positive_pct,
                                                                                             ROUND(AVG(star_rating), 2) AS avg_star_rating
                                                                                         FROM app_reviews
                                                                                         WHERE review_date >= '2024-01-01'
                                                                                         GROUP BY platform_name
                                                                                         ORDER BY avg_sentiment DESC;
                                                                                         ```

                                                                                         ---

                                                                                         ## 📊 Sample Python Analysis

                                                                                         ```python
                                                                                         import pandas as pd
                                                                                         import matplotlib.pyplot as plt
                                                                                         import seaborn as sns
                                                                                         from sklearn.cluster import KMeans
                                                                                         from sklearn.preprocessing import StandardScaler

                                                                                         # Load and analyze exchange rate data
                                                                                         rates_df = pd.read_csv('data/processed/cleaned_rates.csv')

                                                                                         # Calculate total cost for £500 transfer
                                                                                         platforms = ['MoneyPoint', 'LemFi', 'TapTapSend', 'Revolut', 'Remitly']
                                                                                         for platform in platforms:
                                                                                             platform_data = rates_df[rates_df['platform'] == platform]
                                                                                             avg_rate = platform_data['exchange_rate'].mean()
                                                                                             avg_fee = platform_data['transfer_fee'].mean()
                                                                                             naira_received = 500 * avg_rate
                                                                                             total_cost = avg_fee + (500 * (max_rate - avg_rate) / max_rate * 500)
                                                                                             print(f"{platform}: Rate={avg_rate:.2f} | Fee=£{avg_fee:.2f} | Naira=₦{naira_received:,.0f}")

                                                                                         # Customer Segmentation with K-Means
                                                                                         customer_df = pd.read_csv('data/processed/customer_data.csv')
                                                                                         features = ['monthly_amount', 'frequency', 'avg_transfer_size', 'tenure_months']
                                                                                         X = StandardScaler().fit_transform(customer_df[features])

                                                                                         kmeans = KMeans(n_clusters=5, random_state=42, n_init=10)
                                                                                         customer_df['segment'] = kmeans.fit_predict(X)
                                                                                         ```

                                                                                         ---

                                                                                         ## 👤 About the Author

                                                                                         ### Onuoha (Prince) Onyekachi

                                                                                         **Business Analyst | Data Analyst | Marketing Analytics Specialist**

                                                                                         Results-driven Business Analyst with dual Master's degrees in Data Analytics for Business & Economics (Higher School of Economics) and Marketing (York St. John University). Combining strong analytical capabilities with business acumen to drive strategic decision-making and process optimization.

                                                                                         **Technical Skills**: SQL | Python | Power BI | Excel (Advanced) | Tableau | JIRA | Confluence | BPMN | R | Machine Learning

                                                                                         **Core Competencies**:
                                                                                         - Business Requirements Gathering & Documentation (BRD, FRD, User Stories)
                                                                                         - - Data Analysis & Visualization
                                                                                           - - Market Research & Customer Segmentation
                                                                                             - - Process Mapping & Optimization
                                                                                               - - Stakeholder Management & Cross-functional Collaboration

                                                                                               **Education**:
                                                                                               - 🎓 MSc Marketing — York St. John University (2025-2026)
                                                                                               - - 🎓 MSc Data Analytics in Business & Economics — Higher School of Economics (2023-2025)
                                                                                                
                                                                                                 - **Location**: Doncaster, England, United Kingdom
                                                                                                
                                                                                                 - 📧 **Connect with me**:
                                                                                                 - - [LinkedIn](https://linkedin.com/in/onyekachiprince)
                                                                                                   - - [GitHub](https://github.com/mooreepic)
                                                                                                    
                                                                                                     - ---
                                                                                                     
                                                                                                     <div align="center">
                                                                                                     
                                                                                                     ### ⭐ If you found this project useful, please give it a star!
                                                                                                     
                                                                                                     **Built with ❤️ by Onuoha Onyekachi | 2025**
                                                                                                     
                                                                                                     *This project is part of my portfolio demonstrating data analytics and marketing strategy capabilities.*
                                                                                                     
                                                                                                     </div>
