-- ============================================================================
-- FINTECH REMITTANCE MARKETING ANALYTICS - SQL ANALYSIS QUERIES
-- GBP to NGN Corridor: MoneyPoint vs LemFi vs Tap Tap Send vs Revolut vs Remitly
-- Author: Onuoha (Prince) Onyekachi
-- ============================================================================

-- ============================================================================
-- 1. DATABASE SCHEMA CREATION
-- ============================================================================

CREATE TABLE IF NOT EXISTS platforms (
      platform_id SERIAL PRIMARY KEY,
      platform_name VARCHAR(50) NOT NULL,
      founded_year INT,
      headquarters VARCHAR(100),
      key_differentiator TEXT,
      website_url VARCHAR(200)
  );

CREATE TABLE IF NOT EXISTS exchange_rates (
      rate_id SERIAL PRIMARY KEY,
      platform_id INT REFERENCES platforms(platform_id),
      currency_pair VARCHAR(10) DEFAULT 'GBP_NGN',
      exchange_rate DECIMAL(10, 2) NOT NULL,
      transfer_fee DECIMAL(6, 2) DEFAULT 0,
      rate_date DATE NOT NULL,
      rate_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );

CREATE TABLE IF NOT EXISTS customers (
      customer_id SERIAL PRIMARY KEY,
      age_group VARCHAR(20),
      gender VARCHAR(10),
      location_uk VARCHAR(100),
      registration_date DATE,
      preferred_platform INT REFERENCES platforms(platform_id),
      customer_segment VARCHAR(50)
  );

CREATE TABLE IF NOT EXISTS transfers (
      transfer_id SERIAL PRIMARY KEY,
      customer_id INT REFERENCES customers(customer_id),
      platform_id INT REFERENCES platforms(platform_id),
      amount_gbp DECIMAL(10, 2),
      amount_ngn DECIMAL(15, 2),
      exchange_rate_used DECIMAL(10, 2),
      fee_charged DECIMAL(6, 2),
      transfer_date DATE,
      transfer_speed VARCHAR(30),
      status VARCHAR(20) DEFAULT 'completed'
  );

CREATE TABLE IF NOT EXISTS app_reviews (
      review_id SERIAL PRIMARY KEY,
      platform_id INT REFERENCES platforms(platform_id),
      review_source VARCHAR(30), -- 'app_store', 'play_store', 'trustpilot'
    star_rating INT CHECK (star_rating BETWEEN 1 AND 5),
      review_text TEXT,
      sentiment_score DECIMAL(5, 3),
      sentiment_label VARCHAR(20), -- 'positive', 'neutral', 'negative'
    review_date DATE,
      reviewer_country VARCHAR(50)
  );

CREATE TABLE IF NOT EXISTS digital_metrics (
      metric_id SERIAL PRIMARY KEY,
      platform_id INT REFERENCES platforms(platform_id),
      metric_date DATE,
      monthly_web_visits BIGINT,
      app_downloads_monthly INT,
      social_media_followers INT,
      google_trends_score INT,
      seo_domain_authority INT,
      trustpilot_score DECIMAL(3, 1)
  );

-- ============================================================================
-- 2. SEED DATA - Platform Information
-- ============================================================================

INSERT INTO platforms (platform_name, founded_year, headquarters, key_differentiator) VALUES
('MoneyPoint', 2019, 'Nigeria/UK', 'POS + Remittance integration'),
('LemFi', 2020, 'UK/Canada', 'Multi-currency wallet + remittance'),
('Tap Tap Send', 2018, 'UK/USA', 'Zero-fee transfers'),
('Revolut', 2015, 'UK', 'Super-app with FX capabilities'),
('Remitly', 2011, 'USA', 'Speed guarantee + promotional offers');

-- ============================================================================
-- 3. COMPETITIVE ANALYSIS QUERIES
-- ============================================================================

-- Query 1: Exchange Rate Comparison (Latest 30 Days)
SELECT 
    p.platform_name,
    ROUND(AVG(er.exchange_rate), 2) AS avg_rate,
    ROUND(MIN(er.exchange_rate), 2) AS min_rate,
    ROUND(MAX(er.exchange_rate), 2) AS max_rate,
    ROUND(STDDEV(er.exchange_rate), 2) AS rate_volatility,
    ROUND(AVG(er.transfer_fee), 2) AS avg_fee,
    COUNT(*) AS data_points
FROM exchange_rates er
JOIN platforms p ON er.platform_id = p.platform_id
WHERE er.currency_pair = 'GBP_NGN'
    AND er.rate_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY p.platform_name
ORDER BY avg_rate DESC;

-- Query 2: Total Cost Analysis for Different Transfer Amounts
WITH transfer_amounts AS (
      SELECT unnest(ARRAY[100, 250, 500, 1000, 2500, 5000]) AS send_amount
  ),
latest_rates AS (
      SELECT DISTINCT ON (p.platform_name)
          p.platform_name,
          er.exchange_rate,
          er.transfer_fee
      FROM exchange_rates er
      JOIN platforms p ON er.platform_id = p.platform_id
      WHERE er.currency_pair = 'GBP_NGN'
      ORDER BY p.platform_name, er.rate_date DESC
  )
SELECT 
    lr.platform_name,
    ta.send_amount AS "Send (GBP)",
    lr.transfer_fee AS "Fee (GBP)",
    ROUND(ta.send_amount * lr.exchange_rate, 0) AS "Recipient Gets (NGN)",
    ROUND(ta.send_amount + lr.transfer_fee, 2) AS "Total Cost (GBP)",
    ROUND((ta.send_amount * lr.exchange_rate) / (ta.send_amount + lr.transfer_fee), 2) AS "Effective Rate"
FROM latest_rates lr
CROSS JOIN transfer_amounts ta
ORDER BY ta.send_amount, "Recipient Gets (NGN)" DESC;

-- Query 3: Market Share Estimation by Transaction Volume
SELECT 
    p.platform_name,
    COUNT(t.transfer_id) AS total_transfers,
    ROUND(SUM(t.amount_gbp), 2) AS total_volume_gbp,
    ROUND(AVG(t.amount_gbp), 2) AS avg_transfer_size,
    ROUND(100.0 * COUNT(t.transfer_id) / SUM(COUNT(t.transfer_id)) OVER(), 1) AS market_share_pct,
    ROUND(100.0 * SUM(t.amount_gbp) / SUM(SUM(t.amount_gbp)) OVER(), 1) AS volume_share_pct
FROM transfers t
JOIN platforms p ON t.platform_id = p.platform_id
WHERE t.transfer_date >= CURRENT_DATE - INTERVAL '12 months'
    AND t.status = 'completed'
GROUP BY p.platform_name
ORDER BY total_volume_gbp DESC;

-- Query 4: Customer Segmentation Analysis
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    ROUND(AVG(t.amount_gbp), 2) AS avg_transfer_amount,
    ROUND(AVG(monthly_freq.freq), 1) AS avg_monthly_frequency,
    p.platform_name AS preferred_platform,
    ROUND(100.0 * COUNT(DISTINCT c.customer_id) / 
        SUM(COUNT(DISTINCT c.customer_id)) OVER(), 1) AS segment_pct
FROM customers c
JOIN transfers t ON c.customer_id = t.customer_id
JOIN platforms p ON c.preferred_platform = p.platform_id
JOIN LATERAL (
      SELECT COUNT(*) / GREATEST(EXTRACT(MONTH FROM AGE(MAX(transfer_date), MIN(transfer_date))), 1) AS freq
      FROM transfers t2 WHERE t2.customer_id = c.customer_id
  ) monthly_freq ON true
GROUP BY c.customer_segment, p.platform_name
ORDER BY total_customers DESC;

-- Query 5: Sentiment Analysis Summary by Platform
SELECT 
    p.platform_name,
    ar.review_source,
    COUNT(*) AS total_reviews,
    ROUND(AVG(ar.star_rating), 2) AS avg_star_rating,
    ROUND(AVG(ar.sentiment_score), 3) AS avg_sentiment_score,
    ROUND(100.0 * SUM(CASE WHEN ar.sentiment_label = 'positive' THEN 1 ELSE 0 END) / COUNT(*), 1) AS positive_pct,
    ROUND(100.0 * SUM(CASE WHEN ar.sentiment_label = 'neutral' THEN 1 ELSE 0 END) / COUNT(*), 1) AS neutral_pct,
    ROUND(100.0 * SUM(CASE WHEN ar.sentiment_label = 'negative' THEN 1 ELSE 0 END) / COUNT(*), 1) AS negative_pct
FROM app_reviews ar
JOIN platforms p ON ar.platform_id = p.platform_id
WHERE ar.review_date >= '2024-01-01'
GROUP BY p.platform_name, ar.review_source
ORDER BY p.platform_name, ar.review_source;

-- Query 6: Monthly Transfer Trends
SELECT 
    DATE_TRUNC('month', t.transfer_date) AS month,
    p.platform_name,
    COUNT(*) AS transfer_count,
    ROUND(SUM(t.amount_gbp), 2) AS total_volume,
    ROUND(AVG(t.amount_gbp), 2) AS avg_amount,
    ROUND(AVG(t.exchange_rate_used), 2) AS avg_rate
FROM transfers t
JOIN platforms p ON t.platform_id = p.platform_id
WHERE t.transfer_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', t.transfer_date), p.platform_name
ORDER BY month DESC, total_volume DESC;

-- Query 7: Digital Marketing Performance Comparison
SELECT 
    p.platform_name,
    ROUND(AVG(dm.monthly_web_visits), 0) AS avg_monthly_visits,
    ROUND(AVG(dm.app_downloads_monthly), 0) AS avg_monthly_downloads,
    MAX(dm.social_media_followers) AS latest_followers,
    ROUND(AVG(dm.google_trends_score), 1) AS avg_trends_score,
    MAX(dm.seo_domain_authority) AS domain_authority,
    MAX(dm.trustpilot_score) AS trustpilot_rating
FROM digital_metrics dm
JOIN platforms p ON dm.platform_id = p.platform_id
WHERE dm.metric_date >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY p.platform_name
ORDER BY avg_monthly_visits DESC;

-- Query 8: Customer Retention & Churn Analysis
WITH customer_activity AS (
      SELECT 
        c.customer_id,
          c.customer_segment,
          p.platform_name,
          MIN(t.transfer_date) AS first_transfer,
          MAX(t.transfer_date) AS last_transfer,
          COUNT(t.transfer_id) AS total_transfers,
          EXTRACT(DAY FROM MAX(t.transfer_date) - MIN(t.transfer_date)) AS active_days
      FROM customers c
      JOIN transfers t ON c.customer_id = t.customer_id
      JOIN platforms p ON t.platform_id = p.platform_id
      GROUP BY c.customer_id, c.customer_segment, p.platform_name
  )
SELECT 
    platform_name,
    COUNT(*) AS total_customers,
    ROUND(AVG(total_transfers), 1) AS avg_transfers_per_customer,
    ROUND(AVG(active_days), 0) AS avg_active_days,
    ROUND(100.0 * SUM(CASE WHEN total_transfers >= 3 THEN 1 ELSE 0 END) / COUNT(*), 1) AS retention_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN last_transfer < CURRENT_DATE - INTERVAL '90 days' THEN 1 ELSE 0 END) / COUNT(*), 1) AS churn_rate_pct
FROM customer_activity
GROUP BY platform_name
ORDER BY retention_rate_pct DESC;

-- Query 9: Price Sensitivity - Rate Differential Impact
SELECT 
    CASE 
        WHEN rate_diff <= 5 THEN '0-5 NGN difference'
        WHEN rate_diff <= 15 THEN '6-15 NGN difference'
        WHEN rate_diff <= 25 THEN '16-25 NGN difference'
        ELSE '25+ NGN difference'
    END AS rate_differential_bucket,
    COUNT(*) AS switch_events,
    ROUND(AVG(t.amount_gbp), 2) AS avg_transfer_amount,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct_of_switches
FROM (
      SELECT 
        t.*,
          ABS(er1.exchange_rate - er2.exchange_rate) AS rate_diff
      FROM transfers t
      JOIN exchange_rates er1 ON t.platform_id = er1.platform_id AND t.transfer_date = er1.rate_date
      CROSS JOIN LATERAL (
          SELECT exchange_rate FROM exchange_rates 
          WHERE rate_date = t.transfer_date AND platform_id != t.platform_id
          ORDER BY exchange_rate DESC LIMIT 1
      ) er2
  ) sub
JOIN transfers t ON sub.transfer_id = t.transfer_id
GROUP BY rate_differential_bucket
ORDER BY rate_differential_bucket;

-- Query 10: Executive Summary Dashboard View
SELECT 
    p.platform_name,
    COALESCE(rate_data.current_rate, 0) AS current_rate,
    COALESCE(rate_data.avg_fee, 0) AS avg_fee,
    COALESCE(volume_data.monthly_volume, 0) AS monthly_volume_gbp,
    COALESCE(volume_data.monthly_transactions, 0) AS monthly_transactions,
    COALESCE(review_data.avg_rating, 0) AS avg_app_rating,
    COALESCE(review_data.positive_pct, 0) AS positive_sentiment_pct,
    COALESCE(digital_data.monthly_visits, 0) AS monthly_web_visits,
    COALESCE(digital_data.trends_score, 0) AS google_trends_score
FROM platforms p
LEFT JOIN LATERAL (
      SELECT exchange_rate AS current_rate, transfer_fee AS avg_fee
      FROM exchange_rates WHERE platform_id = p.platform_id
      ORDER BY rate_date DESC LIMIT 1
  ) rate_data ON true
LEFT JOIN LATERAL (
      SELECT SUM(amount_gbp) AS monthly_volume, COUNT(*) AS monthly_transactions
      FROM transfers WHERE platform_id = p.platform_id 
      AND transfer_date >= CURRENT_DATE - INTERVAL '30 days'
  ) volume_data ON true
LEFT JOIN LATERAL (
      SELECT ROUND(AVG(star_rating), 2) AS avg_rating,
             ROUND(100.0 * AVG(CASE WHEN sentiment_label = 'positive' THEN 1.0 ELSE 0.0 END), 1) AS positive_pct
      FROM app_reviews WHERE platform_id = p.platform_id
  ) review_data ON true
LEFT JOIN LATERAL (
      SELECT monthly_web_visits AS monthly_visits, google_trends_score AS trends_score
      FROM digital_metrics WHERE platform_id = p.platform_id
      ORDER BY metric_date DESC LIMIT 1
  ) digital_data ON true
ORDER BY monthly_volume_gbp DESC;
