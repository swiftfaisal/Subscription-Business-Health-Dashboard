create database subscription;
use subscription;
CREATE TABLE customers (
    customerID VARCHAR(20),
    gender VARCHAR(10),
    SeniorCitizen INT,
    Partner VARCHAR(5),
    Dependents VARCHAR(5),
    tenure INT,
    PhoneService VARCHAR(5),
    InternetService VARCHAR(20),
    Contract VARCHAR(20),
    PaperlessBilling VARCHAR(5),
    PaymentMethod VARCHAR(50),
    MonthlyCharges DECIMAL(8,2),
    TotalCharges DECIMAL(10,2),
    Churn VARCHAR(5),

    -- engineered columns
    Service_Count INT,
    Has_TechSupport INT,
    Has_Streaming INT,
    Tenure_Group VARCHAR(20),
    Contract_Simple VARCHAR(10),
    Revenue_Tier VARCHAR(10),
    CLV DECIMAL(12,2),
    High_Risk INT,
    Revenue_Lost DECIMAL(12,2)
);

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 9.5/Uploads/telco_churn_cleaned.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    customerID,
    gender,
    SeniorCitizen,
    Partner,
    Dependents,
    tenure,
    PhoneService,
    InternetService,
    Contract,
    PaperlessBilling,
    PaymentMethod,
    MonthlyCharges,
    @TotalCharges,
    Churn,
    Service_Count,
    Has_TechSupport,
    Has_Streaming,
    Tenure_Group,
    Contract_Simple,
    Revenue_Tier,
    CLV,
    High_Risk,
    Revenue_Lost
)
SET
    TotalCharges = NULLIF(@TotalCharges, '');

-- 1 Executive snapshot of subscription business health
SELECT
    COUNT(*) AS total_customers,
    COUNT(CASE WHEN Churn = 'No' THEN 1 END) AS active_customers,
    COUNT(CASE WHEN Churn = 'Yes' THEN 1 END) AS churned_customers,
    ROUND(COUNT(CASE WHEN Churn = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 2) AS churn_rate,
    ROUND(AVG(MonthlyCharges), 2) AS avg_monthly_charge,
    ROUND(AVG(CLV), 2) AS avg_clv
FROM customers;

-- 2 Customer distribution by revenue tier
SELECT
    Revenue_Tier,
    COUNT(*) AS customer_count
FROM customers
GROUP BY Revenue_Tier
ORDER BY
    CASE Revenue_Tier
        WHEN 'Low' THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'High' THEN 3
    END;
    
-- 3 Lifetime value contribution by revenue tier
SELECT
    Revenue_Tier,
    ROUND(SUM(CLV), 2) AS total_clv
FROM customers
GROUP BY Revenue_Tier
ORDER BY total_clv DESC;

-- 4 Churn behavior across tenure stages
SELECT
    Tenure_Group,
    COUNT(*) AS customers,
    COUNT(CASE WHEN Churn = 'Yes' THEN 1 END) AS churned,
    ROUND(COUNT(CASE WHEN Churn = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 2) AS churn_rate
FROM customers
GROUP BY Tenure_Group
ORDER BY
    CASE Tenure_Group
        WHEN '0–12 Months' THEN 1
        WHEN '13–36 Months' THEN 2
        WHEN '36+ Months' THEN 3
    END;
    
-- 5 Churn rate by contract type
SELECT
    Contract_Simple,
    COUNT(*) AS customers,
    COUNT(CASE WHEN Churn = 'Yes' THEN 1 END) AS churned,
    ROUND(COUNT(CASE WHEN Churn = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 2) AS churn_rate
FROM customers
GROUP BY Contract_Simple;

-- 6 Relationship between service engagement and churn
SELECT
    Service_Count,
    COUNT(*) AS customers,
    COUNT(CASE WHEN Churn = 'Yes' THEN 1 END) AS churned,
    ROUND(COUNT(CASE WHEN Churn = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 2) AS churn_rate
FROM customers
GROUP BY Service_Count
ORDER BY Service_Count;

-- 7 Active customers flagged as high risk
SELECT
    COUNT(*) AS high_risk_customers,
    ROUND(SUM(MonthlyCharges), 2) AS mrr_at_risk
FROM customers
WHERE High_Risk = 1
  AND Churn = 'No';
  
-- 8 Churn rate by payment method
SELECT
    PaymentMethod,
    COUNT(*) AS customers,
    COUNT(CASE WHEN Churn = 'Yes' THEN 1 END) AS churned,
    ROUND(COUNT(CASE WHEN Churn = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 2) AS churn_rate
FROM customers
GROUP BY PaymentMethod
ORDER BY churn_rate DESC;

-- 9 Impact of tech support and streaming on churn
SELECT
    Has_TechSupport,
    Has_Streaming,
    COUNT(*) AS customers,
    COUNT(CASE WHEN Churn = 'Yes' THEN 1 END) AS churned,
    ROUND(COUNT(CASE WHEN Churn = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 2) AS churn_rate
FROM customers
GROUP BY Has_TechSupport, Has_Streaming
ORDER BY churn_rate DESC;

-- 10 Revenue exposure from high-risk active customers
SELECT
    Revenue_Tier,
    COUNT(*) AS high_risk_customers,
    ROUND(SUM(MonthlyCharges), 2) AS mrr_at_risk,
    ROUND(SUM(MonthlyCharges) * 12, 2) AS annual_revenue_at_risk
FROM customers
WHERE High_Risk = 1
  AND Churn = 'No'
GROUP BY Revenue_Tier
ORDER BY annual_revenue_at_risk DESC;

-- 11 Retention behavior across tenure cohorts
SELECT
    Tenure_Group AS cohort,
    COUNT(*) AS cohort_size,
    COUNT(CASE WHEN Churn = 'No' THEN 1 END) AS active_customers,
    ROUND(COUNT(CASE WHEN Churn = 'No' THEN 1 END) * 100.0 / COUNT(*), 2) AS retention_rate,
    ROUND(AVG(CLV), 2) AS avg_clv
FROM customers
GROUP BY Tenure_Group
ORDER BY
    CASE Tenure_Group
        WHEN '0–12 Months' THEN 1
        WHEN '13–36 Months' THEN 2
        WHEN '36+ Months' THEN 3
    END;
    

