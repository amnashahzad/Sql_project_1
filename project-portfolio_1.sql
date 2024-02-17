CREATE SCHEMA data_bank_portfolio;
use data_bank_portfolio;

-- regions code start
CREATE TABLE regions (
region_id INTEGER,
region_name VARCHAR(9)
);
INSERT INTO regions
(region_id, region_name)
VALUES
('1', 'Australia'),
('2', 'America'),
('3', 'Africa'),
('4', 'Asia'),
('5', 'Europe');

select * from regions;
-- regions end

-- customer_nodes start
CREATE TABLE customer_nodes (
    customer_id INT,
    region_id INT,
    node_id INT,
    start_date DATE,
    end_date DATE,
    PRIMARY KEY (customer_id, start_date) -- Composite key, assuming a customer can start on a node at a unique date
);
INSERT INTO customer_nodes (customer_id, region_id, node_id, start_date, end_date) VALUES
(1, 3, 4, '2020-01-02', '2020-01-03'),
(2, 3, 5, '2020-01-03', '2020-01-17'),
(3, 5, 4, '2020-01-27', '2020-02-18'),
(4, 5, 4, '2020-01-07', '2020-01-19'),
(5, 3, 3, '2020-01-15', '2020-01-23'),
(6, 1, 1, '2020-01-11', '2020-02-06'),
(7, 2, 5, '2020-01-20', '2020-02-04'),
(8, 1, 2, '2020-01-15', '2020-01-28'),
(9, 4, 5, '2020-01-21', '2020-01-25'),
(10, 3, 4, '2020-01-13', '2020-01-14');
select * from customer_nodes;
-- customer_nodes end

-- table customer_transactions start
CREATE TABLE customer_transactions (
    customer_id INT,
    txn_date DATE,
    txn_type VARCHAR(255),
    txn_amount DECIMAL(10, 2), -- Assuming currency values, adjust precision as needed
    PRIMARY KEY (customer_id, txn_date, txn_type) -- Assuming a composite key; adjust according to actual uniqueness constraints
);
INSERT INTO customer_transactions (customer_id, txn_date, txn_type, txn_amount) VALUES
(429, '2020-01-21', 'deposit', 82.00),
(155, '2020-01-10', 'deposit', 712.00),
(398, '2020-01-01', 'deposit', 196.00),
(255, '2020-01-14', 'deposit', 563.00),
(185, '2020-01-29', 'deposit', 626.00),
(309, '2020-01-13', 'deposit', 995.00),
(312, '2020-01-20', 'deposit', 485.00),
(376, '2020-01-03', 'deposit', 706.00),
(188, '2020-01-13', 'deposit', 601.00),
(138, '2020-01-11', 'deposit', 520.00);
select * from  customer_transactions;

-- case study
-- Count of Unique Nodes:
SELECT COUNT(DISTINCT node_id) AS unique_nodes
FROM customer_nodes;

-- Number of Nodes Per Region:
SELECT region_id, COUNT(DISTINCT node_id) AS nodes_count
FROM customer_nodes
GROUP BY region_id;	

-- Customers Allocated to Each Region:
SELECT region_id, COUNT(DISTINCT customer_id) AS customer_count
FROM customer_nodes
GROUP BY region_id;

-- Average Days Customers are Reallocated:
SELECT AVG(DATEDIFF(end_date, start_date)) AS average_reallocation_days
FROM customer_nodes;

-- Median, 80th, and 95th Percentile for Reallocations:
SET @row_number = 0;
SET @current_region = '';

SELECT region_id,
       AVG(DATEDIFF(end_date, start_date)) AS median_days
FROM (
    SELECT *,
           @row_number:=CASE
               WHEN @current_region = region_id THEN @row_number + 1
               ELSE 1
           END AS rn,
           @current_region:=region_id AS clset_region_id,
           COUNT(*) OVER(PARTITION BY region_id) AS total_rows
    FROM customer_nodes
    ORDER BY region_id, DATEDIFF(end_date, start_date)
) AS ranked
WHERE rn IN (FLOOR((total_rows + 1) / 2), FLOOR((total_rows + 2) / 2))
GROUP BY region_id;
-- column a question complete

-- B column start 
--  Unique Count and Total Amount for Each Transaction Type

SELECT txn_type, COUNT(DISTINCT customer_id) AS unique_customers, SUM(txn_amount) AS total_amount
FROM customer_transactions
GROUP BY txn_type;

-- Average Total Historical Deposit Counts and Amounts for All Customers

SELECT AVG(deposit_count) AS avg_deposit_count, AVG(total_deposit_amount) AS avg_deposit_amount
FROM (
    SELECT customer_id, COUNT(*) AS deposit_count, SUM(txn_amount) AS total_deposit_amount
    FROM customer_transactions
    WHERE txn_type = 'deposit'
    GROUP BY customer_id
) AS deposit_summary;

-- Data Bank Customers Making More Than 1 Deposit and Either 1 Purchase or 1 Withdrawal in a Single Month

SELECT YEAR(sub.txn_date) AS year, MONTH(sub.txn_date) AS month, COUNT(*) AS customers
FROM (
    SELECT customer_id, txn_date,
           SUM(CASE WHEN txn_type = 'deposit' THEN 1 ELSE 0 END) AS deposits,
           SUM(CASE WHEN txn_type IN ('purchase', 'withdrawal') THEN 1 ELSE 0 END) AS withdraws_purchases
    FROM customer_transactions
    GROUP BY customer_id, txn_date
    HAVING deposits > 1 AND withdraws_purchases >= 1
) AS sub
GROUP BY YEAR(sub.txn_date), MONTH(sub.txn_date);

-- 5. Percentage of Customers Who Increase Their Closing Balance by More Than 5%

-- Step 1: Calculate the monthly opening and closing balances for each customer
WITH monthly_balances AS (
  SELECT
    customer_id,
    DATE_FORMAT(txn_date, '%Y-%m') AS month,
    -- Assuming the first transaction of the month is the opening balance
    FIRST_VALUE(txn_amount) OVER (
      PARTITION BY customer_id, DATE_FORMAT(txn_date, '%Y-%m')
      ORDER BY txn_date ASC
    ) AS opening_balance,
    -- Assuming the last transaction of the month is the closing balance
    LAST_VALUE(txn_amount) OVER (
      PARTITION BY customer_id, DATE_FORMAT(txn_date, '%Y-%m')
      ORDER BY txn_date ASC
      RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS closing_balance
  FROM customer_transactions
),
-- Step 2: Calculate the percentage increase for each customer for each month
percentage_increase AS (
  SELECT
    customer_id,
    month,
    ((closing_balance - opening_balance) / opening_balance) * 100 AS percent_increase
  FROM monthly_balances
)
-- Step 3: Calculate the percentage of customers with more than a 5% increase
SELECT
  (COUNT(DISTINCT CASE WHEN percent_increase > 5 THEN customer_id END) /
   COUNT(DISTINCT customer_id)) * 100 AS percent_customers_above_5_increase
FROM percentage_increase;
-- column B question complete

-- C column start 

-- Running Customer Balance Column
SELECT 
  customer_id, 
  txn_date, 
  txn_type, 
  txn_amount,
  SUM(txn_amount) OVER (PARTITION BY customer_id ORDER BY txn_date) AS running_balance
FROM 
  customer_transactions;
  
-- Customer Balance at the End of Each Month
SELECT 
  customer_id, 
  DATE_FORMAT(txn_date, '%Y-%m') AS txn_month, 
  LAST_VALUE(txn_amount) OVER (
    PARTITION BY customer_id, DATE_FORMAT(txn_date, '%Y-%m') 
    ORDER BY txn_date 
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS month_end_balance
FROM 
  customer_transactions;
  
-- Minimum, Average, and Maximum Values of the Running Balance for Each Customer
  
  WITH RunningBalances AS (
  SELECT 
    customer_id, 
    txn_date, 
    SUM(txn_amount) OVER (PARTITION BY customer_id ORDER BY txn_date) AS running_balance
  FROM 
    customer_transactions
)

SELECT 
  customer_id, 
  MIN(running_balance) AS min_balance, 
  AVG(running_balance) AS avg_balance, 
  MAX(running_balance) AS max_balance
FROM 
  RunningBalances
GROUP BY 
  customer_id;
  
-- c column complete

-- d column start

-- Define the initial data allocation (replace X with the actual initial data amount)
SET @initial_data = 1000; -- Example initial data amount

-- Calculate the number of days in the current month
SET @current_month_days = DAY(LAST_DAY(NOW()));

-- Calculate the data at the end of the month without compounding interest
SELECT @initial_data * (1 + (0.06 / 365) * @current_month_days) AS data_at_end_of_month_without_compounding;

-- d column end 