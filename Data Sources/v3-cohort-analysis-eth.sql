WITH user_cohorts AS (
  WITH pair_table AS (
    SELECT
      tx_hash,
      data1 -- Pair Address
    FROM blockchains.all_chains
    WHERE  chain_name = 'eth_mainnet' -- Ethereum
      AND log_emitter = unhex('1F98431c8aD98523631AE4a59f267346ea31F984') -- Uniswap V3 Factory. Contract that emits the Pai
  )
    
  SELECT
    hex(tx_sender) as address,
    min(date_trunc('month', signed_at)) as cohort_month
    
  FROM
    blockchains.all_chains b
    INNER JOIN pair_table p ON extract_address(hex(p.data1)) = extract_address(hex(b.log_emitter)) --Swap Transaction
  
  WHERE  chain_name = 'eth_mainnet' -- Ethereum
    AND b.topic0 = unhex('c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67') -- Swap Function Address
    AND successful = 1
    AND  signed_at BETWEEN '2021-01-01' AND '2022-12-31'
  
  GROUP BY address
),

following_months as (
  WITH pair_table AS (
    SELECT
      tx_hash,
      data1 -- Pair Address
    FROM blockchains.all_chains
    WHERE  chain_name = 'eth_mainnet' -- Ethereum
      AND log_emitter = unhex('1F98431c8aD98523631AE4a59f267346ea31F984') -- Uniswap V3 Factory. Contract that emits the Pai
  )
    
  SELECT
    hex(b.tx_sender) as address,
    date_diff('month', uc.cohort_month, date_trunc('month', b.signed_at)) as month_number
  FROM
    blockchains.all_chains b
    INNER JOIN pair_table p ON extract_address(hex(p.data1)) = extract_address(hex(b.log_emitter)) --Swap Transaction
    LEFT JOIN user_cohorts uc ON hex(b.tx_sender) = uc.address
  
  WHERE  chain_name = 'eth_mainnet' -- Ethereum
    AND b.topic0 = unhex('c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67') -- Swap Function Address
    AND successful = 1
    AND  signed_at BETWEEN '2021-01-01' AND '2022-12-31'
  
  GROUP BY address, month_number
),

cohort_size AS (
  SELECT 
    uc.cohort_month as cohort_month,
    COUNT(*) as num_users
  FROM user_cohorts uc
  GROUP BY cohort_month
),

retention_table AS (
  SELECT
    c.cohort_month as cohort_month,
    o.month_number as month_number,
    COUNT(*) as num_users
  FROM following_months o
    LEFT JOIN user_cohorts c  ON o.address = c.address 
  GROUP BY cohort_month, month_number
)

SELECT
  r.cohort_month,
  s.num_users as new_users,
  r.month_number,
  r.num_users,
  r.num_users/s.num_users as retention
  
FROM
  retention_table r
  LEFT JOIN cohort_size s 
	ON r.cohort_month = s.cohort_month
  --WHERE r.month_number != 0
ORDER BY r.cohort_month, r.month_number ASC

