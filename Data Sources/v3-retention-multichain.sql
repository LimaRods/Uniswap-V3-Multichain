WITH user_cohorts AS (
  WITH pair_table AS (
    SELECT
      tx_hash,
      data1, -- Pair Address
      topic1 -- Token 0 Address
    
    FROM blockchains.all_chains
    WHERE  [chain_name:chainname]
      AND log_emitter = unhex('1F98431c8aD98523631AE4a59f267346ea31F984') -- Uniswap V3 Factory. Contract that emits the Pai
  )
    
  SELECT
    tx_sender as Traders,
    min(date_trunc('month', signed_at)) as cohort_month,
    b.chain_name as Chain_name  
  FROM
    blockchains.all_chains b
    INNER JOIN pair_table p ON extract_address(hex(p.data1)) = extract_address(hex(b.log_emitter)) --Swap Transaction
  
  WHERE [chain_name:chainname] -- Chain
    AND b.topic0 = unhex('c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67') -- Swap Function Address
    AND successful = 1
    AND  signed_at BETWEEN '2021-01-01' AND '2022-12-31'
  
  GROUP BY Traders, Chain_name
),

new_users as (
  SELECT
  cohort_month as month,
  Chain_name,
  uniq(Traders) as new_users
  
FROM
  user_cohorts
GROUP BY 
  month, Chain_name 
),

active_users as (

  SELECT
    Chain_name,
    month,
    active_users,
    lagInFrame(active_users) OVER (PARTITION BY Chain_name ORDER BY month) AS previous_users
  
  FROM
    (WITH pair_table AS (
      SELECT
        tx_hash,
        data1, -- Pair Address
        topic1 -- Token 0 Address
      
      FROM blockchains.all_chains
      WHERE  [chain_name:chainname]
        AND log_emitter = unhex('1F98431c8aD98523631AE4a59f267346ea31F984') -- Uniswap V3 Factory. Contract that emits the Pai
    )
      
    SELECT
       date_trunc('month', signed_at) as month,
        b.chain_name as Chain_name,
        uniq(b.tx_sender) as active_users
    FROM
      blockchains.all_chains b
      INNER JOIN pair_table p ON extract_address(hex(p.data1)) = extract_address(hex(b.log_emitter)) --Swap Transaction
    
    WHERE [chain_name:chainname] -- Chain
      AND b.topic0 = unhex('c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67') -- Swap Function Address
      AND successful = 1
      AND  signed_at BETWEEN '2021-01-01' AND '2022-12-31'
    
    GROUP BY  month, Chain_name) AS X
  
)

SELECT
  a.Chain_name,
  a.month,
  active_users,
  new_users,
  (active_users - new_users)/previous_users as retention_rate
  
FROM
  new_users  n
  INNER JOIN active_users a ON (n.month = a.month AND n.Chain_name = a.Chain_name)
ORDER BY month ASC

