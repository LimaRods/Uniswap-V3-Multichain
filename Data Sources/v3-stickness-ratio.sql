WITH daily_active_users as (
  SELECT
    Chain_name,
    date_trunc('month', day) as month,
    avg(active_users) as avg_dau
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
       date_trunc('day', signed_at) as day,
        b.chain_name as Chain_name,
        uniq(b.tx_sender) as active_users
    FROM
      blockchains.all_chains b
      INNER JOIN pair_table p ON extract_address(hex(p.data1)) = extract_address(hex(b.log_emitter)) --Swap Transaction
    
    WHERE [chain_name:chainname] -- Chain
      AND b.topic0 = unhex('c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67') -- Swap Function Address
      AND successful = 1
      AND  signed_at BETWEEN '2021-01-01' AND '2022-12-31'
    
    GROUP BY  day, Chain_name) AS X
  GROUP BY month, Chain_name
  
),

monthly_active_users AS  (
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
      date_trunc('month', signed_at) as month,
      b.chain_name as Chain_name,
      uniq(b.tx_sender) as mau
    FROM
      blockchains.all_chains b
      INNER JOIN pair_table p ON extract_address(hex(p.data1)) = extract_address(hex(b.log_emitter)) --Swap Transaction
    
    WHERE [chain_name:chainname] -- Chain
      AND b.topic0 = unhex('c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67') -- Swap Function Address
      AND successful = 1
      AND  signed_at BETWEEN '2021-01-01' AND '2022-12-31'
    
    GROUP BY  month, Chain_name
)
SELECT
  dau.month,
  dau.Chain_name,
  (CASE
    WHEN (Chain_name = 'arbitrum_mainnet' AND month <= '2021-09-01') THEN 0 ELSE (avg_dau/mau) 
  END) AS stickness_ratio
FROM
  daily_active_users dau
  LEFT JOIN monthly_active_users mau ON (dau.month = mau.month AND mau.Chain_name = dau.Chain_name)
ORDER BY month ASC, Chain_name