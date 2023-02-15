/*
Arbitrum, 2021-09-01
Optimism, 2021-11-13
Polygon, 2021-12-21
*/
WITH pair_table AS (
  SELECT
    tx_hash,
    data1, -- Pair Address
    topic1 -- Token 0 Address
  
  FROM blockchains.all_chains
  WHERE  [chain_name:chainname]
    AND log_emitter = unhex('1F98431c8aD98523631AE4a59f267346ea31F984') -- Uniswap V3 Factory. Contract that emits the Pai
),

swap_table AS (
  SELECT
    date_trunc('month', signed_at) as Month,
    b.chain_name as Chain_name,
    uniq(b.tx_hash) as Swap_Count,
    --lagInFrame(Swap_Count) OVER (ORDER BY Month) as Swap_Count_Pre,
    uniq(b.tx_sender)as Traders,
    lagInFrame(Traders) OVER (PARTITION BY Chain_name ORDER BY Month) as Traders_Pre
    
  FROM
    blockchains.all_chains b
    INNER JOIN pair_table p ON extract_address(hex(p.data1)) = extract_address(hex(b.log_emitter)) --Swap Transaction

  WHERE [chain_name:chainname] -- Chain
    AND b.topic0 = unhex('c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67') -- Swap Function Address
    AND successful = 1
    AND  signed_at BETWEEN '2021-01-01' AND '2022-12-31'

  GROUP BY Month, Chain_name
  ORDER BY Month ASC, Chain_name)

SELECT
  Month,
  Chain_name,
  Swap_Count,
  --(Swap_Count/Swap_Count_Pre) - 1  as MoM_Growth_Swap,
  Traders,
  (CASE
    WHEN (Chain_name = 'arbitrum_mainnet' AND Month <= '2021-09-01') THEN 0 ELSE (Traders/Traders_Pre) - 1
  END) AS MoM_Growth_Traders
  
FROM
  swap_table