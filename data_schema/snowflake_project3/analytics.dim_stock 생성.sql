CREATE OR REPLACE TABLE analytics.dim_stock (
    symbol STRING PRIMARY KEY,
    company_name STRING
);

INSERT INTO analytics.dim_stock (symbol, company_name) VALUES
  ('005930.KS', '삼성전자'),
  ('000660.KS', 'SK하이닉스'),
  ('373220.KQ', 'LG에너지솔루션'),
  ('247540.KQ', '에코프로비엠'),
  ('068270.KQ', '셀트리온'),
  ('035720.KQ', '카카오'),
  ('034020.KQ', '두산에너빌리티'),
  ('011200.KS', 'HMM'),
  ('086790.KQ', '하나금융지주'),
  ('035760.KQ', 'CJ ENM');
