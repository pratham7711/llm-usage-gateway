-- Development and load-test tenants. Each key is 'sk-dev-' || tenant id; these exist only for the
-- local stack and the k6 suite and must never be deployed anywhere reachable.
insert into tenant (id, name, rate_per_sec, burst, monthly_token_quota) values
  ('demo',   'Demo tenant',                         50,    100,       10000000),
  ('hot',    'Rate-limited tenant (5 rps)',          5,     10,       10000000),
  ('capped', 'Quota-capped tenant (2,000 tokens)', 100,    200,           2000);

insert into tenant (id, name, rate_per_sec, burst, monthly_token_quota)
select 'lt-' || lpad(g::text, 2, '0'), 'Load-test tenant ' || g, 100000, 200000, 9000000000000
from generate_series(1, 50) as g;

insert into api_key (key_hash, tenant_id, label)
select encode(sha256(convert_to('sk-dev-' || id, 'UTF8')), 'hex'), id, 'dev'
from tenant;
