-- Where each row's token counts came from, the gateway's own counts beside the provider's, and cost.
alter table usage_event
  add column usage_source   text   not null default 'provider',
  add column est_tokens_in  integer,
  add column est_tokens_out integer,
  add column cost_nano_usd  bigint not null default 0;

alter table usage_rollup_minute add column cost_nano_usd bigint not null default 0;
alter table tenant_month_usage  add column cost_nano_usd bigint not null default 0;

-- Prices in USD per million tokens. A price applies from effective_from until the next row for the
-- same model, so a price change never rewrites what was already billed.
create table model_price (
  model                text not null,
  effective_from       timestamptz not null,
  input_usd_per_mtok   numeric(12, 4) not null check (input_usd_per_mtok >= 0),
  output_usd_per_mtok  numeric(12, 4) not null check (output_usd_per_mtok >= 0),
  primary key (model, effective_from)
);

-- Example list prices, for development. The mock model is priced like gpt-4o-mini so benchmark
-- cost figures are realistic; local models cost nothing per token.
insert into model_price (model, effective_from, input_usd_per_mtok, output_usd_per_mtok) values
  ('mock-small',   '2026-01-01', 0.15, 0.60),
  ('gpt-4o-mini',  '2026-01-01', 0.15, 0.60),
  ('gpt-4o',       '2026-01-01', 2.50, 10.00),
  ('qwen2.5:0.5b', '2026-01-01', 0, 0),
  ('qwen2.5:1.5b', '2026-01-01', 0, 0);
