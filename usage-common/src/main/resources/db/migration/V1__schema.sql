create table tenant (
  id                  text primary key,
  name                text not null,
  rate_per_sec        integer not null check (rate_per_sec > 0),
  burst               integer not null check (burst > 0),
  monthly_token_quota bigint  not null check (monthly_token_quota >= 0),
  created_at          timestamptz not null default now()
);

-- API keys are stored only as SHA-256 hex digests; the plaintext is shown once at creation.
create table api_key (
  key_hash   text primary key,
  tenant_id  text not null references tenant (id),
  label      text,
  created_at timestamptz not null default now(),
  revoked_at timestamptz
);
create index api_key_tenant_idx on api_key (tenant_id);

-- One row per billed upstream call. The primary key on event_id is what makes redelivery safe.
create table usage_event (
  event_id        uuid primary key,
  tenant_id       text not null,
  model           text not null,
  tokens_in       integer not null,
  tokens_out      integer not null,
  latency_ms      bigint not null,
  status          integer not null,
  streamed        boolean not null,
  occurred_at     timestamptz not null,
  received_at     timestamptz not null default now(),
  kafka_partition integer,
  kafka_offset    bigint
);
create index usage_event_tenant_time_idx on usage_event (tenant_id, occurred_at);

create table usage_rollup_minute (
  tenant_id  text not null,
  minute     timestamptz not null,
  model      text not null,
  requests   bigint not null,
  errors     bigint not null,
  tokens_in  bigint not null,
  tokens_out bigint not null,
  primary key (tenant_id, minute, model)
);

-- Authoritative monthly total; the Redis quota key is a cache of this value.
create table tenant_month_usage (
  tenant_id text not null,
  month     char(7) not null,
  tokens    bigint not null,
  requests  bigint not null,
  primary key (tenant_id, month)
);
