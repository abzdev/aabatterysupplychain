-- ============================================================
-- POP Inventory Balancing System — Supabase Schema
-- Null-safe for Excel/CSV ingest
-- ============================================================

-- ------------------------------------------------------------
-- Cleanup for reruns in dev
-- ------------------------------------------------------------

drop view if exists open_po_history cascade;

drop table if exists audit_log cascade;
drop table if exists transfer_requests cascade;
drop table if exists events cascade;
drop table if exists sku_dc_features cascade;
drop table if exists customer_dc_mapping cascade;
drop table if exists lead_time_lookup cascade;
drop table if exists transfer_cost_lookup cascade;
drop table if exists penalty_history cascade;
drop table if exists transfer_cost_history cascade;
drop table if exists chargebacks cascade;
drop table if exists po_history cascade;
drop table if exists sales_history cascade;
drop table if exists inventory_snapshots cascade;

drop function if exists set_updated_at() cascade;
drop function if exists set_po_is_open() cascade;

drop type if exists confidence_level cascade;
drop type if exists recommended_action cascade;
drop type if exists event_state cascade;
drop type if exists risk_level cascade;
drop type if exists cause_code cascade;
drop type if exists dc_code cascade;

-- ------------------------------------------------------------
-- Enums
-- ------------------------------------------------------------

create type dc_code as enum ('SF', 'NJ', 'LA');

create type risk_level as enum ('LOW', 'MEDIUM', 'HIGH');

create type event_state as enum (
  'DETECTED',
  'ANALYZING',
  'ACTION_PROPOSED',
  'PENDING_APPROVAL',
  'APPROVED',
  'REJECTED',
  'EXECUTED',
  'RESOLVED'
);

create type recommended_action as enum ('TRANSFER', 'WAIT', 'MONITOR');

create type confidence_level as enum ('LOW', 'MEDIUM', 'HIGH');

create type cause_code as enum (
  'CRED-COM', 'CRED-DIS', 'CRED-DMG', 'CRED-FUL', 'CRED-OTH',
  'CRED-PRO', 'CRED-SDT', 'CRED-STO', 'CRED-TRF',
  'CRED01', 'CRED02', 'CRED03', 'CRED04', 'CRED05',
  'CRED06', 'CRED07', 'CRED08', 'CRED09', 'CRED10',
  'CRED10-D', 'CRED11-F', 'CRED11-O', 'CRED12', 'CRED13',
  'CRED14', 'CRED15', 'CRED16', 'CRED17', 'CRED18',
  'CRED19', 'CRED20', 'CRED21', 'CRED99'
);

-- ------------------------------------------------------------
-- Layer 1: Raw ingested data
-- Keep raw ingest permissive. Excel blanks often become NULL.
-- ------------------------------------------------------------

create table inventory_snapshots (
  id              bigserial primary key,
  sku_id          varchar(20)  not null,
  description     varchar(200),
  dc              dc_code      not null,
  available       integer,
  on_hand         integer,
  snapshot_date   date         not null default current_date,
  created_at      timestamptz  not null default now(),
  constraint inventory_snapshots_available_nonnegative
    check (available is null or available >= 0),
  constraint inventory_snapshots_on_hand_nonnegative
    check (on_hand is null or on_hand >= 0),
  unique (sku_id, dc, snapshot_date)
);

create table sales_history (
  id              bigserial primary key,
  source_row_hash varchar(64) not null unique,
  dc              dc_code,
  salesperson_id  varchar(15),
  customer_number varchar(12),
  city            varchar(100),
  state           varchar(100),
  sop_number      varchar(20),
  doc_date        date,
  sku_id          varchar(20),
  item_desc       varchar(200),
  quantity_adj    integer,
  uom             varchar(10),
  qty_base_uom    integer,
  ext_price_adj   numeric(12,2),
  ext_cost_adj    numeric(12,2),
  customer_type   varchar(50),
  product_type    varchar(50),
  gross_profit    numeric(12,2),
  margin_pct      numeric(8,4),
  unit_price_adj  numeric(10,4),
  created_at      timestamptz not null default now()
);

create table po_history (
  id                    bigserial primary key,
  source_row_hash       varchar(64) not null unique,
  po_number             integer,
  po_date               date,
  required_date         date,
  promised_ship_date    date,
  receipt_date          date,
  pop_receipt_number    integer,
  sku_id                varchar(20),
  item_description      varchar(200),
  qty_shipped           integer,
  qty_invoiced          integer,
  unit_cost             numeric(10,4),
  extended_cost         numeric(12,2),
  vendor_id             varchar(20),
  location_code         smallint,
  dc                    dc_code,
  ship_to_address       varchar(50),
  shipping_method       varchar(50),
  is_open               boolean not null default true,
  created_at            timestamptz not null default now(),
  constraint po_history_qty_shipped_nonnegative
    check (qty_shipped is null or qty_shipped >= 0),
  constraint po_history_qty_invoiced_nonnegative
    check (qty_invoiced is null or qty_invoiced >= 0),
  constraint po_history_unit_cost_nonnegative
    check (unit_cost is null or unit_cost >= 0),
  constraint po_history_extended_cost_nonnegative
    check (extended_cost is null or extended_cost >= 0)
);

create table chargebacks (
  id                  bigserial primary key,
  source_row_hash     varchar(64) not null unique,
  location_code       smallint,
  salesperson_id      varchar(15),
  customer_number     varchar(12),
  city                varchar(100),
  state               varchar(100),
  sop_type            varchar(20),
  sop_number          varchar(20),
  customer_po_number  varchar(30),
  doc_date            date,
  cause_code          cause_code,
  cause_code_desc     varchar(100),
  item_description    varchar(200),
  penalty_category    varchar(50),
  extended_price      numeric(12,2),
  created_at          timestamptz not null default now()
);

create table transfer_cost_history (
  id                          bigserial primary key,
  source_row_hash             varchar(64) not null unique,
  journal_entry               integer,
  trx_date                    date,
  account_number              varchar(30),
  account_description         varchar(50),
  dc                          dc_code,
  amount                      numeric(10,2),
  originating_master_name     varchar(100),
  reference                   varchar(100),
  created_at                  timestamptz not null default now()
);

create table penalty_history (
  id                  bigserial primary key,
  source_row_hash     varchar(64) not null unique,
  salesperson_id      varchar(15),
  customer_number     varchar(12),
  customer_name       varchar(100),
  city                varchar(100),
  state               varchar(2),
  sop_number          varchar(20),
  doc_date            date,
  sku_id              varchar(20),
  item_description    varchar(200),
  qty                 numeric(10,2),
  uom                 varchar(10),
  extended_price      numeric(12,2),
  market              varchar(20),
  created_at          timestamptz not null default now()
);

-- ------------------------------------------------------------
-- Layer 2: Derived lookup tables
-- These can stay stricter because they are system-generated.
-- ------------------------------------------------------------

create table transfer_cost_lookup (
  id          bigserial primary key,
  dest_dc     dc_code       not null,
  avg_cost    numeric(10,2),
  min_cost    numeric(10,2),
  max_cost    numeric(10,2),
  sample_size integer,
  updated_at  timestamptz   not null default now(),
  constraint transfer_cost_lookup_avg_cost_nonnegative
    check (avg_cost is null or avg_cost >= 0),
  constraint transfer_cost_lookup_min_cost_nonnegative
    check (min_cost is null or min_cost >= 0),
  constraint transfer_cost_lookup_max_cost_nonnegative
    check (max_cost is null or max_cost >= 0),
  constraint transfer_cost_lookup_sample_size_nonnegative
    check (sample_size is null or sample_size >= 0),
  unique (dest_dc)
);

create table lead_time_lookup (
  id            bigserial primary key,
  dc            dc_code      not null,
  median_days   numeric(5,1),
  avg_days      numeric(5,1),
  sample_size   integer,
  updated_at    timestamptz  not null default now(),
  constraint lead_time_lookup_median_days_nonnegative
    check (median_days is null or median_days >= 0),
  constraint lead_time_lookup_avg_days_nonnegative
    check (avg_days is null or avg_days >= 0),
  constraint lead_time_lookup_sample_size_nonnegative
    check (sample_size is null or sample_size >= 0),
  unique (dc)
);

create table customer_dc_mapping (
  id              bigserial primary key,
  customer_number varchar(12) not null,
  primary_dc      dc_code     not null,
  customer_type   varchar(50),
  order_count     integer,
  updated_at      timestamptz not null default now(),
  constraint customer_dc_mapping_order_count_nonnegative
    check (order_count is null or order_count >= 0),
  unique (customer_number)
);

-- ------------------------------------------------------------
-- Layer 4: Events & orchestrator output
-- ------------------------------------------------------------

create table events (
  id                      bigserial primary key,
  event_key               text             not null unique,
  sku_id                  varchar(20)      not null,
  source_dc               dc_code          not null,
  dest_dc                 dc_code          not null,
  state                   event_state      not null default 'DETECTED',
  days_of_supply          numeric(8,2),
  stockout_date           date,
  transferable_qty        integer,
  network_total           integer,
  relief_arriving         boolean          default false,
  relief_eta              date,
  relief_qty              integer,
  po_at_risk              boolean          default false,
  penalty_risk_level      risk_level,
  penalty_risk_score      numeric(5,4),
  expected_penalty_cost   numeric(10,2),
  recommended_action      recommended_action,
  confidence              confidence_level,
  reasoning               text,
  cost_transfer           numeric(10,2),
  cost_wait               numeric(10,2),
  ai_unavailable          boolean          default false,
  depletion_projection    jsonb,
  created_at              timestamptz      not null default now(),
  updated_at              timestamptz      not null default now(),
  constraint events_source_dest_different
    check (source_dc <> dest_dc),
  constraint events_transferable_qty_nonnegative
    check (transferable_qty is null or transferable_qty >= 0),
  constraint events_network_total_nonnegative
    check (network_total is null or network_total >= 0),
  constraint events_relief_qty_nonnegative
    check (relief_qty is null or relief_qty >= 0),
  constraint events_penalty_risk_score_range
    check (penalty_risk_score is null or (penalty_risk_score >= 0 and penalty_risk_score <= 1)),
  constraint events_expected_penalty_cost_nonnegative
    check (expected_penalty_cost is null or expected_penalty_cost >= 0),
  constraint events_cost_transfer_nonnegative
    check (cost_transfer is null or cost_transfer >= 0),
  constraint events_cost_wait_nonnegative
    check (cost_wait is null or cost_wait >= 0)
);

-- ------------------------------------------------------------
-- Layer 5: Transfer requests & approval flow
-- ------------------------------------------------------------

create table transfer_requests (
  id               bigserial primary key,
  event_id         bigint       not null references events(id) on delete cascade,
  source_dc        dc_code      not null,
  dest_dc          dc_code      not null,
  sku_id           varchar(20)  not null,
  qty              integer      not null,
  estimated_cost   numeric(10,2),
  state            event_state  not null default 'PENDING_APPROVAL',
  rejection_reason text,
  approved_by      varchar(100),
  approved_at      timestamptz,
  created_at       timestamptz  not null default now(),
  updated_at       timestamptz  not null default now(),
  constraint transfer_requests_qty_positive
    check (qty > 0),
  constraint transfer_requests_source_dest_different
    check (source_dc <> dest_dc),
  constraint transfer_requests_estimated_cost_nonnegative
    check (estimated_cost is null or estimated_cost >= 0)
);

-- ------------------------------------------------------------
-- Layer 6: Audit log
-- ------------------------------------------------------------

create table audit_log (
  id            bigserial primary key,
  entity_id     bigint      not null,
  entity_type   varchar(30) not null,
  old_state     event_state,
  new_state     event_state not null,
  actor         varchar(100),
  notes         text,
  created_at    timestamptz not null default now()
);

-- ------------------------------------------------------------
-- Functions
-- ------------------------------------------------------------

create or replace function set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create or replace function set_po_is_open()
returns trigger
language plpgsql
as $$
begin
  new.is_open = (new.receipt_date is null or new.receipt_date > current_date);
  return new;
end;
$$;

-- ------------------------------------------------------------
-- Triggers
-- ------------------------------------------------------------

create trigger trg_events_updated_at
before update on events
for each row
execute function set_updated_at();

create trigger trg_transfers_updated_at
before update on transfer_requests
for each row
execute function set_updated_at();

create trigger trg_po_history_set_is_open
before insert or update of receipt_date
on po_history
for each row
execute function set_po_is_open();

-- ------------------------------------------------------------
-- View
-- ------------------------------------------------------------

create view open_po_history as
select
  id,
  po_number,
  po_date,
  required_date,
  promised_ship_date,
  receipt_date,
  pop_receipt_number,
  sku_id,
  item_description,
  qty_shipped,
  qty_invoiced,
  unit_cost,
  extended_cost,
  vendor_id,
  location_code,
  dc,
  ship_to_address,
  shipping_method,
  is_open,
  created_at,
  (receipt_date is null or receipt_date > current_date) as is_currently_open
from po_history
where receipt_date is null or receipt_date > current_date;

-- ------------------------------------------------------------
-- Indexes
-- ------------------------------------------------------------

create index idx_inventory_sku_dc
  on inventory_snapshots (sku_id, dc);

create index idx_inventory_snapshot_date
  on inventory_snapshots (snapshot_date);

create index idx_sku_dc_features_sku_dc
  on sku_dc_features (sku_id, dc);

create index idx_sku_dc_features_dos
  on sku_dc_features (days_of_supply);

create index idx_sales_sku_dc_date
  on sales_history (sku_id, dc, doc_date);

create index idx_sales_doc_date
  on sales_history (doc_date);

create index idx_sales_customer
  on sales_history (customer_number);

create index idx_po_sku_dc
  on po_history (sku_id, dc);

create index idx_po_receipt_date
  on po_history (receipt_date);

create index idx_po_is_open
  on po_history (is_open);

create index idx_po_sku_dc_receipt
  on po_history (sku_id, dc, receipt_date);

create index idx_chargebacks_cause_code
  on chargebacks (cause_code);

create index idx_chargebacks_customer
  on chargebacks (customer_number);

create index idx_chargebacks_penalty_category
  on chargebacks (penalty_category);

create index idx_events_state
  on events (state);

create index idx_events_days_of_supply
  on events (days_of_supply);

create index idx_events_dest_dc
  on events (dest_dc);

create index idx_events_penalty_cost
  on events (expected_penalty_cost desc);

create index idx_events_sku_source_dest
  on events (sku_id, source_dc, dest_dc);

create index idx_transfers_state
  on transfer_requests (state);

create index idx_transfers_event
  on transfer_requests (event_id);

create index idx_audit_entity
  on audit_log (entity_id, entity_type);

-- ------------------------------------------------------------
-- Seed data
-- ------------------------------------------------------------

insert into transfer_cost_lookup (dest_dc, avg_cost, min_cost, max_cost, sample_size)
values
  ('SF', 2481.72, 149.95, 8070.00, 155),
  ('NJ', 3850.09, 0.00,   6700.00, 203),
  ('LA', 2952.54, 106.50, 8000.00, 109);

insert into lead_time_lookup (dc, median_days, avg_days, sample_size)
values
  ('SF', 65.0, 70.0, null),
  ('NJ', 77.0, 90.2, null),
  ('LA', 35.0, 63.2, null);