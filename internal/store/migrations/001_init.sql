CREATE TYPE op_state AS ENUM (
  'claimed', 'dispatching', 'awaiting_input', 'completed',
  'not_executed', 'unknown', 'released'
);

CREATE TABLE tool_operations (
  op_key         text PRIMARY KEY,          -- key_base plus generation
  key_base       text NOT NULL,             -- covers principal, upstream and tool
  generation     int NOT NULL,              -- only keyless calls move past 0
  key_source     text NOT NULL,
  principal      text NOT NULL,
  upstream       text NOT NULL,
  tool           text NOT NULL,
  policy         text NOT NULL,
  args_hash      bytea NOT NULL,
  state          op_state NOT NULL,
  owner          text,
  fence          bigint NOT NULL DEFAULT 1,
  lease_until    timestamptz,
  attempts       int NOT NULL DEFAULT 0,    -- times the dispatch gate was passed
  result_kind    text,                      -- result, error or input_required
  result         bytea,                     -- removed after the retention period
  result_hash    bytea,
  result_purged  boolean NOT NULL DEFAULT false,
  last_instance  text,                      -- who made the latest change
  last_detail    jsonb,                     -- why, copied into operation_events
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  dispatched_at  timestamptz,
  finished_at    timestamptz,
  UNIQUE (key_base, generation)
);

CREATE INDEX tool_operations_leases ON tool_operations (lease_until)
  WHERE state IN ('claimed', 'dispatching');
CREATE INDEX tool_operations_retention ON tool_operations (finished_at)
  WHERE result IS NOT NULL;
CREATE INDEX tool_operations_state ON tool_operations (state, updated_at);

CREATE TABLE operation_events (           -- append-only audit trail
  id          bigserial PRIMARY KEY,
  op_key      text NOT NULL REFERENCES tool_operations (op_key),
  at          timestamptz NOT NULL DEFAULT now(),
  instance    text,
  from_state  op_state,
  to_state    op_state NOT NULL,
  fence       bigint NOT NULL,
  detail      jsonb
);

CREATE INDEX operation_events_op ON operation_events (op_key, id);

-- Every change of state or fence is recorded by the same statement that
-- makes it, so the trail can never disagree with the operation.
CREATE FUNCTION record_operation_event() RETURNS trigger AS $$
BEGIN
  INSERT INTO operation_events (op_key, instance, from_state, to_state, fence, detail)
  VALUES (
    NEW.op_key, NEW.last_instance,
    CASE WHEN TG_OP = 'UPDATE' THEN OLD.state END,
    NEW.state, NEW.fence, NEW.last_detail
  );
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER operation_inserted AFTER INSERT ON tool_operations
  FOR EACH ROW EXECUTE FUNCTION record_operation_event();

CREATE TRIGGER operation_changed AFTER UPDATE ON tool_operations
  FOR EACH ROW
  WHEN (OLD.state IS DISTINCT FROM NEW.state OR OLD.fence IS DISTINCT FROM NEW.fence)
  EXECUTE FUNCTION record_operation_event();
