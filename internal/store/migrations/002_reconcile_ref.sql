-- The value of a reconcilable tool's match argument, kept so the gateway can
-- check the outcome of an unknown operation without waiting for a retry.
ALTER TABLE tool_operations ADD COLUMN reconcile_ref jsonb;

CREATE INDEX tool_operations_reconcile ON tool_operations (updated_at)
  WHERE state = 'unknown' AND reconcile_ref IS NOT NULL;
