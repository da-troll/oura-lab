-- Migration 004: Chat history tables
-- Adds chat conversations and messages with RLS

BEGIN;

-- Preflight: skip if already applied
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name = 'chat_conversations') THEN
    RAISE EXCEPTION 'Migration 004 already applied';
  END IF;
END $$;

-- ==========================================
-- Chat Conversations
-- ==========================================

CREATE TABLE chat_conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Composite unique for FK from messages
    UNIQUE (id, user_id)
);

CREATE INDEX idx_chat_conversations_user_updated
    ON chat_conversations (user_id, updated_at DESC);

-- RLS
ALTER TABLE chat_conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE chat_conversations FORCE ROW LEVEL SECURITY;

CREATE POLICY user_isolation_select ON chat_conversations FOR SELECT
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_insert ON chat_conversations FOR INSERT
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_update ON chat_conversations FOR UPDATE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid)
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_delete ON chat_conversations FOR DELETE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);

-- ==========================================
-- Chat Messages
-- ==========================================

CREATE TABLE chat_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    conversation_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
    content TEXT NOT NULL DEFAULT '',
    tool_calls JSONB,
    model TEXT,
    tokens_in INT,
    tokens_out INT,
    latency_ms INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Single FK referencing composite unique on conversations
    FOREIGN KEY (conversation_id, user_id)
        REFERENCES chat_conversations(id, user_id) ON DELETE CASCADE
);

CREATE INDEX idx_chat_messages_conv_created
    ON chat_messages (conversation_id, created_at);

-- RLS
ALTER TABLE chat_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE chat_messages FORCE ROW LEVEL SECURITY;

CREATE POLICY user_isolation_select ON chat_messages FOR SELECT
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_insert ON chat_messages FOR INSERT
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_update ON chat_messages FOR UPDATE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid)
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_delete ON chat_messages FOR DELETE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);

-- ==========================================
-- Triggers
-- ==========================================

CREATE TRIGGER set_updated_at_chat_conversations
    BEFORE UPDATE ON chat_conversations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- ==========================================
-- Grants (for app_user role if it exists)
-- ==========================================

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_user') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON chat_conversations TO app_user;
    GRANT SELECT, INSERT, UPDATE, DELETE ON chat_messages TO app_user;
  END IF;
END $$;

COMMIT;
