-- Frozen version-5 schema: compatibility rejection fixture, not a migration.
CREATE TABLE publicworks_schema (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL) STRICT;
INSERT INTO publicworks_schema VALUES (1, 5);
CREATE TABLE publicworks_metadata (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    next_id INTEGER NOT NULL CHECK(next_id BETWEEN 2 AND 9007199254740992),
    next_seq INTEGER NOT NULL CHECK(next_seq BETWEEN 1 AND 9007199254740992)
) STRICT;
INSERT INTO publicworks_metadata VALUES (1, 2, 1);
CREATE TABLE publicworks_ids (
    id INTEGER PRIMARY KEY CHECK(id BETWEEN 1 AND 9007199254740991),
    kind TEXT NOT NULL CHECK(kind IN ('conversation','entry','task','submission','conversation_state')),
    commit_seq INTEGER NOT NULL CHECK(commit_seq BETWEEN 1 AND 9007199254740991)
) STRICT;
CREATE TABLE publicworks_conversations (
    id INTEGER PRIMARY KEY REFERENCES publicworks_ids(id),
    parent_conversation_id INTEGER, parent_at INTEGER,
    owner_conversation_id INTEGER, owner_task_id INTEGER,
    CHECK((parent_conversation_id IS NULL) = (parent_at IS NULL)),
    CHECK((owner_conversation_id IS NULL) = (owner_task_id IS NULL))
) STRICT;
CREATE TABLE publicworks_entries (
    id INTEGER PRIMARY KEY REFERENCES publicworks_ids(id),
    conversation_id INTEGER NOT NULL, kind TEXT NOT NULL,
    model TEXT, data TEXT, head INTEGER, edits TEXT, by_task_id INTEGER
) STRICT;
CREATE TABLE publicworks_tasks (
    id INTEGER PRIMARY KEY REFERENCES publicworks_ids(id),
    conversation_id INTEGER NOT NULL, kind TEXT NOT NULL,
    version BLOB NOT NULL CHECK(length(version)=8), input TEXT NOT NULL, owner INTEGER,
    background INTEGER NOT NULL CHECK(background IN (0,1)),
    abort_requested INTEGER NOT NULL CHECK(abort_requested IN (0,1)),
    status TEXT NOT NULL CHECK(status IN ('pending','running','waiting','completing','terminal')),
    state TEXT NOT NULL, memos TEXT
) STRICT;
CREATE TABLE publicworks_submissions (
    id INTEGER PRIMARY KEY REFERENCES publicworks_ids(id),
    conversation_id INTEGER NOT NULL, request_id TEXT,
    submission_type TEXT NOT NULL CHECK(submission_type IN ('input','write')),
    status TEXT NOT NULL CHECK(status IN ('queued','placed','done','unanswered')),
    entry INTEGER, answer INTEGER, reason TEXT, detail TEXT
) STRICT;
CREATE TABLE publicworks_conversation_states (
    id INTEGER PRIMARY KEY REFERENCES publicworks_ids(id),
    conversation_id INTEGER NOT NULL, run TEXT, inbox TEXT NOT NULL, agent_config TEXT
) STRICT;
CREATE INDEX conversations_owner ON publicworks_conversations(owner_conversation_id,owner_task_id,id);
CREATE INDEX conversations_owner_conversation ON publicworks_conversations(owner_conversation_id,id);
CREATE INDEX conversations_owner_task ON publicworks_conversations(owner_task_id,id);
CREATE INDEX entries_conversation ON publicworks_entries(conversation_id,id);
CREATE INDEX entries_head ON publicworks_entries(conversation_id,id) WHERE head IS NOT NULL;
CREATE INDEX tasks_conversation ON publicworks_tasks(conversation_id,id);
CREATE INDEX tasks_owner ON publicworks_tasks(owner,id);
CREATE INDEX tasks_kind ON publicworks_tasks(kind,id);
CREATE INDEX tasks_status ON publicworks_tasks(status,id);
CREATE INDEX tasks_abort ON publicworks_tasks(abort_requested,id);
CREATE INDEX tasks_background ON publicworks_tasks(background,id);
CREATE INDEX tasks_conversation_status ON publicworks_tasks(conversation_id,status,id);
CREATE INDEX submissions_conversation ON publicworks_submissions(conversation_id,id);
CREATE INDEX submissions_status ON publicworks_submissions(status,id);
CREATE INDEX submissions_conversation_status ON publicworks_submissions(conversation_id,status,id);
CREATE INDEX submissions_request ON publicworks_submissions(conversation_id,request_id,id);
CREATE INDEX states_conversation ON publicworks_conversation_states(conversation_id,id);
