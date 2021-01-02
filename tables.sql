DROP TABLE cids;
DROP TABLE refs;
DROP TABLE blocks;
DROP TABLE atime;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS cids (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cid BLOB UNIQUE
);

CREATE TABLE IF NOT EXISTS refs (
    parent_id INTEGER,
    child_id INTEGER,
    UNIQUE(parent_id,child_id)
    CONSTRAINT fk_parent_id
      FOREIGN KEY (parent_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
    CONSTRAINT fk_child_id
      FOREIGN KEY (child_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
);

CREATE INDEX idx_refs_parent_id
ON refs (parent_id);

CREATE INDEX idx_refs_child_id
ON refs (child_id);

CREATE TABLE IF NOT EXISTS blocks (
    block_id INTEGER PRIMARY_KEY,
    block BLOB UNIQUE,
);

CREATE TABLE IF NOT EXISTS atime (
    atime INTEGER PRIMARY KEY AUTOINCREMENT,
    block_id INTEGER UNIQUE,
    CONSTRAINT fk_block_id
      FOREIGN KEY (block_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS aliases (
    name blob UNIQUE,
    block_id INTEGER,
    CONSTRAINT fk_block_id
      FOREIGN KEY (block_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
);

BEGIN TRANSACTION;
-- note that we would have to use INSERT OR IGNORE here in a real database
INSERT INTO cids (cid) VALUES ("cid_a");
INSERT INTO blocks (block_id, block) VALUES (last_insert_rowid(), "value_a");
COMMIT;

BEGIN TRANSACTION;
-- note that we would have to use INSERT OR IGNORE here in a real database
INSERT INTO cids (cid) VALUES ("cid_b");
INSERT INTO blocks (block_id, block) VALUES (last_insert_rowid(), "value_b");
COMMIT;

BEGIN TRANSACTION;
-- note that we would have to use INSERT OR IGNORE here in a real database
INSERT INTO cids (cid) VALUES ("cid_c");
INSERT INTO blocks (block_id, block) VALUES (last_insert_rowid(), "value_c");
COMMIT;

BEGIN TRANSACTION;
-- note that we would have to use INSERT OR IGNORE here in a real database
INSERT INTO cids (cid) VALUES ("cid_d");
INSERT INTO blocks (block_id, block) VALUES (last_insert_rowid(), "value_d");
COMMIT;

INSERT INTO atime (block_id) VALUES (1);
INSERT INTO atime (block_id) VALUES (2);
INSERT INTO atime (block_id) VALUES (3);
INSERT INTO atime (block_id) VALUES (4);

-- a is parent of b and c
INSERT INTO refs (parent_id, child_id) VALUES (1,2);
INSERT INTO refs (parent_id, child_id) VALUES (1,3);

-- d is parent of b and c
INSERT INTO refs (parent_id, child_id) VALUES (4,2);
INSERT INTO refs (parent_id, child_id) VALUES (4,3);

SELECT
  (SELECT COUNT(parent_id) FROM refs WHERE child_id = 1) +
  (SELECT COUNT(name) FROM aliases WHERE block_id = 1);

DELETE FROM
  cids
WHERE
  (NOT EXISTS(SELECT 1 FROM refs WHERE child_id = id)) AND
  (NOT EXISTS(SELECT 1 FROM aliases WHERE block_id = id));

DELETE FROM
  cids
WHERE
  (NOT EXISTS(SELECT 1 FROM refs WHERE child_id = id));

INSERT INTO aliases (name, block_id) VALUES ("alias2", 4);
INSERT INTO aliases (name, block_id) VALUES ("alias1", 1);

WITH RECURSIVE
  ancestor_of(child_id) AS
    (SELECT parent_id FROM refs WHERE child_id=1
     UNION ALL
     SELECT parent_id FROM refs JOIN ancestor_of USING(child_id))
SELECT DISTINCT refs.parent_id FROM ancestor_of, refs;

WITH RECURSIVE
    ancestor_of(id) AS
    (
        SELECT parent_id FROM refs WHERE child_id=2
        UNION ALL
        SELECT DISTINCT parent_id FROM refs JOIN ancestor_of WHERE ancestor_of.id=refs.child_id
    )
SELECT id FROM ancestor_of;

WITH RECURSIVE
    descendant_of(id) AS
    (
        -- non recursive part - simply look up the immediate children
        SELECT child_id FROM refs WHERE parent_id=11121
        UNION ALL
        -- recursive part - look up parents of all returned ids
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
    )
SELECT id FROM descendant_of;

WITH RECURSIVE
    descendant_of(id) AS
    (
        -- non recursive part - simply look up the immediate children
        SELECT block_id FROM aliases
        UNION ALL
        -- recursive part - look up parents of all returned ids
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
    )
SELECT id FROM descendant_of;

WITH RECURSIVE
    descendant_of(id) AS
    (
        -- non recursive part - simply look up the immediate children
        SELECT block_id FROM aliases
        UNION ALL
        -- recursive part - look up parents of all returned ids
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
    )
DELETE FROM cids WHERE id NOT IN (SELECT id from descendant_of) LIMIT 1000;

WITH RECURSIVE
    descendant_of(id) AS
    (
        -- non recursive part - simply look up the immediate children
        SELECT child_id FROM refs WHERE parent_id=1121101
        UNION ALL
        -- recursive part - look up parents of all returned ids
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
    )
SELECT id FROM descendant_of;

PRAGMA foreign_keys = ON;

SELECT * FROM
    cids
WHERE
    (NOT EXISTS(SELECT 1 FROM refs WHERE child_id = id)) AND
    (NOT EXISTS(SELECT 1 FROM aliases WHERE block_id = id));

DELETE FROM
    cids
WHERE
    (NOT EXISTS(SELECT 1 FROM refs WHERE child_id = id)) AND
    (NOT EXISTS(SELECT 1 FROM aliases WHERE block_id = id));
LIMIT 1;

WITH RECURSIVE
    descendant_of(id) AS (
        -- non recursive part - simply look up the immediate children
        SELECT 1099989
        UNION ALL
        -- recursive part - look up parents of all returned ids
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
    ),
    orphaned_ids as (
      SELECT id FROM descendant_of LEFT JOIN blocks ON descendant_of.id = blocks.block_id WHERE blocks.block_id IS NULL
    )
SELECT cid from cids,orphaned_ids WHERE cids.id = orphaned_ids.id;

WITH RECURSIVE
    descendant_of(id) AS
    (
        -- non recursive part - simply look up the immediate children
        SELECT block_id FROM aliases
        UNION ALL
        -- recursive part - look up parents of all returned ids
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
    )
SELECT id FROM
    cids
WHERE
    id NOT IN (SELECT id FROM descendant_of) AND
    (SELECT atime FROM atime WHERE atime.block_id = id) < 1000000;


EXPLAIN QUERY PLAN WITH RECURSIVE
    -- find descendants of cid, including the id of the cid itself
    descendant_of(id) AS (
        SELECT 11111
        UNION ALL
        SELECT DISTINCT child_id FROM refs JOIN descendant_of ON descendant_of.id=refs.parent_id
    ),
    -- find orphaned ids
    orphaned_ids as (
      SELECT DISTINCT id FROM descendant_of LEFT JOIN blocks ON descendant_of.id = blocks.block_id WHERE blocks.block_id IS NULL
    )
    -- retrieve corresponding cids - this is a set because of select distinct
SELECT cid from cids JOIN orphaned_ids ON cids.id = orphaned_ids.id;

EXPLAIN QUERY PLAN WITH RECURSIVE
    descendant_of(id) AS
    (
        SELECT 11111
        UNION ALL
        SELECT DISTINCT child_id FROM refs JOIN descendant_of ON descendant_of.id=refs.parent_id
    ),
    descendant_ids as (
        SELECT DISTINCT id FROM descendant_of
    )
    SELECT * FROM descendant_ids;