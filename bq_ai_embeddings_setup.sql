-- ================================================================
-- SanctiSight — See Risk Before It Strikes
-- Google Cloud x Kaggle BigQuery AI Hackathon 2025
--
-- Objective:
--   Detect relationships between global news entities (GDELT)
--   and sanctioned entities (OFAC/OpenSanctions) using
--   semantic embeddings + vector search in BigQuery ML.
--
-- Stack:
--   • BigQuery + Vertex AI (text-multilingual-embedding-002)
--   • ML.GENERATE_EMBEDDING + VECTOR_SEARCH + CREATE VECTOR INDEX
-- ================================================================

-- ================================================================
-- 0. COMPACT CORPUS
-- ================================================================
CREATE OR REPLACE TABLE `kagglecov1.Sanctions1.news_corpus`
PARTITION BY d
CLUSTER BY url, source_name
OPTIONS (require_partition_filter = TRUE) AS
SELECT
  GKGRECORDID, doc_id, d, dt, source_name, tone,
  persons, orgs, locations, themes, text_stub, avg_tone, url
FROM `kagglecov1.Sanctions1.v_gkg_with_text`
WHERE d >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);

-- ================================================================
-- 1. ENTITY ROWS VIEW
-- ================================================================
CREATE OR REPLACE VIEW `kagglecov1.Sanctions1.v_gkg_entities_rows` AS
WITH src AS (
  SELECT * FROM `kagglecov1.Sanctions1.news_corpus`
),
pp AS (
  SELECT GKGRECORDID, d, url, source_name, 'person' AS entity_type,
         REGEXP_EXTRACT(p, r'^(.*?)(?:,|$)') AS entity_raw
  FROM src, UNNEST(SPLIT(persons, ';')) AS p
),
oo AS (
  SELECT GKGRECORDID, d, url, source_name, 'org' AS entity_type,
         REGEXP_EXTRACT(o, r'^(.*?)(?:,|$)') AS entity_raw
  FROM src, UNNEST(SPLIT(orgs, ';')) AS o
)
SELECT
  GKGRECORDID, d, url, source_name, entity_type, entity_raw,
  REGEXP_REPLACE(LOWER(TRIM(REGEXP_REPLACE(entity_raw, r'\s+', ' '))),
                 r'[^a-z0-9 ]', '') AS entity_norm
FROM (SELECT * FROM pp UNION ALL SELECT * FROM oo)
WHERE entity_raw IS NOT NULL
  AND TRIM(entity_raw) != ''
  AND ARRAY_LENGTH(SPLIT(entity_norm, ' ')) >= 2
  AND (SELECT MIN(LENGTH(t)) FROM UNNEST(SPLIT(entity_norm, ' ')) t) >= 4;

-- ================================================================
-- 2. GLOBAL ENTITY LEXICON
-- ================================================================
CREATE TABLE IF NOT EXISTS `kagglecov1.Sanctions1.entity_lexicon` (entity_norm STRING);

INSERT INTO `kagglecov1.Sanctions1.entity_lexicon` (entity_norm)
SELECT DISTINCT entity_norm
FROM `kagglecov1.Sanctions1.v_gkg_entities_rows`
WHERE entity_norm IS NOT NULL
  AND LENGTH(entity_norm) >= 4
  AND entity_norm NOT IN (SELECT entity_norm FROM `kagglecov1.Sanctions1.entity_lexicon`);

-- ================================================================
-- 3. SANCTIONS DICTIONARY
-- ================================================================
CREATE OR REPLACE VIEW `kagglecov1.Sanctions1.v_sanction_names` AS
WITH base AS (
  SELECT id, schema, name, aliases, program_ids, dataset
  FROM `kagglecov1.Sanctions1.sanctions_table`
),
ali AS (
  SELECT id, schema, NULLIF(TRIM(x), '') AS alias, program_ids, dataset
  FROM base, UNNEST(SPLIT(REGEXP_REPLACE(COALESCE(aliases, ''), r'[|]', ';'), ';')) AS x
)
SELECT id, schema, program_ids, dataset,
       name AS original_name,
       REGEXP_REPLACE(LOWER(TRIM(REGEXP_REPLACE(name, r'\s+', ' '))),
                      r'[^a-z0-9 ]', '') AS norm_name
FROM base
UNION ALL
SELECT id, schema, program_ids, dataset,
       alias AS original_name,
       REGEXP_REPLACE(LOWER(TRIM(REGEXP_REPLACE(alias, r'\s+', ' '))),
                      r'[^a-z0-9 ]', '') AS norm_name
FROM ali
WHERE alias IS NOT NULL;

-- ================================================================
-- 4. EMBEDDING MODEL
-- ================================================================
CREATE OR REPLACE MODEL `kagglecov1.Sanctions1.text_embedding`
REMOTE WITH CONNECTION `kagglecov1.us.vertex_ai_conn`
OPTIONS (ENDPOINT = 'text-multilingual-embedding-002');

-- ================================================================
-- 5. SANCTION EMBEDDINGS
-- ================================================================
CREATE OR REPLACE TABLE `kagglecov1.Sanctions1._sanc_to_embed_stage` AS
SELECT DISTINCT norm_name AS content
FROM `kagglecov1.Sanctions1.v_sanction_names`;

CREATE OR REPLACE TABLE `kagglecov1.Sanctions1.sanction_embeddings` AS
WITH tvf AS (
  SELECT content, ml_generate_embedding_result
  FROM ML.GENERATE_EMBEDDING(
    MODEL `kagglecov1.Sanctions1.text_embedding`,
    TABLE `kagglecov1.Sanctions1._sanc_to_embed_stage`
  )
)
SELECT
  n.id AS doc_id,
  n.schema,
  n.program_ids,
  n.dataset,
  n.original_name,
  n.norm_name,
  t.ml_generate_embedding_result AS embedding
FROM `kagglecov1.Sanctions1.v_sanction_names` n
JOIN tvf AS t
  ON n.norm_name = t.content;

CREATE OR REPLACE VECTOR INDEX `kagglecov1.Sanctions1.sanction_vec_index`
ON `kagglecov1.Sanctions1.sanction_embeddings` (embedding)
OPTIONS (index_type = 'IVF', distance_type = 'COSINE', num_lists = 100);

-- ================================================================
-- 6. ENTITY EMBEDDINGS (SHARDED)
-- ================================================================
CREATE TABLE IF NOT EXISTS `kagglecov1.Sanctions1.entity_embeddings` (
  doc_id STRING,
  entity_norm STRING,
  embedding ARRAY<FLOAT64>
);

BEGIN
  DECLARE SHARDS INT64 DEFAULT 40;
  DECLARE PER_SHARD_LIMIT INT64 DEFAULT 20000;
  DECLARE s INT64 DEFAULT 0;

  LOOP
    IF s >= SHARDS THEN LEAVE; END IF;

    CREATE OR REPLACE TEMP TABLE _ent_shard AS
    SELECT content
    FROM (
      SELECT content, ROW_NUMBER() OVER () AS rn
      FROM `kagglecov1.Sanctions1.entity_lexicon`
      WHERE MOD(ABS(FARM_FINGERPRINT(content)), SHARDS) = s
    )
    WHERE rn <= PER_SHARD_LIMIT;

    IF (SELECT COUNT(1) FROM _ent_shard) = 0 THEN
      SET s = s + 1;
      ITERATE;
    END IF;

    INSERT INTO `kagglecov1.Sanctions1.entity_embeddings`
    WITH tvf AS (
      SELECT content, ml_generate_embedding_result
      FROM ML.GENERATE_EMBEDDING(
        MODEL `kagglecov1.Sanctions1.text_embedding`,
        (SELECT * FROM _ent_shard)
      )
    )
    SELECT content, content, ml_generate_embedding_result FROM tvf;

    SET s = s + 1;
  END LOOP;
END;

-- ================================================================
-- 7. VECTOR SEARCH MATCHES
-- ================================================================
CREATE OR REPLACE VIEW `kagglecov1.Sanctions1.v_matches_vector` AS
WITH queries AS (
  SELECT entity_norm AS query_id, embedding AS query_embedding
  FROM `kagglecov1.Sanctions1.entity_embeddings`
  WHERE ARRAY_LENGTH(embedding) > 0
)
SELECT
  vs.query.query_id AS entity_norm,
  vs.base.id AS sanction_id,
  vs.base.original_name AS sanction_name,
  vs.base.schema,
  vs.base.program_ids,
  vs.distance
FROM VECTOR_SEARCH(
  TABLE `kagglecov1.Sanctions1.sanction_embeddings`,
  'embedding',
  TABLE queries,
  'query_embedding',
  top_k => 3,
  distance_type => 'COSINE',
  options => '{"fraction_lists_to_search": 0.1}'
) AS vs
WHERE vs.distance <= 0.22;

-- ================================================================
-- 8–13. INSIGHT VIEWS
-- ================================================================
-- 8) Exact matches (normalized equality)
CREATE OR REPLACE VIEW `kagglecov1.Sanctions1.v_matches_exact` AS
SELECT DISTINCT
  e.entity_norm,
  s.id AS sanction_id,
  s.original_name AS sanction_name,
  s.schema,
  s.program_ids,
  0.0 AS distance
FROM `kagglecov1.Sanctions1.entity_lexicon` e
JOIN `kagglecov1.Sanctions1.v_sanction_names` s
  ON e.entity_norm = s.norm_name;


-- 9) Map matches back to articles (exact wins)
CREATE OR REPLACE VIEW `kagglecov1.Sanctions1.v_gkg_sanctions_hits` AS
WITH m AS (
  SELECT
    'exact' AS match_type,
    *
  FROM `kagglecov1.Sanctions1.v_matches_exact`
  
  UNION ALL
  
  SELECT
    'vector' AS match_type,
    *
  FROM `kagglecov1.Sanctions1.v_matches_vector`
),

mdedup AS (
  SELECT
    * EXCEPT(rn)
  FROM (
    SELECT
      m.*,
      ROW_NUMBER() OVER (
        PARTITION BY entity_norm, sanction_id
        ORDER BY
          CASE WHEN match_type = 'exact' THEN 0 ELSE 1 END,
          distance
      ) AS rn
    FROM m
  )
  WHERE rn = 1
)

SELECT
  r.GKGRECORDID,
  r.d,
  r.url,
  r.source_name,
  r.entity_type,
  r.entity_raw,
  r.entity_norm,
  mdedup.sanction_id,
  mdedup.sanction_name,
  mdedup.schema,
  mdedup.program_ids,
  mdedup.match_type,
  mdedup.distance
FROM `kagglecov1.Sanctions1.v_gkg_entities_rows` r
JOIN mdedup
  USING (entity_norm);


-- 10) KPIs (leaderboard & daily series)
CREATE OR REPLACE VIEW `kagglecov1.Sanctions1.v_sanction_mentions_top` AS
SELECT
  sanction_id,
  ANY_VALUE(sanction_name) AS sanction_name,
  ANY_VALUE(schema) AS schema,
  ANY_VALUE(program_ids) AS program_ids,
  COUNT(*) AS mentions,
  MIN(d) AS first_seen,
  MAX(d) AS last_seen
FROM `kagglecov1.Sanctions1.v_gkg_sanctions_hits`
GROUP BY sanction_id
ORDER BY mentions DESC
LIMIT 200;


CREATE OR REPLACE VIEW `kagglecov1.Sanctions1.v_sanction_mentions_daily` AS
SELECT
  d,
  COUNT(*) AS hits
FROM `kagglecov1.Sanctions1.v_gkg_sanctions_hits`
GROUP BY d
ORDER BY d;


-- 11) QA — Distance histogram (tune cutoff)
WITH bins AS (
  SELECT *
  FROM UNNEST([
    STRUCT(0.00 AS lo, 0.05 AS hi, '[0.00,0.05)' AS label),
    STRUCT(0.05 AS lo, 0.10 AS hi, '[0.05,0.10)' AS label),
    STRUCT(0.10 AS lo, 0.15 AS hi, '[0.10,0.15)' AS label),
    STRUCT(0.15 AS lo, 0.20 AS hi, '[0.15,0.20)' AS label),
    STRUCT(0.20 AS lo, 0.22 AS hi, '[0.20,0.22)' AS label),
    STRUCT(0.22 AS lo, 0.30 AS hi, '[0.22,0.30)' AS label),
    STRUCT(0.30 AS lo, 9.99 AS hi, '[0.30,+∞)' AS label)
  ])
)

SELECT
  b.label,
  COUNT(v.sanction_id) AS matches
FROM bins b
LEFT JOIN `kagglecov1.Sanctions1.v_matches_vector` v
  ON v.distance >= b.lo
  AND v.distance < b.hi
GROUP BY b.label, b.lo
ORDER BY b.lo;


-- 12) QA — Top-K preview with context (sample URLs)
WITH best AS (
  SELECT
    entity_norm,
    sanction_id,
    sanction_name,
    schema,
    program_ids,
    distance
  FROM `kagglecov1.Sanctions1.v_matches_vector`
  ORDER BY distance ASC
  LIMIT 20
),

ctx AS (
  SELECT
    b.*,
    ARRAY_AGG(
      STRUCT(d, source_name, url)
      ORDER BY d DESC
      LIMIT 3
    ) AS sample_articles
  FROM best b
  JOIN `kagglecov1.Sanctions1.v_gkg_entities_rows` r
    ON r.entity_norm = b.entity_norm
  GROUP BY
    entity_norm,
    sanction_id,
    sanction_name,
    schema,
    program_ids,
    distance
)

SELECT *
FROM ctx
ORDER BY distance ASC;


-- 13) Sanity counts
SELECT
  'matches_rows' AS metric,
  COUNT(*) AS val
FROM `kagglecov1.Sanctions1.v_gkg_sanctions_hits`

UNION ALL

SELECT
  'top_entities_rows' AS metric,
  COUNT(*) AS val
FROM `kagglecov1.Sanctions1.v_sanction_mentions_top`

UNION ALL

SELECT
  'daily_rows' AS metric,
  COUNT(*) AS val
FROM `kagglecov1.Sanctions1.v_sanction_mentions_daily`;
