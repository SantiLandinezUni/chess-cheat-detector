-- Remove all project schemas and everything in them (used by run_local.sh for a clean rerun).
-- WARNING: this also deletes restricted.user_profiles_raw, the cache of Lichess API responses.
DROP SCHEMA IF EXISTS core, ref, restricted CASCADE;
