# NIA query pipeline

NIA uses the cheapest reliable execution path that can answer each question.

## Execution order

1. **Versioned final-answer cache**
   - Keyed by file ID, file version, normalized question, and community scope.
   - Holds up to 512 answers.
   - Reuses successful LLM-written answers without repeating retrieval or generation.
   - Audio answers are not cached.
2. **Database query planner**
   - Classifies high-confidence factual and aggregate questions without an LLM.
   - Resolves community and school names against canonical normalized values.
   - Executes parameterized PostgreSQL queries against `file_data_normalized`.
3. **In-memory deterministic router**
   - Handles supported canonical, date/death, and record-level cases that are not suitable for the SQL planner.
4. **Constrained answer generation**
   - Sends only the user's question and the compact, verified query result to Gemini Flash.
   - The model writes the final natural-language answer but does not filter, count, compare, or select facts.
   - Complete-list prompts require every returned value to be preserved without truncation.
   - The verified result remains available as a fail-safe if generation is temporarily unavailable.
5. **Structured retrieval**
   - Selects and projects only relevant normalized rows.
6. **Full Gemini generation**
   - Uses Gemini Flash first for interactive latency and cost.
   - Gemini Pro remains the rate-limit fallback.

## Database-first question types

- Total or filtered record counts.
- Existence checks.
- Lists and counts of distinct communities or schools.
- Grouped community or school breakdowns.
- Highest, lowest, top, bottom, most, or fewest groups.
- Group extrema with matching names.
- Filtered or whole-data record/name lists.
- Dataset overviews with top community and school groups.
- Available field/column catalogs from `data_config`.
- Distinct values for arbitrary fields declared in `data_config`.

Text filters are matched against actual canonical community and school values, not interpolated into SQL. Arbitrary configured field names are loaded from `data_config` and passed to parameterized PostgreSQL JSONB functions.

Questions involving narrative interpretation, unsupported filters, ambiguous entities, audio, or details that cannot be proven from a deterministic query continue through structured retrieval and the model.

This separation keeps factual work deterministic while ensuring the user-facing answer is written by the LLM. Routed prompts contain only the compact query result rather than the whole dataset, keeping token use and latency low.

## Diagnostics

The existing chat debug object identifies the selected path:

- `strategy`: `fast_answer_cache`, `database_router`, `deterministic_router`, or structured retrieval.
- `execution_mode`: `cache`, `database_llm`, `deterministic_llm`, or `llm`. A `_fallback` suffix indicates that the verified answer was returned during a model failure.
- `query_type`: the planned operation, such as `count`, `record_list`, or `dataset_overview`.
- `retrieval_mode`: the specific executor used.
- `preparation_ms`, `generation_ms`, and `total_ms`: latency by stage.
- `prompt_bytes`: the compact answer-rendering prompt size for routed answers.

## PostgreSQL indexes

Fresh databases receive the query indexes from `db-init/init.sql`.

For an existing database, apply [scripts/nia_performance_indexes.sql](../scripts/nia_performance_indexes.sql) once with `psql`. Its `CREATE INDEX CONCURRENTLY` statements must run outside an explicit transaction.
