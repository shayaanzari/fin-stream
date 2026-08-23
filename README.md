# transact

`transact` is a Python and SQL data pipeline that turns unstructured bank transaction alerts into structured, queryable data.

I built it to deepen skills in stream processing, SQL data modeling, Python orchestration, and integrating LLMs into applications.

## Tech stack

Python · Polars · Pydantic · PostgreSQL · SQL · Apache Arrow ADBC · OpenAI · LiteLLM · Prefect · Bento · Redpanda/Kafka · Grafana · Docker Compose

## What it does

```text
Bank alerts (raw text)
      |
      v
Bento parses, normalizes, and deduplicates
      |
      v
PostgreSQL stores bronze records
      |
      v
Prefect batches unclassified records
      |
      v
LLM enrichment (classifies vendors)
      |
      v
Pydantic validates; ADBC writes silver records
      |
      v
Grafana panels query the results via SQL
```

Bento accepts bank notification text in several formats, extracts fields such as vendor, amount, timestamp, and transaction type, and stores the parsed records in PostgreSQL. Prefect orchestrates a Python worker that fetches unclassified records in batches, sends them to an LLM for vendor classification, and writes the validated predictions and confidence scores back to PostgreSQL.

## Components

### Bento: parsing and ingestion

Bento consumes bank alerts from Redpanda/Kafka and performs the stream-side work:

- Filters irrelevant notifications (such as balance reviews).
- Normalizes alert text and extracts transaction fields.
- Matches bank-specific formats with parsing rules.
- Inserts valid records into PostgreSQL.
- Routes unparseable messages and persistent ingestion failures to a dead-letter table.

The resulting bronze layer is parsed and normalized rather than an immutable copy of every source event. It retains the original alert text, makes transaction fields queryable, and deduplicates at ingestion.

### PostgreSQL: storage and SQL analytics

PostgreSQL stores the bronze and silver schemas and provides the analytical query layer. I use:

- Keys, indexes, uniqueness constraints, and idempotent inserts to prevent duplicate transactions.
- `NOT EXISTS` to identify records awaiting enrichment.
- Aggregations for daily spending, category totals, and recent transactions.

### Python, Polars, and Prefect: orchestration and tabular I/O

A Python script coordinates the post-ingestion workflow around the LLM classifier:

- Fetches unclassified records from PostgreSQL in micro-batches.
- Uses Polars for typed tabular data handling.
- Sends transaction batches to the LLM application and merges predictions back into the source records.
- Reads from and writes to PostgreSQL using Apache Arrow ADBC.
- Orchestrates retries, fallback behavior, and database writes through Prefect.

### LLM classification: structured application integration

The enrichment worker calls a model on the LiteLLM proxy through the OpenAI API. The proxy forwards requests to the configured language model, which classifies vendors into a constrained spending taxonomy. The application:

- Classifies transactions in batches rather than making one request per record by default.
- Requires structured model output with Pydantic validation.
- Persists the category, confidence score, model identifier, and classification timestamp.
- Falls back to individual requests when a batch cannot be parsed, isolating malformed or problematic records.
- Routes low-confidence results and parsing failures to dead-letter tables for review instead of silently treating them as correct.

### Grafana: query-driven outputs

The Grafana dashboard queries the enriched silver table through PostgreSQL. It displays daily spending, category totals, and recent transactions. 
*These are query-time analytical outputs rather than a separate persisted Gold layer.*

## Design decisions

The project uses a bronze/silver data model so that raw parsed data remains traceable while enriched data is easy to query. The silver table carries the bronze fields forward and appends classification metadata, allowing downstream analytical queries to use one primary table.

LLM output is treated as data that requires validation, not as an unquestioned answer. Every accepted classification includes its category, confidence score, model identifier, and classification timestamp. Results that fail validation or do not meet the confidence threshold are retained separately for inspection.

## Repository structure

```text
silver/
├── models.py          # Typed classification schemas and category taxonomy
└── silver_flow.py     # Fetch, classify, validate, and write workflow
scripts/               # PostgreSQL DDL and API/structured-output checks
config.yaml            # Bento parsing and ingestion pipeline
dashboard.json         # Grafana dashboard definition
docker-compose.yml     # PostgreSQL and Bento services
```

## Running locally

The local stack uses Docker Compose for PostgreSQL and the pipeline services. It also expects configuration for the PostgreSQL connection and the LiteLLM proxy; see the environment references in `docker-compose.yml` and `silver/silver_flow.py`.

```bash
docker compose up --build
```

The database schemas are initialized from the SQL files in `scripts/`. The dashboard definition is available in `transactions_dashboard.json` for use with Grafana or another PostgreSQL-compatible visualization setup.

## Limitations and next steps

This is a portfolio-scale project using transaction notification text rather than a direct bank integration. The LLM classifier is probabilistic, so low-confidence predictions are intentionally surfaced for review. 

Next steps:

- Human-in-the-loop UI for reviewing low-confidence classifiactions; relevant human feedback is injected into the prompt via a few-shot RAG-like system.
- More advanced LLM enrichment, grouping, second passes.
- OpenObserve for pipeline health, metrics, throughput etc.
- Evaluation set for measuring classification quality, self-improving LLM.
