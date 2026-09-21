# Flight Tracker & Trip-Watch Agent

A Databricks capstone project: an action-taking AI agent that tracks flights, sends proactive
delay/cancellation alerts, and answers passenger-rights questions — built on Spark, Lakeflow
Declarative Pipelines, Lakebase, Databricks Vector Search, and deployed as a Databricks App.

**[Live app link]- coming soon** · **[Architecture diagram - coming soon]** · **[Full project proposal - coming soon]**

## What it does

- Watch a flight and get proactive alerts on delays, gate changes, or cancellations
- See historical on-time performance for any route or carrier
- Ask natural-language questions about passenger rights (refunds, rebooking, compensation),
  answered by a Retrieval-Augmented Generation pipeline over DOT guidance and airline
  Contracts of Carriage — with citations
- All of it through a single chat-driven agent with real read/write actions, not just Q&A

## Architecture

![Architecture diagram](docs/architecture.png)

| Layer | Technology |
|---|---|
| Historical data pipeline | Spark batch (BTS On-Time Performance dataset, ~6-7M rows) |
| Live data pipeline | Lakeflow Declarative Pipelines streaming table (adsb.lol) |
| Operational data | Lakebase (Postgres-compatible) |
| Knowledge base | Databricks Vector Search over DOT/airline policy documents |
| Agent | Tool-calling LLM with read + write tools against Lakebase, Delta, and Vector Search |
| Analytics | Change Data Feed → Delta table tracking agent/app activity |
| Frontend + deployment | Databricks App (Streamlit) |

See [`docs/proposal.md`](docs/proposal.md) for the full data sources, integration plan, and
requirement-by-requirement breakdown.

## Why adsb.lol instead of OpenSky

OpenSky Network was the original choice for live flight data, but its API — including its
OAuth token endpoint — is unreachable from any cloud-hosted compute (Databricks, Render, or
otherwise), by OpenSky's own deliberate policy of blocking hosting/cloud-provider IP ranges.
Switched to [adsb.lol](https://api.adsb.lol/docs), a free, no-key community API built for
exactly this use case. See [`docs/setup-playbook.md`](docs/setup-playbook.md) for the full
story and other engineering trade-offs made along the way.

## Repository structure

```
├── notebooks/          # Spark pipelines, Lakebase schema, API test/ingestion notebooks
├── app/                # Databricks App source (app.py, app.yaml, requirements.txt)
├── docs/                # Architecture diagram, full proposal, setup playbook
└── README.md
```

## Data sources

All public, no proprietary or company data used:

- [BTS On-Time Performance](https://www.transtats.bts.gov/) — historical flight delay data
- [OurAirports](https://ourairports.com/data/) — airport reference data
- [adsb.lol](https://api.adsb.lol/docs) — live flight position/status data
- [DOT Fly Rights](https://www.transportation.gov/airconsumer) — passenger rights guidance
- Airline Contracts of Carriage (public, per-airline)

## Status

See commit history for the
build sequence, including dead ends hit and how they were resolved (OpenSky cloud-blocking,
Lakebase OAuth-vs-password authentication, Render admin-access limitations).
