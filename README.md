**SanctiSight — See Risk Before It Strikes**
Hackathon: Google Cloud x Kaggle BigQuery AI Hackathon 2025
Category: AI + Vector Search for Risk Intelligence

💡 Overview
SanctiSight is an AI-powered risk intelligence pipeline that connects global news with sanctions data — spotting risky entities before they appear on official watchlists.

Using BigQuery ML and Vertex AI embeddings, it analyzes the past 90 days of global news from the GDELT dataset and cross-references named entities with OFAC / OpenSanctions databases using semantic similarity.

Unlike traditional keyword matching, SanctiSight detects contextual and semantic relationships — identifying early signs of potential sanctions exposure or associated entities.

🧠 Technical Stack
Layer	Technology	Purpose
Data	BigQuery	Data pipeline, SQL analytics
AI	Vertex AI (text-multilingual-embedding-002)	Embedding model
ML Functions	ML.GENERATE_EMBEDDING, VECTOR_SEARCH	Embedding + similarity
Storage	BigQuery Tables & Views	Incremental entity corpus
Indexing	CREATE VECTOR INDEX (IVF)	Scalable semantic search
⚙️ Pipeline Steps
Data Ingest & Normalization

Load GDELT GKG data (news, tone, entities)
Extract and normalize person/org entities
Sanctions Dictionary

Ingest OFAC/OpenSanctions data
Normalize names + aliases
Embeddings Generation

Create embeddings using Vertex AI
Store in sanction_embeddings and entity_embeddings
Vector Indexing

IVF index for fast cosine similarity search
Matching Layer

VECTOR_SEARCH between entity and sanction embeddings
Cutoff threshold tuned via distance histogram
Insights

v_gkg_sanctions_hits: merged results
v_sanction_mentions_top: top mentions
v_sanction_mentions_daily: trend over time
📊 Outcomes
SanctiSight surfaces potential links between:

Organizations or individuals in recent news
Entities already under sanction or scrutiny
This enables early warning signals for compliance, analysts, or investigative journalism — offering visibility before official listings update.

Built entirely on Google Cloud using BigQuery ML + Vertex AI.
