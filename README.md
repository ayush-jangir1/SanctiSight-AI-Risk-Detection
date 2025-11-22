# ⚡ SanctiSight — See Risk Before It Strikes

## 💡 Overview: AI-Powered Risk Intelligence

**SanctiSight** is an innovative, **AI-powered risk intelligence pipeline** built entirely on **Google Cloud**. It's designed to connect the vast stream of global news with official sanctions data, allowing businesses and analysts to **spot high-risk entities *before* they appear on official watchlists.**

Using **BigQuery ML** and **Vertex AI embeddings**, SanctiSight analyzes the past 90 days of global news from the GDELT dataset and cross-references named entities with OFAC / OpenSanctions databases using **semantic similarity**.

Unlike traditional keyword matching, SanctiSight leverages contextual and semantic relationships—identifying early signs of potential sanctions exposure or associated entities that traditional methods miss.

> 🏆 **Hackathon:** Google Cloud x Kaggle BigQuery AI Hackathon 2025
>
> 🎯 **Category:** AI + Vector Search for Risk Intelligence

---

## 🧠 Technical Stack & Architecture

SanctiSight is engineered for scale and speed, relying entirely on **BigQuery ML** and **Vertex AI** for a powerful, integrated solution.

| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Data Source** | BigQuery | Primary data pipeline and SQL analytics engine. |
| **AI Model** | Vertex AI (`text-multilingual-embedding-002`) | Generates high-quality, multilingual entity embeddings. |
| **ML Functions** | `ML.GENERATE_EMBEDDING`, `VECTOR_SEARCH` | Core functions for embedding generation and similarity matching. |
| **Storage** | BigQuery Tables & Views | Stores incremental entity corpus and sanctions dictionary. |
| **Indexing** | `CREATE VECTOR INDEX` (IVF) | Creates a scalable vector index for sub-second semantic search. |



---

## ⚙️ Pipeline Steps: From Data to Insight

The SanctiSight pipeline is executed in a structured, five-step process to ensure data quality and highly accurate risk signals.

### 1. Data Ingest & Normalization
* Loads **GDELT GKG** data (global news, tone, entities) containing the past 90 days of world events.
* Extracts and normalizes person and organization entities from the raw news text.

### 2. Sanctions Dictionary Setup
* Ingests the **OFAC** and **OpenSanctions** watchlists.
* Normalizes names and aliases across the dictionary for consistent embedding.

### 3. Embeddings Generation
* Uses the **Vertex AI** embedding model to create dense vector representations for both the normalized **sanctioned entities** and the **news entities**.
* Stores these vectors in `sanction_embeddings` and `entity_embeddings` tables.

### 4. Vector Indexing
* Applies a **CREATE VECTOR INDEX (IVF)** on the entity embeddings table. This is crucial for achieving **fast cosine similarity search** across billions of potential matches.

### 5. Matching & Insights Layer
* Performs the core analysis: `VECTOR_SEARCH` between the vast pool of news entity embeddings and the sanctioned entity embeddings.
* A calculated **cutoff threshold** (tuned via distance histogram) filters for the highest-confidence matches.
* **Final Outputs (BigQuery Views):**
    * `v_gkg_sanctions_hits`: Merged, detailed results for individual matches.
    * `v_sanction_mentions_top`: Top entities ranked by risk score and mention count.
    * `v_sanction_mentions_daily`: Trend data showing risk spikes over time.

---

## 📊 Outcomes & Impact

SanctiSight delivers high-value, actionable intelligence by surfacing potential links between:

* **Organizations or individuals** prominent in recent global news.
* **Entities already under sanction** or official scrutiny.

This capability provides **early warning signals** that are critical for:

* ✅ **Compliance Teams:** Proactive due diligence and risk mitigation.
* 📰 **Investigative Journalism:** Identifying complex, hidden financial and political links.
* 💰 **Financial Analysts:** Gaining visibility *before* official listings cause market disruption.

**SanctiSight offers the power to see risk before it strikes.**
