# Metrics Extraction and Processing Documentation

[Overview](#pipeline-overview) • [Extraction](#1-extraction-extractpy) • [Collection Strategy](#collection-strategy) • [Data Schema](#processed-data-schema) • [Filtering](#2-filtering-filterpy) • [Filtering Criteria](#filtering-criteria)

This document describes the design and operation of the scripts used for extracting and filtering Pull Requests (PRs) and Merge Requests (MRs). These tools form the data collection pipeline for research focused on Best Practices (BP) presented at CSEE&T.

The pipeline is centered around two core scripts:

- `metrics/scripts/extract.py`
- `metrics/scripts/filter.py`

---

# Pipeline Overview

The data workflow follows a simplified **Medallion Architecture**:

### Extraction (Bronze Layer)

Raw data ingestion from GitHub and GitLab via GraphQL APIs.

### Filtering (Silver Layer)

Data refinement based on specific organizations, projects, and temporal windows (academic semesters).

## Primary Output Paths

### Bronze Data

```text
metrics/data/bronze/prs.csv
```

(Raw data)

### Silver Data

```text
metrics/data/silver/prs.csv
```

(Filtered/Curated data)

---

# 1. Extraction: `extract.py`

## Objective

To collect comprehensive metrics from GitHub PRs and GitLab MRs at scale.

The script utilizes GraphQL with cursor-based pagination and implements batch-saving to optimize memory usage and allow for incremental execution.

---

## Collection Strategy

### GitHub

#### Single-Phase Query

Fetches a page of 25 PRs including full details:

- commits
- reviews
- comments
- review threads
- limited file sampling

All retrieved in a single request.

#### Pagination

Controlled via:

- `endCursor`
- `hasNextPage`

---

### GitLab

#### Two-Phase Query

##### List Phase

Fetches basic metadata for 25 MRs per page.

##### Detail Phase

Performs individual queries for each MR to retrieve nested data:

- commits
- discussions
- approvals

#### Pagination

Implements cursor-based logic consistent with the GitHub approach.

---

## Processed Data Schema

The following key metrics are extracted for each PR/MR:

| Category | Fields |
|---|---|
| Identification | Platform, Organization, Repository, ID |
| Timeline | `created_at`, `merged_at`, `first_review_at`, `first_human_response_at` |
| Interaction | Reviewers, Commenters, Review count, Total comments |
| Code Metrics | Commits, unique commit authors, average commit message length |
| Changes | Files changed, additions, deletions, code churn |
| Documentation | `doc_files_count`, `is_doc_pr` (boolean), file extensions, file hashes |
| Text Analysis | Title length, description length |
| Metadata | Label count, list of labels |

---

## Bot Detection

To ensure data integrity regarding human collaboration, accounts are flagged as bots based on keywords such as:

- `bot`
- `dependabot`
- `renovate`

This classification is critical for calculating the `first_human_response_at` metric, excluding automated interactions.

---

## Persistence and Incremental Loading

### Batch Saving

Results are committed to the CSV every `SAVE_BATCH_SIZE` records.

### Deduplication

The script checks existing entries in the Bronze CSV by matching:

- platform
- org
- repo
- id

This prevents redundant API calls and skips previously processed PRs.

---

# 2. Filtering: `filter.py`

## Objective

To transform the Bronze (raw) dataset into a Silver (curated) dataset by applying research-specific inclusion criteria.

---

## Filtering Criteria

### Project Selection

Academic repositories are filtered by explicit allowlists:

- `unb-mds` (MDS)
- `mdsreq-fga-unb` (REQ)
- `fga-eps-mds` (EPS)

Other organizations are included as whole orgs (EbL and market benchmarks):

- `lappis-unb/decidimbr` (GitLab)
- `decidim` (GitHub)
- `microsoft` (GitHub)
- `flutter` (GitHub)
- `facebook` (GitHub)
- `kubernetes` (GitHub)
- `tensorflow` (GitHub)

---

### Temporal Windows (Semesters)

Records are retained only if the `created_at` or `merged_at` timestamps fall within defined academic semesters.

These windows are configurable via the `SEMESTERS` constant in the script.

---

## Execution Flow

### Load

Reads the raw dataset:

```text
metrics/data/bronze/prs.csv
```

### Type Conversion

Standardizes date strings into `datetime` objects.

### Org/Project Masking

Applies the academic project masks (three orgs) and the other-org mask.

### Temporal Masking

Applies a logical OR operation across defined semester ranges.

### Export

Saves the refined dataset to:

```text
metrics/data/silver/prs.csv
```

### Console Summary

At the end, the script prints:

- Totals in Bronze and Silver
- Count of repos per academic org
- Count of other orgs
- The semester windows used
- Output path