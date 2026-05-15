# Notebook Documentation

[Overview](#overview) • [Data Inputs](#data-inputs) • [Charts and Tables](#charts-and-tables) • [Processing Steps](#processing-steps) • [Notes](#notes)

This document describes the notebook used for analysis and visualization.
The scope is limited to:

- `metrics/notebooks/prs_viz_bp_paper.ipynb`

---

# Overview

The notebook produces descriptive statistics and publication-ready figures
for PR analysis across academic projects, Brasil Participativo (EbL), and
market benchmarks. The analysis is based on a single input CSV with
precomputed metrics (from the extraction and filtering pipeline).

---

# Data Inputs

## Primary input

- `prs.csv`

The notebook expects `prs.csv` in the working directory (commonly the
Silver output). The file must include, at minimum, these columns:

- `org`, `repo`, `author`, `created_at`, `merged_at`
- `files_changed`, `additions`, `deletions`, `churn`
- `reviews_count`, `reviewers`, `comments`
- `commits`, `file_hashes`

---


# Charts and Tables

The notebook produces the following outputs, in order:

1) PR counts table by org/repo (total vs no-bot)
2) Distribution of files changed (histogram + medians)
3) Distribution of churn (histogram + medians)
4) Refactoring ratio distribution (histogram + medians)
5) Activity heatmap by day/hour (Brasil Participativo)
6) Activity heatmap by day/hour (Academic combined)
7) Knowledge growth over tenure (line chart)
8) Final formatted table of PR counts (image for paper)

---

# Processing Steps

## 1) Setup and filtering

- Loads the CSV into `df_raw`.
- Defines bot keywords and removes bot authors with `is_bot_author`.
- Builds filtered dataframes for the following groups:
  - Brasil Participativo (lappis-unb/decidimbr: decidim-govbr)
  - MDS academic (unb-mds)
  - REQ academic (mdsreq-fga-unb)
  - EPS/MDS academic (fga-eps-mds)
  - Market benchmarks: Decidim, VSCode, React

## 2) PR counts per group

- Counts total PRs and PRs without bots for each org/repo pair used in the notebook.
- Builds a summary table (`prs_counts`).

## 3) Cleaning for distribution plots

For distribution-focused charts, the notebook filters to:
- `files_changed > 0` and `churn > 0`

This avoids degenerate records that would bias medians and histograms.

## 4) Derived metrics

The notebook computes:
- Median files changed and median churn per group
- Refactoring ratio: `deletions / churn`
- Lead time (from `lead_time_hours`, already in the CSV)
- Reviewer counts and reviewer coverage
- Collective ownership metrics using `file_hashes`
- Knowledge growth over time using per-developer timelines

---

# Notes

There are additional figures that were not cited in the paper, collected in:

- `metrics/notebooks/prs_viz_bp_analysis.ipynb`
