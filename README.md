# DGI360 — Automated Taxpayer Data Pipeline

[![CI](https://img.shields.io/github/actions/workflow/status/clintintonifar/dgi-cameroon-automation/dgi_scheduler.yml?label=pipeline&logo=github-actions)](https://github.com/clintintonifar/dgi-cameroon-automation/actions)
[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

Automated, cloud-based pipeline that collects, stores, and maintains Cameroon DGI taxpayer compliance data — fully hands-off, $0/month.

> **Part 1 of 2** — Part 2 will be a Power BI dashboard for interactive compliance analysis (coming soon).

---

## How It Works

```
GitHub Actions (daily 03:00 UTC)
  → Python scraper hits DGI portal
  → Downloads monthly Excel files (~300k rows each)
  → Uploads to Google Drive
  → Cleans up files older than 5 years
```

---

## Features

- **Fully automated** — daily cron via GitHub Actions, no manual steps
- **Retry logic** — 5 attempts with exponential backoff for unreliable connections
- **Idempotent** — skips already-downloaded files, no duplicates
- **Auto-cleanup** — maintains a rolling 5-year window (~60 files, ~1.8 GB)
- **Secure** — all credentials in GitHub Secrets, never in code
- **Free** — runs entirely on GitHub Actions + Google Drive free tiers

---

## Data Source

The [Direction Générale des Impôts (DGI)](https://teledeclaration-dgi.cm) publishes monthly Excel files listing taxpayers who have fulfilled their declaration obligations. Files are public, require no authentication, and typically appear 1–2 weeks after month-end.

| Field | Description |
|---|---|
| `NIU` | Unique taxpayer identifier |
| `RAISON_SOCIALE` | Legal business name |
| `SIGLE` | Business acronym |
| `CRI` | Tax office code |
| `CENTRE_DE_RATTACHEMENT` | Assigned tax collection center |
| `ACTIVITE_PRINCIPALE` | Primary business activity code |

---

## Stack

| Layer | Tool |
|---|---|
| Language | Python 3.11 |
| Orchestration | GitHub Actions |
| Storage | Google Drive API |
| Auth | OAuth 2.0 |
| Scraping | `requests` + retry logic |

---

## Quick Start

**Prerequisites:** GitHub account, Google account, Google Cloud project (all free)

**1. Clone**
```bash
git clone https://github.com/clintintonifar/dgi-cameroon-automation.git
```

**2. Set up Google Drive**
- Create a folder in Google Drive and copy its ID from the URL

**3. Create OAuth credentials**
- Enable the Google Drive API in [Google Cloud Console](https://console.cloud.google.com/)
- Create an OAuth 2.0 Client ID (Desktop app) and generate a refresh token
- See [`docs/SETUP_GUIDE.md`](docs/SETUP_GUIDE.md) for step-by-step instructions

**4. Add GitHub Secrets**

```
DRIVE_FOLDER_ID
GOOGLE_REFRESH_TOKEN
GOOGLE_CLIENT_ID
GOOGLE_CLIENT_SECRET
```

**5. Run**
- Go to **Actions → DGI Monthly Downloader → Run workflow**
- Verify files appear in your Google Drive folder

---

## Repository Structure

```
├── src/
│   └── download_dgi.py        # Core automation script
├── .github/workflows/
│   └── dgi_scheduler.yml      # Daily cron workflow
├── docs/
│   ├── SETUP_GUIDE.md
│   ├── ARCHITECTURE.md
│   └── TROUBLESHOOTING.md
├── requirements.txt
└── README.md
```

---

## Operational Details

| Setting | Value |
|---|---|
| Schedule | Daily at 03:00 UTC |
| Runtime | ~15 min/run |
| Actions usage | ~450 min/month (2,000 free) |
| Storage | ~1.8 GB (15 GB free) |
| Retention window | 5 years (~60 files) |

---

## Part 2: Power BI Dashboard *(coming soon)*

An interactive dashboard built on this pipeline's output — taxpayer search, compliance heatmaps, trend analysis, and regional filtering. Will be released as a separate repository and linked here.

---

## License

[MIT](LICENSE) — Clintin Tonifar · 🇨🇲 Cameroon · [@clintintonifar](https://github.com/clintintonifar)
