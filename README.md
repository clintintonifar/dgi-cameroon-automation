# 🇨🇲 DGI Cameroon Tax Data Automation

[![GitHub Actions](https://img.shields.io/github/actions/workflow/status/clintintonifar/dgi-cameroon-automation/dgi_scheduler.yml?label=auto-run&logo=github-actions)](https://github.com/clintintonifar/dgi-cameroon-automation/actions)
[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green?logo=mit)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Active-brightgreen)](../../actions)
[![Last Run](https://img.shields.io/github/last-commit/clintintonifar/dgi-cameroon-automation/main?label=last%20update)](../../commits)

> **Fully automated, cloud-based data pipeline for Cameroon DGI taxpayer compliance data**

---

## 📋 Executive Summary

This project automates the monthly collection of taxpayer compliance data from the **Cameroon Direction Générale des Impôts (DGI)** website. It downloads, stores, and maintains a **rolling 5-year window** of historical data with **zero manual intervention**.

| Metric | Value |
|--------|-------|
| **Automation** | Daily at 03:00 UTC |
| **Data Sources** | DGI Cameroon (Official Government Portal) |
| **Storage** | Google Drive (15 GB Free Tier) |
| **Retention** | 5-Year Rolling Window (~60 files) |
| **Maintenance** | Zero (Fully Automated) |
| **Cost** | $0/month (100% Free Tier) |

---

## 🏗️ Architecture Overview

```mermaid
graph LR
    A[GitHub Actions Scheduler] -->|Daily 3AM UTC| B[Ubuntu Runner]
    B -->|Python Script| C[DGI Website]
    C -->|Download Excel| D[/tmp/dgi_downloads/]
    D -->|Google Drive API| E[Google Drive]
    E -->|Auto-Sync| F[Power BI / Looker Studio]
    F -->|Dashboard| G[End Users]
    
    style A fill:#2088FF,stroke:#333,stroke-width:2px,color:#fff
    style C fill:#009639,stroke:#333,stroke-width:2px,color:#fff
    style E fill:#1EA362,stroke:#333,stroke-width:2px,color:#fff
    style F fill:#F2C811,stroke:#333,stroke-width:2px,color:#000
