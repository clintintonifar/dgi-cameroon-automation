<div align="center">

# 🇨🇲 DGI360

[![Pipeline](https://img.shields.io/github/actions/workflow/status/clintintonifar/dgi-cameroon-automation/dgi_scheduler.yml?label=pipeline&logo=github-actions&logoColor=white)](https://github.com/clintintonifar/dgi-cameroon-automation/actions)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-22c55e)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Active-brightgreen)]()

**Every day, the Cameroonian government quietly publishes a list of every taxpayer who filed their taxes that month.**  
This project automatically grabs that file, and saves it to the cloud — no clicks, no human involved.

</div>

---

### 🤔 What problem does this solve?

The [DGI portal](https://teledeclaration-dgi.cm) drops a new Excel file every month — ~300,000 taxpayer records, no notification, no API. If you want historical data, you have to manually check and download it yourself.

This project does that automatically, every single day, for free.

---

### ⚙️ What it actually does

```
Every day at 3AM UTC
  → Checks the DGI website for new monthly files
  → Downloads any that are missing (goes back 5 years)
  → Saves them to Google Drive
  → Deletes anything older than 5 years
  → Goes back to sleep
```

No server. No cost. No maintenance.

---

### 📦 What gets collected

Each monthly file contains ~300,000 rows with fields like:

| Field | What it means |
|---|---|
| `NIU` | Unique taxpayer ID |
| `RAISON_SOCIALE` | Company name |
| `CENTRE_DE_RATTACHEMENT` | Their assigned tax office |
| `ACTIVITE_PRINCIPALE` | Their industry/activity code |

---

### 🔢 By the numbers

|  |  |
|---|---|
| 🕒 Runs | Daily, 03:00 UTC |
| 📁 Files kept | ~60 (5-year window) |
| 💾 Storage used | ~1.8 GB |
| 💰 Cost | $0/month |

---

<div align="center">

**Part 1 of 2** — Part 2 will be an interactive Power BI dashboard built on top of this data.

Made with ❤️ by [@clintintonifar](https://github.com/clintintonifar) · 🇨🇲 Cameroon

</div>
