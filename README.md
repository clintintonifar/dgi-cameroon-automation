Perfect — here is your **clean, professional, one-click copy README.md**.

Just copy everything inside the markdown block and save it as `README.md`.

---

```markdown
# 🇨🇲 DGI360 – Automated DGI Cameroon Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green?logo=mit)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Active-brightgreen)](../../actions)
[![Automation](https://img.shields.io/badge/Automation-GitHub%20Actions-black?logo=githubactions)](../../actions)

> **DGI360 – Part 1**  
> A fully automated, cloud-based data pipeline that downloads and maintains Cameroon DGI taxpayer compliance data using GitHub Actions and Google Drive.

---

## 🎯 Overview

DGI360 automates the collection of public taxpayer compliance data published by Cameroon’s tax authority.

It:

- 📥 Downloads new monthly Excel files from the DGI portal  
- 🔄 Runs automatically every day (03:00 UTC)  
- ☁️ Uploads files to Google Drive  
- 🗂 Maintains a 5-year rolling archive  
- 🚫 Requires zero manual intervention  

This repository contains **Part 1 (Data Pipeline)**.  
Part 2 (Power BI Dashboard) will be released separately.

---

## 🏛️ Data Source

**Direction Générale des Impôts (DGI) – Cameroon**

Public Portal:  
https://teledeclaration-dgi.cm

### File Characteristics

| Attribute | Value |
|------------|--------|
| Format | Excel (.xlsx) |
| Naming | `FICHIER_[MONTH]_[YEAR].xlsx` |
| Rows | ~250,000–350,000 taxpayers |
| Size | 15–40 MB |
| Frequency | Monthly publication |

### Key Columns

- `NIU` – Unique taxpayer ID  
- `RAISON_SOCIALE` – Legal business name  
- `SIGLE` – Acronym  
- `CRI` – Tax office code  
- `CENTRE_DE_RATTACHEMENT` – Assigned tax center  
- `ACTIVITE_PRINCIPALE` – Business activity code  

---

## 🏗️ Architecture

```

GitHub Actions (Daily 3AM UTC)
│
▼
Python Automation Script
│
▼
DGI Portal (Excel Download)
│
▼
Google Drive Storage
│
▼
Power BI Dashboard (Part 2)

```

---

## ✨ Features

- 🤖 Fully automated daily execution
- 🔁 Retry logic with exponential backoff
- 🗑 Automatic cleanup (5-year rolling window)
- 🔐 Secure secrets management via GitHub Secrets
- ☁️ 100% cloud-based (no local server required)
- 💰 Operates entirely on free tiers

---

## 📁 Repository Structure

```

dgi-cameroon-automation/
│
├── README.md
├── LICENSE
├── requirements.txt
│
├── src/
│   └── download_dgi.py
│
├── .github/workflows/
│   └── dgi_scheduler.yml
│
└── docs/
├── SETUP_GUIDE.md
├── ARCHITECTURE.md
└── TROUBLESHOOTING.md

````

---

## 🚀 Quick Setup

### 1️⃣ Clone Repository

```bash
git clone https://github.com/clintintonifar/dgi-cameroon-automation.git
cd dgi-cameroon-automation
````

---

### 2️⃣ Create Google Drive Folder

Create a folder in Google Drive (e.g., `DGI_Data`)
Copy the folder ID from:

```
https://drive.google.com/drive/folders/YOUR_FOLDER_ID
```

---

### 3️⃣ Create Google Cloud Credentials

1. Go to Google Cloud Console
2. Enable Google Drive API
3. Create OAuth 2.0 Client ID (Desktop App)
4. Generate refresh token

---

### 4️⃣ Add GitHub Secrets

Repository → Settings → Secrets and variables → Actions → New repository secret

Add:

```
DRIVE_FOLDER_ID
GOOGLE_REFRESH_TOKEN
GOOGLE_CLIENT_ID
GOOGLE_CLIENT_SECRET
```

---

### 5️⃣ Enable Automation

* Go to **Actions**
* Enable workflows
* Run manually once for testing

---

## ⏰ Schedule

| Setting   | Value               |
| --------- | ------------------- |
| Trigger   | Daily at 03:00 UTC  |
| Runtime   | ~15 minutes         |
| Retention | 5 years (~60 files) |
| Storage   | ~1.8 GB             |
| Cost      | $0/month            |

---

## 🛠 Tech Stack

* Python 3.11
* GitHub Actions (CI/CD automation)
* Google Drive API
* OAuth 2.0
* Requests (HTTP client)

---

## 🔐 Security

* Secrets stored securely in GitHub
* No credentials committed to code
* Public government data only
* OAuth scope limited to Drive API

---

## 📊 Roadmap – Part 2

Planned Power BI Dashboard Features:

* 🔍 Taxpayer search (NIU / Company name)
* 📈 Compliance trend analysis
* 🗺 Regional filtering
* 📊 Monthly activity heatmaps
* 📤 Export functionality

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Open a Pull Request

---

## 📄 License

MIT License – see LICENSE file.

---

## 👤 Author

**Clintin Tonifar**
Cameroon 🇨🇲

GitHub: [https://github.com/clintintonifar](https://github.com/clintintonifar)

---

<div align="center">

**DGI360 – Automated Data Pipeline**
Built for Cameroon’s Data Community 🇨🇲

</div>
```

---

That’s your clean, professional, production-ready README.

If you’d like, I can now:

* Make it look more “enterprise-grade”
* Add animated badges
* Add a visual architecture diagram
* Or optimize it specifically for recruiters 👀
