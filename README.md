# 🎬 Netflix ELT Data Pipeline

A production-style ELT pipeline that loads raw Netflix data into SQL Server using Python,
performs in-database transformations, and runs analytical queries on 8,800+ records.

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python&logoColor=white)
![SQL Server](https://img.shields.io/badge/SQL_Server-2019-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.0-150458?style=flat-square&logo=pandas&logoColor=white)
![ELT](https://img.shields.io/badge/ELT-Pipeline-green?style=flat-square)

---

## 🔄 Pipeline Architecture
```
CSV (Netflix Data) → Python (Pandas Load) → SQL Server (Raw Table) → Transform (Clean + Normalize) → Analytics (5 SQL Queries)
```

---

## 📁 Project Structure
```
netflix-elt-pipeline/
│
├── data/                  # Raw Netflix CSV dataset
├── notebooks/             # Python data loading script
├── sql/
│   ├── schema.sql         # Table creation with NVARCHAR optimization
│   ├── cleaning.sql       # Deduplication, normalization, imputation
│   └── analysis.sql       # 5 analytical SQL queries
├── README.md
└── requirements.txt
```

---

## 🗄️ Data Model

| Type | Table | Description |
|------|-------|-------------|
| Fact | `Netflix_raw` | Main cleaned table (show_id PK, type, title, director, country, date_added, rating, duration) |
| Dimension | `Netflix_directors` | Normalized directors per show |
| Dimension | `Netflix_cast` | Normalized cast members per show |
| Dimension | `Netflix_country` | Normalized countries per show |
| Dimension | `Netflix_genre` | Normalized genres per show |

---

## ⚙️ Key Steps

**1. Load Raw Data**
Ingest CSV via Pandas directly into SQL Server `Netflix_raw` table without prior transformation.

**2. Schema Optimization**
Migrate to `NVARCHAR` for multilingual character support. Tune column lengths using Python max-length analysis.

**3. Deduplication**
Remove duplicate rows using `ROW_NUMBER() OVER (PARTITION BY title, type)`. Reduced 8,807 → 8,804 records.

**4. Normalization**
Split multi-value columns (directors, cast, country, genre) into separate dimension tables using `STRING_SPLIT` + `CROSS APPLY`.

**5. Missing Value Imputation**
Fill null countries via director-to-country mapping. Convert date strings and clean duration column.

---

## 📊 Analytical Queries

| # | Query | Key Technique | Insight |
|---|-------|---------------|---------|
| 1 | Directors with both Movies & TV Shows | `GROUP BY` + `HAVING` | Multi-format directors |
| 2 | Country with most Comedy movies | `JOIN` + `COUNT` | USA → 685 comedies |
| 3 | Top director per year by movie count | `ROW_NUMBER() OVER (PARTITION BY year)` | Annual top contributors |
| 4 | Avg movie duration by genre | `CAST` + `AVG` + `GROUP BY` | Genre-wise duration trends |
| 5 | Directors with both Horror & Comedy | `INTERSECT` / Self JOIN | 55 cross-genre directors |

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.10+ | Data extraction and loading |
| Pandas | CSV reading and schema analysis |
| SQL Server 2019 | In-database transformation and storage |
| T-SQL | Cleaning, normalization, analytics |
| pyodbc / SQLAlchemy | Python–SQL Server connection |
| SSMS | Query execution and validation |

---

## 🚀 How to Run

1. **Clone the repo**
```bash
git clone https://github.com/your-username/netflix-elt-pipeline.git
cd netflix-elt-pipeline
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Load data into SQL Server**
```bash
python notebooks/load_data.py
```

4. **Run SQL scripts in order**
```
sql/schema.sql → sql/cleaning.sql → sql/analysis.sql
```

---

## 📦 Dataset

- Source: [Netflix Movies and TV Shows – Kaggle](https://www.kaggle.com/datasets/shivamb/netflix-shows)
- Records: 8,807 (raw) → 8,804 (after deduplication)

---

## 🙋 Author

**Sangita Kar**  
[LinkedIn](https://www.linkedin.com/in/sangitakar/)
