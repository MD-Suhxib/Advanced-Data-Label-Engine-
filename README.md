# 📊 Advanced Data Labelling Engine

A lightweight rule-based data labelling system built as part of the **StratSync AI/ML Engineer Assignment**.  
The project demonstrates practical problem-solving using a custom rule engine, live dashboard, and API-first architecture.

---

## 🌟 Features
- **Rule Engine**  
  - Define flexible rules using conditions (`Price > 10`, `Product = "Chocolate" AND Price < 5`, etc.)  
  - Support for `AND` / `OR` operators.  
  - Rule priority and enable/disable support.  

- **Payload Processing**  
  - Submit JSON payloads.  
  - Rules are evaluated and matching labels applied automatically.  
  - Processing history is stored in memory.  

- **Dashboard (Frontend)**  
  - Clean, real-time dashboard.  
  - Donut chart for label distribution.  
  - Rule management UI.  
  - CSV export of processed statistics.  

- **API (Backend)**  
  - `/api/rules` → Create, update, delete, toggle rules.  
  - `/api/process` → Submit payload and get applied labels.  
  - `/api/statistics` → Get usage and label breakdown.  
  - `/api/health` → Check service health.  

---

## 🚀 Live Demo

- **Frontend (Dashboard)** 👉 [Live on Vercel](https://advanced-data-label-engine.vercel.app/)  
- **Backend API ** 👉 [Live on Render](https://advanced-data-label-engine-1.onrender.com)  
- **GitHub Repository** 👉 [Repo Link](https://github.com/MD-Suhxib/Advanced-Data-Label-Engine-)

---

## 🛠️ Tech Stack
- **Backend**: Flask (Python), Flask-CORS  
- **Frontend**: HTML, JavaScript, Chart.js  
- **Hosting**:  
  - Backend → Render  
  - Frontend → Vercel  

---

## 📦 Installation (Local Setup)

### Backend
```bash
cd api
pip install -r requirements.txt
python app.py
