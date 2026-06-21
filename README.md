# DataVault

**Intelligent Dataset Discovery & Analysis Platform for ML Research and Industry**

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Next.js](https://img.shields.io/badge/Next.js-14.1-black)](https://nextjs.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109-green)](https://fastapi.tiangolo.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://www.python.org/)
[![React](https://img.shields.io/badge/React-18.2-61DAFB)](https://react.dev/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.3-3178C6)](https://www.typescriptlang.org/)

---

## Table of Contents

1. [Overview](#overview)
2. [Features](#features)
3. [Quick Start](#quick-start)
4. [Project Structure](#project-structure)
5. [Architecture](#architecture)
6. [Technology Stack](#technology-stack)
7. [Installation & Setup](#installation--setup)
8. [Configuration](#configuration)
9. [Running Services](#running-services)
10. [API Documentation](#api-documentation)
11. [Development Guide](#development-guide)
12. [Troubleshooting](#troubleshooting)
13. [Contributing](#contributing)
14. [License](#license)

## Overview

DataVault is a sophisticated platform designed to automatically discover, catalog, analyze, and visualize datasets from multiple machine learning and data science sources. Built with modern web technologies (Next.js + FastAPI), the system combines web scraping, natural language processing, vector embeddings, and machine learning to provide researchers, data scientists, and ML practitioners with comprehensive dataset insights and discovery capabilities.

### What Makes DataVault Unique

- **Multi-Source Aggregation**: Unified interface for 10+ dataset repositories
- **AI-Powered Intelligence**: Google Gemini integration for smart summaries and metadata extraction
- **Advanced Analytics**: Trend forecasting, quality scoring, bias detection, and statistical analysis
- **Vector Search**: Semantic understanding of datasets through embedding-based search
- **Real-time Health Monitoring**: Continuous dataset availability and quality checks
- **Production-Ready**: Docker containerized, Redis-backed caching, MongoDB persistence
- **Beautiful UI**: Modern, responsive React interface with interactive visualizations

### Target Users

- **ML Researchers**: Discover datasets for academic papers and experiments
- **Data Scientists**: Find appropriate datasets for project requirements
- **Data Engineers**: Catalog and monitor data sources
- **Data Curators**: Maintain dataset quality and metadata standards
- **Enterprise**: Build custom dataset catalogs for internal use

---

## Features

### 🔍 Data Discovery & Search

- **Semantic Search**: Natural language queries understood through embeddings
- **Advanced Filters**: Filter by modality (text, image, audio), size, license, quality score
- **Trending Datasets**: AI-powered trend detection and forecasting
- **Dataset Recommendations**: Collaborative filtering based on user preferences
- **Live Access Testing**: Verify datasets are still accessible before offering

### 📊 Analytics & Insights

- **Quality Scoring**: Multi-dimensional quality assessment (0-100 scale)
  - Documentation completeness
  - Metadata richness
  - Community engagement metrics
  - Update frequency
  - License clarity
  
- **Trend Forecasting**: Predict future popularity using Prophet, ARIMA, growth analysis
- **Bias Analysis**: Detect potential dataset biases and fairness issues
- **Statistical Metrics**: Distribution analysis, correlation heatmaps, summary statistics

### 📁 Dataset Management

- **Upload Wizard**: Multi-step dataset upload with validation
- **File Processing**: Automatic format detection (CSV, JSON, Parquet, Excel, etc.)
- **Metadata Extraction**: Extract headers, schema, data types, sample data
- **Citation Tracking**: Generate and track dataset citations
- **Version Control**: Track dataset updates and modifications
- **Access History**: Monitor dataset access patterns

### 🧬 Advanced ML Features

- **DNA Fingerprinting**: Unique identifier for each dataset based on statistical properties
- **Embedding Explorer**: Visualize high-dimensional embeddings in interactive 2D/3D
- **Outlier Scanner**: Identify anomalies using statistical and ML methods
- **Training Mix Planner**: Optimize dataset combinations for balanced training
- **Nutritional Label**: Quick visual assessment of dataset quality and characteristics

### 📈 Visualization & Exploration

- **Interactive Charts**: Powered by Recharts, Plotly, D3.js with export capabilities
- **Force Graph**: Network visualization of dataset relationships
- **Trend Visualization**: Time series and forecast displays
- **PII Detection Badge**: Visual indicator of personally identifiable information

### 🔄 Integration & Automation

- **Celery Task Queue**: Async processing for health checks, embeddings, analytics
- **Redis Caching**: High-performance caching for queries, embeddings, sessions
- **MongoDB**: Document-based storage with flexible schema and efficient indexing
---

## Quick Start

### Prerequisites

- Python 3.10+
- Node.js 18+
- Docker & Docker Compose
- Git

### 5-Minute Setup

```bash
# Clone the repository
git clone https://github.com/Shamil-S-Khan/DataVault.git
cd DataVault

# Copy environment template
cp .env.example .env

# Install Python dependencies
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # macOS/Linux
pip install -r requirements.txt

# Install Node dependencies
npm install

# Start Redis container
docker-compose up -d redis

# Terminal 1: Start Backend
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2: Start Frontend
npm run dev
```

Visit:
- Frontend: http://localhost:3000
- Backend API: http://localhost:8000
- API Docs: http://localhost:8000/docs

---

## Project Structure

Key directories:
- **app/** - Backend FastAPI application and React frontend components
- **app/routes/** - API endpoint definitions
- **app/analytics/** - Data analysis and ML metrics
- **app/components/** - React UI components
- **app/tasks/** - Celery async tasks for background processing
- **app/services/** - Business logic layer
- **app/scrapers/** - Data source connectors
- **docs/** - Documentation and guides
- **scripts/** - Utility scripts

For detailed structure, see [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md)

---

## Architecture

### System Overview

```
┌──────────────────────────────────────────────────┐
│    Frontend: Next.js + React (Port 3000)        │
│    TypeScript • Tailwind • React Query          │
└────────────────────┬─────────────────────────────┘
                     │ REST/TRPC API
                     ▼
┌──────────────────────────────────────────────────┐
│   Backend: FastAPI + Uvicorn (Port 8000)        │
│   Python • Pydantic • PyMongo                   │
└──────┬──────────────────────────────┬────────────┘
       │                              │
       ▼                              ▼
┌──────────────────┐        ┌──────────────────┐
│   MongoDB Atlas  │        │  Redis + Celery  │
│   (Persistence)  │        │  (Queue + Cache) │
└──────────────────┘        └────────┬─────────┘
                                     │
                          ┌──────────▼─────────┐
                          │ Celery Workers     │
                          │ (Async Tasks)      │
                          └────────────────────┘

External Services:
- Google Gemini API (AI Analysis)
- HuggingFace (Embeddings & Models)
- Kaggle API (Dataset Scraping)
- OpenML (Dataset Repository)
```

### Data Flow

1. User sends request from frontend
2. Next.js sends API call to FastAPI backend
3. Backend checks Redis cache for results
4. If cache miss, queries MongoDB
5. Long operations queued to Celery workers
6. External APIs called as needed (Gemini, HF, Kaggle)
7. Results cached in Redis
8. Response returned to frontend
9. React components render the data

---

## Technology Stack

### Frontend
- **Framework**: Next.js 14.1.0 with React 18.2
- **Language**: TypeScript 5.3
- **Styling**: Tailwind CSS 3.4, PostCSS
- **UI Components**: Headless UI, Heroicons
- **Charts**: Recharts, Plotly.js, D3.js, react-force-graph
- **State Management**: React Query 4.36, React Hook Form 7.49
- **API Client**: TRPC 10.45 for type-safe API calls

### Backend
- **Framework**: FastAPI 0.109.0
- **Server**: Uvicorn 0.27 (ASGI)
- **Validation**: Pydantic 2.5
- **Database**: MongoDB with PyMongo 4.6
- **Cache**: Redis 5.0
- **Queue**: Celery 5.3
- **Async**: Motor 3.3 (async MongoDB driver)

### ML & AI
- **LLM**: Google Generative AI (Gemini)
- **Embeddings**: sentence-transformers 2.3
- **Vector Search**: FAISS 1.7 for similarity search
- **Forecasting**: Prophet 1.1, statsmodels 0.14 (ARIMA)
- **Anomaly Detection**: scikit-learn 1.4 (Isolation Forest)
- **Scientific**: NumPy 1.26, Pandas 2.1, SciPy 1.11

### Data Sources
- HuggingFace, Kaggle, OpenML, Papers with Code, Zenodo, Data.gov, AWS Open Data, UCI ML

### DevOps
- **Containers**: Docker & Docker Compose
- **Process Management**: Supervisor (for production)
- **Configuration**: Environment variables (.env)

---

## Installation & Setup

### Step 1: Clone Repository

```bash
git clone https://github.com/Shamil-S-Khan/DataVault.git
cd DataVault
```

### Step 2: Python Environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

### Step 3: Node Environment

```bash
npm install
npm run type-check  # Optional: verify types
```

### Step 4: Environment Configuration

```bash
cp .env.example .env
# Edit .env with your credentials
```

### Step 5: Start Services

```bash
# Terminal 1: Start Redis
docker-compose up redis

# Terminal 2: Start Backend
.venv\Scripts\activate
uvicorn app.main:app --reload --port 8000

# Terminal 3: Start Frontend
npm run dev

# Terminal 4: Start Celery (optional, for background tasks)
celery -A app.tasks.celery_app worker --loglevel=info
```

---

## Configuration

### Environment Variables (.env)

```bash
# API Keys & Credentials
GEMINI_API_KEY=your_gemini_api_key
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
GITHUB_TOKEN=your_github_token
HUGGINGFACE_API_KEY=your_hf_api_key

# Database URLs
MONGODB_URI=mongodb+srv://user:pass@cluster.mongodb.net/
MONGODB_DB_NAME=datavault
REDIS_URL=redis://localhost:6379

# Celery Configuration
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=mongodb://user:pass@cluster.mongodb.net/

# LLM Settings
LLM_PROVIDER=gemini
EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2

# Application Settings
DEBUG=False
LOG_LEVEL=INFO
MAX_UPLOAD_SIZE=1073741824
```

### Key Configuration Files

- **app/config.py**: Database, Redis, API settings
- **app/main.py**: FastAPI setup, CORS, middleware
- **docker-compose.yml**: Redis service definition

---

## Running Services

### Development Mode

```bash
# Backend with auto-reload
uvicorn app.main:app --reload --port 8000

# Frontend with hot reload
npm run dev

# Celery worker (separate terminal)
celery -A app.tasks.celery_app worker --loglevel=info

# Celery beat scheduler (separate terminal)
celery -A app.tasks.celery_app beat --loglevel=info
```

### Production Mode

```bash
# Build frontend
npm run build

# Start production frontend
npm run start

# Backend with Gunicorn
gunicorn app.main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker

# Or with Docker
docker-compose up -d --build
```

### Docker Deployment

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Clean up volumes
docker-compose down -v
```

---

## API Documentation

### Interactive Docs

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Main Endpoints

**Datasets**:
```
GET    /api/datasets              # List all
GET    /api/datasets/{id}         # Get one
POST   /api/datasets              # Create
PUT    /api/datasets/{id}         # Update
DELETE /api/datasets/{id}         # Delete
```

**Search**:
```
GET    /api/search?query=...      # Semantic search
GET    /api/search/filters        # Get filters
GET    /api/trending              # Trending datasets
```

**Analytics**:
```
GET    /api/analytics/{id}        # Dataset analytics
GET    /api/analytics/{id}/quality # Quality breakdown
GET    /api/analytics/{id}/trends  # Trend analysis
```

**Uploads**:
```
POST   /api/uploads               # Upload
GET    /api/uploads/{id}          # Status
DELETE /api/uploads/{id}          # Cancel
```

---

## Development Guide

### Code Style

**Python**: Follow PEP 8 using Black formatter
**TypeScript**: Use ESLint + Prettier

### Adding Features

1. Create feature branch: `git checkout -b feature/name`
2. Implement changes in appropriate modules
3. Run tests: `pytest` and `npm test`
4. Lint code: `npm run lint`
5. Type check: `npm run type-check`
6. Commit: `git commit -m "feat: description"`
7. Push and open PR

### Testing

```bash
# Backend tests
pytest

# With coverage
pytest --cov=app

# Frontend tests
npm test

# Type checking
npm run type-check

# Linting
npm run lint
```

---

## Troubleshooting

### MongoDB Connection Timeout

**Problem**: `pymongo.errors.NetworkTimeout`

**Solution**:
```bash
# Check MongoDB URI in .env
# Ensure firewall allows connection
# Whitelist IP in MongoDB Atlas
# Increase timeout in .env:
MONGODB_URI=mongodb+srv://...?serverSelectionTimeoutMS=5000
```

### Redis Connection Refused

**Problem**: `ConnectionRefusedError`

**Solution**:
```bash
# Start Redis
docker-compose up -d redis

# Verify connection
redis-cli ping  # Should return PONG
```

### Celery Tasks Not Running

**Problem**: Tasks stuck in queue

**Solution**:
```bash
# Start worker
celery -A app.tasks.celery_app worker --loglevel=info

# Start scheduler
celery -A app.tasks.celery_app beat --loglevel=info

# Check status
celery -A app.tasks.celery_app inspect active
```

### Port Already in Use

**Problem**: `Error: listen EADDRINUSE :::3000`

**Solution**:
```bash
# Windows: Find and kill process on port 3000
taskkill /PID <pid> /F
# Or use different port
npm run dev -- -p 3001
```

### API Returns 404

**Problem**: Endpoint not found

**Solution**:
- Check endpoint exists in app/routes/
- Verify correct URL and HTTP method
- Check CORS configuration in app/main.py
- Ensure backend is running on port 8000

---

## Contributing

### Workflow

1. Fork repository
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Make changes following code standards
4. Write tests for new functionality
5. Run `pytest` and `npm run lint`
6. Commit: `git commit -m "feat: description"`
7. Push: `git push origin feature/amazing-feature`
8. Open Pull Request

### Code Standards

- **Python**: PEP 8, type hints, comprehensive docstrings
- **TypeScript**: ESLint, Prettier, functional components
- **Maximum line length**: 100 characters
- **Naming**: Descriptive names, avoid abbreviations
- **Tests**: Write tests for new features and bug fixes

### Pull Request Template

- Clear description of changes made
- Link to related issues
- Screenshots for UI changes
- Test results and coverage
- Note any breaking changes

---

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Support & Community

- **Issues**: [GitHub Issues](https://github.com/Shamil-S-Khan/DataVault/issues)
- **Discussions**: [GitHub Discussions](https://github.com/Shamil-S-Khan/DataVault/discussions)
- **Documentation**: See [docs/](docs/) folder
- **Contact**: Project maintainer

---

**Status**: Production Ready ✓  
**Version**: 1.0.0  
**Last Updated**: June 22, 2026
