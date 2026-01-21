# DataVault

**Intelligent Dataset Discovery Platform for ML Research and Industry**

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Next.js](https://img.shields.io/badge/Next.js-14.1-black)](https://nextjs.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109-green)](https://fastapi.tiangolo.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://www.python.org/)

![DataVault Dashboard](docs/images/dashboard.png)
*Main dashboard showing dataset discovery and filtering capabilities*

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Technology Stack](#technology-stack)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Overview

DataVault is an platform designed to automatically discover, catalog, and analyze datasets from multiple machine learning and data science sources. The system combines web scraping, natural language processing, and machine learning to provide researchers and practitioners with comprehensive dataset insights.

### Core Capabilities

- **Automated Data Collection**: Continuously scrapes and indexes datasets from 10+ major data repositories
- **Intelligent Analysis**: Leverages Google Gemini AI to generate summaries, extract metadata, and provide insights
- **Trend Forecasting**: Uses statistical models (Prophet, ARIMA) to predict dataset popularity and relevance
- **Quality Assessment**: Multi-dimensional quality scoring based on documentation, metadata completeness, and community engagement
- **Semantic Search**: Vector-based search using sentence transformers for natural language queries
- **Bias Detection**: Analyzes datasets for potential biases using specialized ML models

![Search Interface](docs/images/search.png)
*Advanced search with semantic understanding and intelligent filters*

## Features

### Data Acquisition

The platform includes dedicated scrapers for:

- **HuggingFace Datasets**: Community-contributed ML datasets
- **Kaggle**: Competition and community datasets
- **Papers with Code**: Academic research datasets
- **OpenML**: Machine learning datasets with standardized metadata
- **Zenodo**: Research data repository
- **Data.gov**: US government open data
- **AWS Open Data**: Amazon's public dataset registry
- **Harvard Dataverse**: Academic data repository
- **UCI Machine Learning Repository**: Classic ML datasets

Each scraper implements:
- Rate limiting and retry logic
- Metadata extraction and normalization
- Duplicate detection using fuzzy matching and embeddings
- Size estimation and file format analysis

### Intelligence & Analytics

**LLM-Powered Analysis**
- Automated dataset summarization using Google Gemini
- Use case identification and task classification
- Domain and modality detection
- Key feature extraction from documentation

**Quality Scoring System**
- Documentation completeness score
- Metadata richness evaluation
- Community engagement metrics (downloads, stars, forks)
- Data freshness and update frequency

**Trend Detection**
- Time series forecasting of dataset popularity
- Emerging domain identification
- Citation and usage trend analysis
- Related dataset recommendations

**Bias & Fairness Analysis**
- Demographic representation analysis
- Dataset balance metrics
- Fairness constraint validation
- Potential bias warnings

![Analytics Dashboard](docs/images/analytics.png)
*Comprehensive analytics showing trends, quality metrics, and recommendations*

### User Interface

**Responsive Design**
- Mobile-first architecture
- Progressive web app capabilities
- System-aware dark/light mode
- Accessibility compliant (WCAG 2.1)

**Interactive Components**
- Real-time search with autocomplete
- Multi-dimensional filtering (domain, modality, quality, source, license)
- Interactive visualizations using Plotly and Recharts
- Dataset comparison views
- Version timeline tracking

**Key Components**
- `DatasetCard`: Comprehensive dataset preview
- `FilterBar`: Advanced filtering interface
- `SemanticSearch`: Natural language search
- `BiasChart`: Bias analysis visualization
- `TrendVisualization`: Time series trend display
- `QualityScore`: Multi-factor quality indicator
- `ModelRecommendations`: ML model suggestions based on dataset characteristics

## Architecture

DataVault follows a microservices architecture with separate frontend, backend, and worker services orchestrated via Docker Compose.

### System Diagram

```
┌──────────────────────────────────────────────────┐
│              Next.js Frontend (Port 3000)         │
│  ┌────────────────────────────────────────────┐  │
│  │  React Components + TypeScript             │  │
│  │  - Dataset Discovery Interface             │  │
│  │  - Analytics Dashboard                     │  │
│  │  - Search & Filter System                  │  │
│  └────────────────────────────────────────────┘  │
└─────────────────────┬────────────────────────────┘
                      │ REST API (HTTP/JSON)
                      ▼
┌──────────────────────────────────────────────────┐
│            FastAPI Backend (Port 8000)            │
│  ┌────────────────────────────────────────────┐  │
│  │  API Routes                                │  │
│  │  - /api/datasets  - /api/search           │  │
│  │  - /api/analytics - /api/admin            │  │
│  └────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────┐  │
│  │  Business Logic                            │  │
│  │  - Query Processing                        │  │
│  │  - Data Validation                         │  │
│  │  - Response Formatting                     │  │
│  └────────────────────────────────────────────┘  │
└─────────────┬──────────────┬─────────────────────┘
              │              │
       ┌──────┴──────┐  ┌───┴──────┐
       │             │  │          │
       ▼             ▼  ▼          ▼
┌────────────┐  ┌────────────┐  ┌──────────────────┐
│  MongoDB   │  │   Redis    │  │ Celery Workers   │
│  Atlas     │  │  (Port     │  │                  │
│            │  │   6379)    │  │ - Scrapers       │
│ Documents: │  │            │  │ - ML Tasks       │
│ - datasets │  │ Cache:     │  │ - LLM Analysis   │
│ - users    │  │ - queries  │  │ - Trend Calc     │
│ - reviews  │  │ - results  │  │                  │
│ - analytics│  │ - sessions │  │ Celery Beat:     │
│            │  │            │  │ - Scheduled Jobs │
└────────────┘  └────────────┘  └──────────────────┘
```

### Component Interaction

1. **User Request Flow**:
   - User interacts with Next.js frontend
   - Frontend makes API calls to FastAPI backend
   - Backend queries MongoDB or checks Redis cache
   - Results are formatted and returned to frontend

2. **Background Processing**:
   - Celery Beat schedules periodic scraping tasks
   - Celery Workers execute scraping jobs in parallel
   - Scraped data is validated and stored in MongoDB
   - LLM analysis tasks are queued for dataset enrichment

3. **Search & Discovery**:
   - User query is processed by semantic search module
   - Query embeddings are generated using sentence transformers
   - Vector similarity search is performed using FAISS
   - Results are ranked by relevance and quality score

## Installation

### Prerequisites

- **Docker Desktop** (recommended) or Docker Engine + Docker Compose
- **Node.js** 18+ (for local development)
- **Python** 3.10+ (for local development)
- **MongoDB Atlas** account (free tier available at mongodb.com)
- **Redis** (provided via Docker or Upstash free tier)
- **API Keys** (optional but recommended):
  - Google Gemini API key for LLM features
  - Kaggle API credentials for Kaggle datasets
  - GitHub Personal Access Token for rate limit increase
  - HuggingFace API token for model downloads

### Docker Installation (Recommended)

```bash
# Clone the repository
git clone https://github.com/Shamil-S-Khan/DataVault.git
cd DataVault

# Copy environment template and configure
cp .env.example .env

# Edit .env with your credentials (see Configuration section)
nano .env  # or use any text editor

# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Access the application
# - Frontend: http://localhost:3000
# - Backend API: http://localhost:8000
# - API Documentation: http://localhost:8000/docs
```

### Manual Installation

```bash
# Clone the repository
git clone https://github.com/Shamil-S-Khan/DataVault.git
cd DataVault

# Backend setup
pip install -r requirements.txt

# Frontend setup
npm install

# Start Redis (if not using Upstash)
redis-server

# Start backend (Terminal 1)
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Start Celery worker (Terminal 2)
celery -A app.tasks.celery_app worker --loglevel=info

# Start Celery beat scheduler (Terminal 3)
celery -A app.tasks.celery_app beat --loglevel=info

# Start frontend (Terminal 4)
npm run dev
```

## Configuration

Create a `.env` file in the project root with the following configuration:

```bash
# Database Configuration
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/
MONGODB_DB_NAME=datavault

# Redis Configuration (use Upstash for free tier)
REDIS_URL=redis://localhost:6379
# or for Upstash:
# REDIS_URL=rediss://:password@host:port

# Celery Configuration
CELERY_BROKER_URL=redis://localhost:6379
CELERY_RESULT_BACKEND=mongodb+srv://username:password@cluster.mongodb.net/

# LLM Configuration
LLM_PROVIDER=gemini  # Options: gemini, huggingface
GEMINI_API_KEY=your_gemini_api_key_here

# API Keys for Scrapers
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
GITHUB_TOKEN=your_github_personal_access_token
HUGGINGFACE_API_KEY=your_huggingface_token

# Application Settings
NEXT_PUBLIC_API_URL=http://localhost:8000
NODE_ENV=development
```

### Obtaining API Keys

**Google Gemini API**:
1. Visit https://makersuite.google.com/app/apikey
2. Create a new API key
3. Add to `.env` as `GEMINI_API_KEY`

**Kaggle API**:
1. Go to https://www.kaggle.com/settings/account
2. Scroll to "API" section and click "Create New API Token"
3. Extract username and key from downloaded `kaggle.json`

**GitHub Token**:
1. Go to https://github.com/settings/tokens
2. Generate new token with `repo` scope
3. Add to `.env` as `GITHUB_TOKEN`

## Usage

### Initial Data Population

After installation, populate the database with initial datasets:

```bash
# Using Docker
docker-compose exec backend python -m app.refresh_datasets

# Or manually
python -m app.refresh_datasets
```

This process will:
- Scrape initial datasets from all configured sources
- Generate embeddings for semantic search
- Calculate quality scores
- Perform LLM analysis on sample datasets

### API Endpoints

The FastAPI backend provides comprehensive REST APIs:

**Dataset Operations**:
- `GET /api/datasets` - List datasets with pagination and filters
- `GET /api/datasets/{id}` - Get dataset details
- `POST /api/datasets/search` - Semantic search
- `GET /api/datasets/{id}/similar` - Find similar datasets

**Analytics**:
- `GET /api/analytics/trends` - Dataset trend analysis
- `GET /api/analytics/quality-distribution` - Quality score distribution
- `GET /api/analytics/source-statistics` - Per-source statistics

**Admin**:
- `POST /api/admin/refresh-datasets` - Trigger data refresh
- `POST /api/admin/calculate-embeddings` - Regenerate embeddings
- `GET /api/admin/scraper-status` - Check scraper health

Full API documentation is available at `http://localhost:8000/docs` (Swagger UI) or `http://localhost:8000/redoc` (ReDoc).

### Frontend Usage

Access the web interface at `http://localhost:3000`:

1. **Browse Datasets**: View curated dataset cards with quality indicators
2. **Search**: Use natural language queries to find relevant datasets
3. **Filter**: Narrow results by domain, modality, quality, license, or source
4. **Analyze**: View trends, bias analysis, and quality metrics
5. **Compare**: Select multiple datasets for side-by-side comparison
6. **Recommendations**: Get ML model suggestions based on dataset characteristics

## Project Structure

```
DataVault/
├── app/                              # Main application directory
│   ├── components/                   # React/TypeScript components
│   │   ├── DatasetCard.tsx          # Dataset display card
│   │   ├── FilterBar.tsx            # Advanced filtering
│   │   ├── SemanticSearch.tsx       # Search interface
│   │   ├── BiasChart.tsx            # Bias visualization
│   │   ├── QualityScore.tsx         # Quality indicator
│   │   └── [27 total components]
│   │
│   ├── db/                           # Database layer
│   │   ├── connection.py            # MongoDB connection
│   │   ├── models.py                # Data models
│   │   └── redis_client.py          # Redis client
│   │
│   ├── routes/                       # API endpoints
│   │   ├── datasets.py              # Dataset CRUD
│   │   ├── analytics.py             # Analytics endpoints
│   │   ├── admin.py                 # Admin operations
│   │   ├── users.py                 # User management
│   │   └── reviews.py               # Dataset reviews
│   │
│   ├── scrapers/                     # Data collection modules
│   │   ├── huggingface.py           # HuggingFace scraper
│   │   ├── kaggle.py                # Kaggle scraper
│   │   ├── papers_with_code.py      # PwC scraper
│   │   ├── openml.py                # OpenML scraper
│   │   ├── zenodo.py                # Zenodo scraper
│   │   ├── datagov.py               # Data.gov scraper
│   │   ├── aws_opendata.py          # AWS Open Data scraper
│   │   ├── dataverse.py             # Dataverse scraper
│   │   ├── base.py                  # Base scraper class
│   │   ├── deduplication.py         # Duplicate detection
│   │   └── size_fetcher.py          # File size estimation
│   │
│   ├── analytics/                    # ML & analytics modules
│   │   ├── bias_analyzer.py         # Bias detection
│   │   ├── fitness_calculator.py    # Quality scoring
│   │   ├── forecasting.py           # Trend prediction
│   │   ├── license_analyzer.py      # License classification
│   │   ├── metrics.py               # Performance metrics
│   │   ├── model_matcher.py         # Model recommendations
│   │   ├── query_parser.py          # Query understanding
│   │   ├── similarity_engine.py     # Dataset similarity
│   │   ├── synthetic_analyzer.py    # Synthetic data detection
│   │   ├── topic_modeling.py        # Topic extraction
│   │   └── version_tracker.py       # Version management
│   │
│   ├── llm/                          # LLM integration
│   │   ├── gemini_client.py         # Google Gemini client
│   │   ├── huggingface_client.py    # HuggingFace client
│   │   └── dataset_intelligence.py  # Dataset analysis
│   │
│   ├── ml/                           # ML utilities
│   │   ├── semantic_search.py       # Vector search
│   │   ├── quality_scorer.py        # Quality models
│   │   ├── recommender.py           # Recommendation engine
│   │   ├── modality_classifier.py   # Modality detection
│   │   └── evaluation.py            # Model evaluation
│   │
│   ├── tasks/                        # Celery tasks
│   │   ├── celery_app.py           # Celery configuration
│   │   ├── scraping_tasks.py       # Scraping jobs
│   │   ├── ml_tasks.py             # ML processing jobs
│   │   └── llm_tasks.py            # LLM analysis jobs
│   │
│   ├── dataset/[id]/                # Dynamic dataset pages
│   │   └── page.tsx                # Dataset detail page
│   │
│   ├── explore/                     # Explore page
│   │   └── page.tsx                # Dataset exploration
│   │
│   ├── main.py                      # FastAPI application
│   ├── config.py                    # Application configuration
│   ├── layout.tsx                   # Root layout
│   ├── page.tsx                     # Home page
│   └── globals.css                  # Global styles
│
├── docs/                            # Documentation
│   ├── README.md                   # Project overview
│   ├── SETUP.md                    # Setup instructions
│   ├── PROJECT_STRUCTURE.md        # Detailed structure
│   └── [additional documentation]
│
├── scripts/                         # Utility scripts
│   ├── fetch_dataset_sizes.py      # Size calculation utility
│   └── archive/                    # Archived scripts
│
├── logs/                            # Application logs
│   └── celery_logs.txt            # Worker logs
│
├── docker-compose.yml               # Docker orchestration
├── Dockerfile                       # Backend container
├── package.json                     # Node.js dependencies
├── requirements.txt                 # Python dependencies
├── next.config.js                   # Next.js configuration
├── tsconfig.json                    # TypeScript configuration
├── tailwind.config.js               # Tailwind CSS configuration
├── .env.example                     # Environment template
└── README.md                        # This file
```

## Technology Stack

### Frontend

| Technology | Version | Purpose |
|------------|---------|---------|
| Next.js | 14.1.0 | React framework with SSR/SSG |
| React | 18.2.0 | UI library |
| TypeScript | 5.3.3 | Type-safe JavaScript |
| Tailwind CSS | 3.4.1 | Utility-first CSS framework |
| Headless UI | 1.7.17 | Unstyled accessible components |
| Heroicons | 2.1.1 | SVG icon library |
| Plotly.js | 2.27.1 | Interactive charts |
| Recharts | 2.10.3 | React chart library |
| React Query | 4.36.1 | Server state management |
| tRPC | 10.45.0 | End-to-end typesafe APIs |

### Backend

| Technology | Version | Purpose |
|------------|---------|---------|
| FastAPI | 0.109.0 | Modern Python web framework |
| Uvicorn | 0.27.0 | ASGI server |
| Pydantic | 2.5.3 | Data validation |
| Celery | 5.3.6 | Distributed task queue |
| PyMongo | 4.6.1 | MongoDB driver |
| Motor | 3.3.2 | Async MongoDB driver |
| Redis | 5.0.1 | Caching and message broker |

### Machine Learning & AI

| Technology | Version | Purpose |
|------------|---------|---------|
| Sentence Transformers | 2.3.1 | Text embeddings |
| Transformers | 4.36.2 | HuggingFace models |
| PyTorch | 2.1.2 | Deep learning framework |
| FAISS | 1.7.4 | Vector similarity search |
| Scikit-learn | 1.4.0 | Traditional ML algorithms |
| Prophet | 1.1.5 | Time series forecasting |
| Statsmodels | 0.14.1 | Statistical models |
| Gensim | 4.3.2 | Topic modeling |
| Google Generative AI | 0.3.2 | Gemini API client |

### Data Processing

| Technology | Version | Purpose |
|------------|---------|---------|
| Pandas | 2.1.4 | Data manipulation |
| NumPy | 1.26.3 | Numerical computing |
| NetworkX | 3.2.1 | Graph analysis |
| BeautifulSoup4 | 4.12.3 | HTML parsing |
| Requests | 2.31.0 | HTTP library |
| HTTPX | 0.26.0 | Async HTTP client |

### Infrastructure

| Technology | Purpose |
|------------|---------|
| Docker | Containerization |
| Docker Compose | Multi-container orchestration |
| MongoDB Atlas | Cloud database (free tier) |
| Redis/Upstash | Caching layer (free tier) |
| GitHub Actions | CI/CD pipeline |

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/test_scrapers.py

# Run tests in Docker
docker-compose exec backend pytest
```

### Code Quality

```bash
# Lint TypeScript/JavaScript
npm run lint

# Type check
npm run type-check

# Format Python code
black app/
isort app/

# Lint Python code
flake8 app/
```

### Database Management

```bash
# Access MongoDB shell (if using local MongoDB)
mongosh "mongodb+srv://cluster.mongodb.net/" --username your_username

# Backup database
mongodump --uri="mongodb+srv://..." --out=./backup

# Restore database
mongorestore --uri="mongodb+srv://..." ./backup

# Clear Redis cache
redis-cli FLUSHALL
```

### Adding a New Scraper

1. Create new scraper file in `app/scrapers/`:
```python
from app.scrapers.base import BaseScraper

class NewSourceScraper(BaseScraper):
    def __init__(self):
        super().__init__(source_name="new_source")
    
    async def scrape(self):
        # Implementation
        pass
```

2. Register scraper in `app/tasks/scraping_tasks.py`
3. Add configuration in `.env`
4. Update documentation

## Contributing

Contributions are welcome and appreciated. Please follow these guidelines:

### Development Workflow

1. Fork the repository
2. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Make your changes following the coding standards
4. Write tests for new functionality
5. Ensure all tests pass:
   ```bash
   pytest
   npm run lint
   npm run type-check
   ```
6. Commit with descriptive messages:
   ```bash
   git commit -m "feat: add new dataset quality metric"
   ```
7. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```
8. Open a Pull Request with:
   - Clear description of changes
   - Link to related issues
   - Screenshots for UI changes

### Coding Standards

**Python**:
- Follow PEP 8 style guide
- Use type hints for function signatures
- Write docstrings for classes and functions
- Maximum line length: 100 characters

**TypeScript/React**:
- Use functional components with hooks
- Follow Airbnb React style guide
- Use TypeScript for type safety
- Use Tailwind CSS for styling

### Reporting Issues

When reporting bugs, include:
- Operating system and version
- Python and Node.js versions
- Steps to reproduce
- Expected vs actual behavior
- Error messages and logs

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

### Third-Party Licenses

This project uses various open-source packages. See individual package licenses in:
- Python dependencies: `requirements.txt`
- Node.js dependencies: `package.json`

## Contact & Support

- **Repository**: https://github.com/Shamil-S-Khan/DataVault
- **Issues**: https://github.com/Shamil-S-Khan/DataVault/issues
- **Documentation**: See `/docs` directory

## Acknowledgments

DataVault is built with and depends on excellent open-source projects:

- **Data Sources**: HuggingFace, Kaggle, Papers with Code, OpenML, Zenodo, Data.gov, AWS Open Data, Harvard Dataverse
- **AI/ML Models**: Google Gemini, Sentence Transformers, HuggingFace Transformers
- **Infrastructure**: MongoDB Atlas (free tier), Upstash Redis (free tier)
- **Frameworks**: Next.js, FastAPI, Celery
