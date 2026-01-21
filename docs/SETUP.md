# DataVault Quick Setup Guide

## Prerequisites Checklist

Before starting, ensure you have:

- [ ] Python 3.10 or higher installed
- [ ] Node.js 18 or higher installed
- [ ] Docker and Docker Compose installed
- [ ] Git installed

## Free Tier Accounts Setup

### 1. MongoDB Atlas (Required)

1. Go to [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)
2. Create a free account
3. Create a new cluster (M0 Free tier)
4. Create a database user:
   - Click "Database Access"
   - Add new user with password
5. Whitelist IP address:
   - Click "Network Access"
   - Add IP: `0.0.0.0/0` (for development)
6. Get connection string:
   - Click "Connect" on your cluster
   - Choose "Connect your application"
   - Copy the connection string
   - Replace `<password>` with your database password

### 2. Upstash Redis (Required)

1. Go to [Upstash](https://upstash.com/)
2. Create a free account
3. Create a new Redis database
4. Copy the Redis URL from the dashboard

### 3. Google Gemini API (Required)

1. Go to [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Sign in with Google account
3. Click "Create API Key"
4. Copy the API key

### 4. Kaggle API (Optional but Recommended)

1. Go to [Kaggle](https://www.kaggle.com/)
2. Sign in or create account
3. Go to Account Settings
4. Scroll to "API" section
5. Click "Create New API Token"
6. Save the downloaded `kaggle.json` file
7. Extract username and key from the file

### 5. GitHub Token (Optional)

1. Go to [GitHub Settings](https://github.com/settings/tokens)
2. Click "Generate new token (classic)"
3. Select scopes: `public_repo`, `read:user`
4. Generate and copy the token

## Installation Steps

### Step 1: Clone and Setup

```bash
# Navigate to project directory
cd e:/Personal_Projects/DataVault

# Create backend .env file
cp backend/.env.example backend/.env
```

### Step 2: Configure Environment Variables

Edit `backend/.env` with your credentials:

```env
# MongoDB (REQUIRED)
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority
MONGODB_DB_NAME=datavault

# Redis (REQUIRED)
REDIS_URL=redis://default:your_password@your-redis.upstash.io:6379

# Gemini API (REQUIRED)
GEMINI_API_KEY=your_gemini_api_key_here

# Kaggle (OPTIONAL)
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key

# GitHub (OPTIONAL)
GITHUB_TOKEN=ghp_your_github_token

# Celery
CELERY_BROKER_URL=redis://default:your_password@your-redis.upstash.io:6379
CELERY_RESULT_BACKEND=mongodb+srv://username:password@cluster.mongodb.net/datavault

# Application
ENVIRONMENT=development
DEBUG=True
CORS_ORIGINS=http://localhost:3000
NEXTAUTH_SECRET=generate_a_random_secret_key_here
```

### Step 3: Start with Docker (Recommended)

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### Step 4: Manual Setup (Alternative)

#### Backend

```bash
cd backend

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Download spaCy model
python -m spacy download en_core_web_sm

# Run the server
uvicorn app.main:app --reload
```

#### Frontend

```bash
cd frontend

# Install dependencies
npm install

# Run development server
npm run dev
```

#### Celery Workers (Optional for testing)

```bash
cd backend

# Terminal 1: Start worker
celery -A app.tasks.celery_app worker --loglevel=info

# Terminal 2: Start beat scheduler
celery -A app.tasks.celery_app beat --loglevel=info

# Terminal 3: Start Flower monitoring
celery -A app.tasks.celery_app flower --port=5555
```

## Verification

### 1. Check Backend Health

```bash
curl http://localhost:8000/health
```

Expected response:
```json
{
  "status": "healthy",
  "services": {
    "mongodb": "healthy",
    "redis": "healthy"
  },
  "environment": "development"
}
```

### 2. Check Frontend

Open browser: http://localhost:3000

You should see the DataVault homepage with search bar and trending datasets section.

### 3. Check API Documentation

Open browser: http://localhost:8000/docs

You should see the interactive Swagger UI with all API endpoints.

### 4. Check Celery Monitoring

Open browser: http://localhost:5555

You should see the Flower dashboard with worker status.

## Testing the Application

### 1. Test API Endpoints

```bash
# Get trending datasets
curl http://localhost:8000/api/datasets/trending?page=1&limit=10

# Get filter options
curl http://localhost:8000/api/datasets/filters/options

# Search datasets
curl -X POST "http://localhost:8000/api/datasets/search?query=imagenet"
```

### 2. Test Scraping (Manual Trigger)

Currently, scraping runs on schedule (2 AM UTC daily). To test manually, you can:

```python
# In Python shell
from app.scrapers.papers_with_code import PapersWithCodeScraper
import asyncio

scraper = PapersWithCodeScraper()
datasets = asyncio.run(scraper.scrape_with_cache())
print(f"Scraped {len(datasets)} datasets")
```

### 3. Test LLM Integration

```python
# In Python shell
from app.llm.gemini_client import gemini_client
import asyncio

summary = asyncio.run(gemini_client.generate_summary(
    "ImageNet",
    "Large-scale image classification dataset",
    {}
))
print(summary)
```

## Troubleshooting

### Issue: MongoDB Connection Failed

**Solution**: 
- Verify connection string is correct
- Check IP whitelist includes your IP or 0.0.0.0/0
- Ensure database user has correct permissions

### Issue: Redis Connection Failed

**Solution**:
- Verify Redis URL is correct
- Check Upstash dashboard for connection details
- Ensure Redis instance is running

### Issue: Gemini API Error

**Solution**:
- Verify API key is valid
- Check rate limits (60 requests/minute)
- Ensure you have Gemini API enabled

### Issue: Frontend Can't Connect to Backend

**Solution**:
- Verify backend is running on port 8000
- Check CORS settings in backend/.env
- Ensure NEXT_PUBLIC_API_URL is set correctly

### Issue: Docker Services Won't Start

**Solution**:
```bash
# Clean up and restart
docker-compose down -v
docker-compose up --build -d
```

### Issue: Port Already in Use

**Solution**:
```bash
# Find and kill process using port 8000
# Windows:
netstat -ano | findstr :8000
taskkill /PID <PID> /F

# macOS/Linux:
lsof -ti:8000 | xargs kill -9
```

## Next Steps

1. **Verify Setup**: Ensure all services are running
2. **Test Endpoints**: Try the API endpoints
3. **Explore Frontend**: Browse the UI at localhost:3000
4. **Review Code**: Check the implementation files
5. **Plan Phase 2**: Review the roadmap in README.md

## Getting Help

- Check [README.md](../README.md) for detailed documentation
- Review [walkthrough.md](walkthrough.md) for implementation details
- Check API docs at http://localhost:8000/docs
- Review logs: `docker-compose logs -f`

## Development Workflow

### Making Changes

1. **Backend Changes**:
   - Edit files in `backend/app/`
   - FastAPI auto-reloads on save
   - Check logs: `docker-compose logs -f backend`

2. **Frontend Changes**:
   - Edit files in `frontend/src/`
   - Next.js auto-reloads on save
   - Check browser console for errors

3. **Database Changes**:
   - Update models in `backend/app/db/models.py`
   - Update indexes in `backend/app/db/connection.py`
   - Restart backend: `docker-compose restart backend`

### Running Tests

```bash
# Backend tests
cd backend
pytest tests/ --cov=app

# Frontend tests (when implemented)
cd frontend
npm run test
```

### Viewing Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f backend
docker-compose logs -f frontend
docker-compose logs -f celery_worker
```

## Production Deployment

See [docs/DEPLOYMENT.md](../docs/DEPLOYMENT.md) for production deployment guide (to be created in Phase 2).

---

**You're all set! 🚀**

The DataVault platform is now running locally. Start exploring the codebase and building Phase 2 features!
