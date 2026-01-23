"""Review API routes - placeholder for Phase 3."""
from fastapi import APIRouter

router = APIRouter()

@router.get("/{dataset_id}")
async def get_reviews(dataset_id: str):
    """Get dataset reviews (Phase 3)."""
    return {"message": "Reviews endpoint - to be implemented in Phase 3"}
