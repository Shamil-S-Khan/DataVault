"""User API routes - placeholder for Phase 2."""
from fastapi import APIRouter

router = APIRouter()

@router.get("/me")
async def get_current_user():
    """Get current user (Phase 2)."""
    return {"message": "User endpoint - to be implemented in Phase 2"}
