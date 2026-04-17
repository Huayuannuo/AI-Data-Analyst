"""AI Data Analyst 智能分析路由（中性别名）"""

from .ai_data_analyst_v6_router import (
    AnalyzeRequest,
    AnalyzeSyncResponse,
    analyze_stream,
    analyze_sync,
    cancel,
    get_analysis_history,
    get_latest_result,
    health,
)

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/ai-data-analyst", tags=["AI Data Analyst"])

router.add_api_route("/analyze", analyze_stream, methods=["POST"], response_class=StreamingResponse)
router.add_api_route("/analyze_sync", analyze_sync, methods=["POST"], response_model=AnalyzeSyncResponse)
router.add_api_route("/health", health, methods=["GET"])
router.add_api_route("/cancel/{session_id}", cancel, methods=["POST"])
router.add_api_route("/result/{session_id}", get_latest_result, methods=["GET"])
router.add_api_route("/history/{session_id}", get_analysis_history, methods=["GET"])

__all__ = [
    "router",
    "AnalyzeRequest",
    "AnalyzeSyncResponse",
]
