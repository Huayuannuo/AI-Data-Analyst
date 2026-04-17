"""
AI Data Analyst Agent V6 - 服务入口

提供：
- SSE 流式执行接口
- 同步执行接口
"""

import json
import logging
import uuid
from typing import Any, AsyncGenerator, Dict, Optional

from .graph import AIDataAnalystGraph

try:
    from config.llm_config import get_config
except ImportError:
    from app.config.llm_config import get_config

logger = logging.getLogger("AIDataAnalystV6Service")


class AIDataAnalystV6Service:
    """课题六服务封装"""
    _latest_results: Dict[str, Dict[str, Any]] = {}

    def __init__(
        self,
        llm_api_key: Optional[str] = None,
        llm_base_url: Optional[str] = None,
        model: Optional[str] = None,
        db_connection_string: Optional[str] = None,
        max_sql_retries: int = 2,
    ):
        config = get_config()

        self.llm_api_key = llm_api_key or config.api_key
        self.llm_base_url = llm_base_url or config.base_url
        self.model = model or config.default_model

        self.graph = AIDataAnalystGraph(
            llm_api_key=self.llm_api_key,
            llm_base_url=self.llm_base_url,
            model=self.model,
            db_connection_string=db_connection_string,
            max_sql_retries=max_sql_retries,
        )

    def _format_sse(self, event: Dict[str, Any]) -> str:
        return f"data: {json.dumps(event, ensure_ascii=False, default=str)}\\n\\n"

    @classmethod
    def get_latest_result(cls, session_id: str) -> Optional[Dict[str, Any]]:
        return cls._latest_results.get(session_id)

    @classmethod
    async def cancel(cls, session_id: str) -> None:
        await AIDataAnalystGraph.request_cancel(session_id)

    @classmethod
    def save_latest_result(cls, session_id: str, payload: Dict[str, Any]) -> None:
        cls._latest_results[session_id] = payload

    async def analyze(
        self,
        query: str,
        session_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AsyncGenerator[str, None]:
        """流式分析（SSE）"""
        if not session_id:
            session_id = str(uuid.uuid4())

        try:
            async for event in self.graph.run(
                query=query,
                session_id=session_id,
                metadata=metadata,
            ):
                yield self._format_sse(event)
        except Exception as e:
            logger.exception("AIDataAnalystV6 analyze failed")
            yield self._format_sse({"type": "error", "content": str(e)})

        yield "data: [DONE]\\n\\n"

    async def analyze_sync(
        self,
        query: str,
        session_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """同步分析（返回聚合结果）"""
        if not session_id:
            session_id = str(uuid.uuid4())

        state = await self.graph.run_sync(query=query, session_id=session_id, metadata=metadata)

        payload = {
            "session_id": session_id,
            "query": query,
            "phase": state.get("phase", ""),
            "intent": state.get("intent", ""),
            "subject_entity": state.get("subject_entity", ""),
            "subject_candidates": state.get("subject_candidates", []),
            "subject_resolution": state.get("subject_resolution", {}),
            "selected_strategy": state.get("selected_strategy", ""),
            "enhancement_mode": state.get("enhancement_mode", "none"),
            "enhancement_agents": state.get("enhancement_agents", []),
            "schema_snapshot": state.get("schema_snapshot", {}),
            "web_context": state.get("web_context", {}),
            "web_sources": state.get("web_sources", []),
            "evidence_status": state.get("evidence_status", {}),
            "evidence_sources": state.get("evidence_sources", []),
            "evidence_summary": state.get("evidence_summary", ""),
            "relation_hypotheses": state.get("relation_hypotheses", []),
            "sql": state.get("current_sql", ""),
            "candidate_sqls": state.get("candidate_sqls", []),
            "query_result": state.get("query_result", {}),
            "analysis": state.get("analysis", {}),
            "analysis_warnings": state.get("analysis_warnings", []),
            "analysis_degraded": state.get("analysis_degraded", False),
            "quality_score": state.get("quality_score", 0.0),
            "unresolved_issues": state.get("unresolved_issues", 0),
            "critic_feedback": state.get("critic_feedback", []),
            "final_answer": state.get("final_answer", ""),
            "sql_errors": state.get("sql_errors", []),
            "retry_count": state.get("retry_count", 0),
            "logs": state.get("logs", []),
        }
        self.save_latest_result(session_id, payload)
        return payload



def create_service(
    llm_api_key: Optional[str] = None,
    llm_base_url: Optional[str] = None,
    model: Optional[str] = None,
    db_connection_string: Optional[str] = None,
    max_sql_retries: int = 2,
) -> AIDataAnalystV6Service:
    """工厂函数：创建课题六服务"""
    return AIDataAnalystV6Service(
        llm_api_key=llm_api_key,
        llm_base_url=llm_base_url,
        model=model,
        db_connection_string=db_connection_string,
        max_sql_retries=max_sql_retries,
    )
