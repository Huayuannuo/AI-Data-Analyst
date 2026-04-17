"""AI Data Analyst 智能分析路由"""

import os
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from core.database import get_db
from models.chat import ChatMessage, ChatSession
from models.knowledge import KnowledgeBase
from models.user import User
from router.auth_router import get_current_user
from service.ai_data_analyst_v6 import AIDataAnalystV6Service
from service.retrieval_service import retrieve_from_knowledge_base

router = APIRouter(prefix="/ai-data-analyst-v6", tags=["AI Data Analyst"])


def _json_dumps_safe(data: Any) -> str:
    """将结果安全序列化为 JSON，兼容 Decimal / datetime 等类型。"""
    return json.dumps(data, ensure_ascii=False, default=str)


def _resolve_db_url() -> Optional[str]:
    db_url = os.getenv("DATABASE_URL", "")
    if db_url:
        return db_url

    pg_host = os.getenv("POSTGRES_HOST", "")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_user = os.getenv("POSTGRES_USER", "")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "")
    pg_db = os.getenv("POSTGRES_DB", "")
    if pg_host and pg_user and pg_db:
        return f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    return None


class AnalyzeRequest(BaseModel):
    query: str = Field(..., description="自然语言分析问题")
    session_id: Optional[str] = Field(None, description="会话ID")
    metadata: Optional[Dict[str, Any]] = Field(None, description="可选上下文，如 schema_snapshot/candidate_sqls")
    max_sql_retries: int = Field(2, ge=1, le=5, description="SQL 最大自修复次数")
    enable_web_enrichment: bool = Field(False, description="是否启用联网增强（默认关闭）")
    enable_original_agents: bool = Field(False, description="是否启用子智能体增强")
    web_top_k: int = Field(3, ge=1, le=10, description="联网增强检索条数")
    data_source_mode: str = Field("database", description="数据源模式：database/frontend_demo")
    save_history: bool = Field(True, description="是否保存到对话历史（需登录）")


class AnalyzeSyncResponse(BaseModel):
    session_id: str
    query: str
    phase: str
    intent: str
    subject_entity: str
    subject_candidates: list
    subject_resolution: Dict[str, Any]
    selected_strategy: str
    enhancement_mode: str
    enhancement_agents: list
    schema_snapshot: Dict[str, Any]
    web_context: Dict[str, Any]
    web_sources: list
    evidence_status: Dict[str, Any]
    evidence_sources: list
    evidence_summary: str
    relation_hypotheses: list
    sql: str
    candidate_sqls: list
    query_result: Dict[str, Any]
    analysis: Dict[str, Any]
    analysis_warnings: list
    analysis_degraded: bool
    quality_score: float
    unresolved_issues: int
    critic_feedback: list
    final_answer: str
    sql_errors: list
    retry_count: int
    logs: list


def _try_parse_uuid(value: Optional[str]) -> Optional[UUID]:
    if not value:
        return None
    try:
        return UUID(value)
    except ValueError:
        return None


def _ensure_analysis_session(
    db: Session,
    current_user: User,
    session_id: str,
    title: str,
) -> ChatSession:
    target_uuid = _try_parse_uuid(session_id)
    if target_uuid:
        existing = db.query(ChatSession).filter(
            ChatSession.id == target_uuid,
            ChatSession.user_id == current_user.id,
        ).first()
        if existing:
            if existing.session_type != "ai_data_analyst":
                existing.session_type = "ai_data_analyst"
                db.commit()
                db.refresh(existing)
            return existing

    session = ChatSession(
        id=target_uuid or uuid.uuid4(),
        user_id=current_user.id,
        title=title,
        session_type="ai_data_analyst",
    )
    db.add(session)
    db.commit()
    db.refresh(session)
    return session


def _save_chat_message(
    db: Session,
    session: ChatSession,
    role: str,
    content: str,
    thinking: Optional[str] = None,
    references_data: Optional[Dict[str, Any]] = None,
) -> None:
    msg = ChatMessage(
        session_id=session.id,
        role=role,
        content=content or "",
        thinking=thinking,
        references_data=references_data,
    )
    db.add(msg)
    session.updated_at = datetime.utcnow()
    if role == "user" and (not session.title or session.title == "新对话"):
        trimmed = (content or "").strip()
        if trimmed:
            session.title = trimmed[:20] + ("..." if len(trimmed) > 20 else "")
    db.commit()


def _build_analysis_evidence_metadata(
    db: Session,
    current_user: Optional[User],
    session_id: Optional[str],
    query: str,
) -> Dict[str, Any]:
    """组装最小证据上下文：历史对话 + 知识库检索结果。"""
    metadata: Dict[str, Any] = {}

    if session_id:
        try:
            session_uuid = UUID(session_id)
            history_messages = db.query(ChatMessage).filter(
                ChatMessage.session_id == session_uuid,
            ).order_by(ChatMessage.created_at.desc()).limit(6).all()
            history_messages = list(reversed(history_messages))
            metadata["session_history_excerpt"] = [
                {
                    "role": msg.role,
                    "content": msg.content,
                    "created_at": msg.created_at.isoformat() if msg.created_at else None,
                }
                for msg in history_messages
            ]
        except Exception:
            metadata["session_history_excerpt"] = []
    else:
        metadata["session_history_excerpt"] = []

    if current_user is None:
        metadata["knowledge_bases"] = []
        metadata["knowledge_evidence"] = []
        metadata["has_knowledge_evidence"] = False
        return metadata

    kb_rows = db.query(KnowledgeBase).filter(
        KnowledgeBase.user_id == current_user.id
    ).order_by(KnowledgeBase.updated_at.desc()).limit(5).all()

    knowledge_evidence: list[Dict[str, Any]] = []
    for kb in kb_rows:
        try:
            docs = retrieve_from_knowledge_base(kb.name, query, top_k=2)
        except Exception:
            docs = []
        if not docs:
            continue
        knowledge_evidence.append(
            {
                "kb_id": str(kb.id),
                "kb_name": kb.name,
                "description": kb.description or "",
                "documents": docs,
            }
        )

    metadata["knowledge_bases"] = [
        {
            "kb_id": str(kb.id),
            "name": kb.name,
            "description": kb.description or "",
            "document_count": kb.document_count or 0,
        }
        for kb in kb_rows
    ]
    metadata["knowledge_evidence"] = knowledge_evidence
    metadata["has_knowledge_evidence"] = bool(knowledge_evidence)

    return metadata


@router.post("/analyze")
async def analyze_stream(
    request: AnalyzeRequest,
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user),
):
    """SSE 流式分析接口"""
    db_url = _resolve_db_url()
    service = AIDataAnalystV6Service(
        db_connection_string=db_url,
        max_sql_retries=request.max_sql_retries,
    )
    effective_session_id = request.session_id or str(uuid.uuid4())

    chat_session: Optional[ChatSession] = None
    if request.save_history and current_user is not None:
        chat_session = _ensure_analysis_session(
            db=db,
            current_user=current_user,
            session_id=effective_session_id,
            title=(request.query[:20] + ("..." if len(request.query) > 20 else "")) or "数据分析对话",
        )
        effective_session_id = str(chat_session.id)
        _save_chat_message(db, chat_session, "user", request.query)

    evidence_metadata = _build_analysis_evidence_metadata(
        db=db,
        current_user=current_user,
        session_id=effective_session_id if (request.session_id or chat_session) else None,
        query=request.query,
    )

    async def _stream():
        final_answer = ""
        final_sql = ""
        final_analysis: Dict[str, Any] = {}
        final_subject = ""
        final_subject_candidates: list = []
        final_subject_resolution: Dict[str, Any] = {}
        final_strategy = ""
        final_enhancement_mode = ""
        final_enhancement_agents: list = []
        final_evidence_status: Dict[str, Any] = {}
        final_analysis_warnings: List[str] = []
        final_analysis_degraded = False
        final_quality_score = 0.0
        final_unresolved_issues = 0
        final_critic_feedback: list = []
        cancelled = False

        async for chunk in service.analyze(
            query=request.query,
            session_id=effective_session_id,
            metadata={
                **(request.metadata or {}),
                "enable_web_enrichment": request.enable_web_enrichment,
                "enable_original_agents": request.enable_original_agents,
                "web_top_k": request.web_top_k,
                "data_source_mode": request.data_source_mode,
                **evidence_metadata,
            },
        ):
            if chunk.startswith("data: "):
                payload = chunk[len("data: "):].strip()
                if payload and payload != "[DONE]":
                    try:
                        event = json.loads(payload)
                        if event.get("type") == "analysis_complete":
                            final_answer = event.get("final_answer", "") or final_answer
                            final_sql = event.get("sql", "") or final_sql
                            final_analysis = event.get("analysis", {}) or final_analysis
                            final_subject = event.get("subject_entity", "") or final_subject
                            final_subject_candidates = event.get("subject_candidates", []) or final_subject_candidates
                            final_subject_resolution = event.get("subject_resolution", {}) or final_subject_resolution
                            final_strategy = event.get("selected_strategy", "") or final_strategy
                            final_enhancement_mode = event.get("enhancement_mode", "") or final_enhancement_mode
                            final_enhancement_agents = event.get("enhancement_agents", []) or final_enhancement_agents
                            final_evidence_status = event.get("evidence_status", {}) or final_evidence_status
                            final_analysis_warnings = event.get("analysis_warnings", []) or final_analysis_warnings
                            final_analysis_degraded = bool(event.get("analysis_degraded", final_analysis_degraded))
                            final_quality_score = float(event.get("quality_score", final_quality_score) or final_quality_score)
                            final_unresolved_issues = int(event.get("unresolved_issues", final_unresolved_issues) or final_unresolved_issues)
                            final_critic_feedback = event.get("critic_feedback", []) or final_critic_feedback
                        if event.get("type") == "analysis_cancelled":
                            cancelled = True
                    except Exception:
                        pass

            yield chunk

        if chat_session:
            if cancelled:
                assistant_text = "任务已取消"
            else:
                assistant_text = final_answer or "分析完成（未返回 final_answer）"
            _save_chat_message(
                db,
                chat_session,
                "assistant",
                assistant_text,
                thinking=_json_dumps_safe(
                    {
                        "sql": final_sql,
                        "analysis": final_analysis,
                        "subject": final_subject,
                        "subject_candidates": final_subject_candidates,
                        "subject_resolution": final_subject_resolution,
                        "strategy": final_strategy,
                        "enhancement_mode": final_enhancement_mode,
                        "enhancement_agents": final_enhancement_agents,
                        "evidence_status": final_evidence_status,
                        "analysis_warnings": final_analysis_warnings,
                        "analysis_degraded": final_analysis_degraded,
                        "quality_score": final_quality_score,
                        "unresolved_issues": final_unresolved_issues,
                        "critic_feedback": final_critic_feedback,
                    }
                ),
                references_data={
                    "sql": final_sql,
                    "analysis": final_analysis,
                    "subject_entity": final_subject,
                    "subject_candidates": final_subject_candidates,
                    "subject_resolution": final_subject_resolution,
                    "selected_strategy": final_strategy,
                    "enhancement_mode": final_enhancement_mode,
                    "enhancement_agents": final_enhancement_agents,
                    "evidence_status": final_evidence_status,
                    "analysis_warnings": final_analysis_warnings,
                    "analysis_degraded": final_analysis_degraded,
                    "quality_score": final_quality_score,
                    "unresolved_issues": final_unresolved_issues,
                    "critic_feedback": final_critic_feedback,
                },
            )

    return StreamingResponse(_stream(), media_type="text/event-stream")


@router.post("/analyze_sync", response_model=AnalyzeSyncResponse)
async def analyze_sync(
    request: AnalyzeRequest,
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user),
):
    """同步分析接口"""
    db_url = _resolve_db_url()
    service = AIDataAnalystV6Service(
        db_connection_string=db_url,
        max_sql_retries=request.max_sql_retries,
    )
    effective_session_id = request.session_id or str(uuid.uuid4())

    chat_session: Optional[ChatSession] = None
    if request.save_history and current_user is not None:
        chat_session = _ensure_analysis_session(
            db=db,
            current_user=current_user,
            session_id=effective_session_id,
            title=(request.query[:20] + ("..." if len(request.query) > 20 else "")) or "数据分析对话",
        )
        effective_session_id = str(chat_session.id)
        _save_chat_message(db, chat_session, "user", request.query)

    evidence_metadata = _build_analysis_evidence_metadata(
        db=db,
        current_user=current_user,
        session_id=effective_session_id if (request.session_id or chat_session) else None,
        query=request.query,
    )

    result = await service.analyze_sync(
        query=request.query,
        session_id=effective_session_id,
        metadata={
            **(request.metadata or {}),
                "enable_web_enrichment": request.enable_web_enrichment,
                "enable_original_agents": request.enable_original_agents,
                "web_top_k": request.web_top_k,
                "data_source_mode": request.data_source_mode,
                **evidence_metadata,
            },
        )
    if chat_session:
        _save_chat_message(
            db,
            chat_session,
            "assistant",
            result.get("final_answer", "") or "分析完成",
            thinking=_json_dumps_safe(
                {
                    "intent": result.get("intent"),
                    "sql": result.get("sql"),
                    "analysis": result.get("analysis"),
                    "logs": result.get("logs", []),
                    "query_result": result.get("query_result"),
                    "web_context": result.get("web_context"),
                    "enhancement_mode": result.get("enhancement_mode", ""),
                    "enhancement_agents": result.get("enhancement_agents", []),
                    "quality_score": result.get("quality_score", 0.0),
                    "unresolved_issues": result.get("unresolved_issues", 0),
                    "critic_feedback": result.get("critic_feedback", []),
                }
            ),
            references_data={
                "candidate_sqls": result.get("candidate_sqls", []),
                "sql_errors": result.get("sql_errors", []),
                "retry_count": result.get("retry_count", 0),
                "phase": result.get("phase", ""),
                "selected_strategy": result.get("selected_strategy", ""),
                "enhancement_mode": result.get("enhancement_mode", ""),
                "enhancement_agents": result.get("enhancement_agents", []),
                "subject_entity": result.get("subject_entity", ""),
                "subject_candidates": result.get("subject_candidates", []),
                "subject_resolution": result.get("subject_resolution", {}),
                "evidence_status": result.get("evidence_status", {}),
                "analysis_warnings": result.get("analysis_warnings", []),
                "analysis_degraded": result.get("analysis_degraded", False),
                "quality_score": result.get("quality_score", 0.0),
                "unresolved_issues": result.get("unresolved_issues", 0),
                "critic_feedback": result.get("critic_feedback", []),
            },
        )
    return AnalyzeSyncResponse(**result)


@router.get("/health")
async def health() -> Dict[str, Any]:
    return {"status": "ok", "service": "ai_data_analyst"}


@router.post("/cancel/{session_id}")
async def cancel(session_id: str) -> Dict[str, Any]:
    await AIDataAnalystV6Service.cancel(session_id)
    return {"success": True, "session_id": session_id, "message": "已发起取消请求"}


@router.get("/result/{session_id}")
async def get_latest_result(session_id: str) -> Dict[str, Any]:
    payload = AIDataAnalystV6Service.get_latest_result(session_id)
    if not payload:
        raise HTTPException(status_code=404, detail="未找到该会话的分析结果")
    return payload


@router.get("/history/{session_id}")
async def get_analysis_history(
    session_id: str,
    current_user: Optional[User] = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    if current_user is None:
        raise HTTPException(status_code=401, detail="请先登录")

    session_uuid = _try_parse_uuid(session_id)
    if not session_uuid:
        raise HTTPException(status_code=400, detail="无效的会话ID")

    session = db.query(ChatSession).filter(
        ChatSession.id == session_uuid,
        ChatSession.user_id == current_user.id,
    ).first()
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")

    messages = db.query(ChatMessage).filter(
        ChatMessage.session_id == session_uuid
    ).order_by(ChatMessage.created_at.asc()).all()

    return {
        "session_id": session_id,
        "title": session.title,
        "session_type": session.session_type,
        "messages": [
            {
                "id": str(m.id),
                "role": m.role,
                "content": m.content,
                "thinking": m.thinking,
                "references_data": m.references_data,
                "created_at": m.created_at.isoformat() if m.created_at else None,
            }
            for m in messages
        ],
    }
