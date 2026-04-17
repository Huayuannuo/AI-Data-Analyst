"""
AI Data Analyst Agent V6 - 状态定义

用于编排复杂业务数仓分析全链路：
理解问题 -> 理解Schema -> 推理关系 -> SQL生成 -> 校验执行 -> 自修复 -> 深度分析 -> 输出
"""

from enum import Enum
from typing import TypedDict, Dict, Any, List, Optional


class AnalystPhase(str, Enum):
    """分析阶段"""

    INIT = "init"
    UNDERSTANDING = "understanding"
    WEB_ENRICHMENT = "web_enrichment"
    SCHEMA_DISCOVERY = "schema_discovery"
    RELATION_REASONING = "relation_reasoning"
    SQL_GENERATION = "sql_generation"
    SQL_VALIDATION = "sql_validation"
    SQL_EXECUTION = "sql_execution"
    SQL_REPAIRING = "sql_repairing"
    DEEP_ANALYSIS = "deep_analysis"
    SYNTHESIZING = "synthesizing"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"


class AnalystState(TypedDict):
    """全局状态（Graph 共享内存）"""

    # 基础输入
    query: str
    session_id: str
    phase: str

    # 任务理解与计划
    intent: str
    subject_entity: str
    subject_candidates: List[Dict[str, Any]]
    subject_resolution: Dict[str, Any]
    analysis_plan: Dict[str, Any]
    selected_strategy: str
    evidence_status: Dict[str, Any]
    evidence_sources: List[Dict[str, Any]]
    evidence_summary: str
    web_enabled: bool
    web_top_k: int
    web_context: Dict[str, Any]
    web_sources: List[Dict[str, Any]]
    enhancement_mode: str
    enhancement_agents: List[str]
    enhancement_trace: Dict[str, Any]

    # Schema / 关系推理
    schema_snapshot: Dict[str, Any]
    relation_hypotheses: List[Dict[str, Any]]

    # SQL 生命周期
    candidate_sqls: List[str]
    current_sql: str
    sql_valid: bool
    query_result: Dict[str, Any]
    executed_sqls: List[str]
    sql_errors: List[str]
    retry_count: int
    max_sql_retries: int

    # 分析与输出
    analysis: Dict[str, Any]
    analysis_warnings: List[str]
    analysis_degraded: bool
    final_answer: str
    quality_score: float
    unresolved_issues: int
    critic_feedback: List[Dict[str, Any]]

    # 调试与流式消息
    logs: List[Dict[str, Any]]
    messages: List[Dict[str, Any]]
    metadata: Dict[str, Any]


def create_initial_state(
    query: str,
    session_id: str,
    max_sql_retries: int = 2,
    enable_web_enrichment: bool = False,
    web_top_k: int = 3,
    metadata: Optional[Dict[str, Any]] = None,
) -> AnalystState:
    """创建初始状态"""
    return AnalystState(
        query=query,
        session_id=session_id,
        phase=AnalystPhase.INIT.value,
        intent="auto",
        subject_entity="",
        subject_candidates=[],
        subject_resolution={},
        analysis_plan={},
        selected_strategy="database_sql",
        evidence_status={},
        evidence_sources=[],
        evidence_summary="",
        web_enabled=enable_web_enrichment,
        web_top_k=web_top_k,
        web_context={},
        web_sources=[],
        enhancement_mode=str((metadata or {}).get("enhancement_mode", "none") or "none"),
        enhancement_agents=[],
        enhancement_trace={},
        schema_snapshot={},
        relation_hypotheses=[],
        candidate_sqls=[],
        current_sql="",
        sql_valid=False,
        query_result={},
        executed_sqls=[],
        sql_errors=[],
        retry_count=0,
        max_sql_retries=max_sql_retries,
        analysis={},
        analysis_warnings=[],
        analysis_degraded=False,
        final_answer="",
        quality_score=0.0,
        unresolved_issues=0,
        critic_feedback=[],
        logs=[],
        messages=[],
        metadata=metadata or {},
    )
