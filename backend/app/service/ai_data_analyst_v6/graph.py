"""
AI Data Analyst 智能分析工作流

课题六目标：
1. Schema 理解
2. 表关联推理
3. SQL 生成与执行
4. 错误自修复
5. 深度分析与综合输出
"""

import asyncio
import logging
import json
import os
import re
from datetime import datetime, date
from itertools import combinations
from typing import Any, AsyncGenerator, Dict, List, Literal, Optional
import httpx

# LangGraph 可选依赖
try:
    from langgraph.graph import END, StateGraph
    LANGGRAPH_AVAILABLE = True
except ImportError:
    LANGGRAPH_AVAILABLE = False

from .state import AnalystPhase, AnalystState, create_initial_state

try:
    from service.text2sql_service import Text2SQLService
    from service.smart_analyzer import SmartDataAnalyzer
    from service.database_explorer import DatabaseExplorer
    from service.deep_research_v2.agents import ChiefArchitect, DeepScout, LeadWriter, CriticMaster
    from service.deep_research_v2.state import create_initial_state as create_research_initial_state
    ORIGINAL_AGENTS_AVAILABLE = True
except ImportError:
    from app.service.text2sql_service import Text2SQLService
    from app.service.smart_analyzer import SmartDataAnalyzer
    from app.service.database_explorer import DatabaseExplorer
    try:
        from app.service.deep_research_v2.agents import ChiefArchitect, DeepScout, LeadWriter, CriticMaster
        from app.service.deep_research_v2.state import create_initial_state as create_research_initial_state
        ORIGINAL_AGENTS_AVAILABLE = True
    except ImportError:
        ChiefArchitect = DeepScout = LeadWriter = CriticMaster = None
        create_research_initial_state = None
        ORIGINAL_AGENTS_AVAILABLE = False

try:
    from sqlalchemy.orm import sessionmaker
except ImportError:
    sessionmaker = None

try:
    from config.llm_config import get_config
except ImportError:
    from app.config.llm_config import get_config

logger = logging.getLogger("AIDataAnalystGraph")


class AIDataAnalystGraph:
    """课题六全链路分析工作流图"""
    _cancelled_sessions: set[str] = set()
    _cancel_lock = asyncio.Lock()
    DATA_SOURCE_DATABASE = "database"
    DATA_SOURCE_FRONTEND_DEMO = "frontend_demo"

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
        self.db_connection_string = db_connection_string
        self.max_sql_retries = max_sql_retries
        self.search_api_key = os.getenv("BOCHA_API_KEY", "") or config.search_api_key

        self.text2sql = Text2SQLService(
            llm_api_key=self.llm_api_key,
            llm_base_url=self.llm_base_url,
            db_connection_string=self.db_connection_string,
            model=self.model,
        )
        self.data_analyzer = SmartDataAnalyzer()
        self.db_engine = self.text2sql.db_engine
        self._original_agent_bundle: Optional[Dict[str, Any]] = None

        self.graph = self._build_langgraph() if LANGGRAPH_AVAILABLE else None

    def _build_langgraph(self):
        """构建 LangGraph 状态图（可选）"""
        workflow = StateGraph(AnalystState)

        workflow.add_node("understand", self._understand_node)
        workflow.add_node("web_enrichment", self._web_enrichment_node)
        workflow.add_node("discover_schema", self._discover_schema_node)
        workflow.add_node("evidence_discovery", self._discover_evidence_node)
        workflow.add_node("reason_relations", self._reason_relations_node)
        workflow.add_node("generate_sql", self._generate_sql_node)
        workflow.add_node("validate_sql", self._validate_sql_node)
        workflow.add_node("execute_sql", self._execute_sql_node)
        workflow.add_node("repair_sql", self._repair_sql_node)
        workflow.add_node("deep_analyze", self._deep_analyze_node)
        workflow.add_node("synthesize", self._synthesize_node)
        workflow.add_node("complete", self._complete_node)
        workflow.add_node("failed", self._failed_node)

        workflow.set_entry_point("understand")

        workflow.add_edge("understand", "web_enrichment")
        workflow.add_edge("web_enrichment", "discover_schema")
        workflow.add_edge("discover_schema", "evidence_discovery")
        workflow.add_conditional_edges(
            "evidence_discovery",
            self._route_after_evidence,
            {
                "reason_relations": "reason_relations",
                "deep_analyze": "deep_analyze",
            },
        )
        workflow.add_edge("reason_relations", "generate_sql")
        workflow.add_edge("generate_sql", "validate_sql")

        workflow.add_conditional_edges(
            "validate_sql",
            self._route_after_validate,
            {
                "execute_sql": "execute_sql",
                "repair_sql": "repair_sql",
            },
        )

        workflow.add_conditional_edges(
            "execute_sql",
            self._route_after_execute,
            {
                "deep_analyze": "deep_analyze",
                "repair_sql": "repair_sql",
            },
        )

        workflow.add_conditional_edges(
            "repair_sql",
            self._route_after_repair,
            {
                "validate_sql": "validate_sql",
                "failed": "failed",
            },
        )

        workflow.add_edge("deep_analyze", "synthesize")
        workflow.add_edge("synthesize", "complete")
        workflow.add_edge("complete", END)
        workflow.add_edge("failed", END)

        return workflow.compile()

    def _emit_message(self, state: AnalystState, phase: str, content: str, message_type: str = "node") -> None:
        event = {
            "type": message_type,
            "phase": phase,
            "content": content,
            "timestamp": datetime.now().isoformat(),
        }
        state["messages"].append(event)
        state["logs"].append(event)

    @classmethod
    async def request_cancel(cls, session_id: str) -> None:
        if not session_id:
            return
        async with cls._cancel_lock:
            cls._cancelled_sessions.add(session_id)

    @classmethod
    async def clear_cancel(cls, session_id: str) -> None:
        if not session_id:
            return
        async with cls._cancel_lock:
            cls._cancelled_sessions.discard(session_id)

    @classmethod
    async def is_cancelled(cls, session_id: str) -> bool:
        if not session_id:
            return False
        async with cls._cancel_lock:
            return session_id in cls._cancelled_sessions

    async def _check_cancelled(self, state: AnalystState) -> bool:
        session_id = state.get("session_id", "")
        if not await self.is_cancelled(session_id):
            return False
        state["phase"] = AnalystPhase.CANCELLED.value
        self._emit_message(state, state["phase"], "任务已取消", message_type="analysis_cancelled")
        return True

    def _infer_intent(self, query: str) -> str:
        q = (query or "").lower()
        trend_keywords = ["趋势", "变化", "同比", "环比", "trend", "over time"]
        compare_keywords = ["对比", "比较", "排名", "vs", "versus", "top"]
        detail_keywords = ["明细", "详情", "list", "哪些", "清单"]

        if any(k in q for k in trend_keywords):
            return "trend"
        if any(k in q for k in compare_keywords):
            return "comparison"
        if any(k in q for k in detail_keywords):
            return "detail"
        return "stats"

    def _mark_failed(self, state: AnalystState, reason: str) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.FAILED.value
        if reason and reason not in state["sql_errors"]:
            state["sql_errors"].append(reason)
        state["final_answer"] = f"分析失败: {reason}"
        self._emit_message(state, state["phase"], reason, message_type="error")
        return state

    def _data_source_mode(self, state: AnalystState) -> str:
        mode = (state.get("metadata", {}) or {}).get("data_source_mode", self.DATA_SOURCE_DATABASE)
        if mode == self.DATA_SOURCE_FRONTEND_DEMO:
            return self.DATA_SOURCE_FRONTEND_DEMO
        return self.DATA_SOURCE_DATABASE

    def _enhancement_mode(self, state: AnalystState) -> str:
        metadata = state.get("metadata", {}) or {}
        mode = metadata.get("enhancement_mode")
        if mode:
            return str(mode)
        if metadata.get("enable_original_agents"):
            return "original_agents"
        return str(state.get("enhancement_mode") or "none")

    def _original_agents_enabled(self, state: AnalystState) -> bool:
        return self._enhancement_mode(state) == "original_agents"

    def _agent_enhancement_list(self) -> List[str]:
        return ["总架构师", "深度侦探", "首席笔杆", "毒舌评论家"]

    def _get_original_agent_bundle(self) -> Dict[str, Any]:
        if self._original_agent_bundle is not None:
            return self._original_agent_bundle
        if not ORIGINAL_AGENTS_AVAILABLE:
            self._original_agent_bundle = {}
            return self._original_agent_bundle

        config = get_config()
        try:
            bundle = {
                "architect": ChiefArchitect(
                    self.llm_api_key,
                    self.llm_base_url,
                    config.agents.architect.model,
                ),
                "scout": DeepScout(
                    self.llm_api_key,
                    self.llm_base_url,
                    self.search_api_key,
                    config.agents.scout.model,
                ),
                "writer": LeadWriter(
                    self.llm_api_key,
                    self.llm_base_url,
                    config.agents.writer.model,
                ),
                "critic": CriticMaster(
                    self.llm_api_key,
                    self.llm_base_url,
                    config.agents.critic.model,
                ),
            }
        except Exception as e:
            logger.warning(f"智能体捆绑初始化失败: {e}")
            bundle = {}

        self._original_agent_bundle = bundle
        return self._original_agent_bundle

    def _build_original_research_state(
        self,
        state: AnalystState,
        phase: str = "init",
        search_web: Optional[bool] = None,
        search_local: Optional[bool] = None,
    ):
        if not create_research_initial_state:
            return None

        metadata = state.get("metadata", {}) or {}
        research_state = create_research_initial_state(
            query=state.get("query", ""),
            session_id=state.get("session_id", ""),
            search_web=bool(state.get("web_enabled", False) if search_web is None else search_web),
            search_local=bool(search_local if search_local is not None else metadata.get("has_knowledge_evidence", False)),
        )
        research_state["phase"] = phase
        research_state["max_iterations"] = 1
        research_state["query"] = state.get("query", "")
        return research_state

    def _fallback_sections_from_query(self, query: str) -> List[Dict[str, Any]]:
        title_base = query.strip()[:16] or "研究"
        return [
            {
                "id": "sec_1",
                "title": f"{title_base}概况",
                "description": "主体与整体概况",
                "section_type": "mixed",
                "status": "pending",
                "requires_data": True,
                "requires_chart": False,
                "search_queries": [query],
            },
            {
                "id": "sec_2",
                "title": f"{title_base}竞争格局",
                "description": "重点公司和市场表现",
                "section_type": "mixed",
                "status": "pending",
                "requires_data": True,
                "requires_chart": False,
                "search_queries": [f"{query} 重点公司", f"{query} 市场规模"],
            },
            {
                "id": "sec_3",
                "title": f"{title_base}技术与趋势",
                "description": "技术、政策与趋势",
                "section_type": "qualitative",
                "status": "pending",
                "requires_data": False,
                "requires_chart": False,
                "search_queries": [f"{query} 技术", f"{query} 政策"],
            },
        ]

    def _normalize_subject_phrase(self, text: str) -> str:
        """把问题中的主体短语归一化成更稳定的业务主体。"""
        subject = (text or "").strip()
        if not subject:
            return ""

        subject = subject.strip("“”\"'`")
        subject = re.sub(r"^[\s，。；、:：-]+", "", subject)

        prefix_patterns = [
            r"^(请帮我分析一下|请帮我分析|请分析一下|请分析|帮我分析一下|帮我分析|帮忙分析|请你分析|请你帮我分析|请你帮我|请你|请帮我|帮我|我想|想要|了解|研究|解读|查看|看看|看下|评估|梳理|判断|比较|分析一下|分析)",
        ]
        for pattern in prefix_patterns:
            subject = re.sub(pattern, "", subject).strip()

        suffix_patterns = [
            r"(?:的)?(?:现状|情况|趋势|市场规模|营收|对比|分析|研究|发展|格局|表现|概况|数据|画像|特征|应用|业务)$",
            r"(?:的)?(?:主要企业|重点公司|核心公司)$",
        ]
        changed = True
        while changed and subject:
            changed = False
            before = subject
            for pattern in suffix_patterns:
                subject = re.sub(pattern, "", subject).strip()
            if subject != before:
                changed = True

        subject = subject.strip("的之与和及、，。；:：- ")
        return subject

    def _generate_subject_candidates(self, query: str, history_text: str = "") -> List[str]:
        """从 query / 历史中生成主体候选。"""
        seeds: List[str] = []
        seen_seeds = set()
        for source_text in (query, history_text):
            if not source_text:
                continue
            raw = self._extract_query_subject(source_text)
            normalized = self._normalize_subject_phrase(raw or source_text)
            for item in (raw, normalized):
                item = (item or "").strip()
                if not item or item in seen_seeds:
                    continue
                seen_seeds.add(item)
                seeds.append(item)

        candidates: List[str] = []
        seen = set()
        for seed in seeds:
            for item in (seed, self._normalize_subject_phrase(seed)):
                item = (item or "").strip()
                if item and item not in seen:
                    seen.add(item)
                    candidates.append(item)
                for derived in self._subject_terms(item):
                    derived = (derived or "").strip()
                    if derived and derived not in seen:
                        seen.add(derived)
                        candidates.append(derived)

        return candidates

    def _rank_subject_candidate(self, candidate: str, query: str, support: Dict[str, Any]) -> Dict[str, Any]:
        """对主体候选做简单打分，便于反查选择。"""
        matches = support.get("matches", []) if isinstance(support, dict) else []
        support_count = 0
        if isinstance(matches, list):
            for item in matches:
                if not isinstance(item, dict):
                    continue
                support_count += int(item.get("count", 0) or 0)

        normalized = self._normalize_subject_phrase(candidate)
        query_text = query or ""
        score = 0.0
        if normalized:
            score += 0.2
        if candidate and candidate in query_text:
            score += 0.2
        if support.get("supported"):
            score += 1.0
        score += min(support_count / 100.0, 0.8)
        score += max(0.0, 0.12 - min(len(candidate), 12) * 0.005)

        return {
            "candidate": candidate,
            "normalized": normalized,
            "supported": bool(support.get("supported")),
            "support_count": support_count,
            "score": round(score, 4),
            "matches": matches,
        }

    def _resolve_subject_entity(self, query: str, history_text: str = "") -> Dict[str, Any]:
        """主体识别：候选 -> 归一化 -> 数据库反查验证。"""
        candidates = self._generate_subject_candidates(query, history_text)
        if not candidates:
            return {
                "subject_entity": "",
                "subject_candidates": [],
                "subject_resolution": {
                    "chosen": "",
                    "reason": "no candidate",
                    "candidates": [],
                    "supported": False,
                },
            }

        ranking: List[Dict[str, Any]] = []
        for candidate in candidates:
            support = self._probe_subject_database_support(candidate)
            ranking.append(self._rank_subject_candidate(candidate, query, support))

        ranking.sort(
            key=lambda item: (
                1 if item.get("supported") else 0,
                item.get("support_count", 0),
                item.get("score", 0.0),
                -len(item.get("candidate", "")),
            ),
            reverse=True,
        )

        chosen_item = ranking[0]
        chosen = chosen_item.get("normalized") or chosen_item.get("candidate") or ""
        if not chosen:
            chosen = candidates[0]

        return {
            "subject_entity": chosen,
            "subject_candidates": ranking,
            "subject_resolution": {
                "chosen": chosen,
                "reason": "database_supported" if chosen_item.get("supported") else "best_effort",
                "supported": bool(chosen_item.get("supported")),
                "support_count": chosen_item.get("support_count", 0),
                "score": chosen_item.get("score", 0.0),
                "candidates": ranking,
            },
        }

    def _extract_query_subject(self, query: str) -> str:
        q = (query or "").strip()
        if not q:
            return ""

        patterns = [
            r"([^\s，。；、]{1,20}?行业)(?:的|现状|情况|趋势|市场规模|营收|发展|分析)?",
            r"分析([^\s，。；、]{1,20}?)(近\d+年|近三年|近两年|近一年|近|过去|最近)",
            r"分析([^\s，。；、]{1,20}?)市场规模",
            r"([^\s，。；、]{1,20}?)市场规模",
            r"关于([^\s，。；、]{1,20}?)的",
        ]
        subject = ""
        for pattern in patterns:
            match = re.search(pattern, q)
            if match:
                subject = (match.group(1) or "").strip()
                break

        subject = re.sub(r"^(请|帮我|一下|一下子|我想|想要|请帮我|请你)", "", subject).strip()
        generic_words = {
            "行业", "市场", "市场规模", "趋势", "对比", "营收", "重点公司", "公司", "数据", "情况"
        }
        if not subject or subject in generic_words:
            return ""
        return subject

    def _subject_terms(self, subject: str) -> List[str]:
        subject = (subject or "").strip()
        if not subject:
            return []

        candidates = [subject]
        for suffix in ("行业", "产业", "领域"):
            if subject.endswith(suffix) and len(subject) > len(suffix):
                candidates.append(subject[: -len(suffix)])

        for keyword in ("行业", "产业", "领域"):
            if keyword in subject and subject != keyword:
                candidates.append(subject.replace(keyword, ""))

        cleaned = []
        seen = set()
        for item in candidates:
            item = item.strip()
            if not item or item in seen:
                continue
            seen.add(item)
            cleaned.append(item)
        return cleaned

    def _history_text(self, state: AnalystState) -> str:
        history = (state.get("metadata", {}) or {}).get("session_history_excerpt", [])
        if not isinstance(history, list):
            return ""
        parts = []
        for item in history[-6:]:
            if not isinstance(item, dict):
                continue
            role = item.get("role", "")
            content = item.get("content", "")
            if content:
                parts.append(f"{role}: {content}")
        return "\n".join(parts)

    def _knowledge_evidence(self, state: AnalystState) -> List[Dict[str, Any]]:
        metadata = state.get("metadata", {}) or {}
        evidence = metadata.get("knowledge_evidence", [])
        if isinstance(evidence, list):
            return [item for item in evidence if isinstance(item, dict)]
        return []

    def _knowledge_evidence_text(self, state: AnalystState) -> str:
        chunks: List[str] = []
        for kb in self._knowledge_evidence(state):
            kb_name = kb.get("kb_name", "")
            documents = kb.get("documents", [])
            if not isinstance(documents, list):
                continue
            for doc in documents[:3]:
                if not isinstance(doc, dict):
                    continue
                title = doc.get("title") or doc.get("document_name") or ""
                snippet = doc.get("content_with_weight") or doc.get("content") or ""
                if title or snippet:
                    chunks.append(f"[{kb_name}] {title}: {snippet}")
        return "\n".join(chunks[:12])

    def _probe_subject_candidates_support(self, candidates: List[str]) -> Dict[str, Any]:
        """检查当前数据库是否真的有主体相关事实数据。"""
        normalized_candidates: List[str] = []
        seen = set()
        for candidate in candidates or []:
            for term in self._subject_terms(candidate):
                term = (term or "").strip()
                if not term or term in seen:
                    continue
                seen.add(term)
                normalized_candidates.append(term)

        if not normalized_candidates or not self.db_engine:
            return {
                "supported": False,
                "matches": [],
                "reason": "no subject or db unavailable",
            }

        from sqlalchemy import text

        supports = []
        try:
            with self.db_engine.connect() as conn:
                for term in normalized_candidates:
                    for table_name, column_name in (
                        ("industry_stats", "industry_name"),
                        ("company_data", "industry"),
                        ("policy_data", "industry"),
                    ):
                        try:
                            sql = text(
                                f"SELECT COUNT(*) FROM {table_name} "
                                f"WHERE {column_name} ILIKE :term"
                            )
                            count = conn.execute(sql, {"term": f"%{term}%"}).scalar() or 0
                            if int(count) > 0:
                                supports.append(
                                    {
                                        "table": table_name,
                                        "column": column_name,
                                        "term": term,
                                        "count": int(count),
                                    }
                                )
                        except Exception:
                            continue
        except Exception as e:
            return {
                "supported": False,
                "matches": [],
                "reason": str(e),
            }

        return {
            "supported": bool(supports),
            "matches": supports,
            "reason": "matched" if supports else "no matched rows",
        }

    def _probe_subject_database_support(self, subject: str) -> Dict[str, Any]:
        """兼容旧调用：对单一主体做数据库支持度检查。"""
        return self._probe_subject_candidates_support([subject] if subject else [])

    def _subject_lock_instruction(self, subject: str) -> str:
        if not subject:
            return ""
        return (
            f"强约束：SQL 必须围绕问题主体“{subject}”，"
            "不得替换为其他固定行业词、默认行业或示例中的行业词。"
        )

    def _safe_number(self, value: Any) -> Optional[float]:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            text = value.replace(",", "").strip()
            if not text:
                return None
            try:
                return float(text)
            except ValueError:
                return None
        return None

    def _is_time_key(self, key: str) -> bool:
        lname = (key or "").lower()
        return any(token in lname for token in ("year", "quarter", "month", "date", "time", "period"))

    def _safe_time_sort_key(self, row: Dict[str, Any]) -> tuple:
        year = self._safe_number(row.get("year"))
        quarter = self._safe_number(row.get("quarter"))
        month = self._safe_number(row.get("month"))

        if year is not None:
            return (0, int(year), int(quarter or 0), int(month or 0))

        for key in ("publish_date", "effective_date", "date", "time", "created_at", "updated_at"):
            value = row.get(key)
            if value in (None, ""):
                continue
            if hasattr(value, "isoformat"):
                return (1, value.isoformat())
            return (1, str(value))

        if month is not None:
            return (2, int(month))
        if quarter is not None:
            return (3, int(quarter))

        for key, value in row.items():
            if not self._is_time_key(key):
                continue
            if value in (None, ""):
                continue
            if hasattr(value, "isoformat"):
                return (4, value.isoformat())
            numeric = self._safe_number(value)
            if numeric is not None:
                return (5, numeric)
            return (6, str(value))

        return (9, "")

    def _classify_analysis_rows(self, rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        market_rows: List[Dict[str, Any]] = []
        company_rows: List[Dict[str, Any]] = []
        other_rows: List[Dict[str, Any]] = []

        for row in rows:
            analysis_type = str(row.get("analysis_type", "") or "")
            company_name = row.get("company_name")
            revenue = row.get("revenue")
            metric_name = str(row.get("metric_name", "") or "")
            market_value = row.get("market_size", row.get("metric_value"))

            if company_name is not None or revenue is not None or "公司营收" in analysis_type:
                company_rows.append(row)
                continue
            if market_value is not None or "市场规模" in analysis_type or "市场规模" in metric_name:
                market_rows.append(row)
                continue
            other_rows.append(row)

        return {
            "market_rows": market_rows,
            "company_rows": company_rows,
            "other_rows": other_rows,
        }

    def _analyze_trend_rows(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not rows:
            return {
                "success": False,
                "insights": [],
                "statistics": {},
                "visualization_hint": "none",
                "error": "趋势分析没有可用数据",
            }

        value_candidates = ("metric_value", "market_size", "revenue", "value", "amount", "count", "profit", "market_share", "net_profit", "gross_margin")
        value_col = None
        best_count = 0
        for candidate in value_candidates:
            count = sum(1 for row in rows if self._safe_number(row.get(candidate)) is not None)
            if count > best_count:
                value_col = candidate
                best_count = count

        if not value_col or best_count == 0:
            return {
                "success": False,
                "insights": [],
                "statistics": {},
                "visualization_hint": "none",
                "error": "未找到可用于趋势分析的数值字段",
            }

        points: List[Dict[str, Any]] = []
        for row in rows:
            value = self._safe_number(row.get(value_col))
            if value is None:
                continue
            time_key = self._safe_time_sort_key(row)
            points.append({
                "time_key": time_key,
                "time_label": row.get("year") or row.get("publish_date") or row.get("date") or row.get("month") or row.get("quarter") or row.get("time") or "",
                "value": value,
            })

        if len(points) < 2:
            return {
                "success": False,
                "insights": [],
                "statistics": {},
                "visualization_hint": "none",
                "error": "趋势分析样本不足",
            }

        points.sort(key=lambda item: item["time_key"])
        values = [item["value"] for item in points]
        first_val = values[0]
        last_val = values[-1]
        change = last_val - first_val
        change_pct = (change / first_val * 100) if first_val not in (0, None) else 0
        trend = "上升" if change > 0 else "下降" if change < 0 else "持平"

        insights = [f"整体呈{trend}趋势，变化幅度 {abs(change_pct):.1f}%"]
        if len(values) > 2:
            growth_rates = []
            for i in range(1, len(values)):
                prev = values[i - 1]
                curr = values[i]
                if prev in (0, None):
                    continue
                growth_rates.append((curr - prev) / prev * 100)
            if growth_rates:
                avg_growth = sum(growth_rates) / len(growth_rates)
                insights.append(f"平均增长率 {avg_growth:.1f}%")

        return {
            "success": True,
            "insights": insights,
            "statistics": {
                "field": value_col,
                "start_value": first_val,
                "end_value": last_val,
                "total_change": change,
                "change_percent": change_pct,
                "trend": trend,
                "data_points": len(values),
            },
            "visualization_hint": "line",
        }

    def _analyze_comparison_rows(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not rows:
            return {
                "success": False,
                "insights": [],
                "statistics": {},
                "visualization_hint": "none",
                "error": "对比分析没有可用数据",
            }

        category_candidates = ("company_name", "industry_name", "metric_name", "region", "category", "type", "name")
        value_candidates = ("revenue", "metric_value", "market_size", "value", "amount", "count", "profit", "market_share", "net_profit", "gross_margin")

        cat_col = None
        cat_count = 0
        for candidate in category_candidates:
            count = sum(1 for row in rows if row.get(candidate) not in (None, ""))
            if count > cat_count:
                cat_col = candidate
                cat_count = count

        value_col = None
        value_count = 0
        for candidate in value_candidates:
            count = sum(1 for row in rows if self._safe_number(row.get(candidate)) is not None)
            if count > value_count:
                value_col = candidate
                value_count = count

        if not cat_col or not value_col or value_count == 0:
            return {
                "success": False,
                "insights": [],
                "statistics": {},
                "visualization_hint": "none",
                "error": "未找到可用于对比分析的分类字段或数值字段",
            }

        grouped: Dict[str, List[tuple]] = {}
        for row in rows:
            category = row.get(cat_col)
            value = self._safe_number(row.get(value_col))
            if category in (None, "") or value is None:
                continue
            grouped.setdefault(str(category), []).append((self._safe_time_sort_key(row), value))

        if not grouped:
            return {
                "success": False,
                "insights": [],
                "statistics": {},
                "visualization_hint": "none",
                "error": "对比分析样本不足",
            }

        latest_values: Dict[str, float] = {}
        for category, items in grouped.items():
            items.sort(key=lambda item: item[0])
            latest_values[category] = items[-1][1]

        sorted_items = sorted(latest_values.items(), key=lambda item: item[1], reverse=True)
        total = sum(v for _, v in sorted_items)

        insights = [f"最高: {sorted_items[0][0]} ({sorted_items[0][1]:.2f})"]
        if len(sorted_items) > 1:
            insights.append(f"最低: {sorted_items[-1][0]} ({sorted_items[-1][1]:.2f})")
        if total > 0:
            top_share = sorted_items[0][1] / total * 100
            insights.append(f"{sorted_items[0][0]} 占比 {top_share:.1f}%")

        return {
            "success": True,
            "insights": insights,
            "statistics": {
                "field": value_col,
                "category_field": cat_col,
                "categories": len(sorted_items),
                "category_values": latest_values,
                "total": total,
            },
            "visualization_hint": "bar" if len(sorted_items) > 1 else "table",
        }

    def _analyze_rows(self, rows: List[Dict[str, Any]], intent: str) -> Dict[str, Any]:
        buckets = self._classify_analysis_rows(rows)
        market_rows = buckets["market_rows"]
        company_rows = buckets["company_rows"]
        other_rows = buckets["other_rows"]

        insights: List[str] = []
        statistics: Dict[str, Any] = {}
        warnings: List[str] = []
        hints: List[str] = []
        success = False

        if intent == "trend":
            market_analysis = self._analyze_trend_rows(market_rows or rows)
            if market_analysis.get("success"):
                success = True
                insights.extend(market_analysis.get("insights", []))
                statistics["market_trend"] = market_analysis.get("statistics", {})
                hints.append(market_analysis.get("visualization_hint", "none"))
            elif market_rows:
                warnings.append(market_analysis.get("error", "市场趋势分析失败"))

            if company_rows:
                company_analysis = self._analyze_comparison_rows(company_rows)
                if company_analysis.get("success"):
                    success = True
                    insights.extend(company_analysis.get("insights", []))
                    statistics["company_comparison"] = company_analysis.get("statistics", {})
                    hints.append(company_analysis.get("visualization_hint", "none"))
                else:
                    warnings.append(company_analysis.get("error", "公司对比分析失败"))

        elif intent == "comparison":
            company_analysis = self._analyze_comparison_rows(company_rows or rows)
            if company_analysis.get("success"):
                success = True
                insights.extend(company_analysis.get("insights", []))
                statistics["company_comparison"] = company_analysis.get("statistics", {})
                hints.append(company_analysis.get("visualization_hint", "none"))
            elif company_rows:
                warnings.append(company_analysis.get("error", "公司对比分析失败"))

            if market_rows:
                market_analysis = self._analyze_trend_rows(market_rows)
                if market_analysis.get("success"):
                    success = True
                    insights.extend(market_analysis.get("insights", []))
                    statistics["market_trend"] = market_analysis.get("statistics", {})
                    hints.append(market_analysis.get("visualization_hint", "none"))

        else:
            general_data = market_rows or company_rows or rows
            market_analysis = self._analyze_trend_rows(general_data)
            if market_analysis.get("success"):
                success = True
                insights.extend(market_analysis.get("insights", []))
                statistics["trend"] = market_analysis.get("statistics", {})
                hints.append(market_analysis.get("visualization_hint", "none"))
            else:
                company_analysis = self._analyze_comparison_rows(general_data)
                if company_analysis.get("success"):
                    success = True
                    insights.extend(company_analysis.get("insights", []))
                    statistics["comparison"] = company_analysis.get("statistics", {})
                    hints.append(company_analysis.get("visualization_hint", "none"))

        if other_rows and not success:
            warnings.append("存在无法直接分析的杂项结果行")

        if not success:
            return {
                "success": False,
                "insights": [],
                "statistics": {},
                "visualization_hint": "none",
                "error": "未能从结果中提取稳定的分析字段",
                "warnings": warnings,
            }

        hints = [h for h in hints if h and h != "none"]
        viz_hint = "mixed" if len(set(hints)) > 1 else (hints[0] if hints else "table")

        return {
            "success": True,
            "insights": insights[:6],
            "statistics": statistics,
            "visualization_hint": viz_hint,
            "warnings": warnings,
        }

    def _validation_subject_terms(
        self,
        subject: str,
        subject_candidates: Optional[List[Dict[str, Any]]] = None,
        query: str = "",
    ) -> List[str]:
        """生成 SQL 主体校验时可接受的主体词集合。"""
        terms: List[str] = []
        seen = set()

        def _push(raw: Any) -> None:
            value = (raw or "").strip()
            if not value:
                return
            normalized = self._normalize_subject_phrase(value)
            for item in (value, normalized):
                item = (item or "").strip()
                if not item or item in seen:
                    continue
                seen.add(item)
                terms.append(item)

        _push(subject)
        if isinstance(subject_candidates, list):
            for item in subject_candidates:
                if not isinstance(item, dict):
                    continue
                _push(item.get("normalized") or item.get("candidate") or "")

        query_subject = self._extract_query_subject(query)
        _push(query_subject)
        return terms

    def _validate_query_sql_consistency(
        self,
        query: str,
        sql: str,
        subject: str = "",
        subject_candidates: Optional[List[Dict[str, Any]]] = None,
    ) -> tuple[bool, str]:
        subject_terms = self._validation_subject_terms(subject, subject_candidates, query)
        sql_text = sql or ""

        if subject_terms and not any(term and term in sql_text for term in subject_terms):
            display_subject = subject or self._normalize_subject_phrase(self._extract_query_subject(query)) or "问题主体"
            return False, f"SQL 未体现问题主体“{display_subject}”"

        if "智慧交通" in sql_text and "智慧交通" not in (query or ""):
            return False, "SQL 与问题主体不一致：包含固定行业词“智慧交通”"

        return True, ""

    def _build_evidence_summary(self, state: AnalystState) -> str:
        subject = state.get("subject_entity", "")
        history_text = self._history_text(state)
        knowledge_text = self._knowledge_evidence_text(state)
        enhancement_trace = state.get("enhancement_trace", {}) or {}
        scout_trace = enhancement_trace.get("scout", {}) if isinstance(enhancement_trace, dict) else {}
        scout_bits: List[str] = []
        if isinstance(scout_trace, dict):
            for fact in (scout_trace.get("facts", []) or [])[:5]:
                if not isinstance(fact, dict):
                    continue
                content = fact.get("content") or ""
                source_name = fact.get("source_name") or fact.get("source") or ""
                if content:
                    scout_bits.append(f"- {source_name + ': ' if source_name else ''}{content}")
            for insight in (scout_trace.get("insights", []) or [])[:5]:
                if insight:
                    scout_bits.append(f"- {insight}")
        scout_text = "\n".join(scout_bits)
        sections = [
            f"问题主体: {subject or '未识别'}",
            f"会话历史:\n{history_text}" if history_text else "会话历史: 无",
            f"知识库证据:\n{knowledge_text}" if knowledge_text else "知识库证据: 无",
        ]
        if scout_text:
            sections.append(f"深度侦探补充证据:\n{scout_text}")
        return "\n\n".join(sections)

    async def _run_original_chief_architect(self, state: AnalystState) -> Dict[str, Any]:
        if not self._original_agents_enabled(state):
            return {}
        bundle = self._get_original_agent_bundle()
        architect = bundle.get("architect")
        if not architect or not create_research_initial_state:
            return {}

        research_state = self._build_original_research_state(state, phase="init")
        if research_state is None:
            return {}
        research_state["phase"] = "init"
        try:
            result = await architect.process(research_state)
        except Exception as e:
            logger.warning(f"总架构师失败: {e}")
            return {}

        outline = result.get("outline", []) if isinstance(result, dict) else []
        normalized_outline: List[Dict[str, Any]] = []
        for idx, section in enumerate(outline or [], start=1):
            if not isinstance(section, dict):
                continue
            item = dict(section)
            item.setdefault("id", f"sec_{idx}")
            item.setdefault("title", f"章节{idx}")
            item.setdefault("description", "")
            item.setdefault("section_type", "mixed")
            item.setdefault("status", "pending")
            item.setdefault("requires_data", idx <= 2)
            item.setdefault("requires_chart", idx <= 2)
            item.setdefault("search_queries", [item.get("title", "")])
            normalized_outline.append(item)
        if not normalized_outline:
            normalized_outline = self._fallback_sections_from_query(state.get("query", ""))

        return {
            "outline": normalized_outline,
            "research_questions": result.get("research_questions", []) if isinstance(result, dict) else [],
            "hypotheses": result.get("hypotheses", []) if isinstance(result, dict) else [],
            "key_entities": result.get("key_entities", []) if isinstance(result, dict) else [],
            "raw_state": result,
        }

    async def _run_original_deep_scout(self, state: AnalystState, outline: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not self._original_agents_enabled(state):
            return {}
        bundle = self._get_original_agent_bundle()
        scout = bundle.get("scout")
        if not scout or not create_research_initial_state:
            return {}

        research_state = self._build_original_research_state(
            state,
            phase="planning",
            search_web=bool(state.get("web_enabled", False) or state.get("metadata", {}).get("enable_web_enrichment", False)),
            search_local=bool(self._knowledge_evidence(state)),
        )
        if research_state is None:
            return {}
        if not research_state.get("search_web") and self._original_agents_enabled(state):
            research_state["search_web"] = True
        research_state["phase"] = "planning"
        research_state["outline"] = outline or self._fallback_sections_from_query(state.get("query", ""))
        research_state["facts"] = []
        research_state["references"] = []
        research_state["data_points"] = []
        research_state["insights"] = []
        try:
            result = await scout.process(research_state)
        except Exception as e:
            logger.warning(f"深度侦探失败: {e}")
            return {}

        return {
            "facts": result.get("facts", []) if isinstance(result, dict) else [],
            "data_points": result.get("data_points", []) if isinstance(result, dict) else [],
            "references": result.get("references", []) if isinstance(result, dict) else [],
            "insights": result.get("insights", []) if isinstance(result, dict) else [],
            "outline": result.get("outline", []) if isinstance(result, dict) else outline,
            "raw_state": result,
        }

    async def _run_original_writer(self, state: AnalystState) -> Dict[str, Any]:
        if not self._original_agents_enabled(state):
            return {}
        bundle = self._get_original_agent_bundle()
        writer = bundle.get("writer")
        if not writer or not create_research_initial_state:
            return {}

        research_state = self._build_original_research_state(state, phase="writing")
        if research_state is None:
            return {}
        research_state["phase"] = "writing"
        research_state["outline"] = [
            {
                "id": "analysis_summary",
                "title": "综合结论",
                "description": "综合分析结果润色",
                "section_type": "mixed",
                "status": "drafted",
                "requires_data": False,
                "requires_chart": False,
                "search_queries": [state.get("query", "")],
            }
        ]
        research_state["draft_sections"] = {"analysis_summary": state.get("final_answer", "")}
        research_state["facts"] = []
        research_state["data_points"] = []
        research_state["insights"] = list(state.get("analysis", {}).get("insights", []) or [])
        research_state["charts"] = []
        research_state["references"] = []
        try:
            result = await writer.process(research_state)
        except Exception as e:
            logger.warning(f"首席笔杆失败: {e}")
            return {}

        return {
            "final_report": result.get("final_report", "") if isinstance(result, dict) else "",
            "references": result.get("references", []) if isinstance(result, dict) else [],
            "raw_state": result,
        }

    async def _run_original_critic(self, state: AnalystState) -> Dict[str, Any]:
        if not self._original_agents_enabled(state):
            return {}
        bundle = self._get_original_agent_bundle()
        critic = bundle.get("critic")
        if not critic or not create_research_initial_state:
            return {}

        research_state = self._build_original_research_state(state, phase="reviewing")
        if research_state is None:
            return {}
        research_state["phase"] = "reviewing"
        research_state["outline"] = [
            {
                "id": "analysis_summary",
                "title": "综合结论",
                "description": "综合分析结果质检",
                "section_type": "mixed",
                "status": "drafted",
                "requires_data": False,
                "requires_chart": False,
                "search_queries": [state.get("query", "")],
            }
        ]
        research_state["draft_sections"] = {"analysis_summary": state.get("final_answer", "")}
        research_state["final_report"] = state.get("final_answer", "")
        research_state["facts"] = []
        research_state["data_points"] = []
        research_state["references"] = []
        try:
            result = await critic.process(research_state)
        except Exception as e:
            logger.warning(f"毒舌评论家失败: {e}")
            return {}

        return {
            "quality_score": result.get("quality_score", 0.0) if isinstance(result, dict) else 0.0,
            "unresolved_issues": result.get("unresolved_issues", 0) if isinstance(result, dict) else 0,
            "critic_feedback": result.get("critic_feedback", []) if isinstance(result, dict) else [],
            "raw_state": result,
        }

    def _evidence_rows(self, state: AnalystState) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for kb in self._knowledge_evidence(state):
            kb_name = kb.get("kb_name", "")
            documents = kb.get("documents", [])
            if not isinstance(documents, list):
                continue
            for doc in documents:
                if not isinstance(doc, dict):
                    continue
                rows.append(
                    {
                        "source_type": "knowledge_base",
                        "kb_name": kb_name,
                        "title": doc.get("title") or doc.get("document_name") or "",
                        "content": doc.get("content_with_weight") or doc.get("content") or "",
                        "score": doc.get("score", 0),
                    }
                )
        web_sources = state.get("web_sources", []) or []
        for item in web_sources:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "source_type": "web",
                    "kb_name": "Web",
                    "title": item.get("title", ""),
                    "content": item.get("summary", "") or item.get("snippet", "") or "",
                    "score": 0.0,
                    "url": item.get("url", ""),
                }
            )
        scout_trace = (state.get("enhancement_trace", {}) or {}).get("scout", {})
        if isinstance(scout_trace, dict):
            for fact in scout_trace.get("facts", []) or []:
                if not isinstance(fact, dict):
                    continue
                rows.append(
                    {
                        "source_type": "deep_scout",
                        "kb_name": "深度侦探",
                        "title": fact.get("source_name", "") or fact.get("source_url", "") or fact.get("content", "")[:60],
                        "content": fact.get("content", ""),
                        "score": fact.get("credibility_score", 0.0),
                        "url": fact.get("source_url", ""),
                    }
                )
                for dp in fact.get("data_points", []) or []:
                    if not isinstance(dp, dict):
                        continue
                    rows.append(
                        {
                            "source_type": "deep_scout_data_point",
                            "kb_name": "深度侦探",
                            "title": dp.get("name", ""),
                            "content": f"{dp.get('name', '')}: {dp.get('value', '')}{dp.get('unit', '')}",
                            "score": fact.get("credibility_score", 0.0),
                            "year": dp.get("year"),
                        }
                    )
        return rows

    async def _analyze_evidence_rows(self, state: AnalystState, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """基于知识库证据生成摘要性分析。"""
        subject = state.get("subject_entity") or "目标主体"
        if not rows:
            return {
                "success": False,
                "insights": [],
                "statistics": {},
                "visualization_hint": "none",
                "error": "无可用证据",
            }

        if not self.llm_api_key:
            top_titles = [row.get("title", "") for row in rows[:3] if row.get("title")]
            insights = [f"检索到 {len(rows)} 条知识库证据"]
            if top_titles:
                insights.append(f"代表文档: {', '.join(top_titles[:3])}")
            return {
                "success": True,
                "insights": insights,
                "statistics": {
                    "evidence_count": len(rows),
                    "knowledge_bases": len({row.get("kb_name", "") for row in rows if row.get("kb_name")}),
                },
                "visualization_hint": "table",
                "warnings": ["未配置 LLM，使用规则化证据摘要"],
            }

        evidence_text = "\n".join(
            [
                f"- 知识库: {row.get('kb_name', '')} | 标题: {row.get('title', '')} | 片段: {str(row.get('content', ''))[:300]}"
                for row in rows[:8]
            ]
        )
        prompt = (
            "你是严谨的行业分析助手，请基于以下知识库证据回答用户问题。\n"
            "要求：\n"
            "1) 只基于证据总结，不要编造数据库里不存在的事实。\n"
            "2) 输出 JSON。\n"
            "3) 给出 3-5 条核心洞察，并说明证据不足之处。\n\n"
            f"问题主体: {subject}\n"
            f"用户问题: {state.get('query', '')}\n"
            f"证据摘要:\n{state.get('evidence_summary', '')}\n\n"
            f"证据明细:\n{evidence_text}\n\n"
            "输出格式:\n"
            '{"insights":["洞察1"],"statistics":{"evidence_count":1},"visualization_hint":"table","warnings":["提示1"]}'
        )

        try:
            response = self.text2sql.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是一个严谨的行业数据分析助手。"},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )
            content = response.choices[0].message.content or ""
            parsed = self.text2sql._extract_json_from_response(content)
            return {
                "success": True,
                "insights": parsed.get("insights", []) if isinstance(parsed.get("insights", []), list) else [],
                "statistics": parsed.get("statistics", {}) if isinstance(parsed.get("statistics", {}), dict) else {},
                "visualization_hint": parsed.get("visualization_hint", "table"),
                "warnings": parsed.get("warnings", []) if isinstance(parsed.get("warnings", []), list) else [],
            }
        except Exception as e:
            top_titles = [row.get("title", "") for row in rows[:3] if row.get("title")]
            return {
                "success": True,
                "insights": [f"检索到 {len(rows)} 条知识库证据"],
                "statistics": {
                    "evidence_count": len(rows),
                    "knowledge_bases": len({row.get("kb_name", "") for row in rows if row.get("kb_name")}),
                },
                "visualization_hint": "table",
                "warnings": [f"证据总结模型失败，改用规则摘要: {e}"] + (["代表文档: " + ", ".join(top_titles[:3])] if top_titles else []),
            }

    def _is_with_query(self, sql: str) -> bool:
        return (sql or "").strip().upper().startswith("WITH")

    def _validate_sql_compatible(self, sql: str) -> tuple[bool, str]:
        """兼容校验：在不改公共模块的前提下支持 CTE (WITH) 查询。"""
        if not sql or not sql.strip():
            return False, "SQL 语句为空"

        stripped = sql.strip()
        upper = stripped.upper()

        if not self._is_with_query(stripped):
            return self.text2sql.validate_sql(sql)

        dangerous = [
            " DROP ", " DELETE ", " UPDATE ", " INSERT ", " ALTER ", " CREATE ",
            " TRUNCATE ", " GRANT ", " REVOKE ", " EXEC ", " EXECUTE ", " XP_", " SP_"
        ]
        padded = f" {upper} "
        if any(k in padded for k in dangerous):
            return False, "CTE SQL 包含危险关键字"
        if "--" in upper or "/*" in upper or "*/" in upper:
            return False, "SQL 中不允许注释"
        if ";" in stripped[:-1]:
            return False, "不允许多条 SQL 语句"
        if "SELECT" not in upper:
            return False, "CTE SQL 必须包含 SELECT 查询"
        if any(token in stripped for token in ("暂无相关数据", "未找到相关数据", "无可用数据", "数据不存在", "请检查数据库")):
            return False, "SQL 不能返回说明性常量结果，请生成真实表查询"
        if re.search(r"FROM\s*\(\s*SELECT\s+1\s*\)", upper):
            return False, "SQL 不能使用哑元子查询作为结果来源"

        return True, ""

    def _execute_sql_compatible(self, sql: str) -> tuple[list[dict], list[str], Optional[str]]:
        """严格执行：只使用真实数据库，不使用任何 mock/fallback。"""
        is_valid, err = self._validate_sql_compatible(sql)
        if not is_valid:
            return [], [], err

        if not self.db_engine:
            return [], [], "数据库连接不可用（严格模式已禁用 mock 数据兜底）"

        try:
            from sqlalchemy import text
            with self.db_engine.connect() as conn:
                result = conn.execute(text(sql))
                columns = list(result.keys())
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                return rows, columns, None
        except Exception as e:
            logger.error(f"CTE SQL execution error: {e}")
            return [], [], str(e)

    def _discover_schema_from_db(self) -> Optional[Dict[str, Any]]:
        """从真实数据库探测 schema；失败返回 None。"""
        if not self.db_engine or not sessionmaker:
            return None

        try:
            session_local = sessionmaker(bind=self.db_engine)
            with session_local() as db:
                explorer = DatabaseExplorer(db)
                tables = explorer.get_tables()

                if not tables:
                    return None

                table_entries: List[Dict[str, Any]] = []
                all_time_fields = set()
                all_dims = set()

                for table in tables[:20]:
                    table_name = table.get("name")
                    if not table_name:
                        continue
                    table_schema = explorer.get_table_schema(table_name)
                    columns = table_schema.get("columns", [])
                    column_names = [col.get("name", "") for col in columns if isinstance(col, dict)]

                    for name in column_names:
                        lname = name.lower()
                        if any(tk in lname for tk in ("year", "quarter", "month", "date", "time")):
                            all_time_fields.add(name)
                        if any(tk in lname for tk in ("industry", "region", "category", "type", "name", "code")):
                            all_dims.add(name)

                    table_entries.append({
                        "name": table_name,
                        "row_count": table.get("row_count", 0),
                        "column_count": table.get("column_count", len(column_names)),
                        "primary_keys": table_schema.get("primary_keys", []),
                        "columns": column_names,
                    })

                return {
                    "tables": table_entries,
                    "dimensions": sorted(all_dims),
                    "time_fields": sorted(all_time_fields),
                    "source": "database_explorer",
                }
        except Exception as e:
            logger.warning(f"Dynamic schema discovery failed: {e}")
            return None

    async def _repair_sql_with_llm(self, state: AnalystState, error: str) -> str:
        """基于错误信息进行 SQL 二次修复。"""
        if not self.llm_api_key:
            return ""

        schema_ctx = state.get("schema_snapshot", {})
        current_sql = state.get("current_sql", "")
        intent = state.get("intent", "stats")
        query = state.get("query", "")
        subject = state.get("subject_entity") or self._extract_query_subject(query)

        prompt = (
            "你是 PostgreSQL SQL 修复专家。请根据错误信息修复 SQL。\n"
            "要求：\n"
            "1) 只返回 JSON。\n"
            "2) 只能输出 SELECT。\n"
            "3) 默认加 LIMIT 100（若已有可保留）。\n"
            "4) 不要使用不存在的表/字段。\n\n"
            f"用户问题: {query}\n"
            f"意图: {intent}\n"
            f"问题主体: {subject or '未识别'}\n"
            f"{self._subject_lock_instruction(subject)}\n"
            f"原SQL: {current_sql}\n"
            f"错误: {error}\n"
            f"Schema: {json.dumps(schema_ctx, ensure_ascii=False)}\n\n"
            "输出格式:\n"
            "{\"sql\":\"修复后的SQL\",\"reason\":\"简短说明\"}"
        )

        try:
            response = self.text2sql.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是严谨的 SQL 修复助手。"},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
            )
            content = response.choices[0].message.content or ""
            parsed = self.text2sql._extract_json_from_response(content)
            sql = (parsed.get("sql") or "").strip()
            if not sql:
                return ""
            valid, _ = self._validate_sql_compatible(sql)
            return sql if valid else ""
        except Exception as e:
            logger.warning(f"LLM sql repair failed: {e}")
            return ""

    async def _understand_node(self, state: AnalystState) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.UNDERSTANDING.value
        state["enhancement_mode"] = self._enhancement_mode(state)
        state["enhancement_agents"] = self._agent_enhancement_list() if self._original_agents_enabled(state) else []
        state["intent"] = self._infer_intent(state["query"])
        resolution = self._resolve_subject_entity(state["query"], self._history_text(state))
        state["subject_entity"] = resolution["subject_entity"]
        state["subject_candidates"] = resolution["subject_candidates"]
        state["subject_resolution"] = resolution["subject_resolution"]
        data_source_mode = self._data_source_mode(state)
        metadata = state.get("metadata", {})
        state["web_enabled"] = bool(metadata.get("enable_web_enrichment", state.get("web_enabled", False)))
        state["web_top_k"] = int(metadata.get("web_top_k", state.get("web_top_k", 3)) or 3)
        state["analysis_plan"] = {
            "goal": "复杂业务数仓智能分析",
            "chain": [
                "schema_understanding",
                "relation_reasoning",
                "sql_generation",
                "sql_validation_execution",
                "sql_self_repair",
                "deep_analysis",
                "answer_synthesis",
            ],
            "intent": state["intent"],
            "data_source_mode": data_source_mode,
        }
        candidate_preview = ", ".join(
            [item.get("candidate", "") for item in state.get("subject_candidates", [])[:3] if item.get("candidate")]
        )
        self._emit_message(
            state,
            state["phase"],
            (
                f"问题理解完成，识别意图: {state['intent']}，主体: {state.get('subject_entity') or '未识别'}"
                f"{f'，候选: {candidate_preview}' if candidate_preview else ''}，数据源: {data_source_mode}"
            ),
        )

        if self._original_agents_enabled(state):
            architect_result = await self._run_original_chief_architect(state)
            if architect_result:
                state.setdefault("enhancement_trace", {})
                state["enhancement_trace"]["architect"] = architect_result
                state["analysis_plan"]["original_architect"] = {
                    "outline_count": len(architect_result.get("outline", [])),
                    "research_questions": architect_result.get("research_questions", []),
                    "key_entities": architect_result.get("key_entities", []),
                }
                if architect_result.get("key_entities"):
                    state["analysis_plan"]["key_entities"] = architect_result.get("key_entities", [])
                self._emit_message(
                    state,
                    "architect_enhancement",
                    f"总架构师增强完成，补充章节 {len(architect_result.get('outline', []))} 个",
                )
        return state

    async def _bocha_search_for_enrichment(self, query: str, count: int = 3) -> List[Dict[str, Any]]:
        if not self.search_api_key:
            return []

        headers = {
            "Authorization": f"Bearer {self.search_api_key}",
            "Content-Type": "application/json",
        }
        payload = {"query": query, "summary": True, "count": count, "page": 1}
        url = "https://api.bochaai.com/v1/web-search"

        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()

            value_list = data.get("data", {}).get("webPages", {}).get("value", [])
            if not isinstance(value_list, list):
                return []

            results: List[Dict[str, Any]] = []
            for item in value_list:
                title = item.get("name", "")
                summary = item.get("summary", "") or item.get("snippet", "")
                link = item.get("url", "")
                if not (title or summary):
                    continue
                results.append(
                    {
                        "title": title,
                        "summary": summary[:400],
                        "url": link,
                        "source": item.get("siteName", ""),
                        "date": item.get("datePublished", ""),
                    }
                )
            return results
        except Exception as e:
            logger.warning(f"Bocha enrichment search failed for query '{query}': {e}")
            return []

    async def _web_enrichment_node(self, state: AnalystState) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.WEB_ENRICHMENT.value

        if not state.get("web_enabled", False):
            state["web_context"] = {"enabled": False, "status": "skipped", "reason": "未启用联网增强"}
            state["web_sources"] = []
            self._emit_message(state, state["phase"], "联网增强未启用，跳过该阶段")
            return state

        if not self.search_api_key:
            state["web_context"] = {"enabled": True, "status": "degraded", "reason": "BOCHA_API_KEY 未配置"}
            state["web_sources"] = []
            self._emit_message(state, state["phase"], "联网增强降级：未配置 BOCHA_API_KEY")
            return state

        top_k = max(1, min(int(state.get("web_top_k", 3)), 10))
        query = state.get("query", "")
        search_queries = [
            query,
            f"{query} 行业定义",
            f"{query} 关键公司 指标",
        ]

        merged_results: List[Dict[str, Any]] = []
        seen_urls = set()
        for q in search_queries:
            items = await self._bocha_search_for_enrichment(q, count=top_k)
            for item in items:
                url = item.get("url", "")
                if url and url in seen_urls:
                    continue
                if url:
                    seen_urls.add(url)
                merged_results.append(item)
            if len(merged_results) >= top_k:
                break

        merged_results = merged_results[:top_k]
        if not merged_results:
            state["web_context"] = {"enabled": True, "status": "degraded", "reason": "联网检索无结果"}
            state["web_sources"] = []
            self._emit_message(state, state["phase"], "联网增强降级：未检索到有效网络上下文")
            return state

        bullet_points = [f"- {r.get('title', '')}: {r.get('summary', '')}" for r in merged_results]
        state["web_context"] = {
            "enabled": True,
            "status": "ok",
            "query": query,
            "summary": "\n".join(bullet_points),
            "count": len(merged_results),
        }
        state["web_sources"] = merged_results
        self._emit_message(state, state["phase"], f"联网增强完成，补充上下文 {len(merged_results)} 条")
        return state

    async def _discover_schema_node(self, state: AnalystState) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.SCHEMA_DISCOVERY.value
        data_source_mode = self._data_source_mode(state)

        schema_from_metadata = state.get("metadata", {}).get("schema_snapshot")
        if data_source_mode == self.DATA_SOURCE_FRONTEND_DEMO:
            if schema_from_metadata:
                state["schema_snapshot"] = schema_from_metadata
            else:
                return self._mark_failed(
                    state,
                    "Schema 发现失败：前端演示模式未提供 schema_snapshot",
                )
        elif schema_from_metadata:
            state["schema_snapshot"] = schema_from_metadata
        else:
            dynamic_schema = self._discover_schema_from_db()
            if dynamic_schema:
                state["schema_snapshot"] = dynamic_schema
            else:
                return self._mark_failed(
                    state,
                    "Schema 发现失败：未获取到数据库结构，请检查 DATABASE_URL/POSTGRES 配置",
                )

        self._emit_message(
            state,
            state["phase"],
            (
                f"Schema 快照完成，识别表数量: {len(state['schema_snapshot'].get('tables', []))}，"
                f"来源: {state['schema_snapshot'].get('source', 'metadata')}"
            ),
        )
        return state

    async def _discover_evidence_node(self, state: AnalystState) -> Dict[str, Any]:
        """综合会话历史、知识库和数据库支持度，决定分析策略。"""
        state = dict(state)
        state["phase"] = "evidence_discovery"

        if not state.get("subject_entity") or not state.get("subject_candidates"):
            resolution = self._resolve_subject_entity(state.get("query", ""), self._history_text(state))
            state["subject_entity"] = resolution["subject_entity"]
            state["subject_candidates"] = resolution["subject_candidates"]
            state["subject_resolution"] = resolution["subject_resolution"]

        evidence_sources = self._knowledge_evidence(state)
        evidence_rows = self._evidence_rows(state)
        support = self._probe_subject_database_support(state.get("subject_entity", ""))

        state["evidence_sources"] = evidence_sources
        state["evidence_summary"] = self._build_evidence_summary(state)
        state["evidence_status"] = {
            "database_support": support,
            "has_knowledge_evidence": bool(evidence_sources),
            "evidence_row_count": len(evidence_rows),
            "subject_candidates": state.get("subject_candidates", []),
            "subject_resolution": state.get("subject_resolution", {}),
        }

        if self._original_agents_enabled(state) and not support.get("supported"):
            architect_outline = []
            architect_trace = (state.get("enhancement_trace", {}) or {}).get("architect", {})
            if isinstance(architect_trace, dict):
                architect_outline = architect_trace.get("outline", []) or []
            scout_result = await self._run_original_deep_scout(state, architect_outline)
            if scout_result:
                state.setdefault("enhancement_trace", {})
                state["enhancement_trace"]["scout"] = scout_result
                scout_facts = scout_result.get("facts", []) if isinstance(scout_result, dict) else []
                scout_insights = scout_result.get("insights", []) if isinstance(scout_result, dict) else []
                scout_refs = scout_result.get("references", []) if isinstance(scout_result, dict) else []
                scout_summary_bits: List[str] = []
                for fact in scout_facts[:5]:
                    if not isinstance(fact, dict):
                        continue
                    content = fact.get("content") or ""
                    source_name = fact.get("source_name") or fact.get("source_url") or ""
                    if content:
                        scout_summary_bits.append(f"- {source_name + ': ' if source_name else ''}{content}")
                for insight in scout_insights[:5]:
                    if insight:
                        scout_summary_bits.append(f"- {insight}")
                if scout_summary_bits:
                    state["evidence_summary"] = (
                        state.get("evidence_summary", "")
                        + "\n\n深度侦探联网补充:\n"
                        + "\n".join(scout_summary_bits)
                    )
                state["evidence_sources"] = evidence_sources + [
                    {
                        "source_type": "deep_scout",
                        "facts_count": len(scout_facts),
                        "insights_count": len(scout_insights),
                        "references_count": len(scout_refs),
                    }
                ]
                state["evidence_status"]["deep_scout_enhanced"] = True
                state["evidence_status"]["deep_scout_facts"] = len(scout_facts)
                self._emit_message(
                    state,
                    state["phase"],
                    f"深度侦探增强完成，补充 {len(scout_facts)} 条事实",
                )

        if support.get("supported"):
            state["selected_strategy"] = "database_sql"
            self._emit_message(
                state,
                state["phase"],
                f"证据判断完成：数据库存在主体相关事实，支持SQL分析，命中 {len(support.get('matches', []))} 处",
            )
        else:
            state["selected_strategy"] = "evidence_only"
            if evidence_sources:
                self._emit_message(
                    state,
                    state["phase"],
                    f"证据判断完成：数据库缺少主体事实，转入知识库证据分析，命中 {len(evidence_sources)} 个知识库",
                )
            else:
                self._emit_message(
                    state,
                    state["phase"],
                    "证据判断完成：数据库缺少主体事实且未找到知识库证据，转入证据不足说明",
                )

        return state

    def _frontend_demo_sql(self, state: AnalystState) -> str:
        subject = state.get("subject_entity") or self._extract_query_subject(state.get("query", "")) or "目标行业"
        intent = state.get("intent", "stats")
        if intent == "trend":
            return (
                "SELECT year, metric_name, metric_value, unit, industry_name "
                "FROM industry_stats "
                f"WHERE industry_name LIKE '%{subject}%' AND metric_name LIKE '%市场规模%' "
                "ORDER BY year LIMIT 100"
            )
        if intent == "comparison":
            return (
                "SELECT company_name, year, quarter, revenue, market_share "
                "FROM company_data "
                f"WHERE industry LIKE '%{subject}%' "
                "ORDER BY revenue DESC LIMIT 100"
            )
        return (
            "SELECT year, metric_name, metric_value, unit, industry_name "
            "FROM industry_stats "
            f"WHERE industry_name LIKE '%{subject}%' "
            "ORDER BY year DESC LIMIT 100"
        )

    def _should_use_fixed_trend_comparison_sql(self, state: AnalystState) -> bool:
        """趋势 + 对比类问题直接使用固定 SQL 模板，避免 LLM 拼 UNION 出错。"""
        query = state.get("query", "")
        intent = state.get("intent", "stats")
        if intent not in {"trend", "comparison"}:
            return False

        trend_markers = ["趋势", "变化", "同比", "环比", "近三年", "近3年", "过去", "最近", "市场规模"]
        compare_markers = ["对比", "比较", "排名", "营收", "公司", "重点公司", "企业"]
        return any(marker in query for marker in trend_markers) and any(marker in query for marker in compare_markers)

    def _build_fixed_trend_comparison_sql(self, state: AnalystState) -> str:
        subject = state.get("subject_entity") or self._normalize_subject_phrase(
            self._extract_query_subject(state.get("query", ""))
        ) or "目标行业"
        subject_sql = subject.replace("'", "''")
        return (
            "WITH market_trend AS (\n"
            "    SELECT\n"
            "        year::int AS year,\n"
            "        metric_value::numeric AS market_size,\n"
            "        unit::text AS unit\n"
            "    FROM industry_stats\n"
            f"    WHERE industry_name = '{subject_sql}'\n"
            "      AND metric_name = '市场规模'\n"
            "      AND year BETWEEN EXTRACT(YEAR FROM CURRENT_DATE)::int - 3 AND EXTRACT(YEAR FROM CURRENT_DATE)::int\n"
            "      AND quarter IS NULL\n"
            "      AND month IS NULL\n"
            "),\n"
            "company_revenue AS (\n"
            "    SELECT\n"
            "        year::int AS year,\n"
            "        company_name::text AS company_name,\n"
            "        revenue::numeric AS revenue\n"
            "    FROM company_data\n"
            f"    WHERE industry = '{subject_sql}'\n"
            "      AND year BETWEEN EXTRACT(YEAR FROM CURRENT_DATE)::int - 3 AND EXTRACT(YEAR FROM CURRENT_DATE)::int\n"
            "      AND revenue IS NOT NULL\n"
            "),\n"
            "top_companies AS (\n"
            "    SELECT company_name\n"
            "    FROM company_revenue\n"
            "    GROUP BY company_name\n"
            "    ORDER BY AVG(revenue) DESC\n"
            "    LIMIT 5\n"
            ")\n"
            "SELECT\n"
            "    '市场规模' AS analysis_type,\n"
            "    year,\n"
            "    market_size,\n"
            "    unit,\n"
            "    NULL::text AS company_name,\n"
            "    NULL::numeric AS revenue\n"
            "FROM market_trend\n"
            "UNION ALL\n"
            "SELECT\n"
            "    '公司营收' AS analysis_type,\n"
            "    year,\n"
            "    NULL::numeric AS market_size,\n"
            "    NULL::text AS unit,\n"
            "    company_name,\n"
            "    revenue\n"
            "FROM company_revenue\n"
            "WHERE company_name IN (SELECT company_name FROM top_companies)\n"
            "ORDER BY analysis_type, year, revenue DESC NULLS LAST\n"
            "LIMIT 100;"
        )

    def _stabilize_union_sql(self, sql: str) -> str:
        """把 UNION 查询规范成更稳的形态，避免分支级 ORDER BY/LIMIT 导致语法错误。"""
        text = (sql or "").strip()
        if not text or "UNION" not in text.upper():
            return text

        segments = re.split(r"(?i)(\bUNION(?:\s+ALL)?\b)", text)
        if len(segments) < 3:
            return text

        branch_index = 0
        total_branches = (len(segments) + 1) // 2
        rebuilt: List[str] = []

        for idx, segment in enumerate(segments):
            if idx % 2 == 1:
                rebuilt.append(segment.upper())
                continue

            part = (segment or "").strip().rstrip(";").strip()
            if not part:
                continue

            if branch_index < total_branches - 1:
                part = re.sub(r"(?is)\border\s+by\b[\s\S]*$", "", part).strip()
                part = re.sub(r"(?is)\blimit\b[\s\S]*$", "", part).strip()

            rebuilt.append(part)
            branch_index += 1

        normalized = " ".join(rebuilt)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if text.endswith(";") and not normalized.endswith(";"):
            normalized += ";"
        return normalized

    def _execute_frontend_demo(self, state: AnalystState) -> tuple[list[dict], list[str], Optional[str]]:
        metadata = state.get("metadata", {}) or {}
        frontend_tables = metadata.get("frontend_tables", {})
        if not isinstance(frontend_tables, dict):
            return [], [], "前端演示模式缺少 frontend_tables 数据"

        industry_rows = frontend_tables.get("industry_stats", {}).get("rows", [])
        company_rows = frontend_tables.get("company_data", {}).get("rows", [])
        if not isinstance(industry_rows, list):
            industry_rows = []
        if not isinstance(company_rows, list):
            company_rows = []

        subject = state.get("subject_entity") or self._extract_query_subject(state.get("query", ""))
        intent = state.get("intent", "stats")

        def _match_subject(text: Any) -> bool:
            if not subject:
                return True
            return subject in str(text or "")

        if intent == "comparison" or "营收" in state.get("query", ""):
            rows = [r for r in company_rows if _match_subject(r.get("industry"))]
            rows = [r for r in rows if isinstance(r.get("revenue"), (int, float))]
            rows.sort(key=lambda x: (x.get("year") or 0, x.get("quarter") or 0, x.get("revenue") or 0), reverse=True)
            rows = rows[:50]
            columns = ["company_name", "industry", "year", "quarter", "revenue", "market_share", "net_profit"]
            normalized = [{c: r.get(c) for c in columns} for r in rows]
            return normalized, columns, None

        rows = [r for r in industry_rows if _match_subject(r.get("industry_name"))]
        if "市场规模" in state.get("query", ""):
            rows = [r for r in rows if "市场规模" in str(r.get("metric_name", ""))]
        rows = [r for r in rows if r.get("year") is not None]
        rows.sort(key=lambda x: (x.get("year") or 0, x.get("quarter") or 0, x.get("month") or 0))
        rows = rows[:100]
        columns = ["industry_name", "metric_name", "metric_value", "unit", "year", "quarter", "region", "source"]
        normalized = [{c: r.get(c) for c in columns} for r in rows]
        return normalized, columns, None

    async def _reason_relations_node(self, state: AnalystState) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.RELATION_REASONING.value

        tables = state.get("schema_snapshot", {}).get("tables", [])
        relations: List[Dict[str, Any]] = []

        table_cols: Dict[str, set] = {}
        for table in tables:
            name = table.get("name")
            cols = table.get("columns", [])
            if name:
                table_cols[name] = {str(c).lower() for c in cols if c}

        for left, right in combinations(table_cols.keys(), 2):
            common = sorted(table_cols[left] & table_cols[right])
            if not common:
                continue

            join_keys = [
                c for c in common
                if any(kw in c for kw in ("id", "code", "industry", "year", "quarter", "month", "date", "region"))
            ][:4]

            if not join_keys:
                continue

            confidence = min(0.55 + 0.1 * len(join_keys), 0.9)
            relations.append({
                "left": left,
                "right": right,
                "keys": join_keys,
                "confidence": round(confidence, 2),
                "reason": f"检测到共享连接键: {', '.join(join_keys)}",
            })

        state["relation_hypotheses"] = relations
        self._emit_message(state, state["phase"], f"表关联推理完成，生成关系假设: {len(relations)} 条")
        return state

    async def _generate_sql_node(self, state: AnalystState) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.SQL_GENERATION.value

        sql = ""
        explanation = ""
        data_source_mode = self._data_source_mode(state)
        subject = state.get("subject_entity") or self._extract_query_subject(state.get("query", ""))
        if subject:
            state["subject_entity"] = subject

        if data_source_mode == self.DATA_SOURCE_FRONTEND_DEMO:
            sql = self._frontend_demo_sql(state)
            state["candidate_sqls"] = [sql]
            state["current_sql"] = sql
            self._emit_message(state, state["phase"], "前端演示模式：已生成演示 SQL 模板")
            return state

        if self._should_use_fixed_trend_comparison_sql(state):
            sql = self._build_fixed_trend_comparison_sql(state)
            state["candidate_sqls"] = [sql]
            state["current_sql"] = sql
            state.setdefault("analysis_plan", {})
            state["analysis_plan"]["sql_template"] = "fixed_trend_comparison"
            self._emit_message(state, state["phase"], "固定趋势+对比 SQL 模板已生成")
            return state

        # 优先使用外部提供的 SQL 候选
        metadata_sqls = state.get("metadata", {}).get("candidate_sqls", [])
        if metadata_sqls:
            state["candidate_sqls"] = [self._stabilize_union_sql(str(s)) for s in metadata_sqls if str(s).strip()]

        if not state["candidate_sqls"]:
            if not self.llm_api_key:
                return self._mark_failed(state, "SQL 生成失败：未配置 LLM API Key")

            try:
                enriched_query = state["query"]
                web_ctx = state.get("web_context", {})
                web_summary = web_ctx.get("summary", "")
                if web_ctx.get("enabled") and web_ctx.get("status") == "ok" and web_summary:
                    enriched_query = (
                        f"{state['query']}\n\n"
                        f"以下为联网补充上下文（仅作实体识别与语义消歧，不可替代数据库事实）:\n"
                        f"{web_summary}\n"
                    )

                if subject:
                    enriched_query = (
                        f"{enriched_query}\n\n"
                        f"{self._subject_lock_instruction(subject)}"
                    )

                evidence_summary = state.get("evidence_summary", "")
                if evidence_summary:
                    enriched_query = (
                        f"{enriched_query}\n\n"
                        "以下为会话历史与知识库证据，仅用于主体消歧与业务理解，不可编造成事实：\n"
                        f"{evidence_summary}"
                    )

                generated = await self.text2sql.generate_sql(enriched_query, state["intent"])
                sql = (generated.get("sql") or "").strip()
                explanation = generated.get("explanation", "")
            except Exception as e:
                logger.warning(f"LLM SQL generation failed: {e}")
                return self._mark_failed(state, f"SQL 生成失败: {e}")

            if not sql:
                return self._mark_failed(state, f"SQL 生成失败: {explanation or '模型返回空 SQL'}")

            sql = self._stabilize_union_sql(sql)
            if self._should_use_fixed_trend_comparison_sql(state):
                sql = self._build_fixed_trend_comparison_sql(state)

            state["candidate_sqls"] = [sql]

        state["current_sql"] = state["candidate_sqls"][0]

        self._emit_message(
            state,
            state["phase"],
            f"SQL 生成完成。说明: {explanation or '候选SQL已准备'}",
        )
        return state

    async def _validate_sql_node(self, state: AnalystState) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.SQL_VALIDATION.value
        data_source_mode = self._data_source_mode(state)

        if data_source_mode == self.DATA_SOURCE_FRONTEND_DEMO:
            state["sql_valid"] = True
            self._emit_message(state, state["phase"], "前端演示模式：跳过严格 SQL 校验")
            return state

        is_valid, error_msg = self._validate_sql_compatible(state.get("current_sql", ""))
        if is_valid:
            is_valid, error_msg = self._validate_query_sql_consistency(
                state.get("query", ""),
                state.get("current_sql", ""),
                subject=state.get("subject_entity", ""),
                subject_candidates=state.get("subject_candidates", []),
            )
        state["sql_valid"] = is_valid

        if not is_valid:
            state["sql_errors"].append(error_msg)
            self._emit_message(state, state["phase"], f"SQL 校验失败: {error_msg}")
        else:
            self._emit_message(state, state["phase"], "SQL 校验通过")

        return state

    async def _execute_sql_node(self, state: AnalystState) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.SQL_EXECUTION.value
        data_source_mode = self._data_source_mode(state)

        sql = state.get("current_sql", "")
        state["executed_sqls"].append(sql)

        if data_source_mode == self.DATA_SOURCE_FRONTEND_DEMO:
            rows, columns, exec_error = self._execute_frontend_demo(state)
            if exec_error:
                state["query_result"] = {
                    "success": False,
                    "error": exec_error,
                    "rows": [],
                    "columns": [],
                    "row_count": 0,
                }
                state["sql_errors"].append(exec_error)
                self._emit_message(state, state["phase"], f"前端演示数据执行失败: {exec_error}")
            else:
                state["query_result"] = {
                    "success": True,
                    "rows": rows,
                    "columns": columns,
                    "row_count": len(rows),
                }
                self._emit_message(state, state["phase"], f"前端演示数据执行成功，返回 {len(rows)} 行")
            return state

        try:
            rows, columns, exec_error = self._execute_sql_compatible(sql)
            if exec_error:
                state["query_result"] = {
                    "success": False,
                    "error": exec_error,
                    "rows": [],
                    "columns": [],
                    "row_count": 0,
                }
                state["sql_errors"].append(exec_error)
                self._emit_message(state, state["phase"], f"SQL 执行失败: {exec_error}")
            else:
                state["query_result"] = {
                    "success": True,
                    "rows": rows,
                    "columns": columns,
                    "row_count": len(rows),
                }
                self._emit_message(state, state["phase"], f"SQL 执行成功，返回 {len(rows)} 行")
        except Exception as e:
            error_msg = str(e)
            state["query_result"] = {
                "success": False,
                "error": error_msg,
                "rows": [],
                "columns": [],
                "row_count": 0,
            }
            state["sql_errors"].append(error_msg)
            self._emit_message(state, state["phase"], f"SQL 执行异常: {error_msg}")

        return state

    def _sanitize_rows_for_analysis(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """将复杂值（dict/list）转换为可分析标量，避免 analyzer 内部类型异常。"""
        cleaned: List[Dict[str, Any]] = []
        for row in rows:
            item: Dict[str, Any] = {}
            for k, v in row.items():
                if isinstance(v, (dict, list, tuple, set)):
                    item[k] = json.dumps(v, ensure_ascii=False)
                elif hasattr(v, "isoformat"):
                    item[k] = v.isoformat()
                else:
                    item[k] = v
            cleaned.append(item)
        return cleaned

    async def _repair_sql_node(self, state: AnalystState) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.SQL_REPAIRING.value

        state["retry_count"] += 1
        last_error = state["sql_errors"][-1] if state["sql_errors"] else "unknown sql error"

        if state["retry_count"] > state["max_sql_retries"]:
            return self._mark_failed(state, "达到 SQL 最大修复次数")

        repaired_sql = await self._repair_sql_with_llm(state, last_error)
        if not repaired_sql:
            return self._mark_failed(state, f"SQL 修复失败（LLM 未返回有效 SQL）: {last_error}")

        repaired_sql = self._stabilize_union_sql(repaired_sql)
        state["current_sql"] = repaired_sql
        state["candidate_sqls"].append(repaired_sql)
        state["sql_valid"] = False

        self._emit_message(
            state,
            state["phase"],
            f"SQL 修复完成（llm），准备重试（{state['retry_count']}/{state['max_sql_retries']}）",
        )
        return state

    async def _deep_analyze_node(self, state: AnalystState) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.DEEP_ANALYSIS.value
        state["analysis_degraded"] = False
        state["analysis_warnings"] = []

        rows = state.get("query_result", {}).get("rows", [])
        intent = state.get("intent", "stats")

        if not rows:
            if state.get("selected_strategy") == "evidence_only":
                evidence_rows = self._evidence_rows(state)
                analysis = await self._analyze_evidence_rows(state, evidence_rows)
                state["analysis"] = analysis
                state["analysis_warnings"] = analysis.get("warnings", []) or []
                state["analysis_degraded"] = bool(state["analysis_warnings"])
                insight_count = len(state["analysis"].get("insights", []))
                self._emit_message(
                    state,
                    state["phase"],
                    f"证据分析完成，提炼洞察 {insight_count} 条",
                )
                return state

            state["analysis_degraded"] = True
            state["analysis"] = {
                "success": False,
                "insights": [],
                "statistics": {},
                "visualization_hint": "none",
                "error": "SQL 无可用结果，跳过深度分析",
            }
            self._emit_message(state, state["phase"], "无结果可分析，已跳过深度分析")
            return state

        sanitized_rows = self._sanitize_rows_for_analysis(rows)
        analysis = self._analyze_rows(sanitized_rows, intent)
        if not analysis.get("success", False):
            analysis_type = "comparison" if intent == "comparison" else ("trend" if intent == "trend" else "auto")
            fallback_analysis = self.data_analyzer.analyze(sanitized_rows, analysis_type=analysis_type)
            if fallback_analysis.get("success", False):
                analysis = fallback_analysis
                analysis["warnings"] = [analysis.get("error", "")] if analysis.get("error") else []
            else:
                state["analysis_degraded"] = True
                state["analysis_warnings"] = analysis.get("warnings", []) or []
                state["analysis"] = {
                    "success": False,
                    "insights": [],
                    "statistics": {},
                    "visualization_hint": "none",
                    "error": analysis.get("error") or fallback_analysis.get("error", "unknown error"),
                    "warnings": state["analysis_warnings"],
                }
                self._emit_message(
                    state,
                    state["phase"],
                    f"深度分析降级失败: {state['analysis']['error']}",
                )
                return state

        state["analysis"] = analysis
        state["analysis_warnings"] = analysis.get("warnings", []) or []
        state["analysis_degraded"] = bool(state["analysis_warnings"])

        insight_count = len(state["analysis"].get("insights", []))
        if state["analysis_warnings"]:
            self._emit_message(
                state,
                state["phase"],
                f"深度分析完成，提炼洞察 {insight_count} 条，附带 {len(state['analysis_warnings'])} 条提示",
            )
        else:
            self._emit_message(state, state["phase"], f"深度分析完成，提炼洞察 {insight_count} 条")
        return state

    async def _synthesize_node(self, state: AnalystState) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.SYNTHESIZING.value

        schema_tables = [t.get("name", "") for t in state.get("schema_snapshot", {}).get("tables", []) if isinstance(t, dict)]
        rows = state.get("query_result", {}).get("rows", [])
        insights = state.get("analysis", {}).get("insights", [])

        answer_lines = [
            f"问题: {state.get('query', '')}",
            f"意图识别: {state.get('intent', '')}",
            f"主体: {state.get('subject_entity') or '未识别'}",
            f"策略: {state.get('selected_strategy', 'database_sql')}",
            f"Schema 触达: {', '.join(schema_tables) if schema_tables else 'N/A'}",
            f"SQL: {state.get('current_sql', '')}",
            f"结果行数: {len(rows)}",
        ]

        if insights:
            answer_lines.append("核心洞察:")
            answer_lines.extend([f"- {item}" for item in insights[:5]])
        elif state.get("analysis", {}).get("error"):
            answer_lines.append(f"分析说明: {state['analysis']['error']}")

        warnings = state.get("analysis_warnings", [])
        if warnings:
            answer_lines.append("分析提示:")
            answer_lines.extend([f"- {item}" for item in warnings[:3] if item])

        evidence_status = state.get("evidence_status", {})
        if evidence_status:
            db_support = evidence_status.get("database_support", {})
            if isinstance(db_support, dict):
                answer_lines.append(
                    f"证据判断: 数据库支持={bool(db_support.get('supported'))}, 知识库证据={bool(evidence_status.get('has_knowledge_evidence'))}"
                )

        base_final_answer = "\n".join(answer_lines)
        state["final_answer"] = base_final_answer
        if self._original_agents_enabled(state):
            writer_result = await self._run_original_writer(state)
            if writer_result:
                state.setdefault("enhancement_trace", {})
                state["enhancement_trace"]["writer"] = writer_result
                polished_report = (writer_result.get("final_report") or "").strip()
                if polished_report:
                    state["final_answer"] = (
                        base_final_answer
                        + "\n\n首席笔杆润色版:\n"
                        + polished_report
                    )
                    self._emit_message(
                        state,
                        state["phase"],
                        f"首席笔杆增强完成，润色输出 {len(polished_report)} 字",
                    )
        self._emit_message(state, state["phase"], "结果综合完成")
        return state

    async def _complete_node(self, state: AnalystState) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.COMPLETED.value
        if self._original_agents_enabled(state):
            critic_result = await self._run_original_critic(state)
            if critic_result:
                state.setdefault("enhancement_trace", {})
                state["enhancement_trace"]["critic"] = critic_result
                state["quality_score"] = float(critic_result.get("quality_score", 0.0) or 0.0)
                state["unresolved_issues"] = int(critic_result.get("unresolved_issues", 0) or 0)
                state["critic_feedback"] = critic_result.get("critic_feedback", []) if isinstance(critic_result.get("critic_feedback", []), list) else []
                verdict = "pass" if state["quality_score"] >= 7 else "needs_revision"
                state["final_answer"] = (
                    state.get("final_answer", "")
                    + f"\n\n毒舌评论家质检: {verdict}，quality_score={state['quality_score']:.1f}，未解决问题={state['unresolved_issues']}"
                )
                self._emit_message(
                    state,
                    state["phase"],
                    f"毒舌评论家质检完成，评分 {state['quality_score']:.1f}/10，未解决问题 {state['unresolved_issues']}",
                )
        self._emit_message(state, state["phase"], "全链路分析完成", message_type="analysis_complete")
        return state

    async def _failed_node(self, state: AnalystState) -> Dict[str, Any]:
        state = dict(state)
        state["phase"] = AnalystPhase.FAILED.value
        if not state.get("final_answer"):
            last_error = state["sql_errors"][-1] if state["sql_errors"] else "未知错误"
            state["final_answer"] = f"分析失败: {last_error}"
        self._emit_message(state, state["phase"], state["final_answer"], message_type="analysis_failed")
        return state

    def _route_after_validate(self, state: AnalystState) -> Literal["execute_sql", "repair_sql"]:
        return "execute_sql" if state.get("sql_valid") else "repair_sql"

    def _route_after_evidence(self, state: AnalystState) -> Literal["reason_relations", "deep_analyze"]:
        return "deep_analyze" if state.get("selected_strategy") == "evidence_only" else "reason_relations"

    def _route_after_execute(self, state: AnalystState) -> Literal["deep_analyze", "repair_sql"]:
        success = state.get("query_result", {}).get("success", False)
        return "deep_analyze" if success else "repair_sql"

    def _route_after_repair(self, state: AnalystState) -> Literal["validate_sql", "failed"]:
        if state.get("phase") == AnalystPhase.FAILED.value:
            return "failed"
        return "validate_sql"

    async def _run_node(
        self,
        state: AnalystState,
        node_func,
        phase_name: str,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        if await self._check_cancelled(state):
            yield state.get("messages", [])[-1]
            return

        yield {
            "type": "phase",
            "phase": phase_name,
            "content": f"开始阶段: {phase_name}",
            "timestamp": datetime.now().isoformat(),
        }

        before = len(state.get("messages", []))
        new_state = await node_func(state)
        state.clear()
        state.update(new_state)

        if await self._check_cancelled(state):
            yield state.get("messages", [])[-1]
            return

        for msg in state.get("messages", [])[before:]:
            yield msg

    async def run(
        self,
        query: str,
        session_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """流式执行工作流"""
        state = create_initial_state(
            query=query,
            session_id=session_id,
            max_sql_retries=self.max_sql_retries,
            enable_web_enrichment=bool((metadata or {}).get("enable_web_enrichment", False)),
            web_top_k=int((metadata or {}).get("web_top_k", 3) or 3),
            metadata=metadata,
        )
        await self.clear_cancel(session_id)

        yield {
            "type": "analysis_start",
            "query": query,
            "session_id": session_id,
            "timestamp": datetime.now().isoformat(),
        }

        async for event in self._run_node(state, self._understand_node, AnalystPhase.UNDERSTANDING.value):
            yield event
        if state.get("phase") == AnalystPhase.CANCELLED.value:
            yield {"type": "analysis_cancelled", "phase": AnalystPhase.CANCELLED.value, "session_id": session_id}
            return
        if state.get("phase") == AnalystPhase.FAILED.value:
            yield {"type": "analysis_failed", "phase": AnalystPhase.FAILED.value, "session_id": session_id, "final_answer": state.get("final_answer", "")}
            return
        async for event in self._run_node(state, self._web_enrichment_node, AnalystPhase.WEB_ENRICHMENT.value):
            yield event
        if state.get("phase") == AnalystPhase.CANCELLED.value:
            yield {"type": "analysis_cancelled", "phase": AnalystPhase.CANCELLED.value, "session_id": session_id}
            return
        if state.get("phase") == AnalystPhase.FAILED.value:
            yield {"type": "analysis_failed", "phase": AnalystPhase.FAILED.value, "session_id": session_id, "final_answer": state.get("final_answer", "")}
            return
        async for event in self._run_node(state, self._discover_schema_node, AnalystPhase.SCHEMA_DISCOVERY.value):
            yield event
        if state.get("phase") == AnalystPhase.CANCELLED.value:
            yield {"type": "analysis_cancelled", "phase": AnalystPhase.CANCELLED.value, "session_id": session_id}
            return
        if state.get("phase") == AnalystPhase.FAILED.value:
            yield {"type": "analysis_failed", "phase": AnalystPhase.FAILED.value, "session_id": session_id, "final_answer": state.get("final_answer", "")}
            return
        async for event in self._run_node(state, self._discover_evidence_node, "evidence_discovery"):
            yield event
        if state.get("phase") == AnalystPhase.CANCELLED.value:
            yield {"type": "analysis_cancelled", "phase": AnalystPhase.CANCELLED.value, "session_id": session_id}
            return
        if state.get("phase") == AnalystPhase.FAILED.value:
            yield {"type": "analysis_failed", "phase": AnalystPhase.FAILED.value, "session_id": session_id, "final_answer": state.get("final_answer", "")}
            return

        if state.get("selected_strategy") != "evidence_only":
            async for event in self._run_node(state, self._reason_relations_node, AnalystPhase.RELATION_REASONING.value):
                yield event
            if state.get("phase") == AnalystPhase.CANCELLED.value:
                yield {"type": "analysis_cancelled", "phase": AnalystPhase.CANCELLED.value, "session_id": session_id}
                return
            if state.get("phase") == AnalystPhase.FAILED.value:
                yield {"type": "analysis_failed", "phase": AnalystPhase.FAILED.value, "session_id": session_id, "final_answer": state.get("final_answer", "")}
                return
            async for event in self._run_node(state, self._generate_sql_node, AnalystPhase.SQL_GENERATION.value):
                yield event
            if state.get("phase") == AnalystPhase.CANCELLED.value:
                yield {"type": "analysis_cancelled", "phase": AnalystPhase.CANCELLED.value, "session_id": session_id}
                return
            if state.get("phase") == AnalystPhase.FAILED.value:
                yield {"type": "analysis_failed", "phase": AnalystPhase.FAILED.value, "session_id": session_id, "final_answer": state.get("final_answer", "")}
                return

        # SQL 校验-执行-修复循环
        if state.get("selected_strategy") != "evidence_only":
            while True:
                if await self._check_cancelled(state):
                    yield {"type": "analysis_cancelled", "phase": AnalystPhase.CANCELLED.value, "session_id": session_id}
                    return
                async for event in self._run_node(state, self._validate_sql_node, AnalystPhase.SQL_VALIDATION.value):
                    yield event
                if state.get("phase") == AnalystPhase.FAILED.value:
                    yield {"type": "analysis_failed", "phase": AnalystPhase.FAILED.value, "session_id": session_id, "final_answer": state.get("final_answer", "")}
                    return

                if self._route_after_validate(state) == "execute_sql":
                    async for event in self._run_node(state, self._execute_sql_node, AnalystPhase.SQL_EXECUTION.value):
                        yield event
                    if state.get("phase") == AnalystPhase.FAILED.value:
                        yield {"type": "analysis_failed", "phase": AnalystPhase.FAILED.value, "session_id": session_id, "final_answer": state.get("final_answer", "")}
                        return

                    if self._route_after_execute(state) == "deep_analyze":
                        break

                async for event in self._run_node(state, self._repair_sql_node, AnalystPhase.SQL_REPAIRING.value):
                    yield event

                if self._route_after_repair(state) == "failed":
                    async for event in self._run_node(state, self._failed_node, AnalystPhase.FAILED.value):
                        yield event
                    yield {
                        "type": "analysis_failed",
                        "phase": AnalystPhase.FAILED.value,
                        "session_id": session_id,
                        "error": state["sql_errors"][-1] if state["sql_errors"] else "unknown error",
                        "final_answer": state.get("final_answer", ""),
                    }
                    return

        async for event in self._run_node(state, self._deep_analyze_node, AnalystPhase.DEEP_ANALYSIS.value):
            yield event
        if state.get("phase") == AnalystPhase.CANCELLED.value:
            yield {"type": "analysis_cancelled", "phase": AnalystPhase.CANCELLED.value, "session_id": session_id}
            return
        if state.get("phase") == AnalystPhase.FAILED.value:
            yield {"type": "analysis_failed", "phase": AnalystPhase.FAILED.value, "session_id": session_id, "final_answer": state.get("final_answer", "")}
            return
        async for event in self._run_node(state, self._synthesize_node, AnalystPhase.SYNTHESIZING.value):
            yield event
        if state.get("phase") == AnalystPhase.CANCELLED.value:
            yield {"type": "analysis_cancelled", "phase": AnalystPhase.CANCELLED.value, "session_id": session_id}
            return
        if state.get("phase") == AnalystPhase.FAILED.value:
            yield {"type": "analysis_failed", "phase": AnalystPhase.FAILED.value, "session_id": session_id, "final_answer": state.get("final_answer", "")}
            return
        async for event in self._run_node(state, self._complete_node, AnalystPhase.COMPLETED.value):
            yield event

        yield {
            "type": "analysis_complete",
            "phase": state.get("phase", AnalystPhase.COMPLETED.value),
            "session_id": session_id,
            "final_answer": state.get("final_answer", ""),
            "sql": state.get("current_sql", ""),
            "analysis": state.get("analysis", {}),
            "subject_entity": state.get("subject_entity", ""),
            "subject_candidates": state.get("subject_candidates", []),
            "subject_resolution": state.get("subject_resolution", {}),
            "selected_strategy": state.get("selected_strategy", ""),
            "enhancement_mode": state.get("enhancement_mode", "none"),
            "enhancement_agents": state.get("enhancement_agents", []),
            "evidence_status": state.get("evidence_status", {}),
            "analysis_warnings": state.get("analysis_warnings", []),
            "analysis_degraded": state.get("analysis_degraded", False),
            "quality_score": state.get("quality_score", 0.0),
            "unresolved_issues": state.get("unresolved_issues", 0),
            "critic_feedback": state.get("critic_feedback", []),
            "row_count": state.get("query_result", {}).get("row_count", 0),
        }

    async def run_sync(
        self,
        query: str,
        session_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AnalystState:
        """同步执行（返回最终状态）"""
        state = create_initial_state(
            query=query,
            session_id=session_id,
            max_sql_retries=self.max_sql_retries,
            enable_web_enrichment=bool((metadata or {}).get("enable_web_enrichment", False)),
            web_top_k=int((metadata or {}).get("web_top_k", 3) or 3),
            metadata=metadata,
        )
        await self.clear_cancel(session_id)

        if await self._check_cancelled(state):
            return state

        state = await self._understand_node(state)
        if await self._check_cancelled(state):
            return state
        if state.get("phase") == AnalystPhase.FAILED.value:
            return state
        state = await self._web_enrichment_node(state)
        if await self._check_cancelled(state):
            return state
        if state.get("phase") == AnalystPhase.FAILED.value:
            return state
        state = await self._discover_schema_node(state)
        if await self._check_cancelled(state):
            return state
        if state.get("phase") == AnalystPhase.FAILED.value:
            return state
        state = await self._discover_evidence_node(state)
        if await self._check_cancelled(state):
            return state
        if state.get("phase") == AnalystPhase.FAILED.value:
            return state

        if state.get("selected_strategy") != "evidence_only":
            state = await self._reason_relations_node(state)
            if await self._check_cancelled(state):
                return state
            if state.get("phase") == AnalystPhase.FAILED.value:
                return state
            state = await self._generate_sql_node(state)
            if await self._check_cancelled(state):
                return state
            if state.get("phase") == AnalystPhase.FAILED.value:
                return state

            while True:
                if await self._check_cancelled(state):
                    return state
                state = await self._validate_sql_node(state)
                if state.get("phase") == AnalystPhase.FAILED.value:
                    return state
                if self._route_after_validate(state) == "execute_sql":
                    state = await self._execute_sql_node(state)
                    if state.get("phase") == AnalystPhase.FAILED.value:
                        return state
                    if self._route_after_execute(state) == "deep_analyze":
                        break
                state = await self._repair_sql_node(state)
                if self._route_after_repair(state) == "failed":
                    state = await self._failed_node(state)
                    return state

        if await self._check_cancelled(state):
            return state
        state = await self._deep_analyze_node(state)
        if await self._check_cancelled(state):
            return state
        if state.get("phase") == AnalystPhase.FAILED.value:
            return state
        state = await self._synthesize_node(state)
        if await self._check_cancelled(state):
            return state
        if state.get("phase") == AnalystPhase.FAILED.value:
            return state
        state = await self._complete_node(state)
        return state


def create_ai_data_analyst_graph(
    llm_api_key: Optional[str] = None,
    llm_base_url: Optional[str] = None,
    model: Optional[str] = None,
    db_connection_string: Optional[str] = None,
    max_sql_retries: int = 2,
) -> AIDataAnalystGraph:
    """工厂函数：创建课题六 Graph"""
    return AIDataAnalystGraph(
        llm_api_key=llm_api_key,
        llm_base_url=llm_base_url,
        model=model,
        db_connection_string=db_connection_string,
        max_sql_retries=max_sql_retries,
    )
