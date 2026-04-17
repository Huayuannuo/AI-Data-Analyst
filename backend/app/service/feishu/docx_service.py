"""飞书新版文档报告发布服务。"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

from .client import FeishuAPIError, FeishuClient


class FeishuDocxService:
    """飞书 Docx 文档创建与内容写入。"""

    def __init__(self, client: FeishuClient):
        self.client = client

    def create_document(self, title: str, folder_token: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"title": title}
        if folder_token:
            payload["folder_token"] = folder_token
        return self.client.request("POST", "/docx/v1/documents", json_body=payload)

    def get_document(self, document_id: str) -> Dict[str, Any]:
        return self.client.request("GET", f"/docx/v1/documents/{document_id}")

    def get_raw_content(self, document_id: str) -> Dict[str, Any]:
        return self.client.request("GET", f"/docx/v1/documents/{document_id}/raw_content")

    def append_text_blocks(
        self,
        document_id: str,
        lines: Iterable[str],
        *,
        parent_block_id: Optional[str] = None,
        batch_size: int = 20,
    ) -> Dict[str, Any]:
        """向文档追加纯文本块。"""
        block_id = parent_block_id or document_id
        children: List[Dict[str, Any]] = []
        for line in lines:
            if line is None:
                continue
            content = str(line).strip()
            if not content:
                continue
            children.append(self._build_text_block(content))
            if len(children) >= batch_size:
                self._create_children(document_id, block_id, children)
                children = []
        if children:
            self._create_children(document_id, block_id, children)
        return {"document_id": document_id, "status": "ok"}

    def publish_report(self, title: str, report: Dict[str, Any], folder_token: Optional[str] = None) -> Dict[str, Any]:
        """根据分析结果生成一篇飞书文档报告。"""
        create_resp = self.create_document(title=title, folder_token=folder_token)
        document = (create_resp.get("data") or {}).get("document") or {}
        document_id = document.get("document_id")
        if not document_id:
            raise FeishuAPIError(f"创建文档成功但未返回 document_id: {create_resp}")

        lines = self._render_report_lines(report)
        self.append_text_blocks(document_id=document_id, lines=lines)
        return {
            "document_id": document_id,
            "revision_id": document.get("revision_id"),
            "title": document.get("title") or title,
            "raw_create_response": create_resp,
        }

    def _create_children(self, document_id: str, block_id: str, children: List[Dict[str, Any]]) -> Dict[str, Any]:
        payload = {
            "index": 0,
            "children": children,
        }
        return self.client.request(
            "POST",
            f"/docx/v1/documents/{document_id}/blocks/{block_id}/children",
            json_body=payload,
        )

    @staticmethod
    def _build_text_block(content: str) -> Dict[str, Any]:
        return {
            "block_type": 2,
            "text": {
                "elements": [
                    {
                        "text_run": {
                            "content": content,
                            "text_element_style": {},
                        }
                    }
                ],
                "style": {},
            },
        }

    @staticmethod
    def _render_report_lines(report: Dict[str, Any]) -> List[str]:
        lines: List[str] = []
        query = report.get("query") or ""
        subject = report.get("subject_entity") or ""
        strategy = report.get("selected_strategy") or ""
        enhancement_mode = report.get("enhancement_mode") or "none"
        quality_score = report.get("quality_score")
        unresolved_issues = report.get("unresolved_issues")
        final_answer = report.get("final_answer") or ""
        sql = report.get("sql") or ""
        analysis = report.get("analysis") or {}
        critic_feedback = report.get("critic_feedback") or []
        evidence_summary = report.get("evidence_summary") or ""

        lines.append("AI Data Analyst 报告")
        lines.append(f"问题: {query}")
        if subject:
            lines.append(f"主体: {subject}")
        if strategy:
            lines.append(f"策略: {strategy}")
        lines.append(f"增强模式: {enhancement_mode}")
        if quality_score is not None:
            lines.append(f"质检评分: {quality_score}")
        if unresolved_issues is not None:
            lines.append(f"未解决问题数: {unresolved_issues}")
        if evidence_summary:
            lines.append("证据摘要:")
            lines.extend([f"- {line}" for line in str(evidence_summary).splitlines() if line.strip()])
        if sql:
            lines.append("SQL:")
            lines.extend([line for line in str(sql).splitlines() if line.strip()])
        if analysis:
            lines.append("分析结果:")
            try:
                analysis_text = json.dumps(analysis, ensure_ascii=False, default=str, indent=2)
            except Exception:
                analysis_text = str(analysis)
            lines.extend([line for line in analysis_text.splitlines() if line.strip()])
        if final_answer:
            lines.append("最终回答:")
            lines.extend([line for line in str(final_answer).splitlines() if line.strip()])
        if critic_feedback:
            lines.append("毒舌评论家质检:")
            try:
                feedback_text = json.dumps(critic_feedback, ensure_ascii=False, default=str, indent=2)
            except Exception:
                feedback_text = str(critic_feedback)
            lines.extend([line for line in feedback_text.splitlines() if line.strip()])
        return lines
