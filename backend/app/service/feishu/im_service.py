"""飞书 IM 消息发送服务。"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from .client import FeishuClient


class FeishuIMService:
    """飞书 IM 消息发送。"""

    def __init__(self, client: FeishuClient):
        self.client = client

    def send_text(
        self,
        receive_id: str,
        message: str,
        receive_id_type: str = "chat_id",
        uuid: Optional[str] = None,
    ) -> Dict[str, Any]:
        if receive_id_type == "chat_id":
            membership = self.check_is_in_chat(receive_id)
            if not membership.get("is_in_chat", False):
                raise ValueError(
                    f"机器人不在群中，无法发送消息。chat_id={receive_id}，"
                    f"请先把机器人加入该群，或检查 chat_id 是否正确。"
                )

        payload = {
            "receive_id": receive_id,
            "msg_type": "text",
            "content": json.dumps({"text": message}, ensure_ascii=False),
        }
        if uuid:
            payload["uuid"] = uuid
        return self.client.request(
            "POST",
            "/im/v1/messages",
            params={"receive_id_type": receive_id_type},
            json_body=payload,
        )

    def check_is_in_chat(self, chat_id: str) -> Dict[str, Any]:
        """检查当前 app/机器人是否在群里。"""
        result = self.client.request(
            "GET",
            f"/im/v1/chats/{chat_id}/members/is_in_chat",
        )
        data = result.get("data") if isinstance(result, dict) else {}
        if isinstance(data, dict):
            return {
                "is_in_chat": bool(data.get("is_in_chat", False)),
                "raw": result,
            }
        return {"is_in_chat": False, "raw": result}
