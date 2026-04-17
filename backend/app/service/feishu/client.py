"""飞书开放平台 API 客户端封装。"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)


class FeishuAPIError(RuntimeError):
    """飞书 API 调用异常"""


@dataclass
class _TokenCache:
    token: str = ""
    expires_at: float = 0.0


class FeishuClient:
    """飞书服务端 API 客户端。"""

    def __init__(
        self,
        app_id: Optional[str],
        app_secret: Optional[str],
        base_url: str = "https://open.feishu.cn/open-apis",
        timeout: int = 30,
    ):
        self.app_id = app_id or ""
        self.app_secret = app_secret or ""
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._tenant_token_cache = _TokenCache()

    def is_configured(self) -> bool:
        return bool(self.app_id and self.app_secret)

    def get_tenant_access_token(self, force_refresh: bool = False) -> str:
        """获取并缓存 tenant_access_token。"""
        if not self.is_configured():
            raise FeishuAPIError("FEISHU_APP_ID 或 FEISHU_APP_SECRET 未配置")

        now = time.time()
        if (
            not force_refresh
            and self._tenant_token_cache.token
            and now < self._tenant_token_cache.expires_at
        ):
            return self._tenant_token_cache.token

        url = f"{self.base_url}/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret,
        }
        response = requests.post(url, json=payload, timeout=self.timeout)
        try:
            data = response.json()
        except Exception:
            raise FeishuAPIError(f"获取 tenant_access_token 失败：HTTP {response.status_code}，响应无法解析")

        if response.status_code >= 400 or data.get("code", 0) != 0:
            raise FeishuAPIError(f"获取 tenant_access_token 失败：{data}")

        token = data.get("tenant_access_token") or data.get("tenant_accessToken") or ""
        expire = int(data.get("expire", 0) or 0)
        if not token:
            raise FeishuAPIError(f"获取 tenant_access_token 失败：返回空 token，响应={data}")

        # 提前 60 秒过期，避免边界问题
        self._tenant_token_cache.token = token
        self._tenant_token_cache.expires_at = now + max(expire - 60, 60)
        return token

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        files: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        use_token: bool = True,
    ) -> Dict[str, Any]:
        """发起飞书 API 请求并返回 JSON。"""
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        req_headers: Dict[str, str] = {
            "Authorization": f"Bearer {self.get_tenant_access_token()}" if use_token else "",
        }
        if headers:
            req_headers.update(headers)
        if not use_token:
            req_headers.pop("Authorization", None)

        response = requests.request(
            method=method.upper(),
            url=url,
            params=params,
            json=json_body,
            data=data,
            files=files,
            headers=req_headers,
            timeout=timeout or self.timeout,
        )

        request_preview = {
            "method": method.upper(),
            "url": url,
            "params": params,
            "json_body": json_body,
        }
        try:
            result = response.json()
        except Exception:
            error_detail = (
                f"飞书 API 返回非 JSON 响应：HTTP {response.status_code}，"
                f"request={json.dumps(request_preview, ensure_ascii=False, default=str)}，"
                f"text={response.text[:1000]}"
            )
            logger.error(error_detail)
            raise FeishuAPIError(error_detail)

        if response.status_code >= 400:
            error_detail = (
                f"飞书 API 调用失败：HTTP {response.status_code}，"
                f"request={json.dumps(request_preview, ensure_ascii=False, default=str)}，"
                f"响应={json.dumps(result, ensure_ascii=False, default=str)}，"
                f"text={response.text[:1000]}"
            )
            logger.error(error_detail)
            raise FeishuAPIError(error_detail)
        if isinstance(result, dict) and result.get("code", 0) not in (0, None):
            error_detail = (
                f"飞书 API 调用失败：request={json.dumps(request_preview, ensure_ascii=False, default=str)}，"
                f"响应={json.dumps(result, ensure_ascii=False, default=str)}"
            )
            logger.error(error_detail)
            raise FeishuAPIError(error_detail)
        return result
