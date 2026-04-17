#!/usr/bin/env python3

"""
最小可用配置检查脚本

用途：
1. 检查关键环境变量是否配置
2. 检查 PostgreSQL 可连通性
3. 检查 DashScope(兼容 OpenAI) LLM 接口可用性
4. 检查 Bocha 搜索接口可用性
5. 输出一份可直接定位问题的总结

运行方式（在 backend 目录）：
    python3 app/scripts/check_min_config.py
    python3 app/scripts/check_min_config.py --skip-network
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

import httpx
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


BOCHA_URL = "https://api.bochaai.com/v1/web-search"
DEFAULT_LLM_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_LLM_MODEL = "qwen-plus"


@dataclass
class CheckResult:
    name: str
    ok: bool
    level: str  # PASS / WARN / FAIL
    detail: str


def mask_secret(value: str) -> str:
    if not value:
        return "(empty)"
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def build_db_url() -> str:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if db_url:
        return db_url
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres123")
    db = os.getenv("POSTGRES_DB", "industry_assistant")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def check_env() -> List[CheckResult]:
    results: List[CheckResult] = []

    dashscope_key = os.getenv("DASHSCOPE_API_KEY", "").strip()
    bocha_key = os.getenv("BOCHA_API_KEY", "").strip()
    bid_app_code = os.getenv("BID_APP_CODE", "").strip()

    if not dashscope_key:
        results.append(CheckResult("DASHSCOPE_API_KEY", False, "FAIL", "未配置"))
    else:
        results.append(CheckResult("DASHSCOPE_API_KEY", True, "PASS", f"已配置: {mask_secret(dashscope_key)}"))

    if not bocha_key:
        results.append(CheckResult("BOCHA_API_KEY", False, "FAIL", "未配置"))
    elif bocha_key.lower().startswith("bearer "):
        results.append(
            CheckResult(
                "BOCHA_API_KEY",
                False,
                "FAIL",
                "值里不应包含 'Bearer ' 前缀；代码会自动拼接 Authorization: Bearer <key>",
            )
        )
    else:
        results.append(CheckResult("BOCHA_API_KEY", True, "PASS", f"已配置: {mask_secret(bocha_key)}"))

    if not bid_app_code:
        results.append(CheckResult("BID_APP_CODE", False, "WARN", "未配置（不影响聊天，但招投标采集会跳过）"))
    else:
        results.append(CheckResult("BID_APP_CODE", True, "PASS", f"已配置: {mask_secret(bid_app_code)}"))

    return results


def check_database(timeout_sec: int) -> CheckResult:
    db_url = build_db_url()
    try:
        engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=1800)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            table_count = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    """
                )
            ).scalar_one()
        return CheckResult(
            "PostgreSQL",
            True,
            "PASS",
            f"连接成功，public 表数量: {table_count}, db_url: {db_url}",
        )
    except Exception as e:
        return CheckResult("PostgreSQL", False, "FAIL", f"连接失败: {e}")


def check_dashscope(timeout_sec: int) -> CheckResult:
    api_key = os.getenv("DASHSCOPE_API_KEY", "").strip()
    if not api_key:
        return CheckResult("DashScope LLM", False, "FAIL", "DASHSCOPE_API_KEY 未配置")

    base_url = os.getenv("LLM_BASE_URL", DEFAULT_LLM_BASE_URL).rstrip("/")
    model = os.getenv("CHECK_LLM_MODEL", DEFAULT_LLM_MODEL)
    url = f"{base_url}/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": "请回复: ok"}],
        "max_tokens": 8,
        "temperature": 0,
    }

    try:
        with httpx.Client(timeout=timeout_sec) as client:
            resp = client.post(url, headers=headers, json=payload)
        if resp.status_code == 200:
            return CheckResult("DashScope LLM", True, "PASS", f"请求成功: {url}, model={model}")
        return CheckResult(
            "DashScope LLM",
            False,
            "FAIL",
            f"请求失败: HTTP {resp.status_code}, body={resp.text[:300]}",
        )
    except Exception as e:
        return CheckResult("DashScope LLM", False, "FAIL", f"请求异常: {e}")


def check_bocha(timeout_sec: int) -> CheckResult:
    api_key = os.getenv("BOCHA_API_KEY", "").strip()
    if not api_key:
        return CheckResult("Bocha Search", False, "FAIL", "BOCHA_API_KEY 未配置")
    if api_key.lower().startswith("bearer "):
        return CheckResult("Bocha Search", False, "FAIL", "BOCHA_API_KEY 不应带 'Bearer ' 前缀")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"query": "智慧交通 政策", "summary": True, "count": 1, "page": 1}
    try:
        with httpx.Client(timeout=timeout_sec) as client:
            resp = client.post(BOCHA_URL, headers=headers, json=payload)
        if resp.status_code == 200:
            return CheckResult("Bocha Search", True, "PASS", "请求成功，搜索接口可用")
        if resp.status_code == 403:
            return CheckResult("Bocha Search", False, "FAIL", "HTTP 403：Key 无效/无权限/额度不足")
        return CheckResult(
            "Bocha Search",
            False,
            "FAIL",
            f"请求失败: HTTP {resp.status_code}, body={resp.text[:300]}",
        )
    except Exception as e:
        return CheckResult("Bocha Search", False, "FAIL", f"请求异常: {e}")


def print_results(results: List[CheckResult]) -> None:
    print("\n=== 最小可用配置检查 ===")
    for r in results:
        print(f"[{r.level:<4}] {r.name:<18} | {r.detail}")

    fail_count = sum(1 for x in results if x.level == "FAIL")
    warn_count = sum(1 for x in results if x.level == "WARN")
    pass_count = sum(1 for x in results if x.level == "PASS")

    print("\n=== 汇总 ===")
    print(f"PASS: {pass_count}, WARN: {warn_count}, FAIL: {fail_count}")
    if fail_count == 0:
        print("最小可用能力已满足。")
    else:
        print("存在阻断项，请优先修复 FAIL。")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="检查 Agent 项目最小可用配置")
    parser.add_argument("--skip-network", action="store_true", help="跳过 DashScope/Bocha 网络请求，仅检查本地配置和数据库")
    parser.add_argument("--timeout", type=int, default=15, help="网络超时时间（秒），默认 15")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # 自动加载 backend/.env（如果可用）
    if load_dotenv is not None:
        backend_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        dotenv_path = os.path.join(backend_root, ".env")
        if os.path.exists(dotenv_path):
            load_dotenv(dotenv_path=dotenv_path, override=False)

    results: List[CheckResult] = []
    results.extend(check_env())
    results.append(check_database(args.timeout))

    if not args.skip_network:
        results.append(check_dashscope(args.timeout))
        results.append(check_bocha(args.timeout))

    print_results(results)

    # 只要有 FAIL，就返回非 0
    has_fail = any(r.level == "FAIL" for r in results)
    return 2 if has_fail else 0


if __name__ == "__main__":
    sys.exit(main())
