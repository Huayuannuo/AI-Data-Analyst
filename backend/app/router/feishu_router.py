"""飞书集成路由"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from core.database import get_db
from schemas.feishu import (
    FeishuBitablePreviewRequest,
    FeishuBitableSyncRequest,
    FeishuDocPublishRequest,
    FeishuIMSendRequest,
)
from service.feishu import FeishuAPIError, FeishuBitableService, FeishuClient, FeishuDocxService, FeishuIMService

router = APIRouter(prefix="/feishu", tags=["Feishu Integration"])


def _build_client() -> FeishuClient:
    return FeishuClient(
        app_id=os.getenv("FEISHU_APP_ID"),
        app_secret=os.getenv("FEISHU_APP_SECRET"),
        base_url=os.getenv("FEISHU_BASE_URL", "https://open.feishu.cn/open-apis"),
    )


def _get_services():
    client = _build_client()
    return {
        "client": client,
        "bitable": FeishuBitableService(client),
        "docx": FeishuDocxService(client),
        "im": FeishuIMService(client),
    }


@router.get("/health")
async def health() -> Dict[str, Any]:
    client = _build_client()
    return {
        "status": "success",
        "configured": client.is_configured(),
        "app_id": bool(os.getenv("FEISHU_APP_ID")),
        "app_secret": bool(os.getenv("FEISHU_APP_SECRET")),
        "base_url": os.getenv("FEISHU_BASE_URL", "https://open.feishu.cn/open-apis"),
    }


@router.get("/token")
async def token_status() -> Dict[str, Any]:
    client = _build_client()
    if not client.is_configured():
        raise HTTPException(status_code=400, detail="FEISHU_APP_ID / FEISHU_APP_SECRET 未配置")
    try:
        token = client.get_tenant_access_token()
        return {"status": "success", "has_token": bool(token), "expires_cached": True}
    except FeishuAPIError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/bitable/preview")
async def preview_bitable(request: FeishuBitablePreviewRequest) -> Dict[str, Any]:
    services = _get_services()
    try:
        return services["bitable"].preview_base(
            app_token=request.app_token,
            table_ids=request.table_ids,
            sample_size=request.sample_size,
        )
    except FeishuAPIError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/bitable/{app_token}/tables")
async def list_bitable_tables(app_token: str) -> Dict[str, Any]:
    services = _get_services()
    try:
        return services["bitable"].list_tables(app_token)
    except FeishuAPIError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/bitable/{app_token}/tables/{table_id}/records")
async def list_bitable_records(app_token: str, table_id: str, page_size: int = 500) -> Dict[str, Any]:
    services = _get_services()
    try:
        records = services["bitable"].list_records(app_token, table_id, page_size=page_size)
        return {"app_token": app_token, "table_id": table_id, "record_count": len(records), "records": records}
    except FeishuAPIError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/bitable/sync")
async def sync_bitable(request: FeishuBitableSyncRequest, db: Session = Depends(get_db)) -> Dict[str, Any]:
    services = _get_services()
    try:
        return services["bitable"].sync_targets(
            db=db,
            app_token=request.app_token,
            targets=[target.model_dump() for target in request.targets],
            sample_only=request.sample_only,
        )
    except FeishuAPIError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/doc/publish")
async def publish_doc(request: FeishuDocPublishRequest) -> Dict[str, Any]:
    services = _get_services()
    try:
        return services["docx"].publish_report(
            title=request.title,
            report=request.result,
            folder_token=request.folder_token,
        )
    except FeishuAPIError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/im/send")
async def send_im(request: FeishuIMSendRequest) -> Dict[str, Any]:
    services = _get_services()
    try:
        return services["im"].send_text(
            receive_id=request.receive_id,
            receive_id_type=request.receive_id_type,
            message=request.message,
            uuid=request.uuid,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FeishuAPIError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/im/check_chat/{chat_id}")
async def check_chat(chat_id: str) -> Dict[str, Any]:
    services = _get_services()
    try:
        return services["im"].check_is_in_chat(chat_id)
    except FeishuAPIError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
