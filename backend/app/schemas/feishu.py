from typing import Any, Dict, List, Optional, Literal

from pydantic import BaseModel, Field


class FeishuTableSyncTarget(BaseModel):
    target_table: Literal["industry_stats", "company_data", "policy_data"]
    table_id: str = Field(..., description="飞书多维表格 table_id")
    field_mapping: Dict[str, str] = Field(default_factory=dict, description="飞书字段 -> 本地模型字段 映射")
    match_fields: Optional[List[str]] = Field(None, description="本地 upsert 匹配字段")
    clear_before_sync: bool = Field(False, description="同步前是否清空目标表")
    sample_limit: int = Field(500, ge=1, le=500, description="每个表最多同步/预览条数")


class FeishuBitablePreviewRequest(BaseModel):
    app_token: str = Field(..., description="飞书多维表格 app_token")
    table_ids: Optional[List[str]] = Field(None, description="可选，只预览指定表")
    sample_size: int = Field(3, ge=1, le=10, description="预览样本数")


class FeishuBitableSyncRequest(BaseModel):
    app_token: str = Field(..., description="飞书多维表格 app_token")
    targets: List[FeishuTableSyncTarget] = Field(..., min_length=1, description="同步目标配置")
    sample_only: bool = Field(False, description="仅预览不写库")


class FeishuDocPublishRequest(BaseModel):
    title: str = Field(..., description="飞书文档标题")
    result: Dict[str, Any] = Field(..., description="分析结果 payload")
    folder_token: Optional[str] = Field(None, description="飞书文档目录 token，可选")


class FeishuIMSendRequest(BaseModel):
    receive_id: str = Field(..., description="接收方 ID，群聊时为 chat_id")
    receive_id_type: str = Field("chat_id", description="接收方类型，默认 chat_id")
    message: str = Field(..., description="要发送的文本内容")
    uuid: Optional[str] = Field(None, description="消息幂等 ID，可选")
