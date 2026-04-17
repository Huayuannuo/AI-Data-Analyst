"""飞书多维表格同步服务。"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Type

from dateutil import parser as date_parser
from sqlalchemy import and_
from sqlalchemy.orm import Session
from sqlalchemy.sql.sqltypes import Date as SQLDate
from sqlalchemy.sql.sqltypes import DateTime as SQLDateTime
from sqlalchemy.sql.sqltypes import Float as SQLFloat
from sqlalchemy.sql.sqltypes import Integer as SQLInteger
from sqlalchemy.sql.sqltypes import JSON as SQLJSON
from sqlalchemy.sql.sqltypes import String as SQLString
from sqlalchemy.sql.sqltypes import Text as SQLText

from core.database import Base
from models.industry_data import CompanyData, IndustryStats, PolicyData

from .client import FeishuAPIError, FeishuClient

TARGET_MODELS: Dict[str, Type[Base]] = {
    "industry_stats": IndustryStats,
    "company_data": CompanyData,
    "policy_data": PolicyData,
}

DEFAULT_MATCH_FIELDS: Dict[str, List[str]] = {
    "industry_stats": ["industry_name", "metric_name", "year", "quarter", "month", "region"],
    "company_data": ["company_name", "stock_code", "year", "quarter"],
    "policy_data": ["policy_name", "policy_number", "publish_date"],
}


class FeishuBitableService:
    """多维表格读写与同步。"""

    def __init__(self, client: FeishuClient):
        self.client = client

    def list_tables(self, app_token: str) -> Dict[str, Any]:
        return self.client.request("GET", f"/bitable/v1/apps/{app_token}/tables")

    def list_table_fields(self, app_token: str, table_id: str) -> Dict[str, Any]:
        return self.client.request("GET", f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields")

    def list_records(
        self,
        app_token: str,
        table_id: str,
        page_size: int = 500,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        while True:
            params: Dict[str, Any] = {"page_size": min(page_size, 500)}
            if page_token:
                params["page_token"] = page_token
            response = self.client.request(
                "GET",
                f"/bitable/v1/apps/{app_token}/tables/{table_id}/records",
                params=params,
            )
            data = response.get("data", {}) or {}
            items = data.get("items") or data.get("records") or []
            records.extend(items)
            if not data.get("has_more"):
                break
            page_token = data.get("page_token") or data.get("pageToken")
            if not page_token:
                break
        return records

    def preview_base(
        self,
        app_token: str,
        table_ids: Optional[Sequence[str]] = None,
        sample_size: int = 3,
    ) -> Dict[str, Any]:
        tables_payload = self.list_tables(app_token)
        tables = self._extract_items(tables_payload.get("data", {}))
        if table_ids:
            tables = [table for table in tables if str(table.get("table_id") or table.get("id")) in set(table_ids)]

        preview_tables: List[Dict[str, Any]] = []
        for table in tables:
            table_id = str(table.get("table_id") or table.get("id") or "")
            if not table_id:
                continue
            records = self.list_records(app_token, table_id, page_size=sample_size)
            preview_tables.append(
                {
                    "table_id": table_id,
                    "name": table.get("name") or table.get("table_name") or table_id,
                    "sample_records": records[:sample_size],
                    "record_count": len(records),
                    "fields": self._safe_extract_fields(table),
                }
            )

        return {
            "app_token": app_token,
            "table_count": len(preview_tables),
            "tables": preview_tables,
        }

    def sync_targets(
        self,
        db: Session,
        app_token: str,
        targets: Sequence[Dict[str, Any]],
        *,
        sample_only: bool = False,
    ) -> Dict[str, Any]:
        summary: Dict[str, Any] = {
            "app_token": app_token,
            "synced_targets": [],
            "errors": [],
            "sample_only": sample_only,
        }

        for target in targets:
            target_table = target.get("target_table")
            table_id = target.get("table_id")
            field_mapping: Dict[str, str] = target.get("field_mapping") or {}
            match_fields: List[str] = list(target.get("match_fields") or DEFAULT_MATCH_FIELDS.get(target_table, []))
            clear_before_sync = bool(target.get("clear_before_sync", False))
            sample_limit = int(target.get("sample_limit", 500) or 500)

            if target_table not in TARGET_MODELS:
                summary["errors"].append(f"未知目标表: {target_table}")
                continue
            if not table_id:
                summary["errors"].append(f"目标表 {target_table} 缺少 table_id")
                continue

            model = TARGET_MODELS[target_table]
            try:
                records = self.list_records(app_token, table_id, page_size=sample_limit)
                if sample_only:
                    summary["synced_targets"].append(
                        {
                            "target_table": target_table,
                            "table_id": table_id,
                            "record_count": len(records),
                            "sample_records": records[: min(3, len(records))],
                        }
                    )
                    continue

                synced = self._sync_records_to_model(
                    db=db,
                    model=model,
                    records=records,
                    field_mapping=field_mapping,
                    match_fields=match_fields,
                    clear_before_sync=clear_before_sync,
                )
                summary["synced_targets"].append(
                    {
                        "target_table": target_table,
                        "table_id": table_id,
                        "record_count": len(records),
                        "inserted": synced["inserted"],
                        "updated": synced["updated"],
                        "skipped": synced["skipped"],
                    }
                )
            except Exception as exc:
                db.rollback()
                summary["errors"].append(f"同步 {target_table} 失败: {exc}")

        if not sample_only:
            db.commit()
        return summary

    def _sync_records_to_model(
        self,
        db: Session,
        model: Type[Base],
        records: Sequence[Dict[str, Any]],
        field_mapping: Dict[str, str],
        match_fields: Sequence[str],
        clear_before_sync: bool = False,
    ) -> Dict[str, int]:
        if clear_before_sync:
            db.query(model).delete(synchronize_session=False)

        inserted = 0
        updated = 0
        skipped = 0

        for record in records:
            fields = record.get("fields") or record.get("field_values") or record.get("data") or {}
            mapped = self._map_record_fields(model, fields, field_mapping)
            if not mapped:
                skipped += 1
                continue

            existing = self._find_existing(db, model, mapped, match_fields)
            if existing is None:
                db.add(model(**mapped))
                inserted += 1
            else:
                for key, value in mapped.items():
                    setattr(existing, key, value)
                updated += 1

        return {"inserted": inserted, "updated": updated, "skipped": skipped}

    def _find_existing(
        self,
        db: Session,
        model: Type[Base],
        mapped: Dict[str, Any],
        match_fields: Sequence[str],
    ):
        filters = []
        for field in match_fields:
            if field not in mapped:
                continue
            filters.append(getattr(model, field) == mapped[field])
        if not filters:
            return None
        return db.query(model).filter(and_(*filters)).first()

    def _map_record_fields(
        self,
        model: Type[Base],
        fields: Dict[str, Any],
        field_mapping: Dict[str, str],
    ) -> Dict[str, Any]:
        mapped: Dict[str, Any] = {}
        for source_key, target_key in field_mapping.items():
            if source_key not in fields:
                continue
            column = getattr(model, target_key, None)
            if column is None:
                continue
            value = self._coerce_value(fields[source_key], column)
            mapped[target_key] = value
        return mapped

    def _coerce_value(self, value: Any, column: Any) -> Any:
        if value is None:
            return None

        column_type = getattr(column, "type", None)
        if isinstance(column_type, (SQLJSON,)):
            return value
        if isinstance(value, (dict, list)):
            if isinstance(column_type, (SQLString, SQLText)):
                return str(value)
            return value

        if isinstance(column_type, SQLInteger):
            try:
                return int(float(value))
            except Exception:
                return None
        if isinstance(column_type, SQLFloat):
            try:
                return float(value)
            except Exception:
                return None
        if isinstance(column_type, SQLDate):
            if isinstance(value, date) and not isinstance(value, datetime):
                return value
            if isinstance(value, datetime):
                return value.date()
            try:
                return date_parser.parse(str(value)).date()
            except Exception:
                return None
        if isinstance(column_type, SQLDateTime):
            if isinstance(value, datetime):
                return value
            if isinstance(value, date):
                return datetime(value.year, value.month, value.day)
            try:
                return date_parser.parse(str(value))
            except Exception:
                return None

        if isinstance(column_type, (SQLString, SQLText)):
            return str(value)
        return value

    @staticmethod
    def _extract_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        for key in ("items", "tables", "records", "fields"):
            items = data.get(key)
            if isinstance(items, list):
                return items
        return []

    @staticmethod
    def _safe_extract_fields(table: Dict[str, Any]) -> Dict[str, Any]:
        fields = table.get("fields") or table.get("field_list") or []
        if isinstance(fields, list):
            return {str(item.get("field_id") or item.get("id") or idx): item for idx, item in enumerate(fields)}
        return fields if isinstance(fields, dict) else {}
