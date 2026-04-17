import { request } from './request'

export interface FeishuHealthResponse {
  status: string
  configured?: boolean
  app_id?: boolean
  app_secret?: boolean
  base_url?: string
}

export interface FeishuBitablePreviewRequest {
  app_token: string
  table_ids?: string[]
  sample_size?: number
}

export interface FeishuBitableSyncTarget {
  target_table: 'industry_stats' | 'company_data' | 'policy_data'
  table_id: string
  field_mapping: Record<string, string>
  match_fields?: string[]
  clear_before_sync?: boolean
  sample_limit?: number
}

export interface FeishuBitableSyncRequest {
  app_token: string
  targets: FeishuBitableSyncTarget[]
  sample_only?: boolean
}

export interface FeishuDocPublishRequest {
  title: string
  result: Record<string, unknown>
  folder_token?: string
}

export interface FeishuIMSendRequest {
  receive_id: string
  receive_id_type?: string
  message: string
  uuid?: string
}

export function getHealth() {
  return request.get<FeishuHealthResponse>('/feishu/health', { loading: false })
}

export function getTokenStatus() {
  return request.get<{ status: string; has_token: boolean; expires_cached: boolean }>('/feishu/token', { loading: false })
}

export function previewBitable(params: FeishuBitablePreviewRequest) {
  return request.post<Record<string, unknown>>('/feishu/bitable/preview', params, { loading: false })
}

export function listTables(appToken: string) {
  return request.get<Record<string, unknown>>(`/feishu/bitable/${appToken}/tables`, { loading: false })
}

export function listRecords(appToken: string, tableId: string, pageSize: number = 500) {
  return request.get<Record<string, unknown>>(
    `/feishu/bitable/${appToken}/tables/${tableId}/records`,
    { params: { page_size: pageSize }, loading: false },
  )
}

export function syncBitable(params: FeishuBitableSyncRequest) {
  return request.post<Record<string, unknown>>('/feishu/bitable/sync', params, { loading: false })
}

export function publishDoc(params: FeishuDocPublishRequest) {
  return request.post<Record<string, unknown>>('/feishu/doc/publish', params, { loading: false })
}

export function sendIM(params: FeishuIMSendRequest) {
  return request.post<Record<string, unknown>>('/feishu/im/send', params, { loading: false })
}
