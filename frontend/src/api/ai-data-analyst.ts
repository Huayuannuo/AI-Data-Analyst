import { request } from './request'

export interface AnalyzeSyncRequest {
  query: string
  session_id?: string
  metadata?: Record<string, unknown>
  max_sql_retries?: number
  enable_web_enrichment?: boolean
  enable_original_agents?: boolean
  web_top_k?: number
  data_source_mode?: 'database' | 'frontend_demo'
  save_history?: boolean
}

export interface SchemaSnapshot {
  tables: Array<{ name: string; role?: string }>
  dimensions?: string[]
  time_fields?: string[]
  source?: string
}

export interface RelationHypothesis {
  left: string
  right: string
  keys: string[]
  confidence?: number
  reason?: string
}

export interface QueryResult {
  success: boolean
  rows: Array<Record<string, unknown>>
  columns: string[]
  row_count: number
  error?: string
}

export interface AnalysisResult {
  success: boolean
  insights: string[]
  statistics?: Record<string, unknown>
  visualization_hint?: string
  error?: string
  fallback?: boolean
}

export interface ProcessLog {
  type?: string
  phase?: string
  content?: string
  timestamp?: string
}

export interface AnalyzeSyncResponse {
  session_id: string
  query: string
  phase: string
  intent?: string
  subject_entity?: string
  subject_candidates?: Array<Record<string, unknown>>
  subject_resolution?: Record<string, unknown>
  selected_strategy?: string
  enhancement_mode?: string
  enhancement_agents?: string[]
  schema_snapshot?: SchemaSnapshot
  web_context?: Record<string, unknown>
  web_sources?: Array<Record<string, unknown>>
  evidence_status?: Record<string, unknown>
  evidence_sources?: Array<Record<string, unknown>>
  evidence_summary?: string
  relation_hypotheses?: RelationHypothesis[]
  sql?: string
  candidate_sqls?: string[]
  query_result?: QueryResult
  analysis?: AnalysisResult
  analysis_warnings?: string[]
  analysis_degraded?: boolean
  quality_score?: number
  unresolved_issues?: number
  critic_feedback?: Array<Record<string, unknown>>
  final_answer?: string
  sql_errors?: string[]
  retry_count?: number
  logs?: ProcessLog[]
}

export function analyzeSync(params: AnalyzeSyncRequest) {
  return request.post<AnalyzeSyncResponse>('/ai-data-analyst/analyze_sync', params, {
    loading: false,
  })
}

export function cancel(sessionId: string) {
  return request.post<{ success: boolean; session_id: string; message: string }>(
    `/ai-data-analyst/cancel/${sessionId}`,
    {},
    { loading: false },
  )
}

export function getLatestResult(sessionId: string) {
  return request.get<AnalyzeSyncResponse>(`/ai-data-analyst/result/${sessionId}`, { loading: false })
}
