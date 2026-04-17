import { useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import { Alert, Button, Checkbox, Empty, Input, InputNumber, Spin, Tag, message } from 'antd'
import { useSearchParams } from 'react-router-dom'
import * as api from '@/api'
import type {
  AnalyzeSyncResponse,
  ProcessLog,
  QueryResult,
  RelationHypothesis,
  SchemaSnapshot,
} from '@/api/ai-data-analyst'
import { getEnhancementAgentLabel, getEnhancementModeLabel } from '@/utils/enhancement'

const palette = {
  bg: '#f7f9fc',
  card: '#ffffff',
  border: '#e9edf3',
  textPrimary: '#1f2937',
  textSecondary: '#4b5563',
  textTertiary: '#6b7280',
  codeBg: '#0f172a',
  codeText: '#e2e8f0',
  accent: '#14532d',
  accentSoft: '#dcfce7',
}

const defaultQuery = '请分析智慧交通近三年的市场规模趋势，并对重点公司营收做对比'

function unwrapResponse<T>(res: T | { data: T }): T {
  if (typeof res === 'object' && res !== null && 'data' in res) {
    return (res as { data: T }).data
  }
  return res as T
}

function renderConfidence(confidence?: number) {
  if (confidence === undefined || confidence === null) return '-'
  return `${Math.round(confidence * 100)}%`
}

function ResultTable({ queryResult }: { queryResult?: QueryResult }) {
  if (!queryResult) return <Empty description="暂无执行结果" image={Empty.PRESENTED_IMAGE_SIMPLE} />

  if (!queryResult.success) {
    return <Alert type="error" message={queryResult.error || 'SQL 执行失败'} showIcon />
  }

  const rows = queryResult.rows || []
  const columns = queryResult.columns || []

  if (rows.length === 0) {
    return <Empty description="SQL 执行成功，但返回空结果" image={Empty.PRESENTED_IMAGE_SIMPLE} />
  }

  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
        <thead>
          <tr>
            {columns.map((col) => (
              <th
                key={col}
                style={{
                  textAlign: 'left',
                  padding: '10px 12px',
                  borderBottom: `1px solid ${palette.border}`,
                  color: palette.textSecondary,
                  whiteSpace: 'nowrap',
                }}
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 20).map((row, idx) => (
            <tr key={idx}>
              {columns.map((col) => (
                <td
                  key={`${idx}-${col}`}
                  style={{
                    padding: '10px 12px',
                    borderBottom: `1px solid ${palette.border}`,
                    color: palette.textPrimary,
                    verticalAlign: 'top',
                  }}
                >
                  {String(row[col] ?? '-')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function PhaseLogs({ logs }: { logs?: ProcessLog[] }) {
  if (!logs?.length) return <Empty description="暂无流程日志" image={Empty.PRESENTED_IMAGE_SIMPLE} />

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {logs.map((log, idx) => (
        <div
          key={`${log.timestamp}-${idx}`}
          style={{
            border: `1px solid ${palette.border}`,
            borderRadius: 8,
            padding: '10px 12px',
            background: '#fafcff',
          }}
        >
          <div style={{ marginBottom: 6, display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
            <Tag color="blue">{log.phase || '-'}</Tag>
            <span style={{ color: palette.textTertiary, fontSize: 12 }}>{log.timestamp || '-'}</span>
          </div>
          <div style={{ color: palette.textPrimary, fontSize: 13 }}>{log.content || '-'}</div>
        </div>
      ))}
    </div>
  )
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section
      style={{
        background: palette.card,
        border: `1px solid ${palette.border}`,
        borderRadius: 12,
        padding: 16,
      }}
    >
      <div style={{ fontSize: 16, fontWeight: 600, color: palette.textPrimary, marginBottom: 12 }}>{title}</div>
      {children}
    </section>
  )
}

export default function AIDataAnalystShowcasePage() {
  const [searchParams] = useSearchParams()
  const [query, setQuery] = useState(defaultQuery)
  const [maxSqlRetries, setMaxSqlRetries] = useState(2)
  const [enableWebEnrichment, setEnableWebEnrichment] = useState(false)
  const [enableOriginalAgents, setEnableOriginalAgents] = useState(false)
  const [webTopK, setWebTopK] = useState(3)
  const [useFrontendDemo, setUseFrontendDemo] = useState(false)
  const [loading, setLoading] = useState(false)
  const [loadingSessionResult, setLoadingSessionResult] = useState(false)
  const [result, setResult] = useState<AnalyzeSyncResponse | null>(null)
  const [sessionIdInput, setSessionIdInput] = useState(searchParams.get('session_id') || '')

  const schema: SchemaSnapshot | undefined = result?.schema_snapshot
  const relations: RelationHypothesis[] = result?.relation_hypotheses || []

  const statusColor = useMemo(() => {
    if (!result) return 'default'
    if (result.phase === 'completed') return 'green'
    if (result.phase === 'failed') return 'red'
    return 'blue'
  }, [result])

  const runAnalyze = async () => {
    if (!query.trim()) {
      message.warning('请输入分析问题')
      return
    }

    setLoading(true)
    try {
      const metadata = useFrontendDemo ? await buildFrontendDemoMetadata() : undefined
      const res = await api.aiDataAnalyst.analyzeSync({
        query: query.trim(),
        max_sql_retries: maxSqlRetries,
        enable_web_enrichment: enableWebEnrichment,
        enable_original_agents: enableOriginalAgents,
        web_top_k: webTopK,
        data_source_mode: useFrontendDemo ? 'frontend_demo' : 'database',
        metadata,
      })
      const payload = unwrapResponse(res)
      setResult(payload)
      message.success('分析完成')
    } catch (error: unknown) {
      const msg = error instanceof Error ? error.message : '请求失败，请检查后端是否运行'
      message.error(msg)
    } finally {
      setLoading(false)
    }
  }

  const buildFrontendDemoMetadata = async (): Promise<Record<string, unknown>> => {
    const tables = ['industry_stats', 'company_data', 'policy_data'] as const
    const frontendTables: Record<string, { columns: string[]; rows: Record<string, unknown>[] }> = {}
    const schemaTables: Array<{ name: string; columns: string[]; row_count: number }> = []

    for (const tableName of tables) {
      const schemaRes = await api.database.getTableSchema(tableName)
      const schemaPayload = unwrapResponse(schemaRes)
      const dataRes = await api.database.getTableData(tableName, { limit: 200, offset: 0 })
      const dataPayload = unwrapResponse(dataRes)

      const columns = (schemaPayload.columns || []).map((c) => c.name)
      const rows = dataPayload.rows || []
      frontendTables[tableName] = { columns, rows }
      schemaTables.push({
        name: tableName,
        columns,
        row_count: dataPayload.total || rows.length,
      })
    }

    return {
      schema_snapshot: {
        tables: schemaTables,
        dimensions: ['industry', 'industry_name', 'region', 'category', 'company_name'],
        time_fields: ['year', 'quarter', 'month', 'publish_date', 'effective_date'],
        source: 'frontend_demo',
      },
      frontend_tables: frontendTables,
    }
  }

  const loadSessionResult = async (targetSessionId?: string) => {
    const sid = (targetSessionId || sessionIdInput).trim()
    if (!sid) {
      message.warning('请输入会话ID')
      return
    }
    setLoadingSessionResult(true)
    try {
      const res = await api.aiDataAnalyst.getLatestResult(sid)
      const payload = unwrapResponse(res)
      setResult(payload)
      setSessionIdInput(payload.session_id)
      message.success('已加载该会话的流程结果')
    } catch (error: unknown) {
      const msg = error instanceof Error ? error.message : '会话结果不存在，请先在对话页执行一次分析'
      message.error(msg)
    } finally {
      setLoadingSessionResult(false)
    }
  }

  useEffect(() => {
    const sid = searchParams.get('session_id')
    if (sid) {
      setSessionIdInput(sid)
      loadSessionResult(sid)
    }
  }, [searchParams])

  return (
    <div style={{ background: palette.bg, minHeight: '100vh', padding: 20 }}>
      <div
        style={{
          maxWidth: 1280,
          margin: '0 auto',
          display: 'flex',
          flexDirection: 'column',
          gap: 14,
        }}
      >
        <Section title="数据分析流程展示">
          <div style={{ color: palette.textSecondary, marginBottom: 12, fontSize: 13 }}>
            比赛展示链路：Schema 理解 {'->'} 表关联推理 {'->'} SQL 生成与修复 {'->'} SQL 执行 {'->'} 深度分析 {'->'} 结论综合
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 220px', gap: 12, marginBottom: 12 }}>
            <Input
              value={sessionIdInput}
              onChange={(e) => setSessionIdInput(e.target.value)}
              placeholder="输入 session_id 回放流程（来自对话页）"
            />
            <Button onClick={() => loadSessionResult()} loading={loadingSessionResult}>
              加载会话流程
            </Button>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 180px 180px 140px', gap: 12 }}>
            <Input.TextArea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              autoSize={{ minRows: 3, maxRows: 5 }}
              placeholder="输入你的业务分析问题"
            />
            <InputNumber
              min={0}
              max={5}
              value={maxSqlRetries}
              onChange={(v) => setMaxSqlRetries(v ?? 2)}
              addonBefore="最大重试"
              style={{ width: '100%' }}
            />
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8, justifyContent: 'center' }}>
              <Checkbox
                checked={enableWebEnrichment}
                onChange={(e) => setEnableWebEnrichment(e.target.checked)}
              >
                联网增强
              </Checkbox>
              <Checkbox
                checked={enableOriginalAgents}
                onChange={(e) => setEnableOriginalAgents(e.target.checked)}
              >
                子智能体增强
              </Checkbox>
              <Checkbox
                checked={useFrontendDemo}
                onChange={(e) => setUseFrontendDemo(e.target.checked)}
              >
                前端演示模式
              </Checkbox>
              <InputNumber
                min={1}
                max={10}
                value={webTopK}
                disabled={!enableWebEnrichment}
                onChange={(v) => setWebTopK(v ?? 3)}
                addonBefore="TopK"
                style={{ width: '100%' }}
              />
            </div>
            <Button type="primary" loading={loading} onClick={runAnalyze} style={{ height: '100%' }}>
              开始分析
            </Button>
          </div>
          {result ? (
            <div style={{ marginTop: 12, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              <Tag color={statusColor}>{`phase: ${result.phase}`}</Tag>
              <Tag>{`intent: ${result.intent || '-'}`}</Tag>
              <Tag>{`strategy: ${result.selected_strategy || '-'}`}</Tag>
              <Tag>{`增强模式: ${getEnhancementModeLabel(result.enhancement_mode)}`}</Tag>
              {(result.enhancement_agents || []).length ? (
                <>
                  {(result.enhancement_agents || []).map((agent) => (
                    <Tag key={agent}>{getEnhancementAgentLabel(agent)}</Tag>
                  ))}
                </>
              ) : null}
              <Tag color={result.quality_score && result.quality_score >= 7 ? 'green' : 'orange'}>
                {`质检分: ${typeof result.quality_score === 'number' ? result.quality_score.toFixed(1) : '-'}`}
              </Tag>
              <Tag>{`未解决问题: ${result.unresolved_issues ?? 0}`}</Tag>
              <Tag>{`subject: ${result.subject_entity || '-'}`}</Tag>
              <Tag>{`retry: ${result.retry_count ?? 0}`}</Tag>
              <Tag>{`web: ${String((result.web_context as any)?.enabled ?? enableWebEnrichment)}`}</Tag>
              <Tag>{`demo: ${String(useFrontendDemo)}`}</Tag>
              <Tag>{`session: ${result.session_id}`}</Tag>
            </div>
          ) : null}
        </Section>

        {loading ? (
          <Section title="执行中">
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <Spin />
              <span style={{ color: palette.textSecondary }}>正在执行全链路分析...</span>
            </div>
          </Section>
        ) : null}

        {!result && !loading ? (
          <Section title="提示">
            <Empty description="请输入问题并点击开始分析" image={Empty.PRESENTED_IMAGE_SIMPLE} />
          </Section>
        ) : null}

        {result ? (
          <>
            <Section title="Schema 理解">
              <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                <div>
                  <div style={{ color: palette.textTertiary, marginBottom: 6, fontSize: 12 }}>表快照</div>
                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    {(schema?.tables || []).map((table) => (
                      <Tag key={table.name} color="geekblue">{`${table.name}${table.role ? ` (${table.role})` : ''}`}</Tag>
                    ))}
                    {!(schema?.tables || []).length ? <span style={{ color: palette.textTertiary }}>-</span> : null}
                  </div>
                </div>
                <div>
                  <div style={{ color: palette.textTertiary, marginBottom: 6, fontSize: 12 }}>维度字段</div>
                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    {(schema?.dimensions || []).map((dim) => (
                      <Tag key={dim}>{dim}</Tag>
                    ))}
                    {!(schema?.dimensions || []).length ? <span style={{ color: palette.textTertiary }}>-</span> : null}
                  </div>
                </div>
                <div>
                  <div style={{ color: palette.textTertiary, marginBottom: 6, fontSize: 12 }}>时间字段</div>
                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    {(schema?.time_fields || []).map((field) => (
                      <Tag key={field} color="cyan">{field}</Tag>
                    ))}
                    {!(schema?.time_fields || []).length ? <span style={{ color: palette.textTertiary }}>-</span> : null}
                  </div>
                </div>
                <div style={{ color: palette.textSecondary, fontSize: 12 }}>{`source: ${schema?.source || '-'}`}</div>
              </div>
            </Section>

            <Section title="联网补充上下文">
              <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                <div style={{ color: palette.textSecondary, fontSize: 13 }}>
                  {`状态: ${String((result.web_context as any)?.status || 'skipped')} | 说明: ${String((result.web_context as any)?.reason || '-')}`}
                </div>
                <pre
                  style={{
                    margin: 0,
                    background: '#f8fafc',
                    border: `1px solid ${palette.border}`,
                    borderRadius: 8,
                    padding: 10,
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-word',
                    color: palette.textSecondary,
                    fontSize: 12,
                    lineHeight: 1.6,
                  }}
                >
                  {String((result.web_context as any)?.summary || '-')}
                </pre>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {((result.web_sources || []) as Array<Record<string, unknown>>).slice(0, 10).map((item, idx) => (
                    <div
                      key={`web-source-${idx}`}
                      style={{ border: `1px solid ${palette.border}`, borderRadius: 8, padding: 10, background: '#fcfffd' }}
                    >
                      <div style={{ fontWeight: 600, color: palette.textPrimary, marginBottom: 4 }}>{String(item.title || '-')}</div>
                      <div style={{ color: palette.textSecondary, fontSize: 12, marginBottom: 4 }}>{String(item.summary || '-')}</div>
                      <a href={String(item.url || '#')} target="_blank" rel="noreferrer" style={{ color: '#1677ff', fontSize: 12 }}>
                        {String(item.url || '-')}
                      </a>
                    </div>
                  ))}
                  {!(result.web_sources || []).length ? <Empty description="未使用或未命中联网结果" image={Empty.PRESENTED_IMAGE_SIMPLE} /> : null}
                </div>
              </div>
            </Section>

            <Section title="表关联推理">
              {relations.length ? (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))', gap: 10 }}>
                  {relations.map((relation, idx) => (
                    <div
                      key={`${relation.left}-${relation.right}-${idx}`}
                      style={{ border: `1px solid ${palette.border}`, borderRadius: 8, padding: 12 }}
                    >
                      <div style={{ marginBottom: 8, color: palette.textPrimary, fontWeight: 600 }}>
                        {relation.left} {'->'} {relation.right}
                      </div>
                      <div style={{ marginBottom: 8, color: palette.textSecondary, fontSize: 13 }}>
                        keys: {relation.keys.join(', ') || '-'}
                      </div>
                      <div style={{ marginBottom: 8, color: palette.textSecondary, fontSize: 13 }}>
                        confidence: {renderConfidence(relation.confidence)}
                      </div>
                      <div style={{ color: palette.textSecondary, fontSize: 13 }}>{relation.reason || '-'}</div>
                    </div>
                  ))}
                </div>
              ) : (
                <Empty description="暂无关系假设" image={Empty.PRESENTED_IMAGE_SIMPLE} />
              )}
            </Section>

            <Section title="SQL 生成与修复">
              <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                <div>
                  <div style={{ marginBottom: 6, color: palette.textTertiary, fontSize: 12 }}>最终 SQL</div>
                  <pre
                    style={{
                      margin: 0,
                      background: palette.codeBg,
                      color: palette.codeText,
                      borderRadius: 8,
                      padding: 12,
                      overflowX: 'auto',
                      fontSize: 12,
                      lineHeight: 1.5,
                    }}
                  >
                    {result.sql || '--'}
                  </pre>
                </div>
                <div>
                  <div style={{ marginBottom: 6, color: palette.textTertiary, fontSize: 12 }}>候选 SQL ({result.candidate_sqls?.length || 0})</div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                    {(result.candidate_sqls || []).map((sql, idx) => (
                      <pre
                        key={`candidate-sql-${idx}`}
                        style={{
                          margin: 0,
                          background: '#0b1220',
                          color: palette.codeText,
                          borderRadius: 8,
                          padding: 10,
                          overflowX: 'auto',
                          fontSize: 12,
                          lineHeight: 1.5,
                        }}
                      >
                        {sql}
                      </pre>
                    ))}
                    {!(result.candidate_sqls || []).length ? <span style={{ color: palette.textTertiary }}>-</span> : null}
                  </div>
                </div>
                <div>
                  <div style={{ marginBottom: 6, color: palette.textTertiary, fontSize: 12 }}>SQL 错误</div>
                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    {(result.sql_errors || []).map((err, idx) => (
                      <Tag key={`sql-err-${idx}`} color="error">{err}</Tag>
                    ))}
                    {!(result.sql_errors || []).length ? <Tag color="success">无</Tag> : null}
                  </div>
                </div>
              </div>
            </Section>

            <Section title="SQL 执行结果">
              <div style={{ marginBottom: 10, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                <Tag color={(result.query_result?.success ?? false) ? 'success' : 'error'}>
                  {`success: ${String(result.query_result?.success ?? false)}`}
                </Tag>
                <Tag>{`row_count: ${result.query_result?.row_count ?? 0}`}</Tag>
              </div>
              <ResultTable queryResult={result.query_result} />
            </Section>

            <Section title="深度分析与结论">
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                <div
                  style={{
                    border: `1px solid ${palette.border}`,
                    borderRadius: 8,
                    padding: 12,
                    background: '#fcfffd',
                  }}
                >
                  <div style={{ marginBottom: 8, fontWeight: 600, color: palette.textPrimary }}>核心洞察</div>
                  {(result.analysis?.insights || []).length ? (
                    <ul style={{ margin: 0, paddingLeft: 16, color: palette.textPrimary, fontSize: 13, lineHeight: 1.8 }}>
                      {(result.analysis?.insights || []).map((insight, idx) => (
                        <li key={`insight-${idx}`}>{insight}</li>
                      ))}
                    </ul>
                  ) : (
                    <Empty description="暂无洞察" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                  )}
                </div>
                <div
                  style={{
                    border: `1px solid ${palette.border}`,
                    borderRadius: 8,
                    padding: 12,
                    background: '#fcfffd',
                  }}
                >
                  <div style={{ marginBottom: 8, fontWeight: 600, color: palette.textPrimary }}>统计信息</div>
                  <pre
                    style={{
                      margin: 0,
                      whiteSpace: 'pre-wrap',
                      wordBreak: 'break-word',
                      color: palette.textSecondary,
                      fontSize: 12,
                      lineHeight: 1.7,
                    }}
                  >
                    {JSON.stringify(result.analysis?.statistics || {}, null, 2)}
                  </pre>
                  <div style={{ marginTop: 8, color: palette.textSecondary, fontSize: 12 }}>
                    {`visualization_hint: ${result.analysis?.visualization_hint || '-'} | fallback: ${String(result.analysis?.fallback || false)}`}
                  </div>
                  <div style={{ marginTop: 6, color: palette.textSecondary, fontSize: 12 }}>
                    {`analysis_degraded: ${String(result.analysis_degraded ?? false)} | warnings: ${(result.analysis_warnings || []).length}`}
                  </div>
                </div>
              </div>
              <div
                style={{
                  marginTop: 12,
                  border: `1px solid ${palette.accentSoft}`,
                  background: '#f4fff7',
                  borderRadius: 8,
                  padding: 12,
                }}
              >
                <div style={{ marginBottom: 6, fontWeight: 600, color: palette.accent }}>最终结论</div>
                <div style={{ whiteSpace: 'pre-wrap', color: palette.textPrimary, fontSize: 13, lineHeight: 1.8 }}>
                  {result.final_answer || '-'}
                </div>
              </div>
            </Section>

            <Section title="毒舌评论家质检">
              <div style={{ marginBottom: 12, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                <Tag color={result.quality_score && result.quality_score >= 7 ? 'green' : 'red'}>
                  {`质检分: ${typeof result.quality_score === 'number' ? result.quality_score.toFixed(1) : '-'}`}
                </Tag>
                <Tag>{`未解决问题: ${result.unresolved_issues ?? 0}`}</Tag>
                <Tag>{`反馈数: ${(result.critic_feedback || []).length}`}</Tag>
              </div>
              {(result.critic_feedback || []).length ? (
                <div style={{ display: 'grid', gap: 10 }}>
                  {(result.critic_feedback || []).map((issue, idx) => {
                    const target = String(issue.target_section || issue.location || '全局')
                    const severity = String(issue.severity || 'minor')
                    const description = String(issue.description || issue.content || '-')
                    const suggestion = String(issue.suggestion || '-')
                    return (
                      <div
                        key={`critic-${idx}`}
                        style={{
                          border: `1px solid ${palette.border}`,
                          borderRadius: 8,
                          padding: 12,
                          background: '#fff9f5',
                        }}
                      >
                        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 8 }}>
                          <Tag color={severity === 'critical' ? 'red' : severity === 'major' ? 'orange' : 'blue'}>
                            {severity}
                          </Tag>
                          <Tag>{target}</Tag>
                          {issue.issue_type ? <Tag>{String(issue.issue_type)}</Tag> : null}
                        </div>
                        <div style={{ color: palette.textPrimary, marginBottom: 6, fontSize: 13, lineHeight: 1.7 }}>
                          {description}
                        </div>
                        <div style={{ color: palette.textSecondary, fontSize: 12, lineHeight: 1.6 }}>
                          {`建议: ${suggestion}`}
                        </div>
                      </div>
                    )
                  })}
                </div>
              ) : (
                <Empty description="暂无质检问题" image={Empty.PRESENTED_IMAGE_SIMPLE} />
              )}
            </Section>

            <Section title="流程日志">
              <PhaseLogs logs={result.logs} />
            </Section>
          </>
        ) : null}
      </div>
    </div>
  )
}
