import { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Button, Checkbox, Empty, Input, InputNumber, List, Popconfirm, Spin, Tag, message } from 'antd'
import { DeleteOutlined } from '@ant-design/icons'
import * as api from '@/api'
import type { AnalyzeSyncResponse } from '@/api/ai-data-analyst'
import type { Message, Session } from '@/api/session'
import { getEnhancementAgentLabel, getEnhancementModeLabel } from '@/utils/enhancement'

const defaultQuery = '请分析智慧交通近三年的市场规模趋势，并对重点公司营收做对比'

function unwrapResponse<T>(res: T | { data: T }): T {
  if (typeof res === 'object' && res !== null && 'data' in res) {
    return (res as { data: T }).data
  }
  return res as T
}

function createTempAssistant(sessionId: string): Message {
  const now = new Date().toISOString()
  return {
    id: `temp-assistant-${Date.now()}`,
    session_id: sessionId,
    role: 'assistant',
    content: '分析中，请稍候...',
    created_at: now,
  }
}

export default function AIDataAnalystChatPage() {
  const navigate = useNavigate()

  const [sessions, setSessions] = useState<Session[]>([])
  const [currentSessionId, setCurrentSessionId] = useState<string>('')
  const [messagesList, setMessagesList] = useState<Message[]>([])
  const [query, setQuery] = useState(defaultQuery)
  const [loadingSessions, setLoadingSessions] = useState(false)
  const [running, setRunning] = useState(false)
  const [runningSessionId, setRunningSessionId] = useState<string>('')
  const [enableWebEnrichment, setEnableWebEnrichment] = useState(false)
  const [enableOriginalAgents, setEnableOriginalAgents] = useState(false)
  const [webTopK, setWebTopK] = useState(3)
  const [useFrontendDemo, setUseFrontendDemo] = useState(false)
  const [latestResult, setLatestResult] = useState<AnalyzeSyncResponse | null>(null)

  const hasMessages = messagesList.length > 0

  const currentSession = useMemo(
    () => sessions.find((item) => item.id === currentSessionId),
    [sessions, currentSessionId],
  )

  const refreshSessions = async () => {
    setLoadingSessions(true)
    try {
      const res = await api.session.getSessions({ limit: 100, session_type: 'ai_data_analyst' })
      const list = unwrapResponse(res)
      setSessions(Array.isArray(list) ? list : [])
    } catch (err: unknown) {
      message.error(err instanceof Error ? err.message : '获取会话失败')
    } finally {
      setLoadingSessions(false)
    }
  }

  const loadSessionMessages = async (sessionId: string) => {
    try {
      const res = await api.session.getSession(sessionId)
      const payload = unwrapResponse(res)
      setCurrentSessionId(sessionId)
      setMessagesList(payload.messages || [])
      try {
        const resultRes = await api.aiDataAnalyst.getLatestResult(sessionId)
        setLatestResult(unwrapResponse(resultRes))
      } catch {
        setLatestResult(null)
      }
    } catch (err: unknown) {
      message.error(err instanceof Error ? err.message : '加载会话失败')
    }
  }

  const deleteSession = async (sessionId: string) => {
    try {
      await api.session.deleteSession(sessionId)
      setSessions((prev) => prev.filter((item) => item.id !== sessionId))
      if (currentSessionId === sessionId) {
        const nextSession = sessions.find((item) => item.id !== sessionId)
        setCurrentSessionId(nextSession?.id || '')
        setMessagesList([])
      }
      message.success('会话已删除')
    } catch (err: unknown) {
      message.error(err instanceof Error ? err.message : '删除会话失败')
    }
  }

  useEffect(() => {
    refreshSessions()
  }, [])

  useEffect(() => {
    if (!sessions.length || currentSessionId) return
    const first = sessions[0]
    if (first) {
      loadSessionMessages(first.id)
    }
  }, [sessions, currentSessionId])

  const ensureSession = async (firstUserMessage: string): Promise<string> => {
    if (currentSessionId) return currentSessionId

    const title = firstUserMessage.slice(0, 20) + (firstUserMessage.length > 20 ? '...' : '')
      const created = await api.session.createSession({
        title,
        session_type: 'ai_data_analyst',
      })
    const createdSession = unwrapResponse(created)

    setSessions((prev) => [createdSession, ...prev])
    setCurrentSessionId(createdSession.id)
    return createdSession.id
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

  const sendQuery = async () => {
    const prompt = query.trim()
    if (!prompt) {
      message.warning('请输入分析问题')
      return
    }

    if (running) {
      message.info('当前有任务执行中，请先停止或等待完成')
      return
    }

    let sessionId = ''
    try {
      sessionId = await ensureSession(prompt)
    } catch (err: unknown) {
      message.error(err instanceof Error ? err.message : '创建会话失败')
      return
    }

    const now = new Date().toISOString()
    const userMsg: Message = {
      id: `temp-user-${Date.now()}`,
      session_id: sessionId,
      role: 'user',
      content: prompt,
      created_at: now,
    }
    const assistantTemp = createTempAssistant(sessionId)

    setMessagesList((prev) => [...prev, userMsg, assistantTemp])
    setRunning(true)
    setRunningSessionId(sessionId)

    try {
      const metadata = useFrontendDemo ? await buildFrontendDemoMetadata() : undefined
      const res = await api.aiDataAnalyst.analyzeSync({
        query: prompt,
        session_id: sessionId,
        metadata,
        save_history: true,
        enable_web_enrichment: enableWebEnrichment,
        enable_original_agents: enableOriginalAgents,
        web_top_k: webTopK,
        data_source_mode: useFrontendDemo ? 'frontend_demo' : 'database',
      })
      const payload = unwrapResponse<AnalyzeSyncResponse>(res)
      setLatestResult(payload)

      localStorage.setItem(`analysis_result_${sessionId}`, JSON.stringify(payload))

      setMessagesList((prev) =>
        prev.map((m) =>
          m.id === assistantTemp.id
            ? {
                ...m,
                content: payload.final_answer || '分析完成',
              }
            : m,
        ),
      )

      await refreshSessions()
      await loadSessionMessages(sessionId)
    } catch (err: unknown) {
      const errMsg = err instanceof Error ? err.message : '分析请求失败'
      setMessagesList((prev) =>
        prev.map((m) =>
          m.id === assistantTemp.id
            ? {
                ...m,
                content: `分析失败: ${errMsg}`,
              }
            : m,
        ),
      )
      message.error(errMsg)
    } finally {
      setRunning(false)
      setRunningSessionId('')
    }
  }

  const stopCurrent = async () => {
    if (!runningSessionId) {
      message.info('当前没有可中断任务')
      return
    }

    try {
      await api.aiDataAnalyst.cancel(runningSessionId)
      message.success('已发送中断请求')
    } catch (err: unknown) {
      message.error(err instanceof Error ? err.message : '中断请求失败')
    }
  }

  return (
    <div style={{ minHeight: '100vh', background: '#f6f8fb', padding: 16 }}>
      <div style={{ maxWidth: 1320, margin: '0 auto', display: 'grid', gridTemplateColumns: '300px 1fr', gap: 16 }}>
        <div style={{ background: '#fff', border: '1px solid #e8edf3', borderRadius: 12, overflow: 'hidden' }}>
          <div style={{ padding: 12, borderBottom: '1px solid #e8edf3', display: 'flex', justifyContent: 'space-between' }}>
            <div style={{ fontWeight: 600 }}>对话历史</div>
            <Button size="small" onClick={refreshSessions} loading={loadingSessions}>刷新</Button>
          </div>
          <div style={{ maxHeight: 'calc(100vh - 140px)', overflowY: 'auto' }}>
            <List
              dataSource={sessions}
              locale={{ emptyText: <Empty description="暂无会话" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
              renderItem={(item) => (
                <List.Item
                  style={{
                    padding: '10px 12px',
                    cursor: 'pointer',
                    background: item.id === currentSessionId ? '#eef7ff' : '#fff',
                    borderLeft: item.id === currentSessionId ? '3px solid #1677ff' : '3px solid transparent',
                  }}
                  onClick={() => loadSessionMessages(item.id)}
                  actions={[
                    <Popconfirm
                      key="delete"
                      title="确定删除这条历史记录？"
                      description="删除后无法恢复"
                      onConfirm={(e) => {
                        e?.stopPropagation?.()
                        void deleteSession(item.id)
                      }}
                      onCancel={(e) => e?.stopPropagation?.()}
                    >
                      <Button
                        type="text"
                        size="small"
                        danger
                        icon={<DeleteOutlined />}
                        onClick={(e) => e.stopPropagation()}
                      />
                    </Popconfirm>,
                  ]}
                >
                  <div>
                    <div style={{ fontWeight: 600, color: '#1f2937' }}>{item.title || '新对话'}</div>
                    <div style={{ fontSize: 12, color: '#6b7280' }}>{item.updated_at?.replace('T', ' ').slice(0, 19)}</div>
                  </div>
                </List.Item>
              )}
            />
          </div>
        </div>

        <div style={{ background: '#fff', border: '1px solid #e8edf3', borderRadius: 12, display: 'flex', flexDirection: 'column' }}>
          <div style={{ padding: 12, borderBottom: '1px solid #e8edf3', display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
            <Tag color="blue">数据分析对话</Tag>
            <Tag>{`会话: ${currentSession?.title || '未选择'}`}</Tag>
            {latestResult ? (
              <>
                <Tag>{`增强模式: ${getEnhancementModeLabel(latestResult.enhancement_mode)}`}</Tag>
                <Tag color={latestResult.quality_score && latestResult.quality_score >= 7 ? 'green' : 'orange'}>
                  {`质检分: ${typeof latestResult.quality_score === 'number' ? latestResult.quality_score.toFixed(1) : '-'}`}
                </Tag>
                <Tag>{`策略: ${latestResult.selected_strategy || '-'}`}</Tag>
                {(latestResult.enhancement_agents || []).length ? (
                  <>
                    {latestResult.enhancement_agents!.map((agent) => (
                      <Tag key={agent}>{getEnhancementAgentLabel(agent)}</Tag>
                    ))}
                  </>
                ) : null}
              </>
            ) : null}
            <Checkbox checked={enableWebEnrichment} onChange={(e) => setEnableWebEnrichment(e.target.checked)}>
              联网增强
            </Checkbox>
            <Checkbox checked={enableOriginalAgents} onChange={(e) => setEnableOriginalAgents(e.target.checked)}>
              子智能体增强
            </Checkbox>
            <Checkbox checked={useFrontendDemo} onChange={(e) => setUseFrontendDemo(e.target.checked)}>
              前端演示模式
            </Checkbox>
            <InputNumber
              min={1}
              max={10}
              value={webTopK}
              disabled={!enableWebEnrichment}
              onChange={(v) => setWebTopK(v ?? 3)}
              addonBefore="TopK"
            />
            {currentSessionId ? (
              <Button size="small" onClick={() => navigate(`/showcase?session_id=${currentSessionId}`)}>
                查看流程
              </Button>
            ) : null}
          </div>

          <div style={{ flex: 1, overflowY: 'auto', padding: 16, background: '#f8fafc' }}>
            {!hasMessages ? (
              <Empty description="发送第一条问题开始对话" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                {messagesList.map((item) => (
                  <div
                    key={item.id}
                    style={{
                      maxWidth: '85%',
                      alignSelf: item.role === 'user' ? 'flex-end' : 'flex-start',
                      background: item.role === 'user' ? '#14532d' : '#ffffff',
                      color: item.role === 'user' ? '#ffffff' : '#1f2937',
                      border: item.role === 'user' ? 'none' : '1px solid #e5e7eb',
                      borderRadius: 10,
                      padding: '10px 12px',
                      whiteSpace: 'pre-wrap',
                      lineHeight: 1.6,
                    }}
                  >
                    {item.content}
                  </div>
                ))}
              </div>
            )}
          </div>

          <div style={{ borderTop: '1px solid #e8edf3', padding: 12 }}>
            <Input.TextArea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              autoSize={{ minRows: 2, maxRows: 4 }}
              placeholder="输入分析问题，回车发送"
              onPressEnter={(e) => {
                if (!e.shiftKey) {
                  e.preventDefault()
                  sendQuery()
                }
              }}
            />
            <div style={{ marginTop: 10, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div style={{ color: '#6b7280', fontSize: 12 }}>
                对话自动保存到会话历史
              </div>
              <div style={{ display: 'flex', gap: 8 }}>
                <Button danger onClick={stopCurrent} disabled={!runningSessionId}>停止分析</Button>
                <Button type="primary" onClick={sendQuery} disabled={running}>
                  {running ? <><Spin size="small" /> 分析中</> : '发送'}
                </Button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
