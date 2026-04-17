import { useEffect, useMemo, useState } from 'react'
import { Alert, Button, Card, Divider, Input, InputNumber, List, message, Row, Col, Space, Spin, Tag, Typography } from 'antd'
import * as api from '@/api'

const { TextArea } = Input
const { Text, Title } = Typography

function unwrapResponse<T>(res: T | { data: T }): T {
  if (typeof res === 'object' && res !== null && 'data' in res) {
    return (res as { data: T }).data
  }
  return res as T
}

function JsonBlock({ value }: { value: unknown }) {
  return (
    <pre
      style={{
        margin: 0,
        padding: 12,
        borderRadius: 8,
        background: '#0f172a',
        color: '#e2e8f0',
        overflowX: 'auto',
        fontSize: 12,
        lineHeight: 1.6,
      }}
    >
      {JSON.stringify(value, null, 2)}
    </pre>
  )
}

export default function FeishuIntegrationPage() {
  const [loading, setLoading] = useState(false)
  const [health, setHealth] = useState<Record<string, unknown> | null>(null)
  const [tokenStatus, setTokenStatus] = useState<Record<string, unknown> | null>(null)
  const [appToken, setAppToken] = useState('')
  const [tableIdsText, setTableIdsText] = useState('')
  const [previewSampleSize, setPreviewSampleSize] = useState(3)
  const [previewResult, setPreviewResult] = useState<Record<string, unknown> | null>(null)
  const [syncTargetsText, setSyncTargetsText] = useState(`[
  {
    "target_table": "industry_stats",
    "table_id": "",
    "field_mapping": {
      "行业名称": "industry_name",
      "指标名称": "metric_name",
      "指标值": "metric_value",
      "单位": "unit",
      "年份": "year",
      "季度": "quarter",
      "月份": "month",
      "地区": "region",
      "来源": "source",
      "来源链接": "source_url",
      "备注": "notes"
    },
    "sample_limit": 200,
    "clear_before_sync": false
  }
]`)
  const [syncResult, setSyncResult] = useState<Record<string, unknown> | null>(null)
  const [resultSessionId, setResultSessionId] = useState('')
  const [analysisResult, setAnalysisResult] = useState<Record<string, unknown> | null>(null)
  const [docTitle, setDocTitle] = useState('AI Data Analyst 分析报告')
  const [folderToken, setFolderToken] = useState('')
  const [publishResult, setPublishResult] = useState<Record<string, unknown> | null>(null)
  const [chatId, setChatId] = useState('')
  const [imMessage, setImMessage] = useState('')
  const [imResult, setImResult] = useState<Record<string, unknown> | null>(null)

  const canPublish = useMemo(() => !!analysisResult, [analysisResult])

  useEffect(() => {
    void loadHealth()
    void loadTokenStatus()
  }, [])

  const loadHealth = async () => {
    try {
      const res = await api.feishu.getHealth()
      setHealth(unwrapResponse(res) as Record<string, unknown>)
    } catch (error) {
      message.error(error instanceof Error ? error.message : '获取飞书健康状态失败')
    }
  }

  const loadTokenStatus = async () => {
    try {
      const res = await api.feishu.getTokenStatus()
      setTokenStatus(unwrapResponse(res) as Record<string, unknown>)
    } catch {
      setTokenStatus(null)
    }
  }

  const handlePreview = async () => {
    if (!appToken.trim()) {
      message.warning('请输入 app_token')
      return
    }
    setLoading(true)
    try {
      const res = await api.feishu.previewBitable({
        app_token: appToken.trim(),
        table_ids: tableIdsText
          .split(/[\n,，]/)
          .map((item) => item.trim())
          .filter(Boolean),
        sample_size: previewSampleSize,
      })
      setPreviewResult(unwrapResponse(res) as Record<string, unknown>)
      message.success('预览完成')
    } catch (error) {
      message.error(error instanceof Error ? error.message : '预览失败')
    } finally {
      setLoading(false)
    }
  }

  const handleSync = async () => {
    if (!appToken.trim()) {
      message.warning('请输入 app_token')
      return
    }
    let targets: unknown
    try {
      targets = JSON.parse(syncTargetsText)
    } catch {
      message.error('同步配置 JSON 格式不正确')
      return
    }
    setLoading(true)
    try {
      const res = await api.feishu.syncBitable({
        app_token: appToken.trim(),
        targets: targets as any,
        sample_only: false,
      })
      setSyncResult(unwrapResponse(res) as Record<string, unknown>)
      message.success('同步完成')
    } catch (error) {
      message.error(error instanceof Error ? error.message : '同步失败')
    } finally {
      setLoading(false)
    }
  }

  const loadAnalysisResult = async () => {
    if (!resultSessionId.trim()) {
      message.warning('请输入会话ID')
      return
    }
    setLoading(true)
    try {
      const res = await api.aiDataAnalyst.getLatestResult(resultSessionId.trim())
      const payload = unwrapResponse(res) as Record<string, unknown>
      setAnalysisResult(payload)
      setImMessage(String(payload.final_answer || ''))
      message.success('结果已加载')
    } catch (error) {
      message.error(error instanceof Error ? error.message : '加载失败')
    } finally {
      setLoading(false)
    }
  }

  const handlePublish = async () => {
    if (!analysisResult) {
      message.warning('请先加载结果')
      return
    }
    setLoading(true)
    try {
      const res = await api.feishu.publishDoc({
        title: docTitle.trim() || 'AI Data Analyst 分析报告',
        result: analysisResult,
        folder_token: folderToken.trim() || undefined,
      })
      setPublishResult(unwrapResponse(res) as Record<string, unknown>)
      message.success('文档已创建')
    } catch (error) {
      message.error(error instanceof Error ? error.message : '文档发布失败')
    } finally {
      setLoading(false)
    }
  }

  const handleSendIM = async () => {
    if (!chatId.trim()) {
      message.warning('请输入群聊 chat_id')
      return
    }
    if (!imMessage.trim()) {
      message.warning('请输入要发送的消息')
      return
    }
    setLoading(true)
    try {
      const res = await api.feishu.sendIM({
        receive_id: chatId.trim(),
        receive_id_type: 'chat_id',
        message: imMessage,
      })
      setImResult(unwrapResponse(res) as Record<string, unknown>)
      message.success('消息已发送')
    } catch (error) {
      message.error(error instanceof Error ? error.message : '消息发送失败')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ padding: 24, background: '#f5f7fb', minHeight: '100vh' }}>
      <Spin spinning={loading}>
        <Space direction="vertical" size={16} style={{ width: '100%' }}>
          <Card>
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              <Title level={3} style={{ margin: 0 }}>飞书接入控制台</Title>
              <Text type="secondary">用于联通飞书多维表格、文档和群消息，与分析结果形成闭环。</Text>
              <Space wrap>
                <Button onClick={loadHealth}>刷新健康状态</Button>
                <Button onClick={loadTokenStatus}>检查 Token</Button>
              </Space>
              <Row gutter={16}>
                <Col span={12}>
                  <Card size="small" title="后端配置状态">
                    <JsonBlock value={health || {}} />
                  </Card>
                </Col>
                <Col span={12}>
                  <Card size="small" title="Token 状态">
                    <JsonBlock value={tokenStatus || {}} />
                  </Card>
                </Col>
              </Row>
            </Space>
          </Card>

          <Card title="多维表格同步">
            <Space direction="vertical" size={12} style={{ width: '100%' }}>
              <Space wrap>
                <Input
                  value={appToken}
                  onChange={(e) => setAppToken(e.target.value)}
                  placeholder="请输入 feishu app_token"
                  style={{ width: 360 }}
                />
                <Input
                  value={tableIdsText}
                  onChange={(e) => setTableIdsText(e.target.value)}
                  placeholder="可选：table_id，多个用逗号分隔"
                  style={{ width: 420 }}
                />
                <InputNumber min={1} max={10} value={previewSampleSize} onChange={(value) => setPreviewSampleSize(value || 3)} />
                <Button onClick={handlePreview}>预览多维表格</Button>
                <Button type="primary" onClick={handleSync}>同步到 PostgreSQL</Button>
              </Space>
              <TextArea rows={10} value={syncTargetsText} onChange={(e) => setSyncTargetsText(e.target.value)} />
              {previewResult ? (
                <Card size="small" title="预览结果">
                  <JsonBlock value={previewResult} />
                </Card>
              ) : null}
              {syncResult ? (
                <Card size="small" title="同步结果">
                  <JsonBlock value={syncResult} />
                </Card>
              ) : null}
            </Space>
          </Card>

          <Card title="分析结果发布到飞书文档 / 群消息">
            <Space direction="vertical" size={12} style={{ width: '100%' }}>
              <Space wrap>
                <Input
                  value={resultSessionId}
                  onChange={(e) => setResultSessionId(e.target.value)}
                  placeholder="请输入会话ID"
                  style={{ width: 360 }}
                />
                <Button onClick={loadAnalysisResult}>加载结果</Button>
              </Space>
              <Space wrap>
                <Input value={docTitle} onChange={(e) => setDocTitle(e.target.value)} placeholder="文档标题" style={{ width: 360 }} />
                <Input value={folderToken} onChange={(e) => setFolderToken(e.target.value)} placeholder="可选：文档目录 token" style={{ width: 360 }} />
                <Button type="primary" disabled={!canPublish} onClick={handlePublish}>生成飞书文档</Button>
              </Space>
              {publishResult ? (
                <Card size="small" title="文档发布结果">
                  <JsonBlock value={publishResult} />
                </Card>
              ) : null}
              <Divider style={{ margin: '12px 0' }} />
              <Space wrap>
                <Input value={chatId} onChange={(e) => setChatId(e.target.value)} placeholder="群聊 chat_id" style={{ width: 360 }} />
                <Button onClick={handleSendIM}>发送群消息</Button>
              </Space>
              <TextArea rows={6} value={imMessage} onChange={(e) => setImMessage(e.target.value)} />
              {imResult ? (
                <Card size="small" title="群消息发送结果">
                  <JsonBlock value={imResult} />
                </Card>
              ) : null}
            </Space>
          </Card>

          <Card title="结果预览">
            {analysisResult ? (
              <JsonBlock value={analysisResult} />
            ) : (
              <Alert message="先加载一个会话结果，再发布到飞书文档/群消息。" type="info" showIcon />
            )}
          </Card>
        </Space>
      </Spin>
    </div>
  )
}
