const MODE_LABELS: Record<string, string> = {
  none: '关闭',
  original_agents: '子智能体增强',
}

const AGENT_LABELS: Record<string, string> = {
  ChiefArchitect: '总架构师',
  DeepScout: '深度侦探',
  LeadWriter: '首席笔杆',
  CriticMaster: '毒舌评论家',
  architect: '总架构师',
  scout: '深度侦探',
  writer: '首席笔杆',
  critic: '毒舌评论家',
}

export function getEnhancementModeLabel(mode?: string) {
  if (!mode) return MODE_LABELS.none
  return MODE_LABELS[mode] || mode
}

export function getEnhancementAgentLabel(name?: string) {
  if (!name) return '-'
  return AGENT_LABELS[name] || name
}
