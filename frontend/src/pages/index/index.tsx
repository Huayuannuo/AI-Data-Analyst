import { Button } from 'antd'
import { useNavigate } from 'react-router-dom'
import styles from './index.module.scss'

const featureTags = ['Schema 理解', '表关联推理', 'SQL 自修复', '深度分析', '飞书发布']

const quickStats = [
  { label: '对话主线', value: '自然语言分析' },
  { label: '数据入口', value: 'PostgreSQL + 飞书' },
  { label: '输出闭环', value: '报告 / 群消息' },
]

export default function Index() {
  const navigate = useNavigate()

  return (
    <div className={styles['index-page']}>
      <div className={styles.hero}>
        <div className={styles['hero__content']}>
          <div className={styles.badge}>数据分析主线</div>
          <h1 className={styles.title}>数据分析助手</h1>
          <p className={styles.desc}>
            面向复杂业务数仓的 AI Data Analyst Agent，聚焦 Schema 理解、SQL 生成、自修复与深度分析。
          </p>

          <div className={styles.actions}>
            <Button type="primary" size="large" onClick={() => navigate('/chat')}>
              进入对话
            </Button>
            <Button size="large" onClick={() => navigate('/showcase')}>
              查看流程
            </Button>
          </div>

          <div className={styles.tags}>
            {featureTags.map((tag) => (
              <span key={tag} className={styles.tag}>
                {tag}
              </span>
            ))}
          </div>
        </div>

        <div className={styles['hero__panel']}>
          {quickStats.map((item) => (
            <div key={item.label} className={styles.statCard}>
              <div className={styles.statLabel}>{item.label}</div>
              <div className={styles.statValue}>{item.value}</div>
            </div>
          ))}
          <div className={styles.panelNote}>
            当前版本已聚焦数据分析主线，保留数据库、对话、流程展示和飞书接入能力。
          </div>
        </div>
      </div>

      <div className={styles['card-list']}>
        <div className={styles['card-item']} onClick={() => navigate('/chat')}>
          <div className={styles['card-item__title']}>对话</div>
          <div className={styles['card-item__desc']}>支持数据库分析、前端演示模式、会话历史和中断控制。</div>
        </div>

        <div className={styles['card-item']} onClick={() => navigate('/showcase')}>
          <div className={styles['card-item__title']}>流程展示</div>
          <div className={styles['card-item__desc']}>展示问题理解、主体识别、SQL 生成、自修复与深度分析闭环。</div>
        </div>

        <div className={styles['card-item']} onClick={() => navigate('/database')}>
          <div className={styles['card-item__title']}>数据库</div>
          <div className={styles['card-item__desc']}>查看核心业务表、字段结构和自然语言查询结果。</div>
        </div>

        <div className={styles['card-item']} onClick={() => navigate('/feishu')}>
          <div className={styles['card-item__title']}>飞书接入</div>
          <div className={styles['card-item__desc']}>联通多维表格、文档和群消息，形成分析发布闭环。</div>
        </div>
      </div>
    </div>
  )
}
