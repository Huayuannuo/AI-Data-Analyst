import IconHome from '@/assets/layout/home.svg'
import IconMessage from '@/assets/layout/message.svg'
import { useLocation } from 'react-router-dom'
import { NavItem } from './nav-item'
import './nav.scss'

export function Nav() {
  const { pathname } = useLocation()

  const items = [
    {
      key: 'home',
      label: '首页',
      icon: IconHome,
      href: '/',
    },
    {
      key: 'chat',
      label: '对话',
      icon: IconMessage,
      href: '/chat',
    },
    {
      key: 'showcase',
      label: '流程展示',
      icon: IconMessage,
      href: '/showcase',
    },
    {
      key: 'database',
      label: '数据库',
      icon: IconMessage,
      href: '/database',
    },
    {
      key: 'feishu',
      label: '飞书接入',
      icon: IconMessage,
      href: '/feishu',
    },
  ] as const

  return (
    <div className="base-layout-nav">
      {items.map((item) => (
        <NavItem
          key={item.key}
          icon={item.icon}
          label={item.label}
          href={item.href}
          active={pathname === item.href}
        />
      ))}
    </div>
  )
}
