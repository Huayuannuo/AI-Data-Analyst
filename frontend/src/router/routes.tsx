import { AuthGuard } from '@/components/auth-guard'
import { BaseLayout } from '@/layout/base'
import NotFound from '@/pages/404'
import LoginPage from '@/pages/auth/login'
import DatabasePage from '@/pages/database'
import FeishuPage from '@/pages/feishu'
import Index from '@/pages/index'
import AIDataAnalystChatPage from '@/pages/chat'
import AIDataAnalystShowcasePage from '@/pages/showcase'
import { Navigate, Outlet, RouteObject, createBrowserRouter } from 'react-router-dom'

export type IRouteObject = {
  children?: IRouteObject[]
  name?: string
  auth?: boolean
  pure?: boolean
  meta?: any
} & Omit<RouteObject, 'children'>

export const routes: IRouteObject[] = [
  {
    path: '/',
    Component: Index,
  },
  {
    path: '/chat',
    Component: AIDataAnalystChatPage,
  },
  {
    path: '/showcase',
    Component: AIDataAnalystShowcasePage,
  },
  {
    path: '/demo',
    Component: AIDataAnalystShowcasePage,
  },
  {
    path: '/database',
    Component: DatabasePage,
  },
  {
    path: '/feishu',
    Component: FeishuPage,
  },
  {
    path: '/404',
    Component: NotFound,
    pure: true,
  },
]

export const router = createBrowserRouter(
  [
    {
      path: '/login',
      element: <LoginPage />,
    },
    {
      path: '/',
      element: (
        <AuthGuard>
          <BaseLayout>
            <Outlet />
          </BaseLayout>
        </AuthGuard>
      ),
      children: routes,
    },
    {
      path: '*',
      element: <Navigate to="/404" />,
    },
  ] as RouteObject[],
  {
    basename: import.meta.env.BASE_URL,
  },
)
