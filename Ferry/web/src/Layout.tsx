import { Compass, Cpu, LayoutDashboard, RefreshCw } from 'lucide-react'
import { useLocation } from 'react-router-dom'
import AppShell, { NavGroup, NavItem } from './ui/AppShell'

// Assessments is one sidebar entry for the Migration Advisor half: Connections, Reports, Sizing
// and Quick scan are tabs on each other (see components/DmsTabs), so the entry stays highlighted
// on all of them. Data sync is the Migration Service half (Launch / Status tabs).
const ASSESSMENT_ROUTES = ['/connections', '/reports', '/sizing', '/quick-scan', '/report']
const DATA_SYNC_ROUTES = ['/data-sync']

const SECTIONS: Array<{ prefixes: string[]; label: string }> = [
  { prefixes: ['/dashboard'], label: 'Overview' },
  { prefixes: ASSESSMENT_ROUTES, label: 'Assessments' },
  { prefixes: DATA_SYNC_ROUTES, label: 'Data sync' },
  { prefixes: ['/llm-settings'], label: 'LLM settings' },
]

/** Shell for every authenticated route: grouped sidebar (Overview / Migrate / Configure) + topbar. */
export default function Layout({ children }: { children: React.ReactNode }) {
  const { pathname } = useLocation()
  const section = SECTIONS.find((sec) => sec.prefixes.some((p) => pathname === p || pathname.startsWith(p + '/')))?.label

  return (
    <AppShell
      section={section}
      nav={
        <>
          <NavGroup label="Overview">
            <NavItem to="/dashboard" icon={LayoutDashboard} label="Overview" />
          </NavGroup>
          <NavGroup label="Migrate">
            <NavItem to="/connections" icon={Compass} label="Assessments" match={ASSESSMENT_ROUTES} />
            <NavItem to="/data-sync" icon={RefreshCw} label="Data sync" match={DATA_SYNC_ROUTES} />
          </NavGroup>
          <NavGroup label="Configure">
            <NavItem to="/llm-settings" icon={Cpu} label="LLM settings" />
          </NavGroup>
        </>
      }
    >
      {children}
    </AppShell>
  )
}
