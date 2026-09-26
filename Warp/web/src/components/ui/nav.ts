// Single source of truth for navigation: the rail groups, the breadcrumb and the per-section tab strips.
// A nav item is a *section* (what the mock shows in the rail). A section may own several routes;
// they render as a tab strip above the page (RouteTabs), so every page stays reachable.
// Wave 2 replaces the placeholder-free tab lists below with real section landing pages.
import {
  Activity, Boxes, FlaskConical, Gauge, KeyRound, Layers, LayoutDashboard, PlugZap, Route, Server, Shield,
  Table2,
} from 'lucide-react'
import type { LucideIcon } from 'lucide-react'

export interface NavTab { to: string; label: string }
export interface NavSection {
  /** Rail label (also the breadcrumb). */
  label: string
  icon: LucideIcon
  /** Route the rail item links to (the first tab). */
  to: string
  /** Every route owned by the section. More than one renders a tab strip. */
  tabs: NavTab[]
}
export interface NavGroup { label: string; items: NavSection[] }

export const NAV_GROUPS: NavGroup[] = [
  {
    label: 'Workspace',
    items: [
      { label: 'Overview', icon: LayoutDashboard, to: '/overview', tabs: [{ to: '/overview', label: 'Overview' }] },
      { label: 'Workloads', icon: Boxes, to: '/workloads', tabs: [
        { to: '/workloads', label: 'Workloads' }, { to: '/data', label: 'Data explorer' }, { to: '/federation-plans', label: 'Federation plans' },
      ] },
      { label: 'Traffic', icon: Activity, to: '/metrics', tabs: [{ to: '/metrics', label: 'Traffic' }, { to: '/topology', label: 'Topology' }] },
      { label: 'Caching', icon: Layers, to: '/rollups', tabs: [{ to: '/rollups', label: 'Rollups' }] },
      { label: 'Routing & QoS', icon: Route, to: '/router', tabs: [
        { to: '/router', label: 'Router rules' }, { to: '/qos', label: 'QoS' }, { to: '/queues', label: 'Queues' },
      ] },
      { label: 'Policies', icon: Shield, to: '/firewall', tabs: [{ to: '/firewall', label: 'SQL firewall' }] },
      { label: 'Access & ACLs', icon: KeyRound, to: '/acl', tabs: [{ to: '/acl', label: 'ACL' }, { to: '/oauth', label: 'OAuth' }] },
      { label: 'Infrastructure', icon: Server, to: '/infrastructure', tabs: [{ to: '/infrastructure', label: 'Backend sets' }] },
    ],
  },
  {
    label: 'Interfaces',
    items: [
      { label: 'SQL drivers', icon: Table2, to: '/interfaces/sql', tabs: [{ to: '/interfaces/sql', label: 'SQL drivers' }] },
      { label: 'API endpoints', icon: Gauge, to: '/interfaces/api', tabs: [{ to: '/interfaces/api', label: 'API endpoints' }] },
      { label: 'MCP servers', icon: PlugZap, to: '/interfaces/mcp', tabs: [{ to: '/interfaces/mcp', label: 'MCP servers' }] },
    ],
  },
  {
    label: 'Migration',
    items: [
      { label: 'Compatibility lab', icon: FlaskConical, to: '/ab-routing', tabs: [
        { to: '/ab-routing', label: 'A/B routing' }, { to: '/llm-config', label: 'Dialect translation (LLM)' },
      ] },
    ],
  },
]

export const ALL_SECTIONS: NavSection[] = NAV_GROUPS.flatMap((g) => g.items)

/** The section owning `pathname` (exact or nested route), or undefined for unknown paths. */
export function sectionFor(pathname: string): NavSection | undefined {
  return ALL_SECTIONS.find((s) => s.tabs.some((t) => pathname === t.to || pathname.startsWith(t.to + '/')))
}
