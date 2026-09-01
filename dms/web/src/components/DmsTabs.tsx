import { NavLink } from 'react-router-dom'
import styles from './DmsTabs.module.css'

export interface DmsTab {
  to: string
  label: string
}

const MIGRATION_ADVISOR_TABS: DmsTab[] = [
  { to: '/connections', label: 'Connections' },
  { to: '/reports', label: 'Reports' },
  { to: '/sizing', label: 'Sizing' },
]

/**
 * A sidebar section's own sub-areas as tabs on one page, not one separate sidebar entry per
 * sub-area -- the sidebar collapses each of DMS's two halves (Migration Advisor, Migration
 * Service) to a single destination; this tab strip is what makes each half's own pages still
 * reachable from each other. A route match on any tab's path renders this same strip via each
 * page composing it at the top, so switching tabs is a normal client-side navigation, not a
 * nested router. Defaults to Migration Advisor's own three tabs (its original, only caller);
 * pass `tabs` explicitly for a different section (e.g. Migration Service's Launch/Status).
 *
 * LLM configuration used to be a fourth Migration Advisor tab here, but it isn't
 * assessment-specific -- the same Primary/Judge model config also drives Warp's SQL
 * translation and any other feature that calls the LLM, so it moved out to its own top-level
 * "Shared" sidebar entry (see Layout).
 */
export default function DmsTabs({ tabs = MIGRATION_ADVISOR_TABS }: { tabs?: DmsTab[] }) {
  return (
    <div className={styles.tabs}>
      {tabs.map((t) => (
        <NavLink key={t.to} to={t.to} className={({ isActive }) => `${styles.tab} ${isActive ? styles.tabActive : ''}`}>
          {t.label}
        </NavLink>
      ))}
    </div>
  )
}
