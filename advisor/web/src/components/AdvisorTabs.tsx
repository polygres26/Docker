import { NavLink } from 'react-router-dom'
import styles from './AdvisorTabs.module.css'

const TABS = [
  { to: '/connections', label: 'Connections' },
  { to: '/reports', label: 'Reports' },
  { to: '/sizing', label: 'Sizing' },
]

/**
 * Migration's three sub-areas as tabs on one page, not three separate sidebar entries -- the
 * sidebar leads with PolyWire and collapses this to a single "Migration" destination; this tab
 * strip is what makes its own pages still reachable from each other. A route match on any TAB
 * path renders this same strip via each page composing it at the top, so switching tabs is a
 * normal client-side navigation, not a nested router.
 *
 * LLM configuration used to be a fourth tab here, but it isn't migration-specific -- the same
 * Primary/Judge model config also drives PolyWire's SQL translation and any other feature that
 * calls the LLM, so it moved out to its own top-level "Shared" sidebar entry (see Layout).
 */
export default function AdvisorTabs() {
  return (
    <div className={styles.tabs}>
      {TABS.map((t) => (
        <NavLink key={t.to} to={t.to} className={({ isActive }) => `${styles.tab} ${isActive ? styles.tabActive : ''}`}>
          {t.label}
        </NavLink>
      ))}
    </div>
  )
}
