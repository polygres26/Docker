import { useLocation } from 'react-router-dom'
import type { ScanResult } from '../api/client'
import { tierTone } from '../lib/tone'
import {
  DataTable, EmptyState, KpiStrip, LinkButton, Notice, PageHeader, Section, StatusPill,
} from '../ui'
import { table } from '../ui/styles'
import styles from './Report.module.css'

export default function Report() {
  const location = useLocation()
  const result = location.state as ScanResult | undefined

  if (!result) {
    return (
      <>
        <PageHeader title="Migration assessment" />
        <Section>
          <EmptyState title="No scan result to show" action={<LinkButton to="/quick-scan" variant="primary">Run a scan</LinkButton>}>
            Run a quick scan first, or open a saved connection.
          </EmptyState>
        </Section>
      </>
    )
  }

  const { snapshot, score } = result
  const [tierName, tierDetail] = score.tier.split(' -- ')

  const inventory: Array<[string, string | number]> = [
    ['Tables', snapshot.tableCount],
    ['Views', snapshot.viewCount],
    ['Materialized views', snapshot.materializedViewCount],
    ['Sequences', snapshot.sequenceCount],
    ['Triggers (simple / complex)', `${snapshot.simpleTriggerCount} / ${snapshot.complexTriggerCount}`],
    ['Packages', snapshot.packageCount],
    ['Standalone procedures / functions', `${snapshot.standaloneProcedureCount} / ${snapshot.standaloneFunctionCount}`],
    ['Database links', snapshot.dbLinkCount],
    ['Scheduled jobs', snapshot.scheduledJobCount],
    ['Partitioned tables', snapshot.partitionedTableCount],
  ]

  return (
    <>
      <PageHeader
        back={{ to: '/quick-scan', label: 'New scan' }}
        title="Migration assessment"
        subtitle={snapshot.sourceVersion ?? undefined}
      />

      <KpiStrip
        label="Assessment summary"
        items={[
          { label: 'Difficulty tier', value: <StatusPill tone={tierTone(tierName)}>{tierName}</StatusPill>, hint: tierDetail },
          { label: 'Total difficulty score', value: score.totalScore },
        ]}
      />

      {score.warnings.length > 0 && (
        <Notice tone="warn" title="Warnings">
          <ul className={styles.list}>{score.warnings.map((w, i) => <li key={i}>{w}</li>)}</ul>
        </Notice>
      )}

      <Section title="Feature inventory" flush>
        <DataTable caption="Feature inventory" minWidth={false}>
          <tbody>
            {inventory.map(([label, value]) => (
              <tr key={label}><td>{label}</td><td className={table.num}>{value}</td></tr>
            ))}
          </tbody>
        </DataTable>
      </Section>

      <Section title="Scoring detail" flush>
        <DataTable caption="Scoring detail">
          <thead>
            <tr><th scope="col">Feature</th><th scope="col" className={table.num}>Count</th><th scope="col" className={table.num}>Weight</th><th scope="col" className={table.num}>Points</th><th scope="col">Note</th></tr>
          </thead>
          <tbody>
            {score.findings.map((f, i) => (
              <tr key={i}>
                <td>{f.feature}</td>
                <td className={table.num}>{f.count}</td>
                <td className={table.num}>{f.weightPerUnit}</td>
                <td className={table.num}>{f.points}</td>
                <td className={styles.muted}>{f.note}</td>
              </tr>
            ))}
          </tbody>
        </DataTable>
      </Section>
    </>
  )
}
