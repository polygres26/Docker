import type { ReportAnalysis } from '../api/client'
import { severityTone, tierTone } from '../lib/tone'
import { Bullets, KpiStrip, List, Section, Stack, StatusPill, Tag } from '../ui'
import styles from './ReportAnalysisView.module.css'

/** Shared render for one {@link ReportAnalysis} -- used by both a single report's detail page and the multi-report combined-analysis view, so the two never drift apart visually. */
export default function ReportAnalysisView({ analysis }: { analysis: ReportAnalysis }) {
  return (
    <Stack>
      <KpiStrip
        label="Analysis summary"
        items={[
          { label: 'Difficulty tier', value: <StatusPill tone={tierTone(analysis.tier)}>{analysis.tier}</StatusPill>, hint: analysis.tierReason },
          ...(analysis.sourceVersion ? [{ label: 'Source version', value: <span className={styles.version}>{analysis.sourceVersion}</span> }] : []),
          ...(analysis.judgeVerdict ? [{ label: 'Judge', value: <StatusPill tone={analysis.judgeVerdict.approved ? 'green' : 'amber'}>{analysis.judgeVerdict.approved ? 'Approved' : 'Flagged'}</StatusPill> }] : []),
        ]}
      />

      {analysis.findings.length > 0 && (
        <Section title="Findings" meta={`${analysis.findings.length} item${analysis.findings.length === 1 ? '' : 's'}`} flush>
          <List>
            {analysis.findings.map((f, i) => (
              <li key={i} className={styles.finding}>
                <Tag tone={severityTone(f.severity)}>{f.severity}</Tag>
                <div>
                  <div className={styles.name}>{f.feature}</div>
                  <div className={styles.note}>{f.note}</div>
                </div>
              </li>
            ))}
          </List>
        </Section>
      )}

      {analysis.topWorkload.length > 0 && (
        <Section title={`Workload observed in the report${analysis.topWorkload.length !== 1 ? 's' : ''}`} flush>
          <List>
            {analysis.topWorkload.map((w, i) => (
              <li key={i}>
                <div>{w.description}</div>
                <div className={styles.detail}>{w.detail}</div>
              </li>
            ))}
          </List>
        </Section>
      )}

      {analysis.caveats.length > 0 && (
        <Section title="Caveats">
          <Bullets>{analysis.caveats.map((c, i) => <li key={i}>{c}</li>)}</Bullets>
        </Section>
      )}
    </Stack>
  )
}
