import { Layers, Radio } from 'lucide-react'
import { useEffect, useState } from 'react'
import { type NodeInfo, listNodes } from '../api/client'
import styles from './Topology.module.css'

const POLL_INTERVAL_MS = 10_000
const UNKNOWN_ZONE_LABEL = 'Unknown zone'

function formatDuration(ms: number): string {
  if (ms < 0) ms = 0
  const seconds = Math.floor(ms / 1000)
  const days = Math.floor(seconds / 86400)
  const hours = Math.floor((seconds % 86400) / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  const secs = seconds % 60
  if (days > 0) return `${days}d ${hours}h`
  if (hours > 0) return `${hours}h ${minutes}m`
  if (minutes > 0) return `${minutes}m ${secs}s`
  return `${secs}s`
}

function formatRelative(ms: number): string {
  if (ms < 1000) return 'just now'
  return `${formatDuration(ms)} ago`
}

function groupByZone(nodes: NodeInfo[]): Array<[string, NodeInfo[]]> {
  const groups = new Map<string, NodeInfo[]>()
  for (const node of nodes) {
    const key = node.zone ?? UNKNOWN_ZONE_LABEL
    const list = groups.get(key)
    if (list) list.push(node)
    else groups.set(key, [node])
  }
  const entries = [...groups.entries()]
  entries.sort((a, b) => {
    if (a[0] === UNKNOWN_ZONE_LABEL) return 1
    if (b[0] === UNKNOWN_ZONE_LABEL) return -1
    return a[0].localeCompare(b[0])
  })
  return entries
}

/**
 * Deployment topology for NexaGate -- which nodes exist, in which availability zones, and
 * whether each is currently healthy. Each instance heartbeats its identity to the shared config
 * Postgres roughly every 10s; a node is marked "stale" once its heartbeat is more than 30s old
 * (could be down, could just be a slow network blip -- not a hard failure signal on its own).
 *
 * This is a separate, much simpler mechanism from Ignite's distributed cache mesh membership --
 * it does not reflect who's actually joined the cache cluster, only who's checked in here.
 *
 * Polls /api/nodes every 10s (matching the backend heartbeat cadence) plus once on mount, so the
 * page stays live without a manual refresh -- mirrors Metrics.tsx's polling pattern.
 */
export default function Topology() {
  const [nodes, setNodes] = useState<NodeInfo[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [now, setNow] = useState(() => Date.now())

  function load() {
    listNodes()
      .then((ns) => { setNodes(ns); setError(null); setLastUpdated(new Date()) })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }

  useEffect(() => {
    load()
    const id = setInterval(load, POLL_INTERVAL_MS)
    return () => clearInterval(id)
  }, [])

  // Tick every second so uptime/relative-heartbeat strings stay fresh between polls.
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(id)
  }, [])

  if (error) {
    return (
      <div className={styles.page}>
        <h1 style={{ fontSize: 22, marginBottom: 4 }}>Topology</h1>
        <p style={{ color: 'var(--error, crimson)', fontSize: 13 }}>{error}</p>
      </div>
    )
  }

  if (!nodes) {
    return (
      <div className={styles.page}>
        <h1 style={{ fontSize: 22, marginBottom: 4 }}>Topology</h1>
        <p style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</p>
      </div>
    )
  }

  const zoneGroups = groupByZone(nodes)
  const upCount = nodes.filter((n) => n.status === 'up').length
  const staleCount = nodes.length - upCount

  return (
    <div className={styles.page}>
      <div className={styles.hero}>
        <div className={styles.heroTop}>
          <div>
            <h1 className={styles.heroTitle}>NexaGate topology</h1>
            <p className={styles.heroSubtitle}>
              Every NexaGate instance in this deployment, grouped by zone.
            </p>
          </div>
          <div className={styles.heroBadge}>
            <span className={styles.liveDot} />
            Live · updated {lastUpdated ? lastUpdated.toLocaleTimeString() : '—'}
          </div>
        </div>
        <div className={styles.heroStats}>
          <div className={styles.heroStat}>
            <div className={styles.heroStatLabel}><Layers size={13} /> Nodes total</div>
            <div className={styles.heroStatValue}>{nodes.length}</div>
            <div className={styles.heroStatSub}>{zoneGroups.length} zone(s)</div>
          </div>
          <div className={styles.heroStat}>
            <div className={styles.heroStatLabel}><Radio size={13} /> Healthy</div>
            <div className={styles.heroStatValue}>{upCount}</div>
            <div className={styles.heroStatSub}>{staleCount} stale</div>
          </div>
        </div>
      </div>

      {nodes.length === 0 ? (
        <div className={styles.card}>
          <div className={styles.empty}>
            No nodes have heartbeated yet. This is expected right after a fresh deploy — the
            heartbeat table populates once at least one NexaGate instance has started and checked
            in against the shared config Postgres.
          </div>
        </div>
      ) : (
        zoneGroups.map(([zone, zoneNodes]) => (
          <div className={styles.card} style={{ marginBottom: 16 }} key={zone}>
            <div className={styles.cardHeadRow}>
              <p className={styles.cardTitle}>{zone}</p>
              <span className={styles.cardCount}>{zoneNodes.length} node(s)</span>
            </div>
            <div className={styles.nodeGrid}>
              {zoneNodes.map((node) => {
                const uptimeMs = now - new Date(node.startedAt).getTime()
                const heartbeatMs = now - new Date(node.lastHeartbeat).getTime()
                return (
                  <div className={styles.nodeCard} key={node.nodeId}>
                    <div className={styles.nodeCardTop}>
                      <span className={styles.nodeHost}>{node.host}:{node.adminPort}</span>
                      <span className={`${styles.statusBadge} ${node.status === 'up' ? styles.statusUp : styles.statusStale}`}>
                        {node.status === 'up' ? 'up' : 'stale'}
                      </span>
                    </div>
                    <div className={styles.nodeId}>{node.nodeId}</div>
                    <div className={styles.nodeMetaRow}>
                      <span>v{node.version}</span>
                      <span>uptime {formatDuration(uptimeMs)}</span>
                    </div>
                    <div className={styles.nodeMetaRow}>
                      <span>last heartbeat {formatRelative(heartbeatMs)}</span>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        ))
      )}
    </div>
  )
}
