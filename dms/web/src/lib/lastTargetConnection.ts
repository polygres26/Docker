// Remembers which target Postgres connection Migration Service was last pointed at, so launching
// a job on the Launch tab and then switching to the Status tab lands you on the right connection's
// progress automatically, without re-selecting it -- and it survives a reload/revisit, unlike
// component state, since the two tabs are separate routes/component instances.
const KEY = 'dms.migrationService.lastTargetConnectionId'

export function getLastTargetConnectionId(): string | null {
  try {
    return localStorage.getItem(KEY)
  } catch {
    return null
  }
}

export function setLastTargetConnectionId(id: string): void {
  try {
    localStorage.setItem(KEY, id)
  } catch {
    // Private-browsing/storage-disabled: this is a convenience, not a requirement -- just skip.
  }
}
