'use client'

import Link from 'next/link'
import { useDeferredValue, useEffect, useMemo, useState } from 'react'
import Nav from '../../components/nav'
import { ActionText, DCBadge, RiskPill } from '../../components/badges'
import { getInventoryHealth } from '../../lib/api'
import { fmtDate, fmtUnits, supplyColor } from '../../lib/format'

const ROWS_PER_PAGE = 100

export default function InventoryPage() {
  const [payload, setPayload] = useState(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState('')
  const [query, setQuery] = useState('')
  const [dcFilter, setDcFilter] = useState('ALL')
  const [statusFilter, setStatusFilter] = useState('ALL')
  const [page, setPage] = useState(1)
  const deferredQuery = useDeferredValue(query)

  useEffect(() => {
    let cancelled = false

    async function load() {
      setIsLoading(true)
      setError('')
      try {
        const data = await getInventoryHealth()
        if (!cancelled) setPayload(data)
      } catch (err) {
        if (!cancelled) setError(err.message || 'Failed to load inventory health.')
      } finally {
        if (!cancelled) setIsLoading(false)
      }
    }

    load()
    return () => {
      cancelled = true
    }
  }, [])

  const items = payload?.items || []
  const summary = payload?.summary

  const filteredItems = useMemo(() => {
    const search = deferredQuery.trim().toLowerCase()
    return items.filter((item) => {
      const matchesSearch =
        !search ||
        [item.sku_id, item.description, item.dc, item.recommended_action, item.risk_level]
          .filter(Boolean)
          .some((value) => String(value).toLowerCase().includes(search))

      const matchesDc = dcFilter === 'ALL' || item.dc === dcFilter
      const matchesStatus = statusFilter === 'ALL' || item.health_status === statusFilter
      return matchesSearch && matchesDc && matchesStatus
    })
  }, [items, deferredQuery, dcFilter, statusFilter])

  useEffect(() => {
    setPage(1)
  }, [deferredQuery, dcFilter, statusFilter])

  const dcOptions = useMemo(() => ['ALL', ...new Set(items.map((item) => item.dc).filter(Boolean))], [items])
  const statusOptions = useMemo(
    () => ['ALL', ...new Set(items.map((item) => item.health_status).filter(Boolean))],
    [items]
  )
  const totalPages = Math.max(1, Math.ceil(filteredItems.length / ROWS_PER_PAGE))
  const currentPage = Math.min(page, totalPages)
  const visibleItems = useMemo(() => {
    const start = (currentPage - 1) * ROWS_PER_PAGE
    return filteredItems.slice(start, start + ROWS_PER_PAGE)
  }, [filteredItems, currentPage])
  const visibleRangeStart = filteredItems.length === 0 ? 0 : (currentPage - 1) * ROWS_PER_PAGE + 1
  const visibleRangeEnd = Math.min(filteredItems.length, currentPage * ROWS_PER_PAGE)

  return (
    <div className="min-h-screen">
      <Nav />
      <main className="page-fade mx-auto max-w-[1400px] px-6 py-8">
        <section className="rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors">
          <div className="flex flex-col gap-6 md:flex-row md:items-end md:justify-between">
            <div>
              <div className="text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
                Inventory health
              </div>
              <h1 className="mt-2 text-3xl font-medium text-[hsl(var(--app-text-strong))]">
                SKU by DC coverage
              </h1>
              <p className="mt-2 max-w-2xl text-sm text-[hsl(var(--app-text-soft))]">
                Latest snapshot with rolling demand-based days of supply, stockout timing, and linked event risk.
              </p>
            </div>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              <MetricCard label="Snapshot" value={summary ? fmtDate(summary.snapshot_date) : '—'} />
              <MetricCard label="Cells" value={summary ? fmtUnits(summary.total_cells) : '—'} />
              <MetricCard label="At risk" value={summary ? fmtUnits(summary.at_risk_cells) : '—'} />
              <MetricCard
                label="Avg days"
                value={summary?.avg_days_of_supply != null ? Math.round(summary.avg_days_of_supply).toString() : '—'}
              />
            </div>
          </div>
        </section>

        <section className="mt-6 rounded-md border border-border bg-[hsl(var(--app-panel))] p-4 transition-colors">
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <input
              type="search"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search SKU, description, DC, action, or risk"
              className="w-full rounded-md border border-border bg-background px-3 py-2.5 text-sm text-foreground placeholder:text-[hsl(var(--app-text-muted))] focus:border-[#F59E0B]/60 focus:outline-none md:max-w-md"
            />
            <div className="flex flex-wrap items-center gap-3">
              <select
                value={dcFilter}
                onChange={(e) => setDcFilter(e.target.value)}
                className="rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground focus:border-[#F59E0B]/60 focus:outline-none"
              >
                {dcOptions.map((option) => (
                  <option key={option} value={option}>
                    {option === 'ALL' ? 'All DCs' : option}
                  </option>
                ))}
              </select>
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
                className="rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground focus:border-[#F59E0B]/60 focus:outline-none"
              >
                {statusOptions.map((option) => (
                  <option key={option} value={option}>
                    {option === 'ALL' ? 'All statuses' : option.replace('_', ' ')}
                  </option>
                ))}
              </select>
              <div className="mono text-xs text-[hsl(var(--app-text-muted))]">
                Showing {visibleRangeStart}-{visibleRangeEnd} of {filteredItems.length} filtered rows
              </div>
            </div>
          </div>
        </section>

        {!isLoading && !error && filteredItems.length > ROWS_PER_PAGE && (
          <section className="mt-4 flex flex-col gap-3 rounded-md border border-border bg-[hsl(var(--app-panel))] p-4 transition-colors md:flex-row md:items-center md:justify-between">
            <div className="text-sm text-[hsl(var(--app-text-soft))]">
              Rendering {ROWS_PER_PAGE} rows per page to keep the table responsive on large snapshots.
            </div>
            <div className="flex items-center gap-3">
              <button
                type="button"
                onClick={() => setPage((value) => Math.max(1, value - 1))}
                disabled={currentPage === 1}
                className="rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground transition-colors hover:border-[#F59E0B]/40 disabled:cursor-not-allowed disabled:opacity-40"
              >
                Previous
              </button>
              <div className="mono text-xs text-[hsl(var(--app-text-muted))]">
                Page {currentPage} of {totalPages}
              </div>
              <button
                type="button"
                onClick={() => setPage((value) => Math.min(totalPages, value + 1))}
                disabled={currentPage === totalPages}
                className="rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground transition-colors hover:border-[#F59E0B]/40 disabled:cursor-not-allowed disabled:opacity-40"
              >
                Next
              </button>
            </div>
          </section>
        )}

        <section className="mt-6 overflow-hidden rounded-md border border-border bg-[hsl(var(--app-panel))] transition-colors">
          <table className="w-full">
            <thead>
              <tr className="border-b border-border bg-[hsl(var(--app-panel-muted))] text-[10px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
                <th className="px-5 py-3 text-left">SKU / DC</th>
                <th className="px-5 py-3 text-left">Available</th>
                <th className="px-5 py-3 text-left">On hand</th>
                <th className="px-5 py-3 text-left">Demand / day</th>
                <th className="px-5 py-3 text-left">Days of supply</th>
                <th className="px-5 py-3 text-left">Stockout</th>
                <th className="px-5 py-3 text-left">Health</th>
                <th className="px-5 py-3 text-left">Linked recommendation</th>
              </tr>
            </thead>
            <tbody>
              {isLoading && <SkeletonRows />}
              {!isLoading && error && (
                <tr>
                  <td colSpan={8} className="px-5 py-8 text-sm text-red-300">
                    {error}
                  </td>
                </tr>
              )}
              {!isLoading && !error && filteredItems.length === 0 && (
                <tr>
                  <td colSpan={8} className="px-5 py-8 text-sm text-[hsl(var(--app-text-soft))]">
                    {items.length === 0 ? 'No inventory cells available.' : 'No inventory cells match the current filters.'}
                  </td>
                </tr>
              )}
              {!isLoading &&
                !error &&
                visibleItems.map((item) => (
                  <tr key={`${item.sku_id}-${item.dc}`} className="border-b border-border last:border-0">
                    <td className="px-5 py-4">
                      <div className="flex items-center gap-3">
                        <DCBadge code={item.dc} />
                        <div>
                          <div className="mono text-sm text-[hsl(var(--app-text-strong))]">{item.sku_id}</div>
                          <div className="text-xs text-[hsl(var(--app-text-muted))]">{item.description || 'No description'}</div>
                        </div>
                      </div>
                    </td>
                    <td className="mono px-5 py-4 text-sm text-[hsl(var(--app-text-strong))]">{renderUnits(item.available)}</td>
                    <td className="mono px-5 py-4 text-sm text-[hsl(var(--app-text-strong))]">{renderUnits(item.on_hand)}</td>
                    <td className="mono px-5 py-4 text-sm text-[hsl(var(--app-text-soft))]">
                      <div>{item.weighted_daily_demand != null ? fmtUnits(Math.round(item.weighted_daily_demand)) : '—'}</div>
                      {item.demand_basis && (
                        <div className="mt-1 text-[10px] tracking-widest text-[hsl(var(--app-text-muted))]">
                          {item.demand_basis === 'LAST_30_DAYS' ? 'LAST 30D' : '365D FALLBACK'}
                        </div>
                      )}
                    </td>
                    <td className={`mono px-5 py-4 text-sm ${item.days_of_supply != null ? supplyColor(item.days_of_supply) : 'text-[hsl(var(--app-text-soft))]'}`}>
                      {item.days_of_supply != null ? Math.round(item.days_of_supply) : '—'}
                    </td>
                    <td className="mono px-5 py-4 text-sm text-[hsl(var(--app-text-soft))]">{fmtDate(item.stockout_date)}</td>
                    <td className="px-5 py-4">
                      <HealthBadge status={item.health_status} />
                    </td>
                    <td className="px-5 py-4">
                      <div className="flex flex-wrap items-center gap-2">
                        {item.risk_level ? <RiskPill risk={item.risk_level} /> : <span className="text-xs text-[hsl(var(--app-text-muted))]">No active event</span>}
                        {item.recommended_action ? <ActionText action={item.recommended_action} /> : null}
                        {item.related_event_id ? (
                          <Link href={`/events/${item.related_event_id}`} className="mono text-[11px] tracking-widest text-[#F59E0B] hover:underline">
                            OPEN EVENT
                          </Link>
                        ) : null}
                      </div>
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
        </section>
      </main>
    </div>
  )
}

function MetricCard({ label, value }) {
  return (
    <div className="rounded-md border border-border bg-[hsl(var(--app-panel-muted))] p-4 transition-colors">
      <div className="text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">{label}</div>
      <div className="mono mt-2 text-lg text-[hsl(var(--app-text-strong))]">{value}</div>
    </div>
  )
}

function HealthBadge({ status }) {
  const tone = {
    AT_RISK: 'bg-[#EF4444]/12 text-[#EF4444] ring-[#EF4444]/30',
    WATCH: 'bg-[#F59E0B]/12 text-[#F59E0B] ring-[#F59E0B]/30',
    HEALTHY: 'bg-[#22C55E]/10 text-[#22C55E] ring-[#22C55E]/30',
    NO_DEMAND_SIGNAL: 'bg-secondary text-[hsl(var(--app-text-soft))] ring-border',
  }[status] || 'bg-secondary text-[hsl(var(--app-text-soft))] ring-border'

  return (
    <span className={`mono inline-flex rounded-full px-2 py-0.5 text-[10px] tracking-widest ring-1 ring-inset ${tone}`}>
      {status.replaceAll('_', ' ')}
    </span>
  )
}

function SkeletonRows() {
  return Array.from({ length: 8 }).map((_, index) => (
    <tr key={index} className="border-b border-border">
      {Array.from({ length: 8 }).map((__, cell) => (
        <td key={cell} className="px-5 py-5">
          <div className="shimmer h-4 w-full rounded" />
        </td>
      ))}
    </tr>
  ))
}

function renderUnits(value) {
  return value == null ? '—' : fmtUnits(value)
}
