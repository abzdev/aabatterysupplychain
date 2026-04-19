'use client'

import { useEffect, useState } from 'react'
import Nav from '../../components/nav'
import { ActionText, DCBadge, StateBadge } from '../../components/badges'
import { getComparison } from '../../lib/api'
import { fmtMoney } from '../../lib/format'

export default function ComparisonPage() {
  const [payload, setPayload] = useState(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let cancelled = false

    async function load() {
      setIsLoading(true)
      setError('')
      try {
        const data = await getComparison()
        if (!cancelled) setPayload(data)
      } catch (err) {
        if (!cancelled) setError(err.message || 'Failed to load comparison view.')
      } finally {
        if (!cancelled) setIsLoading(false)
      }
    }

    load()
    return () => {
      cancelled = true
    }
  }, [])

  const summary = payload?.summary
  const rows = payload?.rows || []
  const assumptions = payload?.assumptions || []

  return (
    <div className="min-h-screen">
      <Nav />
      <main className="page-fade mx-auto max-w-[1400px] px-6 py-8">
        <section className="rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors">
          <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <div className="text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
                Before / after
              </div>
              <h1 className="mt-2 text-3xl font-medium text-[hsl(var(--app-text-strong))]">
                Manual vs system-assisted decisions
              </h1>
              <p className="mt-2 max-w-2xl text-sm text-[hsl(var(--app-text-soft))]">
                Portfolio-level comparison of modeled manual handling versus the current assisted workflow.
              </p>
            </div>
            <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
              <MetricCard label="Events" value={summary ? String(summary.event_count) : '—'} />
              <MetricCard label="Transfers approved" value={summary ? String(summary.approved_transfer_count) : '—'} />
              <MetricCard label="AI coverage" value={summary?.ai_coverage_rate != null ? `${Math.round(summary.ai_coverage_rate * 100)}%` : '—'} />
              <MetricCard label="Approval time" value={summary?.avg_approval_hours != null ? `${summary.avg_approval_hours.toFixed(1)}h` : '—'} />
            </div>
          </div>
        </section>

        <section className="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-3">
          <div className="rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors">
            <div className="text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">Manual baseline</div>
            <div className="mono mt-3 text-4xl text-[hsl(var(--app-text-strong))]">
              {summary ? fmtMoney(summary.manual_baseline_cost) : '—'}
            </div>
            <div className="mt-2 text-sm text-[hsl(var(--app-text-soft))]">
              Historical operational chargeback baseline for the detected SKUs and destination DCs.
            </div>
            <div className="mt-2 text-xs text-[hsl(var(--app-text-muted))]">
              Conservative view only: excludes hidden manual costs like emergency freight, OTIF degradation, and dispute labor.
            </div>
          </div>
          <div className="rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors">
            <div className="text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">System-assisted</div>
            <div className="mono mt-3 text-4xl text-[hsl(var(--app-text-strong))]">
              {summary ? fmtMoney(summary.system_assisted_cost) : '—'}
            </div>
            <div className="mt-2 text-sm text-[hsl(var(--app-text-soft))]">
              Uses the latest transfer request estimate when one exists, otherwise the active recommendation.
            </div>
          </div>
          <div className="rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors" style={{ boxShadow: 'inset 3px 0 0 0 #F59E0B' }}>
            <div className="text-[11px] uppercase tracking-[0.18em] text-[#F59E0B]">Estimated savings</div>
            <div className="mono mt-3 text-4xl text-[hsl(var(--app-text-strong))]">
              {summary ? fmtMoney(summary.estimated_savings) : '—'}
            </div>
            <div className="mt-2 text-sm text-[hsl(var(--app-text-soft))]">
              Positive values mean the assisted workflow is modeling lower portfolio cost than the manual baseline.
            </div>
          </div>
        </section>

        <section className="mt-6 rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h2 className="text-sm uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">Assumptions</h2>
              <p className="mt-1 text-sm text-[hsl(var(--app-text-soft))]">
                The comparison uses current event and transfer-request data, historical operational chargebacks, and one explicit operating assumption for manual approval timing.
              </p>
            </div>
            <div className="mono rounded-full bg-secondary px-3 py-1 text-[11px] tracking-widest text-[hsl(var(--app-text-soft))]">
              manual approval assumption {summary?.manual_approval_hours_assumption != null ? `${summary.manual_approval_hours_assumption}h` : '—'}
            </div>
          </div>
          <div className="mt-4 space-y-3">
            {assumptions.map((assumption) => (
              <div
                key={assumption}
                className="rounded-md border border-dashed border-border bg-[hsl(var(--app-panel-muted))] px-4 py-3 text-sm text-[hsl(var(--app-text-soft))]"
              >
                {assumption}
              </div>
            ))}
          </div>
        </section>

        <section className="mt-6 overflow-hidden rounded-md border border-border bg-[hsl(var(--app-panel))] transition-colors">
          <div className="border-b border-border bg-[hsl(var(--app-panel-muted))] px-5 py-3 text-[10px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
            Event comparison
          </div>
          {isLoading ? (
            <div className="space-y-2 p-4">
              {Array.from({ length: 6 }).map((_, index) => (
                <div key={index} className="shimmer h-14 rounded" />
              ))}
            </div>
          ) : error ? (
            <div className="px-5 py-8 text-sm text-red-300">{error}</div>
          ) : rows.length === 0 ? (
            <div className="px-5 py-8 text-sm text-[hsl(var(--app-text-soft))]">No comparison rows available.</div>
          ) : (
            <table className="w-full">
              <thead>
                <tr className="border-b border-border bg-[hsl(var(--app-panel-muted))] text-[10px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
                  <th className="px-5 py-3 text-left">SKU / route</th>
                  <th className="px-5 py-3 text-left">Manual</th>
                  <th className="px-5 py-3 text-left">System-assisted</th>
                  <th className="px-5 py-3 text-left">Delta</th>
                  <th className="px-5 py-3 text-left">State</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr key={row.event_id} className="border-b border-border last:border-0">
                    <td className="px-5 py-4">
                      <div className="mono text-sm text-[hsl(var(--app-text-strong))]">{row.sku_id}</div>
                      <div className="mt-1 flex items-center gap-2 text-xs text-[hsl(var(--app-text-soft))]">
                        <DCBadge code={row.source_dc} />
                        <span>to</span>
                        <DCBadge code={row.dest_dc} />
                      </div>
                    </td>
                    <td className="px-5 py-4">
                      <ActionText action={row.manual_action} />
                      <div className="mono mt-2 text-sm text-[hsl(var(--app-text-strong))]">{fmtMoney(row.manual_cost)}</div>
                    </td>
                    <td className="px-5 py-4">
                      <div className="flex flex-wrap items-center gap-2">
                        <ActionText action={row.system_action} />
                        {row.request_state ? <StateBadge state={row.request_state} /> : null}
                      </div>
                      <div className="mono mt-2 text-sm text-[hsl(var(--app-text-strong))]">{fmtMoney(row.system_cost)}</div>
                    </td>
                    <td className={`mono px-5 py-4 text-sm ${row.delta_vs_manual >= 0 ? 'text-[#22C55E]' : 'text-[#EF4444]'}`}>
                      {row.delta_vs_manual >= 0 ? '+' : '-'}
                      {fmtMoney(Math.abs(row.delta_vs_manual))}
                    </td>
                    <td className="px-5 py-4">
                      <div className="flex flex-col gap-2">
                        <StateBadge state={row.state} />
                        <div className="mono text-[11px] tracking-widest text-[hsl(var(--app-text-muted))]">
                          {row.confidence || '—'}
                        </div>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
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
