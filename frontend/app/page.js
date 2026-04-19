'use client'

import Link from 'next/link'
import { useRouter } from 'next/navigation'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { ArrowRight, Search } from 'lucide-react'
import Nav from '../components/nav'
import { ActionText, DCBadge, RiskPill, StateBadge } from '../components/badges'
import { getEvents, getLatestAgentRun, runAgent } from '../lib/api'
import { fmtDate, fmtDateTime, fmtMoney, supplyColor } from '../lib/format'

export default function DashboardPage() {
  const router = useRouter()
  const [events, setEvents] = useState([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState('')
  const [query, setQuery] = useState('')
  const [stateFilter, setStateFilter] = useState('ALL')
  const [agentStatus, setAgentStatus] = useState(null)
  const [agentError, setAgentError] = useState('')
  const [isTriggeringAgent, setIsTriggeringAgent] = useState(false)

  const loadEvents = useCallback(async (showLoading = false) => {
    if (showLoading) setIsLoading(true)
    setError('')
    try {
      const data = await getEvents()
      setEvents(Array.isArray(data) ? data : [])
    } catch (err) {
      setError(err.message || 'Failed to load events.')
    } finally {
      if (showLoading) setIsLoading(false)
    }
  }, [])

  const loadAgentStatus = useCallback(async () => {
    try {
      const data = await getLatestAgentRun()
      setAgentStatus(data)
      setAgentError('')
    } catch (err) {
      setAgentError(err.message || 'Failed to load autonomous agent status.')
    }
  }, [])

  useEffect(() => {
    void Promise.all([loadEvents(true), loadAgentStatus()])
  }, [loadEvents, loadAgentStatus])

  const latestRunStatus = agentStatus?.run?.status || ''
  useEffect(() => {
    if (!['PENDING', 'RUNNING'].includes(latestRunStatus)) return
    const timer = setTimeout(() => {
      void Promise.all([loadEvents(false), loadAgentStatus()])
    }, 3000)
    return () => clearTimeout(timer)
  }, [latestRunStatus, loadAgentStatus, loadEvents])

  const hero = useMemo(() => {
    const totalPenalty = events.reduce((sum, event) => sum + Number(event.expected_penalty_cost || 0), 0)
    const transferCount = events.filter((event) => event.recommended_action === 'TRANSFER').length
    return {
      totalPenalty,
      transferCount,
      high: events.filter((event) => event.penalty_risk_level === 'HIGH').length,
      medium: events.filter((event) => event.penalty_risk_level === 'MEDIUM').length,
      low: events.filter((event) => event.penalty_risk_level === 'LOW').length,
    }
  }, [events])

  const filteredEvents = useMemo(() => {
    const search = query.trim().toLowerCase()
    return events.filter((event) => {
      const matchesSearch =
        !search ||
        [
          event.sku_id,
          event.event_key,
          event.source_dc,
          event.dest_dc,
          event.state,
          event.recommended_action,
        ]
          .filter(Boolean)
          .some((value) => String(value).toLowerCase().includes(search))

      const matchesState = stateFilter === 'ALL' || event.state === stateFilter
      return matchesSearch && matchesState
    })
  }, [events, query, stateFilter])

  const stateOptions = useMemo(
    () => ['ALL', ...new Set(events.map((event) => event.state).filter(Boolean))],
    [events]
  )

  const openEvent = (eventId) => router.push(`/events/${eventId}`)

  async function handleRunAgent() {
    setIsTriggeringAgent(true)
    setAgentError('')
    try {
      const queued = await runAgent()
      setAgentStatus((current) => ({
        ...(current || { next_run_at: null, interval_hours: 6, scheduler_running: false }),
        run: queued.run,
        activities: queued.activities,
      }))
      await loadAgentStatus()
    } catch (err) {
      setAgentError(err.message || 'Failed to start autonomous agent run.')
    } finally {
      setIsTriggeringAgent(false)
    }
  }

  return (
    <div className="min-h-screen">
      <Nav />
      <main className="page-fade mx-auto max-w-[1400px] px-6 py-8">
        <section className="relative overflow-hidden rounded-md border border-border bg-[hsl(var(--app-panel))] shadow-[inset_3px_0_0_0_#F59E0B] transition-colors">
          <div className="grid grid-cols-1 gap-6 px-8 py-7 md:grid-cols-[1.2fr_1px_1fr_1fr] md:items-center">
            <div>
              <div className="mono text-[56px] leading-none font-medium tracking-tight text-[hsl(var(--app-text-strong))]">
                {fmtMoney(hero.totalPenalty)}
              </div>
              <div className="mt-2 text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
                projected penalty exposure
              </div>
            </div>
            <div className="hidden h-16 w-px bg-border md:block" />
            <div>
              <div className="flex items-center gap-3">
                <span className="relative inline-flex h-2.5 w-2.5">
                  <span className="pulse-dot absolute inset-0 rounded-full bg-[#F59E0B]" />
                  <span className="absolute inset-0 rounded-full bg-[#F59E0B]" />
                </span>
                <div className="mono text-2xl text-[hsl(var(--app-text-strong))]">
                  {hero.transferCount} <span className="text-base text-[hsl(var(--app-text-soft))]">transfers recommended</span>
                </div>
              </div>
              <div className="mt-2 text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
                live event inventory
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-2 md:justify-end">
              <Pill color="#F59E0B" label={`${hero.high} high`} />
              <Pill color="#EAB308" label={`${hero.medium} medium`} />
              <Pill color="#22C55E" label={`${hero.low} low`} />
            </div>
          </div>
        </section>

        <section className="mt-6 rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <div className="text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
                Autonomous agent
              </div>
              <h2 className="mt-2 text-xl font-medium text-[hsl(var(--app-text-strong))]">
                Scheduled review-only triage
              </h2>
              <p className="mt-2 max-w-2xl text-sm text-[hsl(var(--app-text-soft))]">
                The agent runs the full scan/analyze pipeline automatically, prioritizes by penalty exposure, and decides whether each event should be monitored or flagged for human review.
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-3">
              <RunStatusPill status={agentStatus?.run?.status || 'IDLE'} />
              <button
                type="button"
                onClick={handleRunAgent}
                disabled={isTriggeringAgent || ['PENDING', 'RUNNING'].includes(agentStatus?.run?.status)}
                className="mono rounded-md bg-[#F59E0B] px-4 py-2 text-xs font-medium tracking-widest text-neutral-950 hover:bg-[#F59E0B]/90 disabled:opacity-40"
              >
                {isTriggeringAgent ? 'STARTING…' : ['PENDING', 'RUNNING'].includes(agentStatus?.run?.status) ? 'RUNNING…' : 'RUN AGENT NOW'}
              </button>
            </div>
          </div>
          {agentError && (
            <div className="mt-4 rounded-md border border-red-900/50 bg-red-950/20 px-4 py-3 text-sm text-red-300">
              {agentError}
            </div>
          )}
          <div className="mt-5 grid grid-cols-1 gap-4 md:grid-cols-4">
            <MetricCard
              label="Last run"
              value={agentStatus?.run ? fmtDateTime(agentStatus.run.completed_at || agentStatus.run.created_at) : 'No runs yet'}
            />
            <MetricCard
              label="Next run"
              value={
                agentStatus?.scheduler_running
                  ? fmtDateTime(agentStatus?.next_run_at)
                  : 'Scheduler idle'
              }
            />
            <MetricCard
              label="Flagged"
              value={agentStatus?.run ? String(agentStatus.run.flagged_for_review) : '0'}
            />
            <MetricCard
              label="Monitored"
              value={agentStatus?.run ? String(agentStatus.run.monitored_count) : '0'}
            />
          </div>
        </section>

        <section className="mt-6 rounded-md border border-border bg-[hsl(var(--app-panel))] transition-colors">
          <div className="flex items-center justify-between border-b border-border bg-[hsl(var(--app-panel-muted))] px-5 py-3">
            <div className="text-[10px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
              Agent activity feed
            </div>
            <div className="mono text-xs text-[hsl(var(--app-text-muted))]">
              {agentStatus?.interval_hours ? `every ${agentStatus.interval_hours}h` : 'manual only'}
            </div>
          </div>
          <div className="divide-y divide-border">
            {(agentStatus?.activities || []).length === 0 ? (
              <div className="px-5 py-8 text-sm text-[hsl(var(--app-text-soft))]">
                No agent activity has been recorded yet.
              </div>
            ) : (
              (agentStatus.activities || []).map((entry) => (
                <div key={entry.id || `${entry.action_type}-${entry.created_at}`} className="px-5 py-4">
                  <div className="flex flex-col gap-2 md:flex-row md:items-start md:justify-between">
                    <div>
                      <div className="text-sm text-[hsl(var(--app-text-strong))]">{entry.message}</div>
                      <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
                        <span>{entry.action_type.replaceAll('_', ' ')}</span>
                        {entry.metadata?.sku_id ? <span>{entry.metadata.sku_id}</span> : null}
                        {entry.metadata?.source_dc && entry.metadata?.dest_dc ? (
                          <span>{entry.metadata.source_dc} to {entry.metadata.dest_dc}</span>
                        ) : null}
                      </div>
                    </div>
                    <div className="mono text-xs text-[hsl(var(--app-text-muted))]">
                      {fmtDateTime(entry.created_at)}
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </section>

        <section className="mt-8">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-sm uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">Active risk events</h2>
            <div className="mono text-xs text-[hsl(var(--app-text-muted))]">default sort: expected penalty cost desc</div>
          </div>
          <div className="mb-4 flex flex-col gap-3 rounded-md border border-border bg-[hsl(var(--app-panel))] p-4 transition-colors md:flex-row md:items-center md:justify-between">
            <label className="relative block w-full md:max-w-md">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[hsl(var(--app-text-muted))]" />
              <input
                type="search"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search SKU, event key, route, state, or action"
                className="w-full rounded-md border border-border bg-background px-10 py-2.5 text-sm text-foreground placeholder:text-[hsl(var(--app-text-muted))] focus:border-[#F59E0B]/60 focus:outline-none"
              />
            </label>
            <div className="flex flex-wrap items-center gap-3">
              <select
                value={stateFilter}
                onChange={(e) => setStateFilter(e.target.value)}
                className="rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground focus:border-[#F59E0B]/60 focus:outline-none"
              >
                {stateOptions.map((state) => (
                  <option key={state} value={state}>
                    {state === 'ALL' ? 'All states' : state}
                  </option>
                ))}
              </select>
              <div className="mono text-xs text-[hsl(var(--app-text-muted))]">
                Showing {filteredEvents.length} of {events.length}
              </div>
            </div>
          </div>
          <div className="overflow-hidden rounded-md border border-border bg-[hsl(var(--app-panel))] transition-colors">
            <table className="w-full">
              <thead>
                <tr className="border-b border-border bg-[hsl(var(--app-panel-muted))] text-[10px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
                  <th className="px-5 py-3 text-left">SKU</th>
                  <th className="px-5 py-3 text-left">Route</th>
                  <th className="px-5 py-3 text-left">Days of supply</th>
                  <th className="px-5 py-3 text-left">Stockout</th>
                  <th className="px-5 py-3 text-left">Risk</th>
                  <th className="px-5 py-3 text-left">Recommended</th>
                  <th className="px-5 py-3 text-left">Penalty</th>
                  <th className="px-5 py-3 text-left">State</th>
                  <th className="w-10 px-3" />
                </tr>
              </thead>
              <tbody>
                {isLoading && <SkeletonRows />}
                {!isLoading && error && (
                  <tr>
                    <td colSpan={9} className="px-5 py-8 text-sm text-red-300">
                      {error}
                    </td>
                  </tr>
                )}
                {!isLoading && !error && filteredEvents.length === 0 && (
                  <tr>
                    <td colSpan={9} className="px-5 py-8 text-sm text-[hsl(var(--app-text-soft))]">
                      {events.length === 0 ? 'No events available.' : 'No events match the current filters.'}
                    </td>
                  </tr>
                )}
                {!isLoading &&
                  !error &&
                  filteredEvents.map((event) => (
                    <tr
                      key={event.id}
                      tabIndex={0}
                      role="link"
                      aria-label={`Open event ${event.sku_id}`}
                      onClick={() => openEvent(event.id)}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault()
                          openEvent(event.id)
                        }
                      }}
                      className="group cursor-pointer border-b border-border transition-colors last:border-0 hover:bg-[hsl(var(--app-hover))] focus:outline-none focus:ring-2 focus:ring-[#F59E0B]/40"
                    >
                      <td className="px-5 py-4">
                        <div className="mono text-sm font-medium text-[hsl(var(--app-text-strong))]">{event.sku_id}</div>
                        <div className="text-xs text-[hsl(var(--app-text-muted))]">{event.event_key}</div>
                      </td>
                      <td className="px-5 py-4">
                        <div className="mono inline-flex items-center gap-2 text-sm text-[hsl(var(--app-text-soft))]">
                          <DCBadge code={event.source_dc} />
                          <ArrowRight className="h-3.5 w-3.5 text-[hsl(var(--app-text-muted))]" />
                          <DCBadge code={event.dest_dc} />
                        </div>
                      </td>
                      <td className={`mono px-5 py-4 text-sm ${supplyColor(event.days_of_supply || 0)}`}>
                        {event.days_of_supply ?? '—'}
                      </td>
                      <td className="mono px-5 py-4 text-sm text-[hsl(var(--app-text-soft))]">{fmtDate(event.stockout_date)}</td>
                      <td className="px-5 py-4">
                        <RiskPill risk={event.penalty_risk_level || 'LOW'} />
                      </td>
                      <td className="px-5 py-4">
                        <ActionText action={event.recommended_action} />
                      </td>
                      <td className="mono px-5 py-4 text-sm text-[hsl(var(--app-text-strong))]">
                        {fmtMoney(event.expected_penalty_cost)}
                      </td>
                      <td className="px-5 py-4">
                        <StateBadge state={event.state} />
                      </td>
                      <td className="px-3 py-4 text-right">
                        <span className="inline-flex text-[hsl(var(--app-text-muted))] transition-all group-hover:translate-x-0 group-hover:text-[#F59E0B]">
                          <ArrowRight className="h-4 w-4" />
                        </span>
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  )
}

function Pill({ color, label }) {
  return (
    <span
      className="mono inline-flex items-center gap-2 rounded-full border px-3 py-1 text-[11px] tracking-widest"
      style={{ borderColor: `${color}55`, color, backgroundColor: `${color}12` }}
    >
      <span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: color }} />
      {label.toUpperCase()}
    </span>
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

function RunStatusPill({ status }) {
  const tone = {
    RUNNING: 'bg-blue-500/10 text-blue-300 ring-blue-500/30',
    PENDING: 'bg-amber-500/10 text-amber-300 ring-amber-500/30',
    SUCCEEDED: 'bg-[#22C55E]/10 text-[#22C55E] ring-[#22C55E]/30',
    FAILED: 'bg-[#EF4444]/12 text-[#EF4444] ring-[#EF4444]/30',
    SKIPPED: 'bg-secondary text-[hsl(var(--app-text-soft))] ring-border',
    IDLE: 'bg-secondary text-[hsl(var(--app-text-soft))] ring-border',
  }[status] || 'bg-secondary text-[hsl(var(--app-text-soft))] ring-border'

  return (
    <span className={`mono inline-flex rounded-full px-3 py-1 text-[11px] tracking-widest ring-1 ring-inset ${tone}`}>
      {status}
    </span>
  )
}

function SkeletonRows() {
  return Array.from({ length: 6 }).map((_, i) => (
    <tr key={i} className="border-b border-border">
      {Array.from({ length: 9 }).map((_, j) => (
        <td key={j} className="px-5 py-5">
          <div className="shimmer h-4 w-full rounded" />
        </td>
      ))}
    </tr>
  ))
}
