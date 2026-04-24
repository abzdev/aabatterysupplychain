'use client'

import Link from 'next/link'
import { useParams, useRouter } from 'next/navigation'
import { useEffect, useMemo, useState } from 'react'
import { useTheme } from 'next-themes'
import { LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer } from 'recharts'
import Nav from '../../../components/nav'
import { DCBadge, RiskPill, StateBadge } from '../../../components/badges'
import { analyzeEvent, createTransferRequest, getEvent } from '../../../lib/api'
import { fmtDate, fmtMoney, fmtUnits, supplyColor } from '../../../lib/format'
import { ChevronRight, Loader2, Sparkles } from 'lucide-react'

export default function EventPage() {
  const { id } = useParams()
  const router = useRouter()
  const { resolvedTheme } = useTheme()
  const [event, setEvent] = useState(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState('')
  const [requestQty, setRequestQty] = useState('')
  const [analyzing, setAnalyzing] = useState(false)
  const [submittingRequest, setSubmittingRequest] = useState(false)

  useEffect(() => {
    let cancelled = false

    async function loadEvent() {
      if (!id) return
      setIsLoading(true)
      setError('')
      try {
        const data = await getEvent(id)
        if (!cancelled) {
          setEvent(data)
          setRequestQty(String(data.transferable_qty || ''))
        }
      } catch (err) {
        if (!cancelled) setError(err.message || 'Failed to load event.')
      } finally {
        if (!cancelled) setIsLoading(false)
      }
    }

    loadEvent()
    return () => {
      cancelled = true
    }
  }, [id])

  const projection = useMemo(() => {
    if (!event?.depletion_projection) return []
    return event.depletion_projection.map((point) => ({
      day: Number(point.day ?? 0),
      available: Math.max(0, Number(point.available ?? 0)),
    }))
  }, [event])

  const latestTransferRequest = event?.transfer_requests?.[event.transfer_requests.length - 1] || null
  const canCreateTransfer = !latestTransferRequest && event?.recommended_action === 'TRANSFER'
  const showAnalyzeAction = Boolean(event)
  const normalizedRecommendation =
    event?.recommended_action === 'TRANSFER'
      ? 'TRANSFER'
      : event?.recommended_action === 'WAIT' || event?.recommended_action === 'MONITOR'
        ? 'WAIT_MONITOR'
        : null
  const transferIsRecommended = normalizedRecommendation === 'TRANSFER'
  const waitMonitorIsRecommended = normalizedRecommendation === 'WAIT_MONITOR'
  const tradeoff = useMemo(() => {
    if (!event) return null
    const transferCost = Number(event.cost_transfer || 0)
    const waitCost = Number(event.cost_wait || 0)
    const delta = Math.abs(transferCost - waitCost)
    const cheaperAction =
      transferCost === waitCost ? 'TIED' : transferCost < waitCost ? 'TRANSFER' : 'WAIT'

    let summary = 'Transfer and wait model to the same cost.'
    if (cheaperAction === 'TRANSFER') {
      summary = `Transfer is modeled to save ${fmtMoney(delta)} versus waiting.`
    } else if (cheaperAction === 'WAIT') {
      summary = `Waiting is modeled to save ${fmtMoney(delta)} versus transferring now.`
    }

    return {
      transferCost,
      waitCost,
      delta,
      cheaperAction,
      summary,
      confidenceHint: describeConfidence(event.confidence, delta),
    }
  }, [event])
  const chartTheme = resolvedTheme === 'dark'
    ? {
        grid: '#1a1a1a',
        axis: '#3f3f46',
        tick: '#71717a',
        tooltipBg: '#0A0A0B',
        tooltipBorder: '#27272a',
        tooltipText: '#a1a1aa',
        line: '#F59E0B',
        activeStroke: '#0A0A0B',
      }
    : {
        grid: '#d4d4d8',
        axis: '#a1a1aa',
        tick: '#71717a',
        tooltipBg: '#ffffff',
        tooltipBorder: '#d4d4d8',
        tooltipText: '#52525b',
        line: '#D97706',
        activeStroke: '#ffffff',
      }

  if (isLoading) {
    return (
      <div className="min-h-screen">
        <Nav />
        <main className="mx-auto max-w-[1400px] px-6 py-10">
          <div className="shimmer h-8 w-64 rounded" />
          <div className="shimmer mt-6 h-40 w-full rounded" />
          <div className="shimmer mt-6 h-80 w-full rounded" />
        </main>
      </div>
    )
  }

  if (!event) {
    return (
      <div className="min-h-screen">
        <Nav />
        <main className="mx-auto max-w-[1400px] px-6 py-10">
          <div className="rounded-md border border-red-900/50 bg-red-950/20 px-4 py-3 text-sm text-red-300">
            {error || 'Event not found.'}
          </div>
        </main>
      </div>
    )
  }

  async function reloadEvent() {
    const refreshed = await getEvent(event.id)
    setEvent(refreshed)
    setRequestQty(String(refreshed.transferable_qty || ''))
  }

  async function handleAnalyze() {
    setAnalyzing(true)
    setError('')
    try {
      await analyzeEvent(event.id)
      await reloadEvent()
    } catch (err) {
      setError(err.message || 'Analysis failed.')
    } finally {
      setAnalyzing(false)
    }
  }

  async function handleCreateTransferRequest() {
    setSubmittingRequest(true)
    setError('')
    try {
      await createTransferRequest({
        event_id: event.id,
        source_dc: event.source_dc,
        dest_dc: event.dest_dc,
        sku_id: event.sku_id,
        qty: Number(requestQty),
      })
      router.push('/approvals')
    } catch (err) {
      setError(err.message || 'Failed to create transfer request.')
    } finally {
      setSubmittingRequest(false)
    }
  }

  return (
    <div className="min-h-screen">
      <Nav />
      <main className="page-fade mx-auto max-w-[1400px] px-6 py-8 pb-40">
        <div className="mb-3 flex items-center gap-2 text-xs text-[hsl(var(--app-text-muted))]">
          <Link href="/" className="hover:text-[hsl(var(--app-text-strong))]">Dashboard</Link>
          <ChevronRight className="h-3.5 w-3.5" />
          <span className="mono text-[hsl(var(--app-text-soft))]">{event.sku_id} - {event.dest_dc}</span>
          <div className="ml-auto">
            <StateBadge state={event.state} size="lg" />
          </div>
        </div>

        <div className="rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <div className="flex items-center gap-3">
                <h1 className="text-2xl font-medium text-[hsl(var(--app-text-strong))]">{event.sku_id}</h1>
                <RiskPill risk={event.penalty_risk_level || 'LOW'} />
              </div>
              <div className="mt-2 flex items-center gap-3 text-sm">
                <span className="mono text-[hsl(var(--app-text-soft))]">{event.event_key}</span>
                <span className="text-border">·</span>
                <span className="text-[hsl(var(--app-text-muted))]">Expected penalty {fmtMoney(event.expected_penalty_cost)}</span>
              </div>
            </div>
            <div className="text-right">
              <div className="text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">Route</div>
              <div className="mt-1 flex items-center gap-2 text-sm">
                <DCBadge code={event.source_dc} />
                <span className="text-[hsl(var(--app-text-muted))]">to</span>
                <DCBadge code={event.dest_dc} />
              </div>
            </div>
          </div>
        </div>

        {error && (
          <div className="mt-6 rounded-md border border-red-900/50 bg-red-950/20 px-4 py-3 text-sm text-red-300">
            {error}
          </div>
        )}

        <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-3">
          <SignalCard
            title="Demand"
            rows={[
              ['Days of supply', event.days_of_supply ?? '—'],
              ['Stockout date', fmtDate(event.stockout_date)],
              ['Destination', event.dest_dc],
            ]}
          />
          <SignalCard
            title="Supply"
            rows={[
              ['Transferable qty', fmtUnits(event.transferable_qty)],
              ['Relief arriving', event.relief_arriving ? 'Yes' : 'No'],
              ['Relief ETA', fmtDate(event.relief_eta)],
            ]}
          />
          <SignalCard
            title="Penalty"
            rows={[
              ['Risk level', event.penalty_risk_level || '—'],
              ['Risk score', event.penalty_risk_score ?? '—'],
              ['Expected penalty', fmtMoney(event.expected_penalty_cost)],
            ]}
          />
        </div>

        <section className="mt-6 rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors">
          <div className="mb-4 flex items-center justify-between">
            <div>
              <h3 className="text-sm uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
                {event.dest_dc} depletion forecast
              </h3>
              <p className="mt-1 text-xs text-[hsl(var(--app-text-soft))]">
                Projection stored on the event row by the demand agent.
              </p>
            </div>
          </div>
          <div className="w-full overflow-hidden" style={{ height: 288 }}>
            {projection.length === 0 ? (
              <div className="flex h-full items-center justify-center text-sm text-neutral-500">
                No depletion projection available.
              </div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={projection} margin={{ top: 16, right: 24, left: 8, bottom: 16 }}>
                  <CartesianGrid stroke={chartTheme.grid} strokeDasharray="2 4" />
                  <XAxis
                    dataKey="day"
                    stroke={chartTheme.axis}
                    tick={{ fill: chartTheme.tick, fontSize: 11, fontFamily: 'var(--font-dm-mono)' }}
                  />
                  <YAxis
                    stroke={chartTheme.axis}
                    tickFormatter={(value) => fmtUnits(value)}
                    tick={{ fill: chartTheme.tick, fontSize: 11, fontFamily: 'var(--font-dm-mono)' }}
                  />
                  <Tooltip content={<ChartTooltip theme={chartTheme} />} cursor={{ stroke: chartTheme.line, strokeOpacity: 0.3 }} />
                  <Line
                    type="monotone"
                    dataKey="available"
                    stroke={chartTheme.line}
                    strokeWidth={2}
                    dot={false}
                    activeDot={{ r: 4, fill: chartTheme.line, stroke: chartTheme.activeStroke, strokeWidth: 2 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </section>

        <section className="mt-6 rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h3 className="text-sm uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">Tradeoff summary</h3>
              <p className="mt-1 text-sm text-[hsl(var(--app-text-soft))]">
                {tradeoff?.summary || 'Tradeoff details unavailable.'}
              </p>
            </div>
            <div className="mono rounded-full bg-secondary px-3 py-1 text-[11px] tracking-widest text-[hsl(var(--app-text-soft))]">
              lower-cost path {tradeoff?.cheaperAction || '—'}
            </div>
          </div>
          <div className="mt-5 grid grid-cols-1 gap-4 md:grid-cols-3">
            <MetricCard label="Cost delta" value={tradeoff ? fmtMoney(tradeoff.delta) : '—'} />
            <MetricCard label="Expected penalty" value={fmtMoney(event.expected_penalty_cost)} />
            <MetricCard label="Confidence basis" value={tradeoff?.confidenceHint || '—'} />
          </div>
        </section>

        <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-2">
          <div
            className="rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors"
            style={transferIsRecommended ? { boxShadow: 'inset 3px 0 0 0 #F59E0B' } : undefined}
          >
            <div className="flex items-center justify-between">
              <div className={`text-[11px] uppercase tracking-[0.18em] ${transferIsRecommended ? 'text-[#F59E0B]' : 'text-[hsl(var(--app-text-soft))]'}`}>
                Transfer now
              </div>
              {transferIsRecommended ? (
                <span className="mono rounded-full bg-[#F59E0B]/10 px-2 py-0.5 text-[10px] tracking-widest text-[#F59E0B] ring-1 ring-inset ring-[#F59E0B]/30">
                  RECOMMENDED
                </span>
              ) : (
                <span className="mono rounded-full bg-secondary px-2 py-0.5 text-[10px] tracking-widest text-[hsl(var(--app-text-soft))]">
                  ALTERNATIVE
                </span>
              )}
            </div>
            <div className="mono mt-3 text-4xl font-medium text-[hsl(var(--app-text-strong))]">
              {fmtMoney(event.cost_transfer)}
            </div>
            <div className="mt-1 text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">estimated freight cost</div>
            <div className="mt-3 text-sm text-[hsl(var(--app-text-soft))]">
              {tradeoff?.cheaperAction === 'TRANSFER'
                ? `Best modeled outcome by ${fmtMoney(tradeoff.delta)}.`
                : tradeoff?.cheaperAction === 'WAIT'
                  ? `Costs ${fmtMoney(tradeoff.delta)} more than waiting.`
                  : 'Modeled at parity with waiting.'}
            </div>
            <div className="mt-5 divide-y divide-border text-sm">
              <Row k="Transferable qty" v={`${fmtUnits(event.transferable_qty)} units`} />
              <Row k="Source DC" v={event.source_dc} />
              <Row k="Destination DC" v={event.dest_dc} />
              <Row k="Recommended action" v={event.recommended_action || '—'} />
            </div>
          </div>
          <div
            className="rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors"
            style={waitMonitorIsRecommended ? { boxShadow: 'inset 3px 0 0 0 #F59E0B' } : undefined}
          >
            <div className="flex items-center justify-between">
              <div className={`text-[11px] uppercase tracking-[0.18em] ${waitMonitorIsRecommended ? 'text-[#F59E0B]' : 'text-[hsl(var(--app-text-soft))]'}`}>
                Wait / monitor
              </div>
              {waitMonitorIsRecommended ? (
                <span className="mono rounded-full bg-[#F59E0B]/10 px-2 py-0.5 text-[10px] tracking-widest text-[#F59E0B] ring-1 ring-inset ring-[#F59E0B]/30">
                  RECOMMENDED
                </span>
              ) : (
                <span className="mono rounded-full bg-secondary px-2 py-0.5 text-[10px] tracking-widest text-[hsl(var(--app-text-soft))]">
                  ALTERNATIVE
                </span>
              )}
            </div>
            <div className="mono mt-3 text-4xl font-medium text-[hsl(var(--app-text-soft))]">
              {fmtMoney(event.cost_wait)}
            </div>
            <div className="mt-1 text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">estimated wait cost</div>
            <div className="mt-3 text-sm text-[hsl(var(--app-text-soft))]">
              {tradeoff?.cheaperAction === 'WAIT'
                ? `Best modeled outcome by ${fmtMoney(tradeoff.delta)}.`
                : tradeoff?.cheaperAction === 'TRANSFER'
                  ? `Leaves ${fmtMoney(tradeoff.delta)} of modeled savings on the table.`
                  : 'Modeled at parity with transferring.'}
            </div>
            <div className="mt-5 divide-y divide-border text-sm">
              <Row k="Relief arriving" v={event.relief_arriving ? 'Yes' : 'No'} />
              <Row k="Relief ETA" v={fmtDate(event.relief_eta)} />
              <Row k="PO at risk" v={event.po_at_risk ? 'Yes' : 'No'} />
              <Row k="Confidence" v={event.confidence || '—'} />
            </div>
          </div>
        </div>

        <section className="mt-6 rounded-md border border-border bg-[hsl(var(--app-panel))] transition-colors" style={{ boxShadow: 'inset 0 3px 0 0 #F59E0B' }}>
          <div className="p-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-[#F59E0B]">
                <Sparkles className="h-3.5 w-3.5" /> {event.ai_unavailable ? 'Fallback recommendation' : 'AI recommendation'}
              </div>
              <div className="flex items-center gap-2">
                <span className="mono rounded-full bg-[#F59E0B]/10 px-2 py-0.5 text-[10px] tracking-widest text-[#F59E0B] ring-1 ring-inset ring-[#F59E0B]/30">
                  {event.recommended_action || '—'}
                </span>
                <span className="mono rounded-full bg-neutral-800 px-2 py-0.5 text-[10px] tracking-widest text-neutral-300 ring-1 ring-inset ring-neutral-700">
                  {event.confidence || '—'}
                </span>
              </div>
            </div>
            {event.ai_unavailable && (
              <div className="mt-4 rounded-md border border-amber-900/50 bg-amber-950/20 px-4 py-3 text-sm text-amber-200">
                Live AI analysis is unavailable right now. The values below are a conservative fallback based on the
                existing event signals and cost estimates.
              </div>
            )}
            <p className="mt-3 text-sm leading-relaxed text-[hsl(var(--app-text-strong))]">
              {event.orchestrator_recommendation?.reasoning || event.reasoning || 'No reasoning available.'}
            </p>
            <div className="mt-5 rounded-md border border-dashed border-border bg-[hsl(var(--app-panel-muted))] px-4 py-3 text-sm text-[hsl(var(--app-text-soft))]">
              Primary actions stay pinned in the footer below so you can review the event while keeping analysis and transfer controls in view.
            </div>
          </div>
        </section>

        <section className="mt-6 rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h3 className="text-sm uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">Transfer request</h3>
              <p className="mt-1 text-sm text-[hsl(var(--app-text-soft))]">
                Create a draft transfer request from the current orchestrator recommendation.
              </p>
            </div>
            {latestTransferRequest && <StateBadge state={latestTransferRequest.state} />}
          </div>
          {latestTransferRequest ? (
            <div className="mt-5 grid grid-cols-1 gap-4 md:grid-cols-4">
              <MetricCard label="Request ID" value={`#${latestTransferRequest.id}`} />
              <MetricCard label="Qty" value={fmtUnits(latestTransferRequest.qty)} />
              <MetricCard label="Estimated cost" value={fmtMoney(latestTransferRequest.estimated_cost)} />
              <MetricCard label="Rejected reason" value={latestTransferRequest.rejection_reason || '—'} />
            </div>
          ) : (
            <div className="mt-5 rounded-md border border-dashed border-border bg-[hsl(var(--app-panel-muted))] px-4 py-3 text-sm text-[hsl(var(--app-text-soft))]">
              {canCreateTransfer
                ? 'Use the sticky footer to adjust the quantity and submit the transfer request.'
                : 'Transfer creation is locked until the current recommendation is TRANSFER.'}
            </div>
          )}
        </section>

        <section className="mt-6 rounded-md border border-border bg-[hsl(var(--app-panel))] p-6 transition-colors">
          <h3 className="text-sm uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">State history</h3>
          <div className="mt-4 space-y-3">
            {event.state_history?.length ? (
              event.state_history.map((entry) => (
                <div key={entry.id || `${entry.entity_id}-${entry.created_at}`} className="rounded-md border border-border bg-[hsl(var(--app-panel-muted))] px-4 py-3 transition-colors">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div className="mono text-sm text-[hsl(var(--app-text-strong))]">
                      {(entry.old_state || 'NONE')} to {entry.new_state}
                    </div>
                    <div className="text-xs text-[hsl(var(--app-text-muted))]">
                      {entry.created_at ? new Date(entry.created_at).toLocaleString() : '—'}
                    </div>
                  </div>
                  <div className="mt-1 text-xs text-[hsl(var(--app-text-muted))]">actor: {entry.actor || 'unknown'}</div>
                  {entry.notes && <div className="mt-2 text-sm text-[hsl(var(--app-text-soft))]">{entry.notes}</div>}
                </div>
              ))
            ) : (
              <div className="text-sm text-[hsl(var(--app-text-soft))]">No state transitions recorded yet.</div>
            )}
          </div>
        </section>

        <div className="mt-8 pb-12">
          <Link href="/" className="text-xs text-[hsl(var(--app-text-muted))] hover:text-[hsl(var(--app-text-strong))]">
            ← Back to dashboard
          </Link>
        </div>
      </main>
      <footer className="fixed bottom-0 left-0 right-0 z-40 border-t border-border bg-[hsl(var(--app-overlay))] backdrop-blur">
        <div className="mx-auto flex max-w-[1400px] flex-col gap-3 px-6 py-4 md:flex-row md:items-center md:justify-between">
          <div>
            <div className="mono text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">Event actions</div>
            <div className="mt-1 text-sm text-[hsl(var(--app-text-soft))]">
              {latestTransferRequest
                ? `Transfer request #${latestTransferRequest.id} is ${latestTransferRequest.state}.`
                : canCreateTransfer
                  ? 'Adjust the transfer quantity or re-run analysis without leaving the page.'
                  : `Current recommendation is ${event.recommended_action || 'unavailable'}. Re-run analysis anytime.`}
            </div>
          </div>
          <div className="flex flex-col gap-3 md:flex-row md:items-center">
            {canCreateTransfer && (
              <label className="flex items-center gap-2">
                <span className="mono text-[11px] tracking-widest text-[hsl(var(--app-text-muted))]">QTY</span>
                <input
                  type="number"
                  min="1"
                  value={requestQty}
                  onChange={(e) => setRequestQty(e.target.value)}
                  className="mono w-28 rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground focus:border-[#F59E0B]/60 focus:outline-none"
                />
              </label>
            )}
            <div className="flex flex-wrap items-center gap-2">
              {showAnalyzeAction && (
                <button
                  onClick={handleAnalyze}
                  disabled={analyzing}
                  className="mono inline-flex items-center gap-2 rounded-md border border-border bg-[hsl(var(--app-panel))] px-3 py-2 text-xs tracking-widest text-[hsl(var(--app-text-strong))] hover:border-[#F59E0B]/60 hover:text-[#F59E0B] disabled:opacity-60"
                >
                  {analyzing ? (
                    <><Loader2 className="h-3.5 w-3.5 animate-spin" /> ANALYZING…</>
                  ) : (
                    <>{event.ai_unavailable ? 'RETRY ANALYSIS' : 'RE-RUN ANALYSIS'}</>
                  )}
                </button>
              )}
              {latestTransferRequest ? (
                <button
                  onClick={() => router.push('/approvals')}
                  className="mono rounded-md bg-[#F59E0B] px-4 py-2 text-xs font-medium tracking-widest text-neutral-950 hover:bg-[#F59E0B]/90"
                >
                  VIEW APPROVALS
                </button>
              ) : canCreateTransfer ? (
                <button
                  onClick={handleCreateTransferRequest}
                  disabled={submittingRequest || !requestQty || Number(requestQty) <= 0 || !canCreateTransfer}
                  className="mono rounded-md bg-[#F59E0B] px-4 py-2 text-xs font-medium tracking-widest text-neutral-950 hover:bg-[#F59E0B]/90 disabled:opacity-40"
                >
                  {submittingRequest ? 'CREATING…' : 'CREATE TRANSFER REQUEST'}
                </button>
              ) : (
                <span className="mono rounded-md border border-border bg-[hsl(var(--app-panel))] px-4 py-2 text-xs tracking-widest text-[hsl(var(--app-text-soft))]">
                  {event.recommended_action || 'NO RECOMMENDATION'}
                </span>
              )}
            </div>
          </div>
        </div>
      </footer>
    </div>
  )
}

function Row({ k, v, vClass }) {
  return (
    <div className="flex items-center justify-between py-2.5">
      <span className="text-xs uppercase tracking-widest text-[hsl(var(--app-text-muted))]">{k}</span>
      <span className={`mono text-sm text-[hsl(var(--app-text-strong))] ${vClass || ''}`}>{v}</span>
    </div>
  )
}

function SignalCard({ title, rows }) {
  return (
    <div className="rounded-md border border-border bg-[hsl(var(--app-panel))] p-5 transition-colors">
      <div className="text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">{title}</div>
      <div className="mt-4 divide-y divide-border text-sm">
        {rows.map(([key, value]) => (
          <Row key={key} k={key} v={value} vClass={key === 'Days of supply' ? supplyColor(Number(value) || 0) : ''} />
        ))}
      </div>
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

function ChartTooltip({ active, payload, label, theme }) {
  if (!active || !payload?.length) return null
  const point = payload[0].payload
  return (
    <div
      className="rounded-md px-3 py-2 shadow-lg"
      style={{ border: `1px solid ${theme.tooltipBorder}`, backgroundColor: theme.tooltipBg }}
    >
      <div className="mono text-[11px]" style={{ color: theme.tooltipText }}>Day {label}</div>
      <div className="mono text-sm font-medium" style={{ color: theme.line }}>{fmtUnits(point.available)} units</div>
    </div>
  )
}

function describeConfidence(confidence, delta) {
  if (!confidence) return 'Awaiting recommendation'
  if (confidence === 'HIGH') return `Large enough gap to back a ${fmtMoney(delta)} decision spread`
  if (confidence === 'MEDIUM') return `Useful signal, but the ${fmtMoney(delta)} spread is still worth checking`
  return `Costs are relatively close at ${fmtMoney(delta)} apart`
}
