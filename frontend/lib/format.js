export const fmtMoney = (n) =>
  '$' + Number(n || 0).toLocaleString('en-US', { maximumFractionDigits: 0 })

export const fmtUnits = (n) =>
  Number(n || 0).toLocaleString('en-US', { maximumFractionDigits: 0 })

export const fmtDate = (iso) => {
  if (!iso) return '—'
  const d = new Date(iso)
  return d.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })
}

export const fmtDateTime = (iso) => {
  if (!iso) return '—'
  const d = new Date(iso)
  return d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  })
}

export const daysBetween = (a, b) => {
  const ms = new Date(b).getTime() - new Date(a).getTime()
  return Math.round(ms / (1000 * 60 * 60 * 24))
}

export const supplyColor = (days) => {
  if (days < 7) return 'text-[#EF4444]'
  if (days < 15) return 'text-[#F59E0B]'
  return 'text-[#22C55E]'
}

export const supplyBorder = (days) => {
  if (days < 7) return 'border-[#EF4444]/60'
  if (days < 15) return 'border-[#F59E0B]/60'
  return 'border-[#22C55E]/60'
}
