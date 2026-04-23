'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { Moon, Sun } from 'lucide-react'
import { useTheme } from 'next-themes'
import { useEffect, useState } from 'react'

export default function Nav() {
  const path = usePathname()
  const { resolvedTheme, setTheme } = useTheme()
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
  }, [])

  const isActive = (p) => {
    if (p === '/') return path === '/'
    return path?.startsWith(p)
  }

  const Link2 = ({ href, children }) => (
    <Link
      href={href}
      className={`relative px-1 py-1 text-sm tracking-wide transition-colors ${
        isActive(href)
          ? 'text-[hsl(var(--app-text-strong))]'
          : 'text-[hsl(var(--app-text-soft))] hover:text-[hsl(var(--app-text-strong))]'
      }`}
    >
      {children}
      {isActive(href) && (
        <span className="absolute -bottom-[10px] left-0 right-0 h-[2px] bg-[#F59E0B]" />
      )}
    </Link>
  )

  const isDark = resolvedTheme === 'dark'

  return (
    <header className="sticky top-0 z-40 border-b border-border/80 bg-[hsl(var(--app-overlay))] backdrop-blur transition-colors">
      <div className="mx-auto flex h-14 max-w-[1400px] items-center justify-between px-6">
        <Link href="/" className="mono text-sm font-medium tracking-wider">
          <span className="text-[#F59E0B]">POP</span>
          <span className="text-[hsl(var(--app-text-strong))]">_SENTINEL</span>
        </Link>
        <nav className="flex items-center gap-7">
          <Link2 href="/">Dashboard</Link2>
          <Link2 href="/inventory">Inventory</Link2>
          <Link2 href="/approvals">Approvals</Link2>
        </nav>
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={() => mounted && setTheme(isDark ? 'light' : 'dark')}
            className="inline-flex h-9 items-center gap-2 rounded-full border border-border bg-[hsl(var(--app-panel))] px-3 text-xs text-[hsl(var(--app-text-soft))] transition-colors hover:border-[#F59E0B]/40 hover:text-[hsl(var(--app-text-strong))]"
            aria-label={mounted ? `Switch to ${isDark ? 'light' : 'dark'} mode` : 'Toggle theme'}
          >
            {mounted && isDark ? <Sun className="h-3.5 w-3.5 text-[#F59E0B]" /> : <Moon className="h-3.5 w-3.5 text-[#F59E0B]" />}
            <span className="mono tracking-widest">{mounted ? (isDark ? 'LIGHT' : 'DARK') : 'THEME'}</span>
          </button>
          <div className="mono hidden text-xs text-[hsl(var(--app-text-muted))] sm:block">
            <span className="mr-3 inline-block h-1.5 w-1.5 rounded-full bg-[#22C55E] align-middle" />
            LIVE · v1.2.4
          </div>
        </div>
      </div>
    </header>
  )
}
