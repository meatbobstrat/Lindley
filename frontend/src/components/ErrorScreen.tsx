// What's shown when a screen crashes: in words, with a way out. The error is sent to Lindley's
// log (lib/errors.ts), where it can be found.

import { Component, type ReactNode, useEffect } from 'react'
import { useRouteError } from 'react-router'
import { describe, reportProblem } from '../lib/errors'

export function ErrorScreen({ error }: { error: unknown }) {
  return (
    <div className="crash" role="alert">
      <h1>Something went wrong</h1>
      <p>This screen stopped working. Your scans and documents aren’t affected: Lindley only shows them here.</p>
      <p className="crash-why">{describe(error)}</p>
      <p>The details are in Lindley’s log. Reloading usually puts it right.</p>
      <button onClick={() => window.location.reload()}>Reload Lindley</button>
    </div>
  )
}

/** A screen that crashes while it's drawn is replaced by ErrorScreen, not left blank. */
export class ErrorBoundary extends Component<{ children: ReactNode }, { error: unknown }> {
  state = { error: null as unknown }

  static getDerivedStateFromError(error: unknown) {
    return { error: error ?? new Error('Something threw nothing') }
  }

  componentDidCatch(error: unknown) {
    reportProblem('error', `Screen crashed: ${describe(error)}`, error)
  }

  render() {
    return this.state.error ? <ErrorScreen error={this.state.error} /> : this.props.children
  }
}

/** The router's own errors: a page that isn't there, or one that failed as it loaded. */
export function RouteError() {
  const error = useRouteError()
  useEffect(() => reportProblem('error', `Route failed: ${describe(error)}`, error), [error])
  return <ErrorScreen error={error} />
}
