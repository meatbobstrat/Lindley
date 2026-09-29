export default function Placeholder({ title, description }: { title: string; description: string }) {
  return (
    <section>
      <h2>{title}</h2>
      <p className="muted">{description}</p>
      <p className="muted">Coming in the UI design phase.</p>
    </section>
  )
}
