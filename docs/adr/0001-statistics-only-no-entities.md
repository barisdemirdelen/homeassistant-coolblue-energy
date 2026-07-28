# Statistics-only: the integration creates no entities

This integration exposes Coolblue Energy data exclusively as **external statistics** written
into the recorder. It creates no entities and no device. Six sensors used to exist and were
deliberately dropped: each one only ever restated yesterday's daily total, which the
statistics already carry at full hourly resolution, so keeping them meant maintaining two
sources of truth for the same numbers and inviting them to disagree.

## Considered options

- **Keep the six sensors alongside the statistics.** Rejected: pure duplication, and a sensor
  showing a single stale daily total is a worse view of the data than the Energy Dashboard's.
- **Restore a smaller diagnostic surface** (for example a last-successful-import timestamp).
  Not rejected on merit — it is genuinely non-redundant, unlike the six — but it is a separate
  idea, not part of the statistics-only decision.

## Consequences

- The config entry is **invisible in the UI**. No device page, no entity list; the only
  evidence the integration is working is data appearing in the Energy Dashboard, plus the log.
- There is no `sensor.*` to reference from templates or automations. Consumers read the
  statistics instead.
- The quality-scale entity rules (`entity-unique-id`, `has-entity-name`,
  `entity-event-setup`) are declared `exempt`, not `done`.
- Prior art: the core `elvia` integration has the same shape — statistics-only, no entity
  platforms. See `docs/research/statistics-only-integrations.md`.
