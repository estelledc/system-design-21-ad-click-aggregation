# Ad Click Event Aggregation Lab

This clean-room system-design practice starts with one question: when click events are duplicated, reordered, delayed across a
window boundary, or replayed after a worker crash, what immutable event and window receipts let a query explain its count without
turning “aggregate visible” into an invented claim that a click was valid, attributable, billable, or charged?

The prompt title is “Ad Click Event Aggregation.” This repository does not copy an analytics product, source chapter, protocol,
diagram, benchmark, event stream, ad data, or proprietary behavior. The initial contract is frozen from the title alone before
reading the fixed secondary chapter.

## Current phase

Closed-book contract only. Source comparison, primary references, storage/stream choice, implementation, tests, benchmark, public
remote, CI, deployment, billing/fraud outcome, SLA, and external acceptance are all pending.

Candidate concerns include stable event/batch identity, event time, half-open windows, duplicate/reorder handling, shard/worker
generation, watermark/finalization, late-data correction, immutable aggregate publication, snapshot-stable queries, retention, and
evidence separation. They remain hypotheses until source review and executable validation.

## Read first

- [Closed-book contract](docs/closed-book-contract.md)
- [Security policy](SECURITY.md)

## Evidence boundary

SDK emission, gateway validation, durable event receipt, aggregate contribution, window publication, query visibility, fraud
classification, attribution, billable decision, ledger entry, charge, advertiser report, and business acceptance are different
facts. This lab will name and test only the boundaries it reaches. It will abstain from real click validity, unique-human counts,
conversion attribution, billing accuracy, revenue, fraud prevention, production capacity, SLA, or external acceptance without
matching evidence.

## License

MIT. Third-party study material and real advertising/event data are not included.
