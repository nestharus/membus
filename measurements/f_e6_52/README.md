# F-E6-52 chunk retirement: read-only measurements

These are the measurements behind [docs/CHUNK-RETIREMENT.md](../../docs/CHUNK-RETIREMENT.md). Both scripts read
`D:/projects/membus/state/stripe-{0..3}.log` up to each file's size when it is opened. They put no load on membus and
write nothing to it.

**Account:** claude6-eco's ledger
(`D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/ledger.json`), frontier F-E6-52.
Working records are in that loop's `runs/e17/act-work/F-E6-52/`, including the heartbeat reads, the quota probe count
and the metric day-file sizes.

- `stripe_split.py STATE_DIR OUT.json` splits the records by namespace and key family over four windows: since 00:44Z,
  since 07:21Z, the last 24 h and the last 6 h. It also counts the 24 h distinct op_ids (a lower bound of the dedup
  table's occupancy) and the distinct keys. The output is `stripe-split.json`, read 2026-10-11T08:59:25.765Z.
- `retire_sim.py STATE_DIR OUT.json` replays the records under the plan's classes and its candidate retirement forms
  (wait, wait with the 24 h retry window, carry, checkpoint), with 60 s and 1 h rotation. The output is
  `retire-sim.json`, read 2026-10-11T09:06:03.640Z.

Run with `python -I`. The record format is as `measurements/f_e6_48/window.py` reads it: an MBK1 header and a body
checked by crc32.
