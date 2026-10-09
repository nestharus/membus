# F-E6-2 measurements, 2026-10-09

Raw evidence: D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure. Each case has before/during/after `.context.jsonl` with lineup status, process count and PhysicalDisk samples. Commands and reserved slots are in `commands.jsonl`. The definitive final-build IPC passes are e1 `ipc-final.jsonl` and e2 `ipc-repeat.jsonl`; `ipc-1/2.jsonl` are preliminary single-interpreter thread results and are excluded here.

Ranges below are the minimum and maximum of two observed runs, not confidence intervals or promises about future runs. This shared host is noisy; a p99 based on 64 or 200 disk samples is an empirical order statistic with low tail confidence.

## Append, write-through and flush

Each write extends a newly truncated file, including NTFS allocation, size updates and metadata journal costs. Latencies are write-plus-optional-FlushFileBuffers, not isolated device flush time. Sizes are KiB. `write_through` issues FILE_FLAG_WRITE_THROUGH writes with zero explicit flush API calls; `write_through_flush` adds an explicit flush. The unflushed controls are drained once each outside the timer, counted separately. Write-through-only cases have no metadata drain; later cases may inherit dirty NTFS metadata, affecting their first samples and tails. Application bytes exclude filesystem metadata and NAND amplification.

| Drive | Mode | KiB | p50 µs | p95 µs | p99 µs | Explicit flushes/s | Writes/s | Raw file:line |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C | flush | 4 | 795.7–24593.9 | 899.8–72003.0 | 12571.4–108082.4 | 35–1029 | 35–1029 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:7; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:7 |
| C | flush | 16 | 795.9–23826.4 | 1004.5–71582.7 | 12588.9–120639.9 | 38–1056 | 38–1056 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:8; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:8 |
| C | flush | 64 | 774.2–23917.9 | 908.1–61126.9 | 1005.6–83998.6 | 37–1282 | 37–1282 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:9; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:9 |
| C | unflushed | 4 | 3.2–4.1 | 16.2–20.8 | 28.5–36.4 | 0 | 162022–214546 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:1; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:1 |
| C | unflushed | 16 | 5.7–8.4 | 25.1–42.2 | 90.1–108.1 | 0 | 75517–113993 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:3; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:3 |
| C | unflushed | 64 | 17.7–27.6 | 38.2–96.1 | 61.3–519.6 | 0 | 23491–49315 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:5; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:5 |
| C | write_through | 4 | 69.2–24358.1 | 148.5–83738.3 | 196.6–119862.5 | 0 | 32–12521 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:10; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:10 |
| C | write_through | 16 | 68.6–24947.6 | 150.7–84764.0 | 286.6–132449.4 | 0 | 30–11987 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:11; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:11 |
| C | write_through | 64 | 100.7–23233.7 | 182.3–72658.2 | 253.5–131181.0 | 0 | 42–8702 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:12; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:12 |
| C | write_through_flush | 4 | 804.6–25435.7 | 962.0–72105.2 | 1089.4–96190.5 | 30–1216 | 30–1216 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:13; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:13 |
| C | write_through_flush | 16 | 909.4–13305.5 | 1234.7–48539.5 | 12736.4–60385.2 | 48–854 | 48–854 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:14; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:14 |
| C | write_through_flush | 64 | 890.5–13142.0 | 1067.9–48705.3 | 12474.3–49465.0 | 56–936 | 56–936 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:15; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:15 |
| D | flush | 4 | 717.2–758.4 | 788.7–1205.8 | 12771.4–12970.7 | 554–927 | 554–927 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:7; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:7 |
| D | flush | 16 | 720.5–806.4 | 789.4–13004.1 | 1084.7–14435.5 | 494–1361 | 494–1361 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:8; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:8 |
| D | flush | 64 | 717.8–809.4 | 774.6–12664.0 | 797.7–24680.1 | 541–1396 | 541–1396 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:9; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:9 |
| D | unflushed | 4 | 2.5–2.9 | 9.5–12.8 | 22.8–74.9 | 0 | 206996–287646 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:1; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:1 |
| D | unflushed | 16 | 5.6–7.0 | 13.7–26.8 | 24.6–80.3 | 0 | 100492–154297 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:3; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:3 |
| D | unflushed | 64 | 17.2–19.3 | 35.5–36.7 | 53.0–125.5 | 0 | 39452–51222 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:5; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:5 |
| D | write_through | 4 | 47.0–99.6 | 94.2–166.4 | 120.9–318.9 | 0 | 6078–18246 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:10; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:10 |
| D | write_through | 16 | 52.1–101.5 | 99.7–219.9 | 132.4–12607.0 | 0 | 4101–16600 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:11; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:11 |
| D | write_through | 64 | 66.2–119.1 | 116.6–181.4 | 156.4–232.5 | 0 | 5473–13230 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:12; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:12 |
| D | write_through_flush | 4 | 766.6–809.2 | 825.3–919.7 | 1440.2–1503.8 | 1132–1300 | 1132–1300 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:13; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:13 |
| D | write_through_flush | 16 | 771.9–847.8 | 839.4–1177.1 | 891.2–13223.1 | 857–1290 | 857–1290 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:14; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:14 |
| D | write_through_flush | 64 | 773.8–845.7 | 1072.1–1433.7 | 12914.7–13461.3 | 855–900 | 855–900 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:15; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:15 |

## Group commit

2253 bytes per record (2.2 KiB rounded). Every batch has one explicit flush; each single-record and eight-record case has 200 samples, each 64/512-record case has 64 samples.

| Drive | Records/flush | p50 µs | p95 µs | p99 µs | Records/s | Flushes/s | Raw file:line |
| --- | --- | --- | --- | --- | --- | --- | --- |
| C | 1 | 794.5–13908.6 | 1052.9–60555.4 | 12530.7–83651.0 | 44–828 | 44–828 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:16; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:16 |
| C | 8 | 824.9–12870.7 | 12069.2–48392.5 | 12805.2–72043.5 | 503–4900 | 63–613 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:17; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:17 |
| C | 64 | 1176.3–14809.0 | 1383.3–49762.8 | 1438.5–109089.9 | 2843–54391 | 44–850 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:18; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:18 |
| C | 512 | 4235.3–138237.5 | 6141.7–984145.7 | 18171.2–1836137.8 | 2143–109562 | 4–214 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl:19; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl:19 |
| D | 1 | 724.9–758.1 | 917.3–1315.6 | 1373.7–13256.7 | 933–1187 | 933–1187 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:16; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:16 |
| D | 8 | 759.7–809.0 | 866.9–1018.8 | 1496.7–12573.7 | 8988–9084 | 1123–1135 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:17; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:17 |
| D | 64 | 889.9–969.6 | 1053.1–1088.9 | 1211.8–1585.5 | 64941–69142 | 1015–1080 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:18; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:18 |
| D | 512 | 2238.1–2891.3 | 2405.3–3993.6 | 2813.4–14552.5 | 163030–228295 | 318–446 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl:19; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl:19 |

## IPC

Both languages use the same dependency-free Rust echo server executable, SHA256 recorded in every definitive row. No membus storage, serialization or subscription work occurs. Each client warms up for 16 echoes, then records 500 echoes with a unique client/request header. Python uses N spawned interpreter processes, Rust N client threads. Blocking TCP and TCP_NODELAY on both ends; byte-mode local named pipes with 64 KiB buffers. Connections and warmup are untimed. Rates are aggregate completed round trips/s for N closed-loop clients and include coordination/validation overhead; they are not per-client rates or server maximum capacity. Python start uses an Event and shared QPC end timestamps; Rust uses two barriers and final joins.

| Client/transport | Bytes | Clients | p50 µs | p99 µs | Aggregate round trips/s | Raw file:line |
| --- | --- | --- | --- | --- | --- | --- |
| python_pipe | 64 | 1 | 10.8–13.3 | 29.1–37.4 | 57427–79667 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:20; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:20 |
| python_pipe | 64 | 8 | 20.7–23.6 | 35.6–51.8 | 287486–337613 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:21; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:21 |
| python_pipe | 64 | 32 | 29.3–36.1 | 92.3–453.3 | 330799–417819 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:24; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:24 |
| python_pipe | 2253 | 1 | 12.2–27.3 | 42.4–56.2 | 32234–60624 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:26; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:26 |
| python_pipe | 2253 | 8 | 23.7–29.3 | 40.1–83.7 | 221733–296138 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:27; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:27 |
| python_pipe | 2253 | 32 | 34.3–37.5 | 199.7–202.9 | 339791–367250 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:30; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:30 |
| python_pipe | 51200 | 1 | 42.6–48.7 | 81.9–93.9 | 19429–21080 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:32; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:32 |
| python_pipe | 51200 | 8 | 66.3–75.7 | 126.9–144.5 | 94891–100200 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:33; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:33 |
| python_pipe | 51200 | 32 | 108.7–126.8 | 353.8–399.2 | 137097–179299 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:36; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:36 |
| python_tcp | 64 | 1 | 24.3–33.3 | 69.7–73.7 | 26215–32236 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:2; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:2 |
| python_tcp | 64 | 8 | 40.5–48.2 | 63.7–106.9 | 145566–180746 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:3; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:3 |
| python_tcp | 64 | 32 | 66.1–78.4 | 219.1–366.6 | 169060–220762 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:6; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:6 |
| python_tcp | 2253 | 1 | 28.9–39.4 | 69.5–79.1 | 23017–29067 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:8; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:8 |
| python_tcp | 2253 | 8 | 48.2–61.0 | 73.1–96.3 | 120928–153488 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:9; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:9 |
| python_tcp | 2253 | 32 | 76.9–77.5 | 216.3–246.4 | 189611–212574 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:12; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:12 |
| python_tcp | 51200 | 1 | 262.1–328.0 | 489.7–508.7 | 3018–3688 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:14; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:14 |
| python_tcp | 51200 | 8 | 412.4–432.9 | 626.8–638.7 | 17661–18409 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:15; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:15 |
| python_tcp | 51200 | 32 | 964.0–1004.2 | 2237.7–2279.2 | 28084–28319 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:18; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:18 |
| rust_pipe | 64 | 1 | 9.1–14.4 | 26.0–33.4 | 64922–89749 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:19; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:19 |
| rust_pipe | 64 | 8 | 18.8–19.4 | 34.2–42.0 | 373542–401007 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:22; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:22 |
| rust_pipe | 64 | 32 | 23.1–24.0 | 61.7–71.7 | 1024321–1066460 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:23; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:23 |
| rust_pipe | 2253 | 1 | 8.7–32.0 | 20.9–97.5 | 27929–110205 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:25; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:25 |
| rust_pipe | 2253 | 8 | 19.1–21.7 | 29.8–46.6 | 352206–402771 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:28; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:28 |
| rust_pipe | 2253 | 32 | 17.4–19.9 | 54.7–60.1 | 1083585–1127857 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:29; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:29 |
| rust_pipe | 51200 | 1 | 38.0–42.8 | 69.8–74.4 | 23104–26697 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:31; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:31 |
| rust_pipe | 51200 | 8 | 69.0–73.8 | 112.4–148.7 | 101308–111051 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:34; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:34 |
| rust_pipe | 51200 | 32 | 113.5–118.7 | 303.9–412.6 | 215038–218881 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:35; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:35 |
| rust_tcp | 64 | 1 | 18.2–49.3 | 61.9–97.3 | 20213–41245 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:1; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:1 |
| rust_tcp | 64 | 8 | 39.1–46.5 | 58.1–87.5 | 162998–196907 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:4; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:4 |
| rust_tcp | 64 | 32 | 55.7–56.5 | 129.1–190.4 | 305792–379841 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:5; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:5 |
| rust_tcp | 2253 | 1 | 20.5–49.7 | 58.2–96.6 | 19983–39784 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:7; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:7 |
| rust_tcp | 2253 | 8 | 43.8–51.3 | 66.7–100.7 | 139717–173879 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:10; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:10 |
| rust_tcp | 2253 | 32 | 71.0–75.8 | 194.6–247.5 | 284608–287467 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:11; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:11 |
| rust_tcp | 51200 | 1 | 224.5–338.1 | 425.6–502.5 | 2969–3976 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:13; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:13 |
| rust_tcp | 51200 | 8 | 404.7–427.9 | 619.2–750.0 | 17547–18908 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:16; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:16 |
| rust_tcp | 51200 | 32 | 601.3–1085.9 | 2181.2–5921.8 | 25760–27052 | D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/ipc-final.jsonl:17; D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/ipc-repeat.jsonl:17 |

## DiskSpd

Microsoft v2.3 official ZIP SHA256 `18609981e8b04b4f645f1fd182ad0541615fe71722de36a9a933c54f747218a3`, matched the release asset digest. `trustcheck check microsoft/diskspd` returned `below thresholds`, Scorecard 3.3 < 5.0; the historical e1 runs preceded the required scan/review. e2 directory and ZIP scans both report ClamAV clean (exit 0); no GuardDog result is reported for this non-Python package. No DiskSpd ran in e2. Admission review and lane87 question receipt are located in the repeat folder. Each run is 15 s, one thread, queue depth one, 32 MiB target, no warmup/cooldown, unbuffered write-through (`-Sh`), 100% writes. Small-write runs use random 4 KiB; sequential runs use 64 KiB. These are observed QD1 write-through capacities under co-tenant load, not maximum device capacities. DiskSpd does not expose actual NVMe flush counts and issues no FlushFileBuffers loop. Its overwrite case avoids the std probe’s append allocation cost.

| Drive | Workload | Run | App write bytes | Writes | MiB/s | Writes/s | p50 µs | p99 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C | small | 1 | 783372288 | 191253 | 49.8 | 12750.2 | 21.0 | 601.0 |
| C | small | 2 | 3743744 | 914 | 0.2 | 60.9 | 12134.0 | 119618.0 |
| C | sequential | 1 | 20297154560 | 309710 | 1290.5 | 20647.3 | 25.0 | 333.0 |
| C | sequential | 2 | 75366400 | 1150 | 4.8 | 76.7 | 12041.0 | 83993.0 |
| D | small | 1 | 1505652736 | 367591 | 95.7 | 24506.1 | 16.0 | 203.0 |
| D | small | 2 | 558596096 | 136376 | 35.5 | 9091.7 | 17.0 | 650.0 |
| D | sequential | 1 | 13242269696 | 202061 | 841.9 | 13470.7 | 24.0 | 360.0 |
| D | sequential | 2 | 3517317120 | 53670 | 223.6 | 3578.0 | 24.0 | 12213.0 |

## Device baseline

PhysicalDisk cooked values are 5 s averages, excluding `_total`. No suite disk probes run inside either baseline window. Other agents’ work and the IPC experiment still appear in these shared-machine counters. E: is baseline-only: the first-log plan weighs C: and D:.

Baseline 1: 2026-10-09T10:46:11.9883217Z through 2026-10-09T10:56:16.6113245Z, 121 samples, processes 670–921. Lineup snapshots: `baseline-1.jsonl.tenants.jsonl`.

Baseline 2: 2026-10-09T11:03:53.1754379Z through 2026-10-09T11:14:00.2760773Z, 121 samples, processes 815–960. Lineup snapshots: `baseline-2.jsonl.tenants.jsonl`.

| Run | Device | Counter | Mean | Min | Max |
| --- | --- | --- | --- | --- | --- |
| 1 | 0 e: | % idle time | 61.1 | 3.4 | 99.9 |
| 1 | 0 e: | disk transfers/sec | 497.8 | 1.0 | 1374.8 |
| 1 | 0 e: | disk write bytes/sec | 16890351.6 | 13851.8 | 121372723.0 |
| 1 | 1 c: | % idle time | 97.2 | 44.7 | 99.7 |
| 1 | 1 c: | disk transfers/sec | 300.6 | 36.1 | 4089.8 |
| 1 | 1 c: | disk write bytes/sec | 5427186.9 | 737206.2 | 19137152.7 |
| 1 | 2 d: | % idle time | 91.1 | 27.2 | 99.7 |
| 1 | 2 d: | disk transfers/sec | 866.4 | 5.4 | 12293.7 |
| 1 | 2 d: | disk write bytes/sec | 3834481.2 | 35983.9 | 65645252.6 |
| 2 | 0 e: | % idle time | 56.5 | 0.4 | 99.8 |
| 2 | 0 e: | disk transfers/sec | 1043.7 | 1.8 | 16711.1 |
| 2 | 0 e: | disk write bytes/sec | 10686168.4 | 48716.2 | 64921075.4 |
| 2 | 1 c: | % idle time | 69.5 | 0 | 99.7 |
| 2 | 1 c: | disk transfers/sec | 3785.4 | 23.1 | 22470.0 |
| 2 | 1 c: | disk write bytes/sec | 18073899.3 | 779695.2 | 119794610.9 |
| 2 | 2 d: | % idle time | 87.9 | 39.8 | 99.0 |
| 2 | 2 d: | disk transfers/sec | 623.2 | 40.6 | 4404.8 |
| 2 | 2 d: | disk write bytes/sec | 6175013.9 | 516989.7 | 52106676.9 |

## Application load

IPC wire bytes below include untimed warmup. Python pipe timing includes a bytearray slice copy on each write; TCP has no matching copy. Language order is reversed at eight clients, and remains Rust-first at one and 32 clients. These are measurement limitations, and the recorded host noise prevents treating a small difference as a settled transport advantage.

```json
{
  "std_flush_suite": {
    "D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-1.jsonl": {
      "application_bytes": 155922792,
      "explicit_flush_calls": 1731
    },
    "D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-C-2.jsonl": {
      "application_bytes": 155922792,
      "explicit_flush_calls": 1731
    },
    "D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-2.jsonl": {
      "application_bytes": 155922792,
      "explicit_flush_calls": 1731
    },
    "D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl": {
      "application_bytes": 155922792,
      "explicit_flush_calls": 1731
    },
    "D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e1/act-work/measure/flush-D-1.jsonl": {
      "application_bytes": 155922792,
      "explicit_flush_calls": 1731
    }
  },
  "diskspd_total_write_bytes_by_device": {
    "C": 21159636992,
    "D": 18823835648
  },
  "definitive_ipc_wire_bytes": 18115290432,
  "ipc_disk_payload_writes": 0
}
```

## Limits and source checks

Write-cache policy is decoded in `policy.py` / `write-cache-policy.jsonl`: both T700 disks report write-back enabled, write-through supported, FlushCacheSupported=true, UserDefinedPowerProtection=false and NVCacheEnabled=false. Microsoft’s [API documentation](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddstor/ns-ntddstor-_storage_write_cache_property) defines the fields. Its [classpnp source](https://github.com/microsoft/Windows-driver-samples/blob/main/storage/class/classpnp/src/utils.c#L1260) maps UserDefinedPowerProtection to DEV_POWER_PROTECTED. Registry device keys exist but Device Parameters/Disk and the CacheIsPowerProtected/UserWriteCacheSetting overrides were absent. The policy conclusion that Windows does not suppress flushes is an inference from the API and driver source, not a hardware command trace.

The manufacturer [T700 specification flyer](https://assets.micron.com/adobe/assets/urn%3Aaaid%3Aaem%3A3fd11ecf-6ec7-4271-90fb-78cbdcb55b5d/renditions/original/as/crucial-t700-b2c-product-flyer-en.pdf) was not successfully opened: both primary PDF addresses and the product page returned Request Rejected. No in-flight capacitor-backed power-loss guarantee was located; see e2 CRUCIAL-SOURCE.md. PLP is therefore unlocated from this primary source; absence of a published promise is not proof of absence. No power-loss experiment or actual NVMe command trace was run. Larger latency for FlushFileBuffers than for unflushed writes and the Windows policy support host-issued flushing; they do not prove the device honours flushes across a power cut.

Survey read: D:/dev/handoff/fast-local-store-survey.md SHA256 b128d2588930d54819fceae4047299e4591535763f7048d89c4cf31a9fcfd10f. Design record read SHA256 074d22267522e57ad4ed32e1d95aef64833358a41fc9908e1bf958d99d2ffcb6; this is the current e2 read; e1 review hashes remain in review-return.json. The survey’s 20–60 µs Python p50 and 100–200 µs p99 are workload-dependent estimates: compare them to the 1-client rows and separately to 8/32-process rows, not to one universal latency. Its 0.3–3 ms NVMe durable-write estimate describes some D: medians, but the loaded C: repeat has medians and tails far above it. Neither that estimate nor the two-run empirical range is a latency guarantee; this study measures API completion, not power-loss durability.

Observe reviews and each response are located in the raw folder’s review-return.json, review-followup-return.json and review-responses.md. No code dependencies were fetched. Build/test output stays in ignored target directories. Temporary C:/Users/xteam/AppData/Local/Temp/membus-F-E6-2-probe.bin and the raw-folder probe-D.bin are removed through cognition.remove; cleanup receipts are kept in the e2 repeat folder.

## Build identity, repeat contexts and survey coverage

D: tables pair e1 flush-D-2 with e2 flush-D-repeat, both clean 599f33d at different UTC times. e1 flush-D-1 used an older binary and is excluded from this range; historical numbers remain in its raw file and earlier observations. C: retains e1 run1/run2, which are mixed builds and therefore not two runs of one build; every C: number is marked mixed-build evidence. C: was not remeasured. IPC tables pair e1 ipc-final with e2 ipc-repeat (500 timed echoes/client), both 599f33d, matching executable and client hashes in the rows. ipc-reviewed-1 (2000/client) is an older build and is excluded; ipc-reviewed-2 never ran. Two-run min/max is not a confidence interval. The repeated Get-FileHash failure is in flush-repeat-exit.txt; unchanged fallback records clean commit, and Python still records its executable/client hashes. No measurement code changed.

Survey 4.4: server memory get/put <1-2 us and >1M ops/s were not measured (echo server has no store). Rust named-pipe 5-20 us compares to single-client rows, varying by payload; 8/32-client tails cannot confirm a single-client estimate. Python 20-60 us p50 / 100-200 us p99 is workload dependent: many small named-pipe one-client rows are faster than those estimates; 50 KiB and multi-process rows add different costs. Per-client 15-40K ops/s is compatible with many one-client rows, not a universal bound. Consumer flush 0.3-3 ms fits D: medians but D: sparse tails reach about 13-25 ms and loaded C: medians of 13-25 ms exceed it. Enterprise PLP 50-150 us was not measured. Group commit raises records/s as shown; QD1 probes do not establish GB/s WAL capacity or a server maximum.

Each DiskSpd table row maps by drive/workload/run to raw diskspd-<workload>-<drive>-<run>.xml, fields TimeSpan/Thread/Target WriteBytes/BytesCount, WriteCount, TimeSpan/TestTimeSeconds and TimeSpan/Latency/Bucket. Baseline rows map to baseline-<run>.jsonl, matching instance and counter in each samples element. Baseline ranges are over all cooked samples, not paired latency runs. Load JSON totals sum every bytes_written/flushes in the corresponding JSONL file, including untimed control drains. Historical e1 std writes are on both devices; repeat writes are D: only.

Admission and cleanup: e2 DISKSPD-ADMISSION.md, CRUCIAL-SOURCE.md and REPORT.md. Raw data remains outside git.

Repeat flush-D-repeat.jsonl: 2026-10-09T12:35:00.0389954Z through 2026-10-09T12:35:03.8175750Z; reserved cpu=2,mem=1,disk:d; processes 705-711; 3 before/during/after PhysicalDisk samples; full co-tenants in D:\projects\oulipoly\trunk\research\87_operations_representation\claude6-eco\runs\e2\act-work\F-E6-2/flush-D-repeat.jsonl.context.jsonl. Git 599f33d1b17387777b1cfe6fee56178d02122b17; dirty [].

Repeat ipc-repeat.jsonl: 2026-10-09T12:35:30.5674255Z through 2026-10-09T12:35:40.6069539Z; reserved cpu=4,mem=1; processes 711-754; 6 before/during/after PhysicalDisk samples; full co-tenants in D:\projects\oulipoly\trunk\research\87_operations_representation\claude6-eco\runs\e2\act-work\F-E6-2/ipc-repeat.jsonl.context.jsonl. Git 599f33d1b17387777b1cfe6fee56178d02122b17; dirty [].
