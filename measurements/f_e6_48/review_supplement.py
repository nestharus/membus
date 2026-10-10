"""Recompute scoped review counterevidence from saved endpoint/rate artifacts."""
import collections, datetime, json
from window import OUT, save

def read(name):return json.loads((OUT/name).read_text())
def unix(value):return datetime.datetime.fromisoformat(value).timestamp()
def flat(endpoint):return [r for log in endpoint['logs'].values() for r in log['records']]

def main():
    a,b,p,rates=map(read,['resident-start.json','resident-end.json','projection.json','ledger-rates.json'])
    change=unix('2026-10-10T01:35:00+00:00')
    prior={(r['namespace'],r['key']) for r in flat(a)}
    tail=[r for name,log in b['logs'].items() for r in log['records'] if r['offset']>=a['logs'][name]['complete_bytes']]
    # A key maps to exactly one stripe, whose offset orders its writes.
    counts=collections.Counter()
    for row in tail:
        key=(row['namespace'],row['key'])
        if key not in prior and row['at_ms']/1000>=change:counts[row['namespace']]+=1
        prior.add(key)
    post_hours=p['publisher_change_eras'][1]['hours']
    warnings=[]
    for basis,hours,new_global,new_slots in [('full-window',p['hours'],b['health']['quota']['keys']-a['health']['quota']['keys'],b['health']['slots']['namespace_keys']-a['health']['slots']['namespace_keys']),
                                            ('post-change',post_hours,sum(counts.values()),counts['slots'])]:
        for name,start,rate,cap in [('slots-namespace',b['health']['slots']['namespace_keys'],new_slots/hours,b['health']['slots']['namespace_quota']),
                                  ('global-index',b['health']['quota']['keys'],new_global/hours,b['health']['quota']['index_cap'])]:
            warnings.append(dict(basis=basis,scope=name,current_keys=start,new_keys_per_hour=rate,
                warning_keys=.7*cap,warning_days=(.7*cap-start)/rate/24,
                cap_keys=cap,cap_days=(cap-start)/rate/24))
    token=[row for row in tail if row['namespace']=='quota' and row['family']=='tokens/' and row['at_ms']/1000>=change]
    selected=[row for row in rates['loops'] if unix(row['most_recent_at'])>=unix(rates['measured_at'])-3600]
    idle_corrections=[row for row in rates['loops'] if 'corrections' in row['instance']]
    active_added=sum(row['publisher_upper_bytes_per_hour'] for row in selected)
    base=p['publisher_change_eras'][1]['bytes_per_hour']
    result=dict(token_post_records=len(token),token_post_records_per_hour=len(token)/post_hours,
        token_post_bytes=sum(row['bytes'] for row in token),owner_stated_token_max_per_hour=42,
        needed_total_bytes_per_hour_for_30days={str(target):(target-p['complete_log_bytes_end'])/720 for target in (6*1024**3,8*1024**3)},
        key_capacity=warnings,post_new_keys=dict(counts),
        publisher_active_last_hour_subset=dict(ledgers=len(selected),rows_last_24h=sum(row['rows_last_24h'] for row in selected),
            bytes_per_hour=active_added,combined_share=active_added/(base+active_added)),
        publisher_corrections_subset=dict(ledgers=len(idle_corrections),rows_last_24h=sum(row['rows_last_24h'] for row in idle_corrections),
            bytes_per_hour=sum(row['publisher_upper_bytes_per_hour'] for row in idle_corrections)),
        disk_start_offset_seconds=unix(p['physical_disk']['first'])-unix(p['window']['started_at']),
        disk_end_offset_seconds=unix(p['physical_disk']['last'])-unix(p['window']['completed_at']),
        ledger_read_anchor=rates['measured_at'],
        limits='Key dates assume constant new-key rates, no reclamation/cap changes. Publisher last-hour subset is an activity proxy at the ledger-read anchor, not a selected deployment; this frontier itself contributes ledger rows. Token-rate difference is counterevidence to the stated42/hour maximum. The owner rule also permits10% moves without a stated frequency cap; that could explain higher writes but actual cause is unconfirmed. Actual File::write and sync-attempt counters were not captured; logical persisted records and health flush calls were. No6/8GiB bootability or earliest-failure prediction follows from byte projections.')
    save('review-supplement.json',result)
    text=['# F-E6-48 review counterevidence and limits','',
        f'Observed post-change quota tokens: {len(token)} records/{post_hours:.6f}h = {len(token)/post_hours:.3f}/h; {result["token_post_bytes"]:,} bytes. Lane88 seq1155 states at most42/h. The located writes exceed that stated maximum; cause is unknown. The projection uses actual records, not that owner bound.',
        '',f'30day byte thresholds:6GiB requires {result["needed_total_bytes_per_hour_for_30days"][str(6*1024**3)]:,.3f}B/h;8GiB requires {result["needed_total_bytes_per_hour_for_30days"][str(8*1024**3)]:,.3f}B/h. All four located base/publisher scenarios stay beyond30days for both targets; METRIC-RETENTION.md separately models an unknown extra metric rate.',
        '', '| Basis | Capacity | New keys/hour | 70% warning days | Limit days |','|---|---|---:|---:|---:|']
    for row in warnings:text.append(f'| {row["basis"]} | {row["scope"]} | {row["new_keys_per_hour"]:.3f} | {row["warning_days"]:.3f} | {row["cap_days"]:.3f} |')
    text.extend(['', 'Under measured-only post-change rates, key warnings/caps precede stripe-byte dates. With the all26 proposed publisher, slots70%warning217.706days precedes6GiB226.300days, while8GiB301.759days precedes slotsquota311.049days. Other memory/expiry/index dynamics remain unqualified.', '',f'Trailing24h deployment sensitivity anchored at ledger read {rates["measured_at"]}: {len(idle_corrections)} corrections ledgers contribute {result["publisher_corrections_subset"]["rows_last_24h"]:,} rows and {result["publisher_corrections_subset"]["bytes_per_hour"]:,.3f}B/h. The {len(selected)} ledgers with a row during the preceding hour contribute {result["publisher_active_last_hour_subset"]["rows_last_24h"]:,} trailing24h rows, {active_added:,.3f}B/h and {100*active_added/(base+active_added):.2f}% of that subset’s combined rate. This frontier’s own loop contributes some rows. The all26-ledger commissioning scenario remains the published upper deployment scenario; no adoption decision is made.',
        '',f'Physical interval offsets: +{result["disk_start_offset_seconds"]:.6f}s at start, +{result["disk_end_offset_seconds"]:.6f}s at end relative to WINDOW markers. Queue statistics are point samples. Host busy_pct is retained raw with unverified scaling and is not interpreted as a percentage of device utilization.',
        '', 'Replay: synthetic records use at_ms=1 and therefore exclude retry fingerprints. The268.4MB shape retains200000 distinct live keys; scaling it unchanged to6/8GiB would violate default namespace/index/value caps. The resident overwrite shape differs. Two-point linear readiness estimates therefore describe conditional arithmetic only, with uncontrolled warm cache and no server-internal replay timer.',
        '', 'Owner retention answer: latest-version-only state with no expiry is a key-version policy, distinct from whole-chunk time expiry with no rewriting. A chunk containing a rarely rewritten latest value cannot simply retire; mechanism and quota-slot reclamation remain unresolved for Orient/Decide. No policy implementation occurs in this frontier.', '',result['limits']])
    (OUT/'REVIEW-SUPPLEMENT.md').write_text('\n'.join(text)+'\n')
    print(json.dumps(result))

if __name__=='__main__':main()
