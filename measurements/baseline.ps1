param([string]$Output, [int]$Seconds=600)
$ErrorActionPreference='Stop'
$start=Get-Date
@{utc=$start.ToUniversalTime().ToString('o');processes=(Get-Process).Count;lineup=(& lineup.cmd status | Out-String);duration_seconds=$Seconds} | ConvertTo-Json -Compress | Set-Content ($Output+'.context.json')
$paths=@('\PhysicalDisk(*)\% Idle Time','\PhysicalDisk(*)\Disk Transfers/sec','\PhysicalDisk(*)\Disk Write Bytes/sec')
$i=0
Get-Counter -Counter $paths -SampleInterval 5 -MaxSamples ([math]::Ceiling($Seconds/5)+1) | ForEach-Object {
  $set=$_
  $rows=$set.CounterSamples | Where-Object {$_.InstanceName -ne '_total'} | ForEach-Object { @{path=$_.Path;instance=$_.InstanceName;value=$_.CookedValue;status=$_.Status} }
  @{utc=$set.Timestamp.ToUniversalTime().ToString('o');processes=(Get-Process).Count;samples=@($rows)} | ConvertTo-Json -Depth 5 -Compress | Add-Content $Output
  if (($i % 12) -eq 0) { @{utc=(Get-Date).ToUniversalTime().ToString('o');lineup=(& lineup.cmd status | Out-String)} | ConvertTo-Json -Compress | Add-Content ($Output+'.tenants.jsonl') }
  $i++
}
@{utc=(Get-Date).ToUniversalTime().ToString('o');elapsed_seconds=((Get-Date)-$start).TotalSeconds;processes=(Get-Process).Count;lineup=(& lineup.cmd status | Out-String)} | ConvertTo-Json -Compress | Set-Content ($Output+'.end.json')
