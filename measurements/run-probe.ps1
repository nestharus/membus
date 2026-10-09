param([string]$Kind,[string]$Output,[string]$ProbePath,[int]$Count=2000,[string]$DiskSpdExe)
$ErrorActionPreference='Stop'
$base=$PSScriptRoot
$exe=$base+'/target/release/membus-measure.exe'
@{kind=$Kind;output=$Output;probe_path=$ProbePath;timed_requests_per_client=$Count;exe_sha256=(Get-FileHash -LiteralPath $exe -Algorithm SHA256).Hash.ToLowerInvariant();client_sha256=(Get-FileHash -LiteralPath ($base+'/client.py') -Algorithm SHA256).Hash.ToLowerInvariant();wrapper_sha256=(Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash.ToLowerInvariant();git_commit=(& git -C $base rev-parse HEAD);git_dirty=@(& git -C $base status --porcelain);python_version=(& python --version);rust_version=(& rustc --version);utc=(Get-Date).ToUniversalTime().ToString('o')} | ConvertTo-Json -Depth 3 -Compress | Set-Content ($Output+'.provenance.json')
function Context {
  @{utc=(Get-Date).ToUniversalTime().ToString('o');processes=(Get-Process).Count;lineup=(& lineup.cmd status | Out-String);physical=@((Get-Counter -Counter '\PhysicalDisk(*)\% Idle Time','\PhysicalDisk(*)\Disk Transfers/sec','\PhysicalDisk(*)\Disk Write Bytes/sec' -SampleInterval 1 -MaxSamples 1).CounterSamples | ForEach-Object {@{instance=$_.InstanceName;path=$_.Path;value=$_.CookedValue;status=$_.Status}})} | ConvertTo-Json -Depth 5 -Compress | Add-Content ($Output+'.context.jsonl')
}
Context
if ($Kind -eq 'ipc') {
  $proc=Start-Process python -WindowStyle Hidden -PassThru -ArgumentList @($base+'/client.py','--exe',$exe,'--output',$Output,'--count',"$Count") -RedirectStandardOutput ($Output+'.stdout.txt') -RedirectStandardError ($Output+'.stderr.txt')
} elseif ($Kind -eq 'flush') {
  $proc=Start-Process $exe -WindowStyle Hidden -PassThru -ArgumentList @('flush',$ProbePath) -RedirectStandardOutput $Output -RedirectStandardError ($Output+'.stderr.txt')
} else {
  if (-not $DiskSpdExe) { throw 'Pass -DiskSpdExe for a trustchecked binary with its published hash verified.' }
  $diskspd=$DiskSpdExe
  $flags=@('-c32M','-d15','-W0','-C0','-t1','-o1','-w100','-Sh','-L','-Rxml')
  if ($Kind -eq 'diskspd-small') { $flags+=@('-b4K','-r') } else { $flags+=@('-b64K','-s') }
  $flags+=$ProbePath
  @{kind=$Kind;executable=$diskspd;arguments=$flags;explicit_FlushFileBuffers_calls=0;hardware_flush_count='not exposed by diskspd; write-through enabled'} | ConvertTo-Json -Depth 3 | Set-Content ($Output+'.command.json')
  $proc=Start-Process $diskspd -WindowStyle Hidden -PassThru -ArgumentList $flags -RedirectStandardOutput $Output -RedirectStandardError ($Output+'.stderr.txt')
}
$handle=$proc.Handle
$deadline=(Get-Date).AddMinutes(5)
while (-not $proc.HasExited) {
  if ((Get-Date) -gt $deadline) { & taskkill /PID $proc.Id /T /F | Out-Null; throw 'probe exceeded five-minute deadline; its process tree was stopped' }
  Context; $proc.WaitForExit(1000) | Out-Null
}
$proc.WaitForExit()
Context
if ($proc.ExitCode -ne 0) { throw "probe exited $($proc.ExitCode)" }
