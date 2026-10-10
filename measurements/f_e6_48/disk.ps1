$taskDir='D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e9/act-work/F-E6-48'
$ErrorActionPreference='Stop'
do {
  $stamp=[DateTimeOffset]::UtcNow.ToString('o')
  $raw=Get-CimInstance Win32_PerfRawData_PerfDisk_PhysicalDisk | Where-Object Name -eq '2 D:'
  $formatted=Get-CimInstance Win32_PerfFormattedData_PerfDisk_PhysicalDisk | Where-Object Name -eq '2 D:'
  [pscustomobject]@{at=$stamp;collector_pid=$PID;physical_device='PHYSICALDRIVE2';raw=($raw | Select-Object Name,DiskWriteBytesPersec,DiskWritesPersec,CurrentDiskQueueLength,Timestamp_PerfTime,Frequency_PerfTime,Timestamp_Sys100NS);formatted=($formatted | Select-Object DiskWriteBytesPersec,DiskWritesPersec,CurrentDiskQueueLength)} | ConvertTo-Json -Depth 4 -Compress | Add-Content -LiteralPath "$taskDir/disk-counters.jsonl" -Encoding utf8
  if ((Test-Path -LiteralPath "$taskDir/WINDOW-COMPLETE.json") -or (Test-Path -LiteralPath "$taskDir/WINDOW-FAILED.json")) {break}
  Start-Sleep -Seconds 10
} while ($true)
