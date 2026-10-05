$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$MySqlHome = 'E:\nyc311_runtime\mysql-8.4.11-winx64'
$Config = 'E:\nyc311_runtime\my.ini'
$Runtime = 'E:\nyc311_runtime'
$PidFile = Join-Path $Runtime 'mysql.pid'
$Admin = Join-Path $MySqlHome 'bin\mysqladmin.exe'
$Server = Join-Path $MySqlHome 'bin\mysqld.exe'

if (Test-Path -LiteralPath $PidFile) {
    $ExistingPid = Get-Content -LiteralPath $PidFile -ErrorAction SilentlyContinue
    if ($ExistingPid -and (Get-Process -Id $ExistingPid -ErrorAction SilentlyContinue)) {
        Write-Output "MySQL already running with PID $ExistingPid"
        exit 0
    }
}

New-Item -ItemType Directory -Path (Join-Path $Runtime 'log') -Force | Out-Null
$Process = Start-Process -FilePath $Server -ArgumentList "--defaults-file=$Config" -WindowStyle Hidden -PassThru
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Milliseconds 500
    & $Admin --defaults-file=$Config ping --silent 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Output "MySQL ready with PID $($Process.Id) on 127.0.0.1:3307"
        exit 0
    }
}
throw 'MySQL did not become ready; inspect runtime/mysql/log/mysql-error.log'
