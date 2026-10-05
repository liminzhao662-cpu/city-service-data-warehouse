$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$MySqlHome = 'E:\nyc311_runtime\mysql-8.4.11-winx64'
$Config = 'E:\nyc311_runtime\my.ini'
$EnvFile = Join-Path $ProjectRoot '.env'
if (-not (Test-Path -LiteralPath $EnvFile)) { throw '.env not found' }
$Values = @{}
Get-Content -LiteralPath $EnvFile | Where-Object { $_ -match '=' } | ForEach-Object {
    $Name, $Value = $_ -split '=', 2
    $Values[$Name] = $Value
}
$env:MYSQL_PWD = $Values['MYSQL_ROOT_PASSWORD']
try {
    & (Join-Path $MySqlHome 'bin\mysqladmin.exe') --defaults-file=$Config -uroot shutdown
} finally {
    Remove-Item Env:\MYSQL_PWD -ErrorAction SilentlyContinue
}
