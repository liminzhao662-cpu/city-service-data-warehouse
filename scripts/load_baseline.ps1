$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
& (Join-Path $ProjectRoot '.venv\Scripts\python.exe') (Join-Path $ProjectRoot 'src\load\load_mysql.py')
