$ErrorActionPreference = "Stop"
$project = Split-Path -Parent $PSScriptRoot
$javaRoot = (Get-ChildItem -LiteralPath 'E:\nyc311_runtime\jdk-17' -Filter javac.exe -Recurse | Select-Object -First 1).Directory.Parent.FullName
$env:JAVA_HOME = $javaRoot
$env:HADOOP_HOME = 'E:\nyc311_runtime\hadoop'
$env:PATH = (Join-Path $javaRoot 'bin') + ';' + (Join-Path $env:HADOOP_HOME 'bin') + ';' + $env:PATH
$python = Join-Path $project ".venv\Scripts\python.exe"
& $python (Join-Path $project "src\governance\run_governance.py") --project-root $project
exit $LASTEXITCODE
