$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$JavaRoot = (Get-ChildItem -LiteralPath 'E:\nyc311_runtime\jdk-17' -Filter javac.exe -Recurse | Select-Object -First 1).Directory.Parent.FullName
$env:JAVA_HOME = $JavaRoot
$env:HADOOP_HOME = 'E:\nyc311_runtime\hadoop'
$env:PATH = (Join-Path $JavaRoot 'bin') + ';' + (Join-Path $env:HADOOP_HOME 'bin') + ';' + $env:PATH
& (Join-Path $ProjectRoot '.venv\Scripts\python.exe') (Join-Path $ProjectRoot 'src\mining\train_closure_model.py')
