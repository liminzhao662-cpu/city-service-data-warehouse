$ErrorActionPreference = 'Stop'
$project = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$service = Join-Path $project 'java-governance-service'
$javaRoot = (Get-ChildItem -LiteralPath 'E:\nyc311_runtime\jdk-17' -Filter javac.exe -Recurse | Select-Object -First 1).Directory.Parent.FullName
$mavenHome = 'E:\nyc311_runtime\apache-maven-3.9.16'
$config = @{}
Get-Content -LiteralPath (Join-Path $project '.env') | ForEach-Object {
    if ($_ -and -not $_.TrimStart().StartsWith('#') -and $_.Contains('=')) {
        $parts = $_.Split('=', 2); $config[$parts[0].Trim()] = $parts[1].Trim()
    }
}
$env:JAVA_HOME = $javaRoot
$env:PATH = (Join-Path $javaRoot 'bin') + ';' + (Join-Path $mavenHome 'bin') + ';' + $env:PATH
$env:GOVERNANCE_DB_URL = "jdbc:mysql://$($config['MYSQL_HOST']):$($config['MYSQL_PORT'])/$($config['MYSQL_DATABASE'])?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"
$env:GOVERNANCE_DB_USER = $config['MYSQL_USER']
$env:GOVERNANCE_DB_PASSWORD = $config['MYSQL_PASSWORD']
& (Join-Path $mavenHome 'bin\mvn.cmd') -f (Join-Path $service 'pom.xml') spring-boot:run
