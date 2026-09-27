$ErrorActionPreference = "Stop"
$JarDir = Join-Path $PSScriptRoot "..\spark\jars"
New-Item -ItemType Directory -Force -Path $JarDir | Out-Null
$OracleVersion = "23.26.3.0.0"
$PostgresVersion = "42.7.13"
$OracleUrl = "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc17/$OracleVersion/ojdbc17-$OracleVersion.jar"
$PostgresUrl = "https://repo1.maven.org/maven2/org/postgresql/postgresql/$PostgresVersion/postgresql-$PostgresVersion.jar"
Invoke-WebRequest -Uri $OracleUrl -OutFile (Join-Path $JarDir "ojdbc17-$OracleVersion.jar")
Invoke-WebRequest -Uri $PostgresUrl -OutFile (Join-Path $JarDir "postgresql-$PostgresVersion.jar")
Get-ChildItem $JarDir -Filter "*.jar" | Select-Object Name,Length
