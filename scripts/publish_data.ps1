param(
  [string]$Source = "C:\Users\av_ch\Downloads\8 Factors\8 factors - 8 factors.csv",
  [switch]$Push
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$syncScript = Join-Path $repo "scripts\sync_market_data.py"
$outCsv = Join-Path $repo "data\8-factors.csv"

python $syncScript --source $Source --output $outCsv

git -C $repo add -- "data/8-factors.csv"
git -C $repo diff --cached --quiet
if ($LASTEXITCODE -eq 0) {
  Write-Output "No data changes to commit."
  exit 0
}

$ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$msg = "data: sync 8-factors $ts"
git -C $repo commit -m $msg -- "data/8-factors.csv"

if ($Push) {
  git -C $repo push origin main
  Write-Output "Pushed to origin/main."
} else {
  Write-Output "Committed locally. Re-run with -Push to publish."
}
