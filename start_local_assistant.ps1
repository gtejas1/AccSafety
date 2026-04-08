$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

$venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    throw "Virtual environment Python not found at $venvPython"
}

$ollamaExe = Get-Command ollama -ErrorAction SilentlyContinue
if (-not $ollamaExe) {
    throw "Ollama is not installed or not on PATH."
}

$ollamaHost = if ($env:OLLAMA_HOST) { $env:OLLAMA_HOST.TrimEnd("/") } else { "http://127.0.0.1:11434" }

$env:CHAT_PROVIDER = "ollama"
$env:CHAT_MODEL = if ($env:CHAT_MODEL) { $env:CHAT_MODEL } else { "llama3.2" }
$env:EMBEDDING_PROVIDER = "ollama"
$env:EMBEDDING_MODEL = if ($env:EMBEDDING_MODEL) { $env:EMBEDDING_MODEL } else { "nomic-embed-text" }
$env:OLLAMA_HOST = $ollamaHost
$env:CHAT_BASE_URL = "$ollamaHost/api/chat"
$env:EMBEDDING_BASE_URL = "$ollamaHost/api/embed"

Write-Host "Checking Ollama models..."
$ollamaModels = & ollama list

if ($ollamaModels -notmatch [regex]::Escape($env:CHAT_MODEL)) {
    Write-Host "Pulling chat model $($env:CHAT_MODEL)..."
    & ollama pull $env:CHAT_MODEL
}

if ($ollamaModels -notmatch [regex]::Escape($env:EMBEDDING_MODEL)) {
    Write-Host "Pulling embedding model $($env:EMBEDDING_MODEL)..."
    & ollama pull $env:EMBEDDING_MODEL
}

Write-Host "Starting AccSafety on http://127.0.0.1:5000/"
& $venvPython .\gateway.py
