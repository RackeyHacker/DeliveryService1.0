param([switch]$IncludeFailureDemo)
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$base = 'http://localhost:8080'
$trace = 'demo-' + [guid]::NewGuid().ToString('N')
$headers = @{ 'X-Request-ID' = $trace }
$body = @{ productId = 1; quantity = 1; address = 'Demo delivery address' } | ConvertTo-Json
function Request($method, $path, $payload = $null) {
    $args = @{ Method=$method; Uri="$base$path"; Headers=$headers; TimeoutSec=10 }
    if ($null -ne $payload) { $args.ContentType='application/json'; $args.Body=$payload }
    Invoke-RestMethod @args
}
function Wait-Status($id, $expected) {
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    do {
        $current = Request 'Get' "/api/orders/$id"
        if ($current.status -eq $expected) { Write-Host "Order $id -> $expected"; return }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for $expected; current status: $($current.status)"
}
function Compose([string[]]$ComposeArgs) {
    & docker compose @ComposeArgs
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed: $ComposeArgs" }
}
Push-Location (Join-Path $PSScriptRoot '..')
try {
    $health = Request 'Get' '/api/health'
    foreach ($service in @('gateway','orders','deliveries','suppliers')) {
        if ($health.$service.status -ne 'UP') { throw "$service unavailable" }
    }
    $order = Request 'Post' '/api/orders' $body
    Write-Host "Created order $($order.id); X-Request-ID=$trace"
    Wait-Status $order.id 'DELIVERY_CREATED'
    $delivery = Request 'Get' "/api/deliveries/by-order/$($order.id)"
    Request 'Patch' "/api/deliveries/by-order/$($order.id)/status" '{"status":"IN_TRANSIT"}' | Out-Null
    Wait-Status $order.id 'IN_TRANSIT'
    Request 'Patch' "/api/deliveries/by-order/$($order.id)/status" '{"status":"DELIVERED"}' | Out-Null
    Wait-Status $order.id 'DELIVERED'
    # Same status is idempotent.
    $again = Request 'Patch' "/api/deliveries/by-order/$($order.id)/status" '{"status":"DELIVERED"}'
    if ($again.version -ne 3 -or $again.id -ne $delivery.id) { throw 'Duplicate status changed delivery identity/version' }
    # Curl collects the replayed SSE history; timeout is expected because SSE stays open.
    $sse = (& curl.exe -sS -N --max-time 3 "$base/api/orders/$($order.id)/events" 2>$null) -join "`n"
    foreach ($status in @('CREATED','DELIVERY_CREATED','IN_TRANSIT','DELIVERED')) {
        if ($sse -notmatch ('"status":"' + $status + '"')) { throw "SSE missing $status" }
    }
    Write-Host 'SSE history verified'
    if ($IncludeFailureDemo) {
        try {
            Compose -ComposeArgs @('stop','supplier-service')
            $before = @(Request 'Get' '/api/orders').Count
            for ($i=0; $i -lt 4; $i++) {
                $watch = [Diagnostics.Stopwatch]::StartNew()
                try { Request 'Post' '/api/orders' $body | Out-Null; throw 'Expected 503' }
                catch {
                    if ($null -eq $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -ne 503) { throw }
                    Write-Host "Supplier OFF -> 503 in $($watch.ElapsedMilliseconds) ms"
                }
                if ($watch.Elapsed.TotalSeconds -gt 8) { throw 'Failure took too long' }
            }
            $state = Request 'Get' '/api/orders/resilience'
            if ($state.supplierCircuitBreaker -ne 'OPEN') { throw 'Circuit breaker did not open' }
            if (@(Request 'Get' '/api/orders').Count -ne $before) { throw 'Failed POST created an order' }
        }
        finally { Compose -ComposeArgs @('start','supplier-service') }
        $deadline = [DateTime]::UtcNow.AddSeconds(45)
        $recovered = $null
        do {
            Start-Sleep -Seconds 2
            try { $recovered = Request 'Post' '/api/orders' $body }
            catch {
                if ($null -eq $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -ne 503) { throw }
            }
        } while ($null -eq $recovered -and [DateTime]::UtcNow -lt $deadline)
        if ($null -eq $recovered) { throw 'Supplier did not recover' }
        Wait-Status $recovered.id 'DELIVERY_CREATED'
        Write-Host 'Recovery verified'
    }
    Write-Host 'PASS: end-to-end scenario'
    Write-Host "Trace: docker compose logs | Select-String '$trace'"
}
finally { Pop-Location }
