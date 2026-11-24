param(
    [Parameter(Mandatory)][string]$Perfil,
    [Parameter(Mandatory)][string]$BaseTracking,
    [Parameter(Mandatory)][string]$CorreoAlertas,
    [string]$ConexionTracking = 'electrocasa_sqlserver',
    [ValidateSet('dev', 'prod')][string]$Target = 'dev',
    [ValidateRange(1, 3)][int]$DesdeLote = 1
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$project = Split-Path -Parent $PSScriptRoot
$landing = "dbfs:/Volumes/electrocasa_$Target/default/vol_landing"
$variables = "tracking_database=$BaseTracking,alert_email=$CorreoAlertas,tracking_connection=$ConexionTracking,schedule_status=PAUSED"
$evidence = Join-Path $project "evidencias/$Target"
$dates = @('2026-05-31', '2026-08-31', '2026-10-31')
$manifest = @{
    1 = @('catalogo/catalogo_productos_2026-03-01.json', 'resenas/resenas_lote_01_2026-07-31.json', 'sucursales.csv')
    2 = @('catalogo/catalogo_productos_2026-08-01.json', 'resenas/resenas_lote_02_2026-10-31.json')
    3 = @()
}
foreach ($batch in 1..3) {
    foreach ($source in @('ventas', 'empleados', 'devoluciones')) {
        $manifest[$batch] += "$source/${source}_lote_$($batch.ToString('00'))_$($dates[$batch - 1]).csv"
    }
}

function Invoke-ElectrocasaRun([string]$EvidenceName) {
    # Obtiene el identificador para persistir el resultado del notebook.
    $launch = databricks bundle run electrocasa_job -t $Target -p $Perfil --var $variables --no-wait 2>&1
    $launch | ForEach-Object { Write-Host $_ }
    if (($launch -join ' ') -notmatch '/runs/(\d+)') { throw 'No se pudo identificar la corrida iniciada.' }
    $id = $Matches[1]
    do {
        $run = databricks jobs get-run $id -p $Perfil -o json | ConvertFrom-Json
        if ($run.state.life_cycle_state -notin 'TERMINATED', 'SKIPPED', 'INTERNAL_ERROR') { Start-Sleep -Seconds 12 }
    } while ($run.state.life_cycle_state -notin 'TERMINATED', 'SKIPPED', 'INTERNAL_ERROR')
    if ($run.state.result_state -ne 'SUCCESS') { throw "La corrida $id terminó en $($run.state.result_state)." }
    $task = $run.tasks | Where-Object task_key -eq 'validar'
    $result = databricks jobs get-run-output $task.run_id -p $Perfil -o json | ConvertFrom-Json
    if (-not $result.notebook_output.result) { throw "La corrida $id no generó evidencia de validación." }
    $proof = $result.notebook_output.result | ConvertFrom-Json
    $proof | Add-Member -NotePropertyName run_url -NotePropertyValue $run.run_page_url
    $proof | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $evidence "$EvidenceName.json") -Encoding utf8
    Write-Host "$EvidenceName SUCCESS ($id)"
}

Push-Location -LiteralPath $project
try {
    # Valida todos los archivos antes de crear recursos o cargar un lote parcial.
    $missing = @($manifest.Values | ForEach-Object { $_ } |
        Where-Object { -not (Test-Path -LiteralPath "datos/$_" -PathType Leaf) })
    if ($missing.Count) { throw "Faltan archivos de origen: $($missing -join ', ')." }
    $null = New-Item -ItemType Directory -Path $evidence -Force
    databricks bundle validate --strict -t dev -p $Perfil --var $variables
    databricks bundle validate --strict -t prod -p $Perfil --var $variables
    databricks bundle deploy -t $Target -p $Perfil --var $variables

    foreach ($batch in $DesdeLote..3) {
        foreach ($file in $manifest[$batch]) {
            $destination = if ($file -eq 'sucursales.csv') { 'sucursales/sucursales.csv' } else { $file }
            $local = (Get-Item -LiteralPath "datos/$file").FullName
            databricks fs cp $local "$landing/$destination" --overwrite -p $Perfil
        }
        Invoke-ElectrocasaRun "lote-$($batch.ToString('00'))"
    }
    Invoke-ElectrocasaRun 'repeticion'
}
finally {
    Pop-Location
}
