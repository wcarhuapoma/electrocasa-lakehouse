param(
    [Parameter(Mandatory)][string]$Perfil,
    [Parameter(Mandatory)][string]$BaseTracking,
    [Parameter(Mandatory)][string]$CorreoAlertas,
    [string]$ConexionTracking = 'electrocasa_sqlserver',
    [ValidateRange(1, 3)][int]$DesdeLote = 1
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$project = Split-Path -Parent $PSScriptRoot
$landing = 'dbfs:/Volumes/electrocasa_dev/default/vol_landing'
$variables = "tracking_database=$BaseTracking,alert_email=$CorreoAlertas,tracking_connection=$ConexionTracking"

Push-Location -LiteralPath $project
try {
    databricks bundle validate --strict -t dev -p $Perfil --var $variables
    databricks bundle validate --strict -t prod -p $Perfil --var $variables
    databricks bundle deploy -t dev -p $Perfil --var "$variables,schedule_status=UNPAUSED"

    try {
        foreach ($batch in $DesdeLote..3) {
            foreach ($source in @('ventas', 'empleados', 'devoluciones', 'resenas')) {
                $pattern = "*_lote_$($batch.ToString('00'))_*"
                Get-ChildItem -LiteralPath "datos/$source" -File -Filter $pattern | ForEach-Object {
                    databricks fs cp $_.FullName "$landing/$source/$($_.Name)" --overwrite -p $Perfil
                }
            }
            if ($batch -eq 1) {
                databricks fs cp 'datos/sucursales.csv' "$landing/sucursales/sucursales.csv" --overwrite -p $Perfil
            }
            if ($batch -le 2) {
                $snapshot = @('catalogo_productos_2026-03-01.json', 'catalogo_productos_2026-08-01.json')[$batch - 1]
                databricks fs cp "datos/catalogo/$snapshot" "$landing/catalogo/$snapshot" --overwrite -p $Perfil
            }
            databricks bundle run electrocasa_job -t dev -p $Perfil --var "$variables,schedule_status=UNPAUSED"
        }

        databricks bundle run electrocasa_job -t dev -p $Perfil --var "$variables,schedule_status=UNPAUSED"
    }
    finally {
        databricks bundle deploy -t dev -p $Perfil --var "$variables,schedule_status=PAUSED"
    }
}
finally {
    Pop-Location
}
