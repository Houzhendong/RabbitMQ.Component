<#
.SYNOPSIS
Runs the integration tests using existing RabbitMQ environment settings.
.DESCRIPTION
Requires RABBITMQ_PRIMARY_URI and RABBITMQ_SECONDARY_URI to be set in the
current process. Does not display, persist, or modify their values, provision
infrastructure, or perform remote management operations.
#>
[CmdletBinding()]
param (
    [ValidateNotNullOrEmpty()]
    [string] $Configuration = 'Release',

    [string] $Filter
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
# Preserve dotnet's exit code even if the caller enables native error handling.
$PSNativeCommandUseErrorActionPreference = $false

$missingVariables = @(
    foreach ($name in @('RABBITMQ_PRIMARY_URI', 'RABBITMQ_SECONDARY_URI')) {
        if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name, 'Process'))) {
            $name
        }
    }
)

if ($missingVariables.Count -gt 0) {
    Write-Error -Message (
        'Required environment variables are missing or empty: {0}. Set them in the current process before running integration tests.' -f
        ($missingVariables -join ', ')
    ) -ErrorAction Continue
    exit 1
}

$projectPath = [IO.Path]::GetFullPath(
    (Join-Path -Path $PSScriptRoot -ChildPath '../tests/RabbitMQ.Component.IntegrationTests/RabbitMQ.Component.IntegrationTests.csproj')
)
$testArguments = @('test', $projectPath, '--configuration', $Configuration)
if (-not [string]::IsNullOrWhiteSpace($Filter)) {
    $testArguments += @('--filter', $Filter)
}

& dotnet @testArguments
exit $LASTEXITCODE
