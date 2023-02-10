$pathExists = Test-Path -Path ${env:ProgramFiles}\WindowsPowerShell\Modules\ForEach-DB
if (!$pathExists) {
    New-Item ${env:ProgramFiles}\WindowsPowerShell\Modules\ForEach-DB -itemtype directory > $null    
}
Copy-Item .\ForEach-DB.psm1 ${env:ProgramFiles}\WindowsPowerShell\Modules\ForEach-DB
Copy-Item .\ForEach-DB.psd1 ${env:ProgramFiles}\WindowsPowerShell\Modules\ForEach-DB

$pathExists = Test-Path -Path ${env:ProgramFiles}\WindowsPowerShell\Modules\PSWriteColor
if ($pathExists) {
	Write-Output "	PSWriteColor PowerShell package already installed - Skipping expand archive operation"
} else {
	Expand-Archive .\deps\PSWriteColor.zip ${env:ProgramFiles}\WindowsPowerShell\Modules\ -Force
}

$pathExists = Test-Path -Path ${env:ProgramFiles}\WindowsPowerShell\Modules\SqlServer
if ($pathExists) {
	Write-Output "	SqlServer PowerShell package already installed - Skipping expand archive operation"
} else {
	Expand-Archive .\deps\SqlServer.zip ${env:ProgramFiles}\WindowsPowerShell\Modules\ -Force
}

$ErrorActionPreference = 'SilentlyContinue'
try {
    remove-module -Name foreach-db
} catch {
    if (!$_.Exception.Message.Contains("remove-module : No modules were removed")) {
        throw
    }
}
$ErrorActionPreference = 'Continue'

import-module PSWriteColor -DisableNameChecking
import-module SqlServer -DisableNameChecking
import-module foreach-db -DisableNameChecking
