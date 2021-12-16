$ErrorActionPreference = 'SilentlyContinue'
$pathExists = Test-Path -Path ${env:ProgramFiles}\WindowsPowerShell\Modules\ForEach-DB
if (!$pathExists) {
    New-Item ${env:ProgramFiles}\WindowsPowerShell\Modules\ForEach-DB -itemtype directory > $null    
}
Copy-Item .\ForEach-DB.psm1 ${env:ProgramFiles}\WindowsPowerShell\Modules\ForEach-DB
Copy-Item .\ForEach-DB.psd1 ${env:ProgramFiles}\WindowsPowerShell\Modules\ForEach-DB
try {
    remove-module -Name foreach-db
} catch {
    if (!$_.Exception.Message.Contains("remove-module : No modules were removed")) {
        throw
    }
}
import-module foreach-db -DisableNameChecking
