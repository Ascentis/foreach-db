[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Install-PackageProvider -Name NuGet -MinimumVersion 2.8.5.201 -Force
Install-Module –Name PowerShellGet –Force -AllowClobber
Update-Module
Set-ExecutionPolicy Unrestricted
Install-Module ForEach-DB -Force
Import-Module foreach-db -DisableNameChecking