$webclient=New-Object System.Net.WebClient
$webclient.Proxy.Credentials = [System.Net.CredentialCache]::DefaultNetworkCredentials
Publish-Module -Name ForEach-DB -NuGetApiKey <key>
