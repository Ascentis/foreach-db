function ForEach-DB {
	[CmdletBinding(PositionalBinding = $false)]
	param(
		[Parameter(Mandatory = $true, Position = 0, HelpMessage="Enter the MSSQL server name")][Alias("Srv", "S")][string]$server, 
		[Parameter(Position = 1)][Alias("Q")][string]$query = 'SELECT DB_NAME() DBNAME, GETDATE() DATE',
		[Parameter(Position = 2)][Alias("Out", "O")][string]$resultSetFileOutput,
		[Alias("Script", "SF")][string]$file,
		[Alias("Except", "Ignore", "I")][string]$dbExceptionFileInput,
		[Alias("Para", "P")][ValidateRange(1, 16)][int]$parallelLevel = 4,
		[Alias("Warn", "W")][string]$dbWarningsFileOutput,
		[Alias("Err", "E")][string]$dbErrorsFileOutput,
		[Alias("DetError", "D")][string]$detailedErrorLogFileOutput,
		[Alias("Format", "F")][string][ValidateSet('TAB', 'CSV', 'JSON', 'XLSX', 'PIPE', 'DELIMITED', 'HTML', 'XML')]$resultSetFormat,
		[Alias("OpEx", "Ex")][switch]$openExcel,
		[Alias("ReRun", "R")][switch]$reRunDBFromErrorsFile,
		[Alias("Vars", "BindVars", "V")][string[]]$bindVariables,
		[Alias("Duplex", "Con", "Dup")][switch]$consoleDuplex,
		[Alias("DBDriver", "DQ")][string]$dbDriverQueryFile,
		[Alias("EachJobSumm", "EachJobSummary", "JS")][switch]$showEachExecSummary,
		[Alias("ShowParameters", "SP")][switch]$showParams,
		[Alias("Silent", "Silence", "Sil")][switch]$consoleSilence,
		[Alias("Delim", "Del")][string]$delimiter,
		[Alias("PumpSrv", "PPS")][string]$pumpToServer,
		[Alias("PumpDB", "PPD")][string]$pumpToDatabase,
		[Alias("PumpTbl", "PPT")][string]$pumpToTable,
		[Alias("PumpFlds", "PPF")][string[]]$pumpToTableInsertFields,
		[Alias("PPBS")][ValidateRange(1, 1000)][string]$pumpBulkSize = 50
	)

	function CheckParameterCombinationValidity () {
		if ($pumpToServer -and (!$pumpToTable -or !$pumpToDatabase)) {
			throw "If -PumpToServer is set then -PumpToTable and -PumpToDatabase must be set"
		}
		if (($pumpToTable -and !$pumpToDatabase) -or (!$pumpToTable -and $pumpToDatabase)) {
			throw "If either -PumpToTable or -PumpToDatabase are set both must be set"
		}
		if ($pumpToTableInsertFields -and !$pumpToTable) {
			throw "If -PumpToTableInsertFields is set then -PumpToTable must be set"
		}
		if ($pumpToTable -and ($resultSetFileOutput -or $resultSetFormat -or $delimiter)) {
			throw "-PumpToServer, -PumpToTable, -PumpToDatabase and -PumpToTableInsertFields are mutually exclusive with -ResultSetFileOutput, -ResultSetFormat and -Delimiter"
		}
		if ($consoleSilence -and ($consoleDuplex -or $showEachExecSummary)) {
			throw "-ConsoleSilence can't be specified together with -ConsoleDuplex or -ShowEachExecSummary parameters"
		}
		if ($query -and $file) {
			throw "-Query and -File parameters can be specified at the same time"
		}
		if (!$resultSetFileOutput -and !$pumpToTable -and $consoleDuplex) {
			throw "-ConsoleDuplex can't be used if -ResultSetFileOutput or -PumpToTable are not provided"
		}
		if ($resultSetFormat -eq 'XLSX' -and !$resultSetFileOutput) {
			throw "-ResultSetFormat set to XLSX but -ResultSetFileOutput is blank. You need to provide -ResultSetFileOutput for XLSX output format"
		}	
		if ((!$delimiter -and $resultSetFormat -eq 'DELIMITED') -or ($delimiter -and $resultSetFormat -ne 'DELIMITED')) {
			throw "If -Delimiter is provided then -ResultSetFileOutput must be equal to DELIMITED and if -ResultSetFileOutput = DELIMITED -Delimiter must be provided"
		}
	}

	function CoalesceFile($fileName) {
		if ($fileName) {
			try {
				Get-Content "${fileName}.Job-*" | Out-File "${fileName}" -Append -Encoding utf8
				Remove-Item "${fileName}.Job-*"
			} catch {
				# If there's no warnings or errors other than source file not existing we will silence the exception
				if (!$_.Exception.Message.Contains('does not exist, or has been filtered')) {
					throw
				}
				# No sense creating a target blank file. Let's remove the zero bytes file created by Out-File above
				if ($fileName -and (Test-Path -Path $fileName) -and ((Get-Item $fileName).Length -le 0)) {
					Remove-Item $fileName
				}
			}
		}
	}

	# If Excel has kept XLSX files open, we will add a numeric suffix to the filename right before the extension
	$global:fileNameSuffix = 0

	function PrepareOutputFile($fileName, [ref]$returnFileContents) {
		if ($fileName) {
			$fileName = [System.IO.Path]::GetFullPath($fileName)
			if ($global:fileNameSuffix -gt 0) {
				$filename = $fileName -replace '(\.)([a-zA-Z0-9]*)$', "_${global:fileNameSuffix}.`$2"
			}
			$pathExists = Test-Path -Path $fileName
			if ($pathExists) {
				if ($returnFileContents) {
					$returnFileContents.Value = Get-Content -Path $fileName
				}
				try {
					Remove-Item $fileName
				} catch {
					# If we have already increased fileNameSuffix to a value > 0 the slot MUST be open
					# for all ancillary files such as error log, warning log, etc.
					# In summary, logic to increase fileNameSuffix should only execute for the file file
					# which happens to be the output file. Other calls simply inherit the pre-set suffix
					if ($global:fileNameSuffix -gt 0) {
						throw
					}
					while ($true) {
						try {
							$global:fileNameSuffix++
							$tmpFileName = $filename -replace '(\.)([a-zA-Z0-9]*)$', "_${global:fileNameSuffix}.`$2"
							if (Test-Path -Path $tmpFileName) {
								Remove-Item $tmpFileName
							}
							$filename = $tmpFileName
							break
						} catch {
							# we will stop trying new filenames at 100. Who will have 100 open Excel windows?
							if ($global:fileNameSuffix -gt 100) {
								throw
							}
						}
					}
				}
			} 
		}
		return $fileName
	}

	function PrintParameterValues() {
		if($showParams) {
			Write-Color -Text "Current parameters:"
			Write-Host ""
			$CommandName = $PSCmdlet.MyInvocation.InvocationName
			$ParameterList = (Get-Command -Name $CommandName).Parameters
			foreach ($Parameter in $ParameterList.Keys) {
				if ($Parameter -match '^Verbose$') {
					break
				}
				$varValue = Get-Variable -Name $Parameter -ErrorAction SilentlyContinue
				Write-color -Text "${Parameter}: ", $varValue.Value -Color White, Cyan
			}
			Write-Host ""
		}
	}

	function InitializeFileOutput() {
		# So far only file type requiring initialization is JSON type
		# In order to build a correct JSON file we enclose all returned objects in an array
		if ($resultSetFormat -eq "JSON") {
			output "[" $consoleDuplex $consoleSilence $resultSetFileOutput
		}
	}

	function FinalizeFileOutput() {
		switch ($resultSetFormat) {
			'XML' { output "</Objects>" $consoleDuplex $consoleSilence $resultSetFileOutput }
			'HTML' { output "</table></body>" $consoleDuplex $consoleSilence $resultSetFileOutput }
			'JSON' {
				# We will close the JSON array of objects and we will add an empty object at the end
				# given the fact workers add a comma after every object returned from the result sets
				output "{}]" $consoleDuplex $consoleSilence $resultSetFileOutput
			}
			'CSV' {
				if (!$resultSetFileOutput -or !(Test-Path -Path $resultSetFileOutput)) {
					return;
				}				
				if ($openExcel) {
					$excel = New-Object -comobject Excel.Application
					$workbook = $excel.Workbooks.Open($resultSetFileOutput)
					$excel.Visible = $true
				}
			}
			'XLSX' {
				if (!$resultSetFileOutput -or !(Test-Path -Path $resultSetFileOutput)) {
					return;
				}
				$xlWorkbookDefault = 51
				$xlNoChange =  1
				$xlLocalSessionChanges =  2

				$excel = New-Object -comobject Excel.Application

				if (Test-Path -Path "${resultSetFileOutput}.csv") {
					Remove-Item "${resultSetFileOutput}.csv"
				}
				Rename-Item -Path $resultSetFileOutput -NewName "${resultSetFileOutput}.csv"

				$workbook = $excel.Workbooks.Open($resultSetFileOutput + '.csv')
				try {
					$workbook.Worksheets[1].Columns["A:Z"].AutoFit() > $null
					$workbook.SaveAs($resultSetFileOutput, $xlWorkbookDefault, [Type]::Missing, [Type]::Missing, $false, $false, $xlNoChange, $xlLocalSessionChanges)
					Remove-Item "${resultSetFileOutput}.csv"
				} finally {
					if ($openExcel) {
						$excel.Visible = $true
					} else {
						$workbook.Close()
					}
				}
			}
		}
	}

	# The following command set is going to be executed in parallel using Start-Job
	$cmd = {
		param(
			$jobID
		)

		# parameters imported from local scope
		$file = $using:file
		$query = $using:query
		$resultSetFileOutput = $using:resultSetFileOutput
		$dbWarningsFileOutput = $using:dbWarningsFileOutput
		$dbErrorsFileOutput = $using:dbErrorsFileOutput
		$detailedErrorLogFileOutput = $using:detailedErrorLogFileOutput
		$resultSetFormat = $using:resultSetFormat
		$dbs = $using:dbs
		$dbExceptionDBs = $using:dbExceptionDBs
		$bindVariables = $using:bindVariables
		$showEachExecSummary = $using:showEachExecSummary
		$columnsFile = $using:columnsFile
		$consoleSilence = $using:consoleSilence
		$consoleDuplex = $using:consoleDuplex
		$delimiter = $using:delimiter
		# data pumping parameters
		$pumpToServer = $using:pumpToServer
		$pumpToTable = $using:pumpToTable
		$pumpToTableInsertFields = $using:pumpToTableInsertFields
		$pumpBulkSize = $using:pumpBulkSize
		$pumpToDatabase = $using:pumpToDatabase

		$delimitedExtensions = @{'CSV' = ','; 'XLSX' = ','; 'TAB' = "`t"; 'PIPE' = '|'}

		function initInsertStatement() {
			if ($pumpToTable) {
				$result = "INSERT INTO ${pumpToTable}"
				if ($pumpToTableInsertFields) {
					$fldsCommaText = $pumpToTableInsertFields -join ","
					$result += " (${fldsCommaText})"
				}
				$result += " VALUES`r`n"
			}
			return $result
		}

		function flushInsertStatement($insertToTableStatement, [ref]$rowsFetched, [ref]$firstRow) {
			Invoke-SQLCmd -ServerInstance $pumpToServer -Database $pumpToDatabase -Query $insertToTableStatement -AbortOnError -MaxCharLength 65535
			if ($firstRow) {
				$firstRow.Value = $true
			}
			if ($rowsFetched) {
				$rowsFetched.Value = 0
			}
			return initInsertStatement
		}

		function processPump($result, [ref]$insertToTableStatement, [ref]$rowsFetched, [ref]$firstRow) {
			$startFrom = 0
			$result = $result | ConvertTo-Csv -NoTypeInformation -Delimiter ',' `
					| Select-Object -Skip 1 `
					| ForEach-Object {($_ -replace "'", "''") -replace '"', "'"}
			while ($true) {
				$bulkSize = IIf ($rowsFetched.Value -gt 0) ($pumpBulkSize - $rowsFetched.Value) $pumpBulkSize
				$values = $result | Select-Object -Skip $startFrom `
					| Select-Object -First $bulkSize `
					| ForEach-Object {(IIf ($firstRow.Value -eq $true) '' ',') + "(${_})`r`n"; $firstRow.Value = $false}
				$startFrom += $bulkSize
				if (!$values -or $values.length -le 0) {
					break
				}
				$resultLen = IIf ($values -and $values.GetType().fullname -eq 'System.String') 1 $values.length
				$insertToTableStatement.Value += $values
				$rowsFetched.Value += $resultLen
				if ($rowsFetched.Value -ge $pumpBulkSize) {
					$insertToTableStatement.Value = flushInsertStatement $insertToTableStatement.Value $rowsFetched $firstRow
				}
			}
		}

		function handleException ($e, $db, $server, [ref]$dbErrors, [ref]$dbWarnings, $localStartTime) {
			if ($localStartTime) {
				$elapsedTotalTime = elapsedTime $localStartTime
			} else {
				$elapsedTotalTime = 'N/A'
			}
			if ($e.Exception.Message.Contains('user error 50000, severity 20')) {
				$dbWarnings.Value++
				if ($showEachExecSummary) {
					Write-Color -Text "Job-${jobID}: ", "${db}...", " WARNING", ": Database incompatible. Elapsed: ${elapsedTotalTime}" -Color White, Cyan, Yellow, White
					if ($dbWarningsFileOutput) {
						Out-File -FilePath "${dbWarningsFileOutput}.Job-${jobID}" -InputObject "${db}=${server}" -Append -Encoding utf8
					}
				}
			} else {
				$dbErrors.Value++
				if (!$consoleSilence) {
					Write-Color -Text "Job-${jobID}: ", "${db}...", " ERROR", ": script failed. Elapsed: ${elapsedTotalTime}" -Color White, Cyan, Red, White
				}
				if ($dbErrorsFileOutput) {
					Out-File -FilePath "${dbErrorsFileOutput}.Job-${JobID}" -InputObject "${db}=${server}" -Append -Encoding utf8
				}
				if ($detailedErrorLogFileOutput) {
					$props = @{
						Server = $server
						Database = $db
						ErrorMessage = $e.Exception.Message
					}
					$ErrorMsgObj = New-Object psobject -Property $props
					$errorAsJson = ConvertTo-Json -InputObject $ErrorMsgObj
					Out-File -FilePath "${detailedErrorLogFileOutput}.Job-${JobID}" -InputObject "${errorAsJson}," -Append -Encoding utf8
				}
			}
		}

		function processScript($dbs) {
			Write-Progress -Id $jobID -Activity "Job-${jobID}" -Status "Starting..." -PercentComplete 0
			$dbProcessedCount = 0
			$dbSkipped = 0
			$dbWarnings = 0
			$dbErrors = 0
			$insertJob = 0
			$lastPercentComplete = 0
			# Variables used when pumping data target tables
			$firstRow = $true
			$rowsFetched = 0
			$insertToTableStatement = initInsertStatement

			$ErrorActionPreference = 'SilentlyContinue'
			try {
				foreach($item in $dbs) {
					$localStartTime =  [System.Diagnostics.Stopwatch]::StartNew()
					$db = $item | Select-Object -exp DBNAME
					$server = $item | Select-Object -exp SERVERNAME
					$percentComplete = (100 * ($dbProcessedCount + $dbSkipped) / $dbs.Count)
					if ($percentComplete -ge $lastPercentComplete + 5) {
						$lastPercentComplete = $percentComplete
						Write-Progress -Id $jobID -Activity "Job-${jobID}" -Status "Processing ${db}..." -PercentComplete $percentComplete
					}
					$exceptServer = $dbExceptionDBs[$db]
					if ($exceptServer -and $server -match $exceptServer) {
						$dbSkipped++
						continue
					}
					$dbProcessedCount++
					try {
						if($query) {
							$result = Invoke-SQLCmd -ServerInstance $server -Database $db -Query $query -AbortOnError -MaxCharLength 65535 -Variable $bindVariables
						} else {
							$result = Invoke-SQLCmd -ServerInstance $server -Database $db -InputFile $file -AbortOnError -MaxCharLength 65535 -Variable $bindVariables
						}
						if($result) {
							if ($pumpToTable) {
								try {
									processPump $result ([ref]$insertToTableStatement) ([ref]$rowsFetched) ([ref]$firstRow)
									if ($consoleDuplex) {
										Write-Output $result | ConvertTo-Csv -NoTypeInformation -Delimiter ',' | Select-Object -Skip 1
									}
								} catch {
									$rowsFetched = 0
									$firstRow = $true
									$insertToTableStatement = initInsertStatement
									throw
								}
							} else {
								if ($delimitedExtensions[$resultSetFormat]) {
									$resultTable = resultSetToDelimited $result $columnsFile $delimitedExtensions[$resultSetFormat]
								} else {
									switch($resultSetFormat) {
										'JSON' {
											$resultTable = $result | Select-Object * -ExcludeProperty ItemArray, Table, RowError, RowState, HasErrors | ConvertTo-Json
											$resultTable = $resultTable -replace '(^\[[ \r\n]*)|([ \r\n]*\]$)', ''
											$resultTable += ","
										} 
										'DELIMITED' { $resultTable = resultSetToDelimited $result $columnsFile $delimiter }
										'HTML' { $resultTable = resultSetToHtml $result $columnsFile }
										'XML' { $resultTable = resultSetToXml $result $columnsFile }
										default { throw "Invalid value '${resultSetFormat}' for parameter -ResultSetFormat" }
									}
								}
								output $resultTable $consoleDuplex $consoleSilence (IIf ${resultSetFileOutput} "${resultSetFileOutput}.Job-${jobID}" "")
							}
						}
						$localTotalTime = elapsedTime $localStartTime
						if ($showEachExecSummary) {
							if (!$result) {
								Write-Color -Text "Job-${jobID}: ", "${db}", "... OK. Elapsed: ${localTotalTime}" -Color White, Cyan, White	
							} else {
								$recCount = ($result | Measure).Count
								Write-Color -Text "Job-${jobID}: ", "${db}", "... Retrieved ", "${recCount}", " records. Elapsed: ${localTotalTime}" -Color White, Cyan, White, Green, White
							}
						}
					} catch {
						handleException $_ $db $server ([ref]$dbErrors) ([ref]$dbWarnings) $localStartTime
					}
				}
				if ($firstRow -eq $false) {
					try {
						flushInsertStatement $insertToTableStatement > $null
					} catch {
						handleException $_ $pumpToServer $pumpToDatabase ([ref]$dbErrors) ([ref]$dbWarnings)
					}
				}
			} finally {
				$ErrorActionPreference = 'Stop'
			}
			Write-Progress -Id $jobID -Activity "Job-${jobID}" -Status "Completed" -PercentComplete 100 -Completed
			if (!$consoleSilence) {
				Write-Color -Text "Job-${jobID} finished. ",
								"Summary: ", "processed ", "${dbProcessedCount}",
								", Skipped ", "${dbSkipped}", 
								", Warnings ", "${dbWarnings}",
								", Errrors ", "${dbErrors}", "." -Color White, 
								Green, White, Green,
								White, Yellow, 
								White, Yellow,
								White, Red, White
			}
		}

		processScript $dbs
	}

	# Environment initialization
	$ErrorActionPreference = "Stop"
	# Set the current directory on the .NET layer so we can normalize file names when calling PrepareOutputFile
	[System.IO.Directory]::SetCurrentDirectory(((Get-Location -PSProvider FileSystem).ProviderPath))

	# Paramaters setup and verification
	if ($file) {
		# $file parameter has precedence over $query
		$query = $null
	}
	CheckParameterCombinationValidity
	if ($pumpToTable -and !$pumpToServer) {
		$pumpToServer = $server
	}
	if ($file) {
		$file = [System.IO.Path]::GetFullPath($file)	
	}
	$resultSetFileOutput = PrepareOutputFile $resultSetFileOutput
	$dbWarningsFileOutput = PrepareOutputFile $dbWarningsFileOutput
	[string[]]$erroredOutDBs = @()
	$dbErrorsFileOutput = PrepareOutputFile $dbErrorsFileOutput ([ref]$erroredOutDBs)
	if ((!$dbErrorsFileOutput -or !$erroredOutDBs) -and $reRunDBFromErrorsFile) {
		throw "-DBErrorsFileOutput not provided or error file not found. -ReRunDBFromErrorsFile was set but no error file was loaded. Can't run."
	}
	$detailedErrorLogFileOutput = PrepareOutputFile $detailedErrorLogFileOutput
	$columnsFile = PrepareOutputFile "output_columns.tmp"
	if (!$resultSetFormat) {
		if ($resultSetFileOutput) {
			if (($resultSetFileOutput -match '\.([a-zA-Z0-9]*)$') -and (@('TAB', 'CSV', 'XLSX', 'JSON', 'PIPE', 'HTML', 'XML')) -contains $Matches[1]) {
				$resultSetFormat = $Matches[1]
			} else {
				$resultSetFormat = 'CSV'
			}
		} else {
			$resultSetFormat = 'TAB'
		}
	}
	if (!(@('XLSX', 'CSV') -contains $resultSetFormat) -and $openExcel) {
		throw "-OpenExcel option can be used only when -ResultSetFormat is equal to XLSX or CSV"
	}
	# Finished parameters setup and verification

	# Detailed error file will be an array of JSON objects. Let's output the array opening bracket [
	if ($detailedErrorLogFileOutput) {
		Out-File -FilePath $detailedErrorLogFileOutput -InputObject "[" -Encoding utf8
	}

	if ($dbExceptionFileInput) {
		$dbExceptionFileInput = [System.IO.Path]::GetFullPath($dbExceptionFileInput)
		$dbExceptionDBs = Get-Content -Path $dbExceptionFileInput -Raw -ErrorAction Stop | ConvertFrom-StringData
	} else {
		$dbExceptionDBs = @{}
	}

	# Let's retrieve the list of databases in the server
	if ($dbDriverQueryFile) {
		if (Test-Path -Path $dbDriverQueryFile) {
			$dbsQuery = Get-Content -Path $dbDriverQueryFile -Raw
			# User should use the token {parallelLevel} to create buckets matching the number of parallelism
			$dbsQuery = $dbsQuery -replace '{parallelLevel}', "${parallelLevel}"
		} else {
			throw "Could not find driver file ${dbDriverQueryFile}"
		}
	} else {
		$dbsQuery = "SELECT (ROW_NUMBER() OVER (ORDER BY NAME) - 1) % ${parallelLevel} BUCKET, NAME AS DBNAME, '${server}' SERVERNAME FROM sys.databases"
	}

	$databases = Invoke-Sqlcmd -ServerInstance ${server} -Database master -query ${dbsQuery}
	if ($reRunDBFromErrorsFile -and $erroredOutDBs.Count -gt 0) {
		$databases = $databases.Where({ ($erroredOutDBs -match (($_ | Select-Object -exp DBNAME) + ' *= *' + ($_ | Select-Object -exp SERVERNAME) + '($|[\r\n\s]+)')) })
	}

	# Split into parallel jobs using the BUCKET field
	$db_splits = @(0..($parallelLevel - 1))
	$db_splits[0] = $databases
	for ($i = 0; $i -lt $parallelLevel - 1; $i++) {
		$db_splits[$i], $db_splits[$i + 1] = $db_splits[$i].Where({($_ | Select-Object -exp BUCKET) -eq $i}, 'Split')
	}

	$startTime = [System.Diagnostics.Stopwatch]::StartNew()

	PrintParameterValues
	InitializeFileOutput $consoleDuplex $consoleSilence $resultSetFileOutput

	# We will capture Ctrl-C to cancel all child jobs
	[console]::TreatControlCAsInput = $true
	try {
		# Stop abandoned jobs that never properly started
		Get-Job -State NotStarted | Stop-Job
		Get-Job | Where({($_ | Select-Object -exp State) -ne 'Running'}) | Remove-Job
		$jobID = 1

		# env:\MODULE_PATH environment variable is used in order to be able to import functions
		# needed within the codeblock used below when callint Start-Job. Essentially this trick
		# allows the parallel workers to use specific functions from the main module (this module)
		if (Test-Path -Path env:\MODULE_PATH) {
			Remove-Item -Path env:\MODULE_PATH
		}
		New-Item -Name MODULE_PATH -Value $MyInvocation.MyCommand.Module.Path -Path env:\ > $null
		try {
			foreach($dbs in $db_splits) {
				Start-Job -ScriptBlock $cmd -ArgumentList ($jobID++) -InitializationScript {
					Import-Module -Name $env:MODULE_PATH -DisableNameChecking
				} > $null
			}

			$runningJobs = $true
			while ($runningJobs) {
				$runningJobs = Get-Job -State Running
				# Randomize the list so screen updates is not always from top to bottom
				$runningJobs = $runningJobs | Sort-Object {Get-Random}
				if($runningJobs) {
					if (($consoleDuplex -and $resultSetFileOutput) -or (!$consoleSilence -and !$resultSetFileOutput)) {
						Receive-Job $runningJobs
					} else {
						Receive-Job $runningJobs > $null
					}
					Wait-Job $runningJobs -Timeout 0 > $null
					# if Ctrl-C is pressed we will cancel all of our child jobs
					if ([console]::KeyAvailable) {
						$key = [system.console]::readkey($true)
						if (($key.modifiers -band [consolemodifiers]"control") -and
							($key.key -eq "C"))
						{
							Write-Color -Text "Terminating..." -Color Red
							$runningJobs | Stop-Job
							break
						}
					}
				}
			}
		} finally {
			Remove-Item -Path env:\MODULE_PATH
		}
	} finally {
		[console]::TreatControlCAsInput = $false
	}

	# Even though our execution run has completed we may not have fetched all output from the child jobs
	$jobs = Get-Job -HasMoreData $true
	if ($jobs) {
		if (($consoleDuplex -and $resultSetFileOutput) -or (!$consoleSilence -and !$resultSetFileOutput)) {
			$jobs | Receive-Job
		} else {
			$jobs | Receive-Job > $null
		}
	}

	if ($resultSetFileOutput -and (Test-Path -Path $columnsFile)) {
		Get-Content -Path $columnsFile | Out-File -FilePath $resultSetFileOutput -Encoding utf8
		Remove-Item $columnsFile
	}	
	CoalesceFile $resultSetFileOutput
	CoalesceFile $dbWarningsFileOutput
	CoalesceFile $dbErrorsFileOutput
	CoalesceFile $detailedErrorLogFileOutput	
	if ($detailedErrorLogFileOutput) {
		# We will close our JSON array of detailed errors. Note the need for {}
		# due to the extra comma after the last error object logged
		Out-File -FilePath $detailedErrorLogFileOutput -InputObject "{}]" -Append -Encoding utf8
	}

	FinalizeFileOutput

	# Final step: display total elapsed time
	$elapsedTime = $startTime.Elapsed
	$totalTime = $([string]::Format("{0:d2}:{1:d2}:{2:d2}.{3:d2}",
									$elapsedTime.hours,
									$elapsedTime.minutes,
									$elapsedTime.seconds,
									$elapsedTime.milliseconds))
	if (!$consoleSilence) {
		Write-Host ""
		Write-Color -Text "Total elapsed ", "${totalTime}" -Color White, Cyan
	}
<#
 .SYNOPSIS
  This script executes commands in a .sql script file or parameter in all databases
  present in the specified SQL instance or provided by a custom query in multiple server instances.
  It has the ability to produce a single output file in any of multiple supported formats 
  (CSV, TAB delimited, Pipe delimited, XLSX, JSON, XML) or pump the data to a single table in a 
  specific server in a specific database.

.DESCRIPTION
  If any error occurs the script reports the database that failed and the process continues.
  If -dbErrorsFileOutput or -dbWarningsFileOutput are provided, the database names of the
  databases that caused the errors or warnings are written the files specified in these params.
  If the parameter -dbExceptionFileInput is provided file specified in this parameter should
  contains a list of database names and regex matching server names
  to be skipped when processing. This is used to avoid processing databases known not to be 
  incompatible with the script.
  Authentication method used current user in Active Directory.
  Default level of parallelism if -parallelLevel is not provided is 4 meaning there will be 4
  concurrent jobs executing the script.
  In case Format is not specified while a result set file output is specified, the script will attempt
  to derive the format type from the file extension otherwise defaulting to CSV.

 .Parameter Server
  Name of the MSSQL server instance to connect to.

 .Parameter File
  Filename with a .SQL script to execute against the matched databases.

 .Parameter DBExceptionFileInput
  File name containing databases to ignore when processing the command provided
  in -Query or -File parameter. The format is:
  dbname1=regex to match against server name
  dbname2=regex to match against server name
  Please notice in this file there must be only one entry per DBName. If you need to match a single database name
  against multiple databases you need to solve this with a regular expression. For example, the following entry will
  avoid processing database name TEST against ANY server:
  TEST=.*

 .Parameter ParallelLevel
  Level of parallelism used to execute the parameters. Bu default the value of
  this parameter is 4, meaning 4 parallel processes are going to be spawned and
  the output of this jobs is going to be collected and reported in a single 
  pipeline. You need to experiment with this parameter to find the best performance depending on your workload.

 .Parameter DBWarningsFileOutput
  Filename where to output database names of databases where the provided script
  errored out with special RAISERROR used to denote a warning. The type of exception
  raised is as follows:
	RAISERROR('Not a compatible database', 20, -1) with log

 .Parameter DBErrorsFileOutput
  Filename where to output database names of databases where the provided script
  errored out when executing the provided statements.

 .Parameter DetailedErrorLogFileOutput
  If provided all errors resulting from SQL commands execution will be logged to this file in JSON format. The objects will
  contain the database name where the error was captured and the resulting error message.

 .Parameter Query
  Query/command to execute against matching databases

 .Parameter ResultSetFormat
  Format to use when query returns data (SELECT statement). Valid formats are:
    CSV - Comma delimited output
    TAB - TAB character delimited output
    XLSX - Native Excel file format
    JSON - JSON array of objects
    PIPE - Pipe delimited file
    DELIMITED - A delimited file with delimiter specified by -Delimiter parameter
    HTML - Produces an HTML file
    XML - Xml format

 .Parameter Delimiter
  When using -ResultSetFormat = DELIMITED -Delimiter specifies the character to use as delimiter in the output

 .Parameter ResultSetFileOutput
  File name of target file where to output result sets returned from SELECT calls.
  Result set will also be shown on the console.

 .Parameter OpenExcel
  If specified and if output format type is CSV or XLSX Excel will be opened with the target
  file specified in ResultSetFileOutput

 .Parameter ReRunDBFromErrorsFile
  If this switch is specified the script will re-run all databases contained in the file provided in DbErrorsFileOutput parameter.
  Please notice the format of every entry in this file is <DBName>=<ServerName>. The match for this feature has to be exact in contrast
  with the setting -DBExceptionFileInput which takes a regular expression at the right side of the equal sign on the DBNAME=SERVERNAME 
  entries. 

 .Parameter BindVariables
  Pass here an array of key value pairs of SQLCMD variables and their corresponding values. 
  The format for each element in the array is as follows: VARIABLE=VALUE.
  See examples for more details

 .Parameter DBDriverQueryFile
  File name of file containing a driver query returning the server names, database names and buckets to split the list of target databases
  to execute the query or script. The query must return the following fields:
    * BUCKET
    * DBNAME
    * SERVERNAME
  This is the query used by default pulling databases from MSSQL metadata:
    SELECT (ROW_NUMBER() OVER (ORDER BY NAME) - 1) % {parallelLevel} BUCKET, NAME AS DBNAME, 'vm-pc-sql02' SERVERNAME FROM sys.databases
  When using a custom query make sure to use the variable $parallelLevel in order to create matching buckets for the list of databases.

 .Parameter ConsoleDuplex
  When this switch is specified together with ResultSetFileOutput the result set generated will be also output to console. This will
  slow down processing when using parallel level > 1.

 .Parameter ShowEachExecSummary
  This switch controls if showing summary after every database job is processed.

 .Parameter ShowParams
  This switch enables showing the values of all parameters passed to this cmdlet

 .Parameter ConsoleSilence
  Controls if displaying error messages, final summary per job and total time elapsed. If ConsoleDuplex us enabled result sets will be
  shown in the console ignoring ConsoleSilence. If not ResultSetFileOutput is specified result sets will also be output to console.

 .Parameter PumpToServer
  Specifies the target server that contains the target database and target table to output the aggregate result set. 
  If ommited it will default to the Server parameter.

 .Parameter PumpToDatabase
  Target database in the target server containing the table where to output the aggregate result set.

 .Parameter PumpToTable
  Target table where to insert the records produced by the result set.

 .Parameter PumpToTableInsertFields
  Array of strings containing the fields to be part of the insert statement used to output into the target table.

 .Parameter PumpBulkSize
  Number of records to insert into the target table per operation. Default value is 50
  When setting this parameter take into account that the maximum supported rows for an insert statement is 1000.

 .Example
  # Executes the default query SELECT DB_NAME() DBNAME in each database of server vm-pc-sql02 
  ForEach-DB -Server vm-pc-sql02

 .Example
  # When using a SELECT statement it's possible to output directly to an Excel compatible .csv file.
  ForEach-DB -server vm-pc-sql02 -query "select db_name(), getdate()" -ResultSetFormat CSV -out output.csv

 .Example
  # The following example runs query SELECT DB_NAME() DBNAME, GETDATE() DATE in all databases, outputs warnings, errors and detailed error messages
  # to specified files. After completing the generation of CSV output file will call Excel and open the target file
  ForEach-DB -server vm-pc-sql02 -out output.csv -format CSV -OpenExcel -Warn Warnings.log -Err Errors.log -Query "SELECT DB_NAME() DBNAME, GETDATE() DATE" -Deterror DetError.json

 .Example
  # This command binds a variable in the query body passed using the BindVariables parameter
  ForEach-DB -server vm-pc-sql02 -out output.csv -format CSV -Query "SELECT DB_NAME() DBNAME, `$(A) AS A" -BindVars @("A='1'")

 .Example
  # This command inserts the values read from all data sources into target table TEST_TBL contained in database TESTDB
  ForEach-DB vm-pc-sql02 "select db_name() dbname, e.firstname from employee e" -pumpToTable "TEST_TBL" -PumpDB TESTDB
#>
}

function IIf($If, $Right, $Wrong) {
	If ($If) { 
		$Right 
	} Else { 
		$Wrong 
	}
}

function resultSetToTarget ($headersCodeBlock, $exportCodeBlock, $columnsFile) {
	if ($columnsFile -and !(Test-Path -Path $columnsFile)) {
		try {
			Out-File -FilePath $columnsFile -InputObject (&$headersCodeBlock)
		} catch {
			# It's possible for two or more threads to try to output the header portion of a delimited file at the same time
			# we will ignore this errors and assume the file that has the file locked will output the headers successfully
			if (!$_.Exception.Message.Contains("The process cannot access the file")) {
				throw
			}
		}
	}
	return &$exportCodeBlock
}

function resultSetToXml ($result, $columnsFile) {
	return resultSetToTarget {
		$result | Select-Object * -ExcludeProperty ItemArray, Table, RowError, RowState, HasErrors | ConvertTo-Xml -As Stream | Select-Object -First 2
	} {
		$result | Select-Object * -ExcludeProperty ItemArray, Table, RowError, RowState, HasErrors | ConvertTo-Xml -As Stream | Select-Object -Skip 2 | Select-Object -SkipLast 1
	} $columnsFile
}

function resultSetToHtml ($result, $columnsFile) {
	return resultSetToTarget {
		$result | Select-Object * -ExcludeProperty ItemArray, Table,  RowError, RowState, HasErrors | ConvertTo-Html | Select-Object -First 8
	} {
		$result | Select-Object * -ExcludeProperty ItemArray, Table, RowError, RowState, HasErrors | ConvertTo-Html | Select-Object -Skip 8 | Select-Object -SkipLast 2
	} $columnsFile
}


function resultSetToDelimited ($result, $columnsFile, $delimiter) {
	return resultSetToTarget {
		$result | ConvertTo-Csv -NoTypeInformation -Delimiter $delimiter | Select-Object -First 1
	} {
		$result | ConvertTo-Csv -NoTypeInformation -Delimiter $delimiter | Select-Object -Skip 1
	} $columnsFile
}


function elapsedTime($startTime) {
	$localElapsedTime = $startTime.Elapsed
	$localTotalTime = $([string]::Format("{0:d2}:{1:d2}:{2:d2}.{3:d2}", $localElapsedTime.hours, $localElapsedTime.minutes,	$localElapsedTime.seconds, $localElapsedTime.milliseconds))
	return $localTotalTime
}

function output($content, $consoleDuplex, $consoleSilence, $resultSetFileOutput) {	
	if (($consoleDuplex -and $resultSetFileOutput) -or (!$consoleSilence -and !$resultSetFileOutput)) {
		Write-Output $content
	}
	if ($resultSetFileOutput) {
		Out-File -FilePath $resultSetFileOutput -InputObject $content -Append -Encoding utf8
	}
}

Export-ModuleMember -Function ForEach-DB
# The following functions are used from within the codeblock used when calling Start-Job
Export-ModuleMember -Function resultSetToDelimited
Export-ModuleMember -Function resultSetToHtml
Export-ModuleMember -Function resultSetToXml
Export-ModuleMember -Function elapsedTime
Export-ModuleMember -Function output
Export-ModuleMember -Function IIf