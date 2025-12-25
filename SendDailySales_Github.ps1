# ============================================================
# SendDailySales_SYNC_7DAYS_v7.ps1
# 
# ✅ SYNC 7 DAYS - Check từng ngày, update nếu thiếu/có phát sinh
# ✅ AUTO-DETECT: Lấy StoreID từ Database (lRetailStoreID)
# ✅ MULTI-STORE: Loop qua tất cả stores tự động
# ✅ SAFETY CHECK: Nếu 7 ngày DB = 0 thì SKIP
# ✅ SECURE: API Key loaded from Environment Variable (not hardcoded)
# ✅ NO WARNINGS: Use .NET WebClient for Firebase upload (ZERO warnings)
# ✅ NEW v7: Doanh thu bán, doanh thu trả, số bill được discount
# ============================================================

# ========== CONFIG ==========
$DbName   = "TPCentralDB"
$Server   = $env:COMPUTERNAME
$Conn     = "Server=$Server;Database=$DbName;Trusted_Connection=Yes;"

# Firebase Config - LOAD FROM ENVIRONMENT VARIABLE
$FirebaseProjectId = "mxd-pos"
$env_var_name = "FIREBASE_API_KEY_MXD-POS"
$FirebaseApiKey = [Environment]::GetEnvironmentVariable($env_var_name, [EnvironmentVariableTarget]::User)

# Check if API Key is available
if ([string]::IsNullOrEmpty($FirebaseApiKey)) {
    Write-Host "❌ ERROR: Environment variable '$env_var_name' not found!" -ForegroundColor Red
    Write-Host "⚠️  Please set environment variable first (Admin PowerShell):" -ForegroundColor Yellow
    Write-Host "   [Environment]::SetEnvironmentVariable('FIREBASE_API_KEY_MXD-POS','YOUR_API_KEY',[EnvironmentVariableTarget]::User)" -ForegroundColor Yellow
    exit 1
}

$FirebaseCollection = "DailySales"

# Output path
$JsonOutputPath = "C:\Temp"

# ===========================

# Calculate 7-day range
$todayDate = Get-Date
$startDate = $todayDate.AddDays(-6)  # 7 ngày gần nhất (T-6 đến T)

$todayInt = [int]$todayDate.ToString('yyyyMMdd')
$startInt = [int]$startDate.ToString('yyyyMMdd')

Write-Host ""
Write-Host "🔄 Daily Sales SYNC 7 DAYS v7 - Enhanced Metrics (No Warnings)" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "Server: $Server" -ForegroundColor Cyan
Write-Host "Database: $DbName" -ForegroundColor Cyan
Write-Host "Date Range: $($startDate.ToString('yyyy-MM-dd')) to $($todayDate.ToString('yyyy-MM-dd')) (7 days)" -ForegroundColor Cyan
Write-Host "✅ API Key: Loaded from Environment Variable" -ForegroundColor Green
Write-Host "✨ NEW: Sales Amount | Return Amount | Discounted Bills" -ForegroundColor Magenta
Write-Host ""

# ============================================================
# SQL CONNECTION FUNCTION
# ============================================================

function Invoke-Sql([string]$ConnStr, [string]$Query) {
    $cn = New-Object System.Data.SqlClient.SqlConnection($ConnStr)
    $da = New-Object System.Data.SqlClient.SqlDataAdapter($Query, $cn)
    $dt = New-Object System.Data.DataTable
    
    try {
        [void]$da.Fill($dt)
        return $dt
    }
    catch {
        Write-Host "❌ SQL Error: $_" -ForegroundColor Red
        throw $_
    }
    finally {
        if ($cn.State -ne 'Closed') { $cn.Close() }
        $da.Dispose()
        $cn.Dispose()
    }
}

# ============================================================
# FIREBASE UPLOAD FUNCTION (.NET WebClient - NO WARNINGS)
# ============================================================

function Upload-ToFirebase([string]$Uri, [string]$JsonBody) {
    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        
        # Use .NET WebClient instead of Invoke-WebRequest
        # This NEVER triggers Security Warning
        $webClient = New-Object System.Net.WebClient
        $webClient.Headers.Add("Content-Type", "application/json")
        $webClient.Encoding = [System.Text.Encoding]::UTF8
        
        $response = $webClient.UploadString($Uri, "PATCH", $JsonBody)
        $webClient.Dispose()
        
        return @{
            Success = $true
            Response = $response
            StatusCode = 200
        }
    }
    catch {
        return @{
            Success = $false
            Error = $_.Exception.Message
            StatusCode = 0
        }
    }
}

# ============================================================
# STEP 0: AUTO-DETECT STORES FROM DATABASE
# ============================================================

Write-Host "🔍 STEP 0: Auto-detecting stores from database..." -ForegroundColor Yellow

$GetStoresQuery = @"
DECLARE @StartDate INT = $startInt;
DECLARE @EndDate INT = $todayInt;

SELECT DISTINCT lRetailStoreID as StoreID
FROM dbo.TxSaleLineItem WITH (NOLOCK)
WHERE szDate >= @StartDate
  AND szDate <= @EndDate
  AND szType IN ('ART_SALE', 'ART_RETURN')
  AND TRIM(ISNULL(szPrintCodes, '')) != ''
ORDER BY lRetailStoreID;
"@

try {
    $storesDb = Invoke-Sql $Conn $GetStoresQuery
    
    if ($storesDb -is [System.Data.DataTable]) {
        $storesList = @($storesDb.Rows) | ForEach-Object { [int]$_['StoreID'] }
    }
    else {
        $storesList = @($storesDb) | ForEach-Object { [int]$_['StoreID'] }
    }
    
    if ($storesList.Count -eq 0) {
        Write-Host "❌ No stores found in database!" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "✅ Found $($storesList.Count) store(s): $($storesList -join ', ')" -ForegroundColor Green
}
catch {
    Write-Host "❌ Failed to get stores: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""

# ============================================================
# STEP 1: LOOP THROUGH EACH STORE
# ============================================================

$totalResults = @()

foreach ($StoreId in $storesList) {
    Write-Host ""
    Write-Host "====================================================" -ForegroundColor Magenta
    Write-Host "📍 STORE: $StoreId" -ForegroundColor Magenta
    Write-Host "====================================================" -ForegroundColor Magenta
    Write-Host ""
    
    # ============================================================
    # STEP 1.1: SAFETY CHECK - Query 7 days for this store
    # ============================================================
    
    Write-Host "🔍 Safety Check (7-day database)..." -ForegroundColor Yellow
    
    $SafetyCheckQuery = @"
DECLARE @StartDate INT = $startInt;
DECLARE @EndDate INT = $todayInt;
DECLARE @StoreID INT = $StoreId;

SELECT COUNT(*) as TotalRecords
FROM dbo.TxSaleLineItem WITH (NOLOCK)
WHERE szDate >= @StartDate
  AND szDate <= @EndDate
  AND lRetailStoreID = @StoreID
  AND szType IN ('ART_SALE', 'ART_RETURN')
  AND TRIM(ISNULL(szPrintCodes, '')) != '';
"@
    
    try {
        $safetyDt = Invoke-Sql $Conn $SafetyCheckQuery
        
        if ($safetyDt -is [System.Data.DataTable]) {
            $safetyRecords = @($safetyDt.Rows)
        }
        else {
            $safetyRecords = @($safetyDt)
        }
        
        $totalRecords = 0
        if ($safetyRecords.Count -gt 0) {
            $totalRecords = [int]$safetyRecords[0]['TotalRecords']
        }
        
        Write-Host "✅ Total records (7 days): $totalRecords" -ForegroundColor Green
        
        if ($totalRecords -eq 0) {
            Write-Host "⚠️  No sales data for this store - SKIP" -ForegroundColor Yellow
            continue
        }
    }
    catch {
        Write-Host "❌ Safety Check failed: $_" -ForegroundColor Red
        continue
    }
    
    # ============================================================
    # STEP 1.2: LOOP THROUGH EACH DAY (7 days)
    # ============================================================
    
    Write-Host ""
    Write-Host "📊 Syncing each day..." -ForegroundColor Yellow
    Write-Host ""
    
    $syncResults = @()
    $successCount = 0
    $failureCount = 0
    
    for ($i = 6; $i -ge 0; $i--) {
        $dateToSync = $todayDate.AddDays(-$i)
        $dateStr = $dateToSync.ToString('yyyy-MM-dd')
        $dateInt = [int]$dateToSync.ToString('yyyyMMdd')
        
        Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
        Write-Host "📅 $dateStr" -ForegroundColor Cyan
        
        # ============================================================
        # QUERY DATA FOR THIS DAY
        # ============================================================
        
        $SqlQuery = @"
DECLARE @BizDate INT = $dateInt;
DECLARE @StoreID INT = $StoreId;

SELECT
    lRetailStoreID AS StoreID,
    lTaNmbr AS BillNumber,
    szType AS TransactionType,
    dTaQty AS Quantity,
    dTaPrice AS UnitPrice,
    dTaDiscount AS LineDiscount,
    dTaTotal AS NetAmount
FROM dbo.TxSaleLineItem WITH (NOLOCK)
WHERE szDate = @BizDate
  AND lRetailStoreID = @StoreID
  AND szType IN ('ART_SALE', 'ART_RETURN')
  AND TRIM(ISNULL(szPrintCodes, '')) != ''
ORDER BY lTaNmbr;
"@
        
        try {
            $dt = Invoke-Sql $Conn $SqlQuery
            
            if ($dt -is [System.Data.DataTable]) {
                $records = @($dt.Rows)
            }
            else {
                $records = @($dt)
            }
            
            $recordCount = $records.Count
            
            if ($recordCount -eq 0) {
                Write-Host "   ⚠️  No sales today" -ForegroundColor Yellow
                $syncResults += @{
                    date = $dateStr
                    status = "NO_DATA"
                    records = 0
                }
                continue
            }
            
            # ============================================================
            # PROCESS METRICS FOR THIS DAY (UPDATED v7)
            # ============================================================
            
            $Metrics = @{
                StoreID = $StoreId
                TotalBills = 0
                SalesBills = 0
                ReturnBills = 0
                SalesAmount = 0      # ✅ NEW: Doanh thu bán hàng
                ReturnAmount = 0     # ✅ NEW: Doanh thu trả hàng
                DiscountedBills = 0  # ✅ NEW: Số bill được discount
                TotalDiscount = 0
                TotalAmount = 0
                TotalQuantity = 0
            }
            
            $billNumbers = @()
            $discountedBillNumbers = @()  # ✅ NEW: Để đếm bill discount
            
            foreach ($row in $records) {
                $billNum = [string]$row['BillNumber']
                $txType = [string]$row['TransactionType']
                $lineDiscount = [double]$row['LineDiscount']
                $netAmount = [double]$row['NetAmount']
                $quantity = [double]$row['Quantity']
                
                # Đếm bill
                if ($billNum -notin $billNumbers) {
                    $billNumbers += $billNum
                    $Metrics.TotalBills++
                    
                    if ($txType -eq 'ART_SALE') {
                        $Metrics.SalesBills++
                    }
                    else {
                        $Metrics.ReturnBills++
                    }
                }
                
                # ✅ NEW: Tách doanh thu bán vs trả
                if ($txType -eq 'ART_SALE') {
                    $Metrics.SalesAmount += $netAmount
                }
                else {
                    $Metrics.ReturnAmount += $netAmount
                }
                
                # ✅ NEW: Đếm bill được discount
                if ($lineDiscount -gt 0 -and $billNum -notin $discountedBillNumbers) {
                    $discountedBillNumbers += $billNum
                    $Metrics.DiscountedBills++
                }
                
                # Cộng dồn các trường
                $Metrics.TotalDiscount += $lineDiscount
                $Metrics.TotalAmount += $netAmount
                $Metrics.TotalQuantity += $quantity
            }
            
            Write-Host "   ✅ Bills: $($Metrics.TotalBills) (Sales: $($Metrics.SalesBills), Returns: $($Metrics.ReturnBills))" -ForegroundColor Green
            Write-Host "   💰 Sales: $([math]::Round($Metrics.SalesAmount, 0)) VND | Returns: $([math]::Round($Metrics.ReturnAmount, 0)) VND | Discounted Bills: $($Metrics.DiscountedBills)" -ForegroundColor Green
            Write-Host "   💎 Total: $([math]::Round($Metrics.TotalAmount, 0)) VND (Discount: $([math]::Round($Metrics.TotalDiscount, 0)) VND)" -ForegroundColor Green
            
            # ============================================================
            # BUILD JSON FOR THIS DAY (UPDATED v7)
            # ============================================================
            
            $jsonObject = @{
                summary = @{
                    exportDate = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
                    businessDate = $dateStr
                    storeID = $Metrics.StoreID
                    totalBills = $Metrics.TotalBills
                    salesBills = $Metrics.SalesBills
                    returnBills = $Metrics.ReturnBills
                    salesAmount = [math]::Round($Metrics.SalesAmount, 2)      # ✅ NEW
                    returnAmount = [math]::Round($Metrics.ReturnAmount, 2)    # ✅ NEW
                    discountedBills = $Metrics.DiscountedBills                # ✅ NEW
                    totalQuantity = [math]::Round($Metrics.TotalQuantity, 0)
                    totalDiscount = [math]::Round($Metrics.TotalDiscount, 2)
                    totalAmount = [math]::Round($Metrics.TotalAmount, 2)
                }
            }
            
            # ============================================================
            # SAVE JSON LOCALLY
            # ============================================================
            
            $dateStrNoHyphen = $dateToSync.ToString('yyyyMMdd')
            $JsonFileName = "DailySales_$($Metrics.StoreID)_$dateStrNoHyphen.json"
            $JsonFilePath = Join-Path $JsonOutputPath $JsonFileName
            
            $jsonContent = $jsonObject | ConvertTo-Json -Depth 10
            Set-Content -Path $JsonFilePath -Value $jsonContent -Encoding UTF8
            Write-Host "   📝 Local: $JsonFileName" -ForegroundColor Gray
            
            # ============================================================
            # UPLOAD TO FIREBASE (.NET WebClient - NO WARNINGS)
            # ============================================================
            
            Write-Host "   📤 Firebase: Uploading..." -ForegroundColor Yellow
            
            try {
                $body = @{
                    fields = @{
                        summary = @{
                            mapValue = @{
                                fields = @{
                                    exportDate = @{ stringValue = $jsonObject.summary.exportDate }
                                    businessDate = @{ stringValue = $jsonObject.summary.businessDate }
                                    storeID = @{ integerValue = [string]$jsonObject.summary.storeID }
                                    totalBills = @{ integerValue = [string]$jsonObject.summary.totalBills }
                                    salesBills = @{ integerValue = [string]$jsonObject.summary.salesBills }
                                    returnBills = @{ integerValue = [string]$jsonObject.summary.returnBills }
                                    salesAmount = @{ doubleValue = $jsonObject.summary.salesAmount }          # ✅ NEW
                                    returnAmount = @{ doubleValue = $jsonObject.summary.returnAmount }       # ✅ NEW
                                    discountedBills = @{ integerValue = [string]$jsonObject.summary.discountedBills }  # ✅ NEW
                                    totalQuantity = @{ integerValue = [string]$jsonObject.summary.totalQuantity }
                                    totalDiscount = @{ doubleValue = $jsonObject.summary.totalDiscount }
                                    totalAmount = @{ doubleValue = $jsonObject.summary.totalAmount }
                                }
                            }
                        }
                    }
                } | ConvertTo-Json -Depth 100
                
                # Create document ID with StoreID (PREVENT OVERWRITE between stores)
                $firebaseDocId = "DailySales_$($Metrics.StoreID)_$dateStrNoHyphen"
                
                Write-Host "   📌 Document: $firebaseDocId" -ForegroundColor Gray
                
                $Uri = "https://firestore.googleapis.com/v1/projects/$FirebaseProjectId/databases/(default)/documents/$FirebaseCollection/$firebaseDocId`?key=$FirebaseApiKey"
                
                # Upload using .NET WebClient (NO WARNINGS)
                $uploadResult = Upload-ToFirebase -Uri $Uri -JsonBody $body
                
                if ($uploadResult.Success) {
                    Write-Host "   ✅ Firebase: Success! (200)" -ForegroundColor Green
                    
                    $syncResults += @{
                        date = $dateStr
                        status = "SUCCESS"
                        records = $recordCount
                        salesAmount = $Metrics.SalesAmount
                        returnAmount = $Metrics.ReturnAmount
                        discountedBills = $Metrics.DiscountedBills
                        totalAmount = $Metrics.TotalAmount
                        bills = $Metrics.TotalBills
                    }
                    
                    $successCount++
                }
                else {
                    Write-Host "   ❌ Firebase: FAILED - $($uploadResult.Error)" -ForegroundColor Red
                    
                    $syncResults += @{
                        date = $dateStr
                        status = "FAILED"
                        records = $recordCount
                        error = $uploadResult.Error
                    }
                    
                    $failureCount++
                }
                
            }
            catch {
                Write-Host "   ❌ Firebase: ERROR - $_" -ForegroundColor Red
                
                $syncResults += @{
                    date = $dateStr
                    status = "ERROR"
                    records = $recordCount
                    error = $_
                }
                
                $failureCount++
            }
            
        }
        catch {
            Write-Host "   ❌ Query failed: $_" -ForegroundColor Red
            
            $syncResults += @{
                date = $dateStr
                status = "ERROR"
                error = $_
            }
            
            $failureCount++
        }
    }
    
    # ============================================================
    # SUMMARY REPORT FOR THIS STORE
    # ============================================================
    
    Write-Host ""
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
    Write-Host ""
    Write-Host "📊 Store $StoreId - Summary" -ForegroundColor Cyan
    Write-Host ""
    
    $syncResults | ForEach-Object {
        $status = $_.status
        $date = $_.date
        
        if ($status -eq "SUCCESS") {
            Write-Host "   ✅ $date" -ForegroundColor Green
            Write-Host "      Bills: $($_.bills) | Sales: $([math]::Round($_.salesAmount, 0)) VND | Returns: $([math]::Round($_.returnAmount, 0)) VND | Discounted: $($_.discountedBills)" -ForegroundColor Gray
        }
        elseif ($status -eq "NO_DATA") {
            Write-Host "   ⚠️  $date - No sales" -ForegroundColor Yellow
        }
        else {
            Write-Host "   ❌ $date - $status" -ForegroundColor Red
        }
    }
    
    Write-Host ""
    Write-Host "📈 Store $StoreId Results:" -ForegroundColor Cyan
    Write-Host "   ✅ Success: $successCount days" -ForegroundColor Green
    Write-Host "   ❌ Failed: $failureCount days" -ForegroundColor Red
    Write-Host "   ⏭️  Skipped: $($syncResults.Count - $successCount - $failureCount) days" -ForegroundColor Yellow
    Write-Host ""
    
    # Store results for final summary
    $totalResults += @{
        storeId = $StoreId
        success = $successCount
        failed = $failureCount
        skipped = $syncResults.Count - $successCount - $failureCount
    }
}

# ============================================================
# FINAL SUMMARY - ALL STORES
# ============================================================

Write-Host ""
Write-Host "====================================================" -ForegroundColor Green
Write-Host "📊 FINAL SUMMARY - ALL STORES" -ForegroundColor Green
Write-Host "====================================================" -ForegroundColor Green
Write-Host ""

$totalResults | ForEach-Object {
    Write-Host "Store $($_.storeId):" -ForegroundColor Cyan
    Write-Host "   ✅ Success: $($_.success) days | ❌ Failed: $($_.failed) days | ⏭️  Skipped: $($_.skipped) days" -ForegroundColor Gray
}

Write-Host ""
Write-Host "🛡️  Auto-Detect: $($storesList.Count) store(s) synced successfully" -ForegroundColor Green
Write-Host "✨ v7 Enhanced Metrics: Sales Amount | Return Amount | Discounted Bills" -ForegroundColor Magenta
Write-Host ""
Write-Host "✨ Done! $(Get-Date -Format 'MM/dd/yyyy HH:mm:ss')" -ForegroundColor Green
Write-Host ""

