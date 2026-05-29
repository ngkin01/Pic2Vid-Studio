@echo off
cd /d %~dp0
title Product Studio - Setup lan dau

echo ======================================
echo    SETUP LAN DAU - PRODUCT STUDIO
echo ======================================
echo.

:: Kiem tra Node.js
node -v >nul 2>&1
if %errorlevel% neq 0 (
    echo X Chua cai Node.js!
    echo.
    echo   Vui long tai ve tai: https://nodejs.org
    echo   Chon ban LTS ^(nut xanh lon^)
    echo   Cai xong roi chay lai file nay.
    echo.
    pause
    exit /b 1
)

echo OK Node.js da co:
node -v
echo.

:: Cai packages
echo Dang cai packages ^(cho 1-2 phut^)...
call npm install
if %errorlevel% neq 0 (
    echo X Loi khi cai packages!
    pause
    exit /b 1
)
echo.

:: Cai Playwright browser
echo Dang cai trinh duyet tu dong...
call npx playwright install chromium
echo.

:: Dang nhap
echo ======================================
echo    BUOC DANG NHAP
echo ======================================
echo.
echo Sap mo 2 cua so Chrome de ban dang nhap:
echo   1. Gemini ^(dang nhap bang Google^)
echo   2. Meta AI ^(dang nhap bang Facebook^)
echo.
echo Moi lan dang nhap xong - quay lai day nhan Enter
echo.
pause
node export-cookies.js

echo.
echo ======================================
echo OK Setup hoan tat!
echo.
echo Tu gio chi can double-click START.bat de dung
echo ======================================
echo.
pause
