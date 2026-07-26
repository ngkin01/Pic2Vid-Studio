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
echo Sap mo lan luot 2 cua so Chrome de ban dang nhap:
echo   1. Gemini    ^(dang nhap bang tai khoan Google^)
echo   2. Vibes.ai  ^(dang nhap bang tai khoan Vibes.ai^)
echo.
echo Dang nhap xong o moi cua so thi DONG cua so Chrome do lai.
echo Script se TU DONG chuyen sang buoc tiep theo, khong can quay lai
echo day nhan Enter.
echo.
pause

echo.
echo --- [1/2] Dang nhap Gemini (Slot 1) ---
call node add-account.js gemini 1

echo.
echo --- [2/2] Dang nhap Vibes.ai (Slot 1) ---
call node add-account.js vibes 1

echo.
echo ======================================
echo OK Setup hoan tat!
echo.
echo Tu gio chi can double-click START.bat de dung.
echo ^(Neu muon them tai khoan du phong, chay:
echo   node add-account.js gemini 2
echo   node add-account.js vibes 2 ^)
echo ======================================
echo.
pause