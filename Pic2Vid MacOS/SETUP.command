#!/bin/bash
cd "$(dirname "$0")"

echo "======================================"
echo "   SETUP LẦN ĐẦU — PRODUCT STUDIO"
echo "======================================"
echo ""

# Kiểm tra Node.js
if ! command -v node &> /dev/null; then
  echo "❌ Chưa cài Node.js!"
  echo "👉 Tải về tại: https://nodejs.org (chọn bản LTS)"
  echo "   Cài xong rồi chạy lại file này."
  read -p "Nhấn Enter để thoát..."
  exit 1
fi

echo "✅ Node.js đã có: $(node -v)"
echo ""

# Cài packages
echo "📦 Đang cài packages..."
npm install
echo ""

# Cài Playwright browser
echo "🌐 Đang cài trình duyệt tự động..."
npx playwright install chromium
echo ""

# Login
echo "======================================"
echo "   BƯỚC ĐĂNG NHẬP"
echo "======================================"
echo ""
echo "Sắp mở 2 cửa sổ Chrome để bạn đăng nhập:"
echo "  1. Gemini (đăng nhập bằng Google)"
echo "  2. Meta AI (đăng nhập bằng Facebook)"
echo ""
echo "Mỗi lần đăng nhập xong → quay lại đây nhấn Enter"
echo ""
read -p "Nhấn Enter để bắt đầu đăng nhập Gemini..."
node export-cookies.js

echo ""
echo "======================================"
echo "✅ Setup hoàn tất!"
echo ""
echo "👉 Từ giờ chỉ cần double-click start.sh để dùng"
echo "======================================"
read -p "Nhấn Enter để thoát..."
