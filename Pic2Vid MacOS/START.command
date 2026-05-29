#!/bin/bash
cd "$(dirname "$0")"

# Kiểm tra Node.js
if ! command -v node &> /dev/null; then
  echo "❌ Chưa cài Node.js!"
  echo "👉 Tải về tại: https://nodejs.org (chọn bản LTS)"
  echo "   Cài xong rồi chạy lại file này."
  read -p "Nhấn Enter để thoát..."
  exit 1
fi

# Cài packages nếu chưa có
if [ ! -d "node_modules" ]; then
  echo "📦 Đang cài packages lần đầu (chờ 1-2 phút)..."
  npm install
fi

# Kiểm tra profile login
if [ ! -d "profile_gemini" ] || [ ! -d "profile_meta" ]; then
  echo ""
  echo "⚠️  Chưa đăng nhập Gemini và Meta AI!"
  echo "👉 Chạy lệnh sau để đăng nhập:"
  echo "   node export-cookies.js"
  echo ""
  read -p "Nhấn Enter để thoát..."
  exit 1
fi

# Mở browser tự động
echo "🚀 Đang khởi động Product Studio..."
sleep 1
open "http://localhost:3000" &

# Chạy server
node server.js
