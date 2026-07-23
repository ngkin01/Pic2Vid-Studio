/**
 * add-account.js — Đăng nhập thêm account Gemini hoặc Meta AI
 *
 * Cách dùng:
 *   node add-account.js gemini 2   → mở Chrome profile_gemini_2 để đăng nhập
 *   node add-account.js meta 2     → mở Chrome profile_meta_2 để đăng nhập
 *   node add-account.js vibes 1    → mở Chrome profile_vibes_1 để đăng nhập
 *   node add-account.js gemini 3   → mở Chrome profile_gemini_3 để đăng nhập
 *
 * Sau khi đăng nhập xong, đóng cửa sổ Chrome lại.
 * Server sẽ tự động dùng account mới khi account cũ hết quota.
 */

const { chromium } = require("playwright");
const path = require("path");
const fs = require("fs");

const service = process.argv[2]; // "gemini" hoặc "meta"
const slot    = process.argv[3]; // "1", "2", "3"...

if (!service || !slot || !["gemini", "meta", "vibes"].includes(service)) {
  console.log("❌ Cách dùng: node add-account.js [gemini|meta|vibes] [số slot]");
  console.log("   Ví dụ: node add-account.js gemini 2");
  console.log("   Ví dụ: node add-account.js meta 2");
  console.log("   Ví dụ: node add-account.js vibes 1");
  process.exit(1);
}

const profileDir = path.join(__dirname, `profile_${service}_${slot}`);
const loginUrls = {
  gemini: "https://gemini.google.com",
  meta: "https://meta.ai",
  vibes: "https://vibes.ai",
};
const loginUrl = loginUrls[service];
const serviceLabel = { gemini: "Gemini (Google)", meta: "Meta AI", vibes: "Vibes.ai" }[service];

console.log(`\n🔐 Mở Chrome để đăng nhập ${serviceLabel} — Slot ${slot}`);
console.log(`📂 Profile: ${profileDir}`);
console.log(`🌐 URL: ${loginUrl}`);
console.log(`\n👉 Đăng nhập xong thì đóng cửa sổ Chrome lại.\n`);

(async () => {
  const ctx = await chromium.launchPersistentContext(profileDir, {
    headless: false,
    channel: "chrome",
    args: ["--disable-blink-features=AutomationControlled"],
    viewport: { width: 1280, height: 800 },
  });

  const page = await ctx.newPage();
  await page.goto(loginUrl, { waitUntil: "domcontentloaded" });

  console.log("✅ Chrome đã mở. Đăng nhập vào tài khoản rồi đóng cửa sổ lại.");

  // Chờ user đóng browser
  await ctx.waitForEvent("close").catch(() => {});
  console.log(`\n✅ Đã lưu profile ${service}_${slot}. Server sẽ tự dùng account này khi cần.\n`);
})();
