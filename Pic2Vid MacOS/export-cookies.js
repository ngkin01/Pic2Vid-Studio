/**
 * CHẠY TRÊN LAPTOP — 1 LẦN DUY NHẤT
 * node export-cookies.js
 * 
 * Sẽ mở Chrome, bạn login Gemini + Meta AI,
 * rồi xuất ra cookies dán vào Render env vars.
 */

const { chromium } = require("playwright");

async function exportCookies(name, url, profileDir) {
  console.log(`\n🚀 Mở Chrome để login ${name}...`);
  console.log(`👉 Login xong nhấn ENTER\n`);

  const context = await chromium.launchPersistentContext(profileDir, {
    headless: false,
    channel: "chrome",
    args: ["--disable-blink-features=AutomationControlled"]
  });

  const page = await context.newPage();
  await page.goto(url);

  await new Promise(resolve => {
    process.stdin.resume();
    process.stdin.once("data", resolve);
  });

  const cookies = await context.cookies();
  const storage = await page.evaluate(() => ({
    localStorage: Object.fromEntries(Object.entries(localStorage)),
    sessionStorage: Object.fromEntries(Object.entries(sessionStorage))
  })).catch(() => ({}));

  const state = { cookies, origins: [{ origin: url, localStorage: storage.localStorage || {} }] };
  const encoded = Buffer.from(JSON.stringify(state)).toString("base64");

  console.log(`\n✅ ${name} cookies exported!`);
  console.log(`\n📋 Copy chuỗi sau vào Render env var "COOKIES_${name.toUpperCase()}":\n`);
  console.log(encoded);
  console.log(`\n${"─".repeat(60)}`);

  await context.close();
  return encoded;
}

(async () => {
  console.log("=== EXPORT COOKIES CHO RENDER ===\n");
  console.log("Sẽ lần lượt mở Gemini rồi Meta AI.\n");

  await exportCookies("GEMINI", "https://gemini.google.com/app", "./profile_gemini");
  await exportCookies("META", "https://meta.ai", "./profile_meta");

  console.log("\n🎉 Xong! Dán 2 chuỗi trên vào Render > Environment Variables.");
  process.exit(0);
})();
