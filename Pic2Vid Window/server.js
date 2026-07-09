const express = require("express");
const multer = require("multer");
const cors = require("cors");
const path = require("path");
const fs = require("fs");
const { v4: uuidv4 } = require("uuid");
const { chromium } = require("playwright");

const app = express();
const PORT = process.env.PORT || 3000;
const IS_LOCAL = !process.env.COOKIES_GEMINI;

app.use(cors());
app.use(express.json());
app.use("/outputs", express.static(path.join(__dirname, "outputs")));
app.use(express.static(path.join(__dirname, "public")));

["uploads", "outputs", "public"].forEach(dir => {
  const p = path.join(__dirname, dir);
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
});

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, path.join(__dirname, "uploads")),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || ".jpg";
    cb(null, `${Date.now()}_${Math.random().toString(36).slice(2,7)}${ext}`);
  }
});
const upload = multer({
  storage,
  limits: { fileSize: 20 * 1024 * 1024 },
  fileFilter: (req, file, cb) =>
    file.mimetype.startsWith("image/") ? cb(null, true) : cb(new Error("Images only"))
});

// ─── JOB STORE ────────────────────────────────────────
const jobs = {};
function createJob(id, filename, index, geminiPrompt, metaPrompt) {
  jobs[id] = {
    id, step: "queued", logs: [],
    enhancedImage: null, videoUrl: null,
    error: null, createdAt: Date.now(),
    filename: filename || "", index: index || 0,
    retryCount: 0,
    geminiPrompt: geminiPrompt || "",
    metaPrompt: metaPrompt || ""
  };
  return jobs[id];
}
function update(id, data) { if (jobs[id]) Object.assign(jobs[id], data); }
function log(id, msg) {
  if (jobs[id]) { jobs[id].logs.push(msg); console.log(`[${id.slice(0,8)}] ${msg}`); }
}

// ─── PIPELINE OVERLAP QUEUES ──────────────────────────
// Gemini queue và Meta AI queue chạy độc lập
// Gemini xong → tự đẩy vào Meta AI queue ngay
const geminiQueue = [];   // { jobId, imagePath, geminiPrompt, metaPrompt }
const metaQueue = [];
const vibesQueue = [];    // { groupId, tasks: [{jobId, imagePath}] } — 1 phan tu = 1 batch project

const GEMINI_CONCURRENCY = 3; // 3 tab Gemini song song
const META_CONCURRENCY = 2;   // 2 tab Meta AI song song
const VIBES_CONCURRENCY = 1;  // 1 account Vibes -> chay tuan tu
let geminiActive = 0;
let metaActive = 0;
let vibesActive = 0;

// Provider duoc chon cho moi group (mac dinh "meta")
// groupProvider[groupId] = "meta" | "vibes"
const groupProvider = {};
// Dung de tranh day trung 1 group vao vibesQueue nhieu lan
const vibesGroupQueued = new Set();

// Track batch groups de biet khi nao tat ca job xong thi clear cache
// batchGroups[groupId] = [jobId1, jobId2, ...]
const batchGroups = {};

async function checkAndClearCacheIfGroupDone(groupId) {
  const jobIds = batchGroups[groupId];
  if (!jobIds) return;
  // Kiem tra tat ca job da done hoac error (khong con queued/running)
  const allFinished = jobIds.every(id => {
    const job = jobs[id];
    if (!job) return true;
    return job.step === 'meta_done' || job.step === 'done' || job.step === 'error';
  });
  if (!allFinished) return;
  // Tat ca xong — close ctx (chi khi khong con tab active), clear cache
  console.log(`\n🧹 Batch group ${groupId.slice(0,8)} done — closing contexts & clearing cache...`);
  if (geminiActive === 0 && sharedCtx.gemini) {
    await sharedCtx.gemini.close().catch(() => {});
    sharedCtx.gemini = null;
  }
  if (metaActive === 0 && sharedCtx.meta) {
    await sharedCtx.meta.close().catch(() => {});
    sharedCtx.meta = null;
  }
  if (vibesActive === 0 && sharedCtx.vibes) {
    await sharedCtx.vibes.close().catch(() => {});
    sharedCtx.vibes = null;
  }
  const gSlots = getProfileSlots("gemini");
  const mSlots = getProfileSlots("meta");
  const vSlots = getProfileSlots("vibes");
  [...gSlots, ...mSlots, ...vSlots].forEach(({ profileDir }) => clearBrowserCache(profileDir));
  delete batchGroups[groupId];
  delete groupProvider[groupId];
  vibesGroupQueued.delete(groupId);
  console.log("✅ All done — browser windows closed.");
}

function processGeminiQueue() {
  while (geminiActive < GEMINI_CONCURRENCY && geminiQueue.length > 0) {
    const task = geminiQueue.shift();
    geminiActive++;
    update(task.jobId, { step: "gemini_running" });

    (async () => {
      try {
        const enhanced = await runGemini(task.jobId, task.imagePath, task.geminiPrompt);
        update(task.jobId, { step: "gemini_done", enhancedImage: enhanced });
        const provider = groupProvider[task.groupId] || "meta";

        if (provider === "vibes") {
          log(task.jobId, "✅ Gemini done — chờ cả batch xong Gemini để qua Vibes.ai");
          checkGeminiGroupDoneForVibes(task.groupId);
        } else {
          log(task.jobId, "✅ Gemini done — queued for Meta AI");
          const localPath = path.join(__dirname, enhanced.replace("/outputs/", "outputs/"));
          metaQueue.push({ jobId: task.jobId, groupId: task.groupId, enhancedPath: localPath, metaPrompt: task.metaPrompt });
          processMetaQueue();
        }

      } catch (err) {
        const retryCount = (jobs[task.jobId]?.retryCount || 0);
        const provider = groupProvider[task.groupId] || "meta";
        if (isQuotaError(err.message)) {
          log(task.jobId, `⚠️ Gemini quota/session: ${err.message.slice(0,80)}`);
          const rotated = await rotateAccount("gemini");
          if (rotated) {
            log(task.jobId, `🔄 Switched Gemini account, retrying...`);
            update(task.jobId, { step: "queued", retryCount: retryCount + 1, error: null });
            geminiQueue.unshift(task);
          } else {
            log(task.jobId, `❌ Gemini quota, no more accounts`);
            update(task.jobId, { step: "error", error: err.message });
            if (provider === "vibes") checkGeminiGroupDoneForVibes(task.groupId);
          }
        } else if (retryCount < 2) {
          update(task.jobId, { step: "queued", retryCount: retryCount + 1, error: null });
          log(task.jobId, `⚠️ Gemini failed (${err.message}) — retry ${retryCount + 1}/2...`);
          geminiQueue.push(task);
        } else {
          log(task.jobId, `❌ Gemini error after 2 retries: ${err.message}`);
          update(task.jobId, { step: "error", error: err.message });
          if (provider === "vibes") checkGeminiGroupDoneForVibes(task.groupId);
        }
      } finally {
        geminiActive--;
        // Khi Gemini queue trong va khong con tab nao chay -> close ctx, clear cache, mo lai
        if (geminiQueue.length === 0 && geminiActive === 0) {
          if (sharedCtx.gemini) {
            await sharedCtx.gemini.close().catch(() => {});
            sharedCtx.gemini = null;
          }
          const slot = currentSlot.gemini;
          if (slot) clearBrowserCache(slot.profileDir);
        }
        processGeminiQueue();
      }
    })();
  }
}

function processMetaQueue() {
  while (metaActive < META_CONCURRENCY && metaQueue.length > 0) {
    const task = metaQueue.shift();
    metaActive++;
    update(task.jobId, { step: "meta_running" });

    (async () => {
      try {
        const video = await runMetaAI(task.jobId, task.enhancedPath, task.metaPrompt);
        update(task.jobId, { step: "meta_done", videoUrl: video });
        log(task.jobId, "🎉 Done!");
        if (task.groupId) checkAndClearCacheIfGroupDone(task.groupId);

      } catch (err) {
        const retryCount = (jobs[task.jobId]?.retryCount || 0);
        if (isQuotaError(err.message)) {
          log(task.jobId, `⚠️ Meta AI quota/session: ${err.message.slice(0,80)}`);
          const rotated = await rotateAccount("meta");
          if (rotated) {
            log(task.jobId, `🔄 Switched Meta AI account, retrying...`);
            update(task.jobId, { step: "queued", retryCount: retryCount + 1, error: null });
            metaQueue.unshift(task);
          } else {
            log(task.jobId, `❌ Meta AI quota, no more accounts`);
            update(task.jobId, { step: "error", error: err.message });
            if (task.groupId) checkAndClearCacheIfGroupDone(task.groupId);
          }
        } else if (retryCount < 2) {
          update(task.jobId, { step: "queued", retryCount: retryCount + 1, error: null });
          log(task.jobId, `⚠️ Meta AI failed (${err.message}) — retry ${retryCount + 1}/2...`);
          metaQueue.push(task);
        } else {
          log(task.jobId, `❌ Meta AI error after 2 retries: ${err.message}`);
          update(task.jobId, { step: "error", error: err.message });
          if (task.groupId) checkAndClearCacheIfGroupDone(task.groupId);
        }
      } finally {
        metaActive--;
        processMetaQueue();
      }
    })();
  }
}

// Khi 1 job trong group xong (hoac loi) buoc Gemini, kiem tra xem CA group
// da xong buoc Gemini chua — neu roi thi gom danh sach anh thanh cong
// (giu dung thu tu goc, bo qua anh loi) va day 1 "batch task" vao vibesQueue.
function checkGeminiGroupDoneForVibes(groupId) {
  const jobIds = batchGroups[groupId];
  if (!jobIds) return;
  const allGeminiFinished = jobIds.every(id => {
    const job = jobs[id];
    if (!job) return true;
    return job.step === "gemini_done" || job.step === "error";
  });
  if (!allGeminiFinished) return;

  // Tranh day trung group 2 lan (vi ham nay co the duoc goi tu nhieu job cung luc)
  if (vibesGroupQueued.has(groupId)) return;
  vibesGroupQueued.add(groupId);

  const successTasks = jobIds
    .filter(id => jobs[id]?.step === "gemini_done")
    .map(id => ({
      jobId: id,
      imagePath: path.join(__dirname, jobs[id].enhancedImage.replace("/outputs/", "outputs/")),
      // FIX: doc lai metaPrompt tu jobs[id] (da luu tu luc tao job) — truoc day
      // successTasks khong mang theo metaPrompt nen luon bi undefined, khien
      // runVibesBatch luon fallback ve prompt mac dinh du nguoi dung da nhap gi.
      metaPrompt: jobs[id].metaPrompt || "",
    }));

  if (successTasks.length === 0) {
    log(jobIds[0], "⚠️ Không có ảnh nào Gemini thành công trong batch — bỏ qua Vibes.ai");
    checkAndClearCacheIfGroupDone(groupId);
    return;
  }

  log(successTasks[0].jobId, `✅ Cả batch xong Gemini (${successTasks.length}/${jobIds.length} ảnh thành công) — bắt đầu Vibes.ai`);
  vibesQueue.push({ groupId, tasks: successTasks });
  processVibesQueue();
}

function processVibesQueue() {
  while (vibesActive < VIBES_CONCURRENCY && vibesQueue.length > 0) {
    const batchTask = vibesQueue.shift();
    vibesActive++;
    batchTask.tasks.forEach(t => update(t.jobId, { step: "meta_running" })); // tai su dung step de UI khong doi

    (async () => {
      try {
        await runVibesBatch(batchTask.groupId, batchTask.tasks);
      } finally {
        vibesActive--;
        checkAndClearCacheIfGroupDone(batchTask.groupId);
        processVibesQueue();
      }
    })();
  }
}

// ─── BROWSER HELPER ───────────────────────────────────
function loadCookieState(envKey) {
  const raw = process.env[envKey];
  if (!raw) return null;
  try { return JSON.parse(Buffer.from(raw, "base64").toString("utf8")); }
  catch { return null; }
}

// ─── MULTI-ACCOUNT MANAGER ────────────────────────────
// Tim tat ca profile slot co san: profile_gemini_1, profile_gemini_2, ...
// Fallback ve profile_gemini neu khong co slot nao
function getProfileSlots(service) {
  const slots = [];
  // Legacy profile truoc (slot 0) — dung truoc, het quota moi sang slot tiep
  const legacy = path.join(__dirname, `profile_${service}`);
  if (fs.existsSync(legacy)) slots.push({ slot: 0, profileDir: `profile_${service}` });
  // Cac slot co so: profile_gemini_2, profile_gemini_3, ...
  for (let i = 1; i <= 10; i++) {
    const p = path.join(__dirname, `profile_${service}_${i}`);
    if (fs.existsSync(p)) slots.push({ slot: i, profileDir: `profile_${service}_${i}` });
  }
  return slots;
}

// Track slot hien tai cho moi service
const currentSlot = { gemini: null, meta: null, vibes: null };
// Shared contexts — 1 browser per service
const sharedCtx = { gemini: null, meta: null, vibes: null };
const ctxLock   = { gemini: false, meta: false, vibes: false };

// Keyword nhan biet loi quota/session -> can rotate account
const QUOTA_KEYWORDS = [
  "quota", "rate limit", "limit exceeded", "too many requests",
  "session expired", "not logged in", "sign in", "429",
];
function isQuotaError(msg) {
  const m = (msg || "").toLowerCase();
  return QUOTA_KEYWORDS.some(k => m.includes(k));
}

// Rotate sang slot tiep theo, tra ve true neu thanh cong
async function rotateAccount(service) {
  const slots = getProfileSlots(service);
  if (slots.length <= 1) {
    console.log(`[${service}] Chi co 1 account, khong the rotate. Them account: node add-account.js ${service} 2`);
    return false;
  }
  const cur = currentSlot[service];
  const curIdx = slots.findIndex(s => s.slot === (cur ? cur.slot : -1));
  const nextIdx = (curIdx + 1) % slots.length;
  const next = slots[nextIdx];
  if (next.slot === (cur ? cur.slot : -999)) {
    console.log(`[${service}] Da thu het tat ca ${slots.length} account`);
    return false;
  }
  console.log(`[${service}] Rotating account: slot ${cur ? cur.slot : "legacy"} -> slot ${next.slot} (${next.profileDir})`);
  if (sharedCtx[service]) {
    await sharedCtx[service].close().catch(() => {});
    sharedCtx[service] = null;
  }
  // Clear cache profile moi truoc khi dung
  clearBrowserCache(next.profileDir);
  currentSlot[service] = next;
  return true;
}

async function getSharedPage(service) {
  const cookieKeyMap = { gemini: "COOKIES_GEMINI", meta: "COOKIES_META", vibes: "COOKIES_VIBES" };
  const cookieKey = cookieKeyMap[service];

  // Cho neu thread khac dang khoi tao context
  while (ctxLock[service]) await new Promise(r => setTimeout(r, 200));

  // Neu context da co va con song -> mo tab moi
  if (sharedCtx[service]) {
    try {
      return await sharedCtx[service].newPage();
    } catch (_) {
      sharedCtx[service] = null;
    }
  }

  ctxLock[service] = true;
  try {
    // Double-check sau khi lay duoc lock — job khac co the da tao xong
    if (sharedCtx[service]) {
      return await sharedCtx[service].newPage();
    }

    if (IS_LOCAL) {
      // Lay slot hien tai, neu chua co thi lay slot dau tien
      if (!currentSlot[service]) {
        const slots = getProfileSlots(service);
        if (slots.length === 0) throw new Error(`Khong tim thay profile nao cho ${service}. Chay: node add-account.js ${service} 1`);
        currentSlot[service] = slots[0];
      }
      const { profileDir, slot } = currentSlot[service];
      console.log(`[${service}] Opening profile: ${profileDir} (slot ${slot})`);
      sharedCtx[service] = await chromium.launchPersistentContext(
        path.join(__dirname, profileDir),
        { headless: false, channel: "chrome", acceptDownloads: true,
          args: ["--disable-blink-features=AutomationControlled"] }
      );
      // Dung lai tab about:blank co san thay vi mo tab moi
      const existingPages = sharedCtx[service].pages();
      if (existingPages.length > 0) return existingPages[0];
    } else {
      const cookieState = loadCookieState(cookieKey);
      if (!cookieState) throw new Error(`${cookieKey} not set`);
      const browser = await chromium.launch({
        headless: true,
        args: ["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage","--disable-blink-features=AutomationControlled"]
      });
      const ctx = await browser.newContext({
        userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        viewport: { width: 1280, height: 800 }
      });
      if (cookieState?.cookies?.length) await ctx.addCookies(cookieState.cookies);
      sharedCtx[service] = ctx;
    }
    // Cloud mode: newPage()
    return await sharedCtx[service].newPage();
  } finally {
    ctxLock[service] = false;
  }
}


// Clear cache cua browser profile -- CHI xoa cache, KHONG dung cookies/session
function clearBrowserCache(profileDir) {
  const safeCacheFoldersInDefault = [
    "Cache", "Cache_Data", "Code Cache", "GPUCache",
    "DawnCache", "ShaderCache", "blob_storage",
    "GrShaderCache", "GraphiteDawnCache",
    "BrowserMetrics", "DeferredBrowserMetrics",
    "extensions_crx_cache", "component_crx_cache",
    "Crashpad", "Safe Browsing", "segmentation_platform",
  ];
  const safeCacheFoldersTopLevel = [
    "Cache", "Code Cache", "GPUCache", "ShaderCache",
    "GrShaderCache", "GraphiteDawnCache",
    "BrowserMetrics", "DeferredBrowserMetrics",
    "extensions_crx_cache", "component_crx_cache",
    "Crashpad", "Safe Browsing", "segmentation_platform",
  ];
  let cleared = 0;
  const profilePath = path.join(__dirname, profileDir, "Default");
  if (fs.existsSync(profilePath)) {
    for (const folder of safeCacheFoldersInDefault) {
      const p = path.join(profilePath, folder);
      if (fs.existsSync(p)) {
        try { fs.rmSync(p, { recursive: true, force: true }); cleared++; } catch (_) {}
      }
    }
  }
  const topPath = path.join(__dirname, profileDir);
  for (const folder of safeCacheFoldersTopLevel) {
    const p = path.join(topPath, folder);
    if (fs.existsSync(p)) {
      try { fs.rmSync(p, { recursive: true, force: true }); cleared++; } catch (_) {}
    }
  }
  if (cleared > 0) console.log(`🧹 Cleared ${cleared} cache folders from ${profileDir}`);
}

async function closeSharedCtx(service) {
  if (sharedCtx[service]) {
    await sharedCtx[service].close().catch(() => {});
    sharedCtx[service] = null;
  }
}

// ─── GEMINI ───────────────────────────────────────────
async function runGemini(jobId, imagePath, prompt) {
  log(jobId, "\u{1F680} Opening Gemini...");
  const page = await getSharedPage("gemini");
  try {
    // Intercept response: bat anh full-size khi click download button
    // (Gemini chi request lh3.googleusercontent.com khi download, khong phai luc render)
    let capturedBuf = null;
    page.on("response", async (res) => {
      try {
        const url = res.url();
        const ct = res.headers()["content-type"] || "";
        if (
          capturedBuf === null &&
          ct.startsWith("image/") &&
          res.status() === 200 &&
          url.includes("googleusercontent.com") &&
          !url.includes("gstatic.com")
        ) {
          const buf = await res.body().catch(() => null);
          if (buf && buf.length > 200 * 1024) {
            capturedBuf = buf;
            log(jobId, `\u{1F4F8} Intercepted: ${url.slice(0, 70)} (${(buf.length/1024).toFixed(0)}KB)`);
          }
        }
      } catch (_) {}
    });

    await page.goto("https://gemini.google.com/app/new", { waitUntil: "domcontentloaded", timeout: 60000 });
    // Cho Gemini load xong thay vi wait cung 8s
    await page.waitForSelector('[role="textbox"], textarea, div[contenteditable="true"]', { timeout: 30000 }).catch(() => {});
    if (page.url().includes("accounts.google.com")) throw new Error("Gemini session expired");

    log(jobId, "\u{1F4E4} Uploading image...");
    await page.click('button[aria-label*="Upload"], button[aria-label*="upload"], button[aria-label*="Add"]').catch(() => {});
    await page.waitForTimeout(2000);
    await page.locator("text=Upload files").first().click().catch(() => {});
    await page.waitForTimeout(3000);

    let fileInput = null;
    for (let i = 0; i < 20; i++) {
      fileInput = await page.$('input[type="file"]');
      if (fileInput) break;
      await page.waitForTimeout(1000);
    }
    if (!fileInput) throw new Error("Upload input not found on Gemini");
    await fileInput.setInputFiles(imagePath);
    log(jobId, "\u2705 Image uploaded");
    await page.waitForTimeout(5000);

    // Tim prompt box
    let promptBox = null;
    for (let i = 0; i < 30; i++) {
      let box = await page.$("textarea");
      if (box && await box.isVisible().catch(() => false)) { promptBox = box; break; }
      box = await page.$('div[contenteditable="true"]');
      if (box) { promptBox = box; break; }
      await page.waitForTimeout(2000);
    }
    if (!promptBox) throw new Error("Prompt box not found");

    // Snapshot anh hien co TRUOC khi gui prompt (de poll anh moi)
    const existingImgSrcs = new Set();
    for (const img of await page.$$("img")) {
      try {
        const src = await img.getAttribute("src");
        if (src) existingImgSrcs.add(src);
      } catch (_) {}
    }

    await promptBox.fill(prompt);
    await promptBox.press("Enter");
    log(jobId, "\u23F3 Waiting for Gemini to generate image...");

    // Poll anh moi (max 2.5 phut, moi 3s)
    let foundImg = null;
    for (let attempt = 0; attempt < 50; attempt++) {
      await page.waitForTimeout(3000);
      // Neu da bat duoc response image -> van can tim element de confirm xong
      let largestArea = 0;
      let bestImg = null;
      for (const img of await page.$$("img")) {
        try {
          const src = await img.getAttribute("src");
          if (src && existingImgSrcs.has(src)) continue;
          const box = await img.boundingBox();
          if (!box || box.width < 200 || box.height < 200) continue;
          const area = box.width * box.height;
          if (area > largestArea) { largestArea = area; bestImg = img; }
        } catch (_) {}
      }
      if (bestImg && largestArea > 80000) {
        foundImg = bestImg;
        log(jobId, `\u2705 Image generated (${(attempt + 1) * 3}s)`);
        break;
      }
      if (attempt % 10 === 0 && attempt > 0) log(jobId, `\u23F3 Still waiting... (${attempt * 3}s)`);
    }

    if (!foundImg) throw new Error("Generated image not found after 2.5 minutes");

    const outPath = path.join(__dirname, "outputs", `${jobId}_enhanced.png`);

    // Method 1: hover anh -> click download button -> intercept response full-size
    // Gemini chi request lh3.googleusercontent.com khi click download (khong phai luc render)
    try {
      const imgBox = await foundImg.boundingBox();
      await foundImg.scrollIntoViewIfNeeded().catch(() => {});
      await page.mouse.move(imgBox.x + imgBox.width / 2, imgBox.y + imgBox.height / 2);
      await page.waitForTimeout(1500);

      const downloadBtn =
        await page.$('button[aria-label*="Download full size"]') ||       // EN
        await page.$('button[aria-label*="Download image"]') ||           // EN alt
        await page.$('button[aria-label*="download"]') ||                 // EN lowercase
        await page.$('button[aria-label*="T\u1EA3i \u1EA3nh c\u00F3 k\u00EDch th\u01B0\u1EDBc"]') ||   // VI: "Tải ảnh có kích thước..."
        await page.$('button[aria-label*="T\u1EA3i xu\u1ED1ng"]') ||               // VI: "Tải xuống"
        await page.$('button[aria-label*="t\u1EA3i xu\u1ED1ng"]') ||               // VI lowercase
        await page.$('button[aria-label*="Download"]');                   // EN generic fallback

      if (downloadBtn && await downloadBtn.isVisible().catch(() => false)) {
        capturedBuf = null; // reset truoc khi click
        await downloadBtn.click();
        // Cho intercept bat duoc response (toi da 10s)
        for (let w = 0; w < 20 && !capturedBuf; w++) await page.waitForTimeout(500);
        if (capturedBuf) {
          fs.writeFileSync(outPath, capturedBuf);
          log(jobId, `\u2705 Image saved (intercept, ${(capturedBuf.length / 1024).toFixed(0)}KB)`);
          return `/outputs/${jobId}_enhanced.png`;
        }
        // Intercept khong bat duoc -> thu Playwright download event
        const dl = await page.waitForEvent("download", { timeout: 10000 }).catch(() => null);
        if (dl) {
          await dl.saveAs(outPath);
          const size = fs.statSync(outPath).size;
          log(jobId, `\u2705 Image saved (download event, ${(size / 1024).toFixed(0)}KB)`);
          return `/outputs/${jobId}_enhanced.png`;
        }
      }
    } catch (e) {
      log(jobId, `\u26A0\uFE0F Download btn failed: ${e.message.slice(0, 50)}`);
    }

    // Method 2: canvas fallback (render resolution)
    log(jobId, "\u26A0\uFE0F Falling back to canvas...");
    const base64 = await page.evaluate((imgEl) => {
      return new Promise((resolve, reject) => {
        const w = imgEl.naturalWidth || imgEl.width;
        const h = imgEl.naturalHeight || imgEl.height;
        const canvas = document.createElement("canvas");
        canvas.width = w;
        canvas.height = h;
        canvas.getContext("2d").drawImage(imgEl, 0, 0, w, h);
        resolve(canvas.toDataURL("image/png").split(",")[1]);
      });
    }, foundImg);
    fs.writeFileSync(outPath, Buffer.from(base64, "base64"));
    const size = fs.statSync(outPath).size;
    const dims = await page.evaluate((imgEl) => ({ w: imgEl.naturalWidth, h: imgEl.naturalHeight }), foundImg).catch(() => ({ w: 0, h: 0 }));
    log(jobId, `\u2705 Image saved (canvas, ${dims.w}x${dims.h}, ${(size / 1024).toFixed(0)}KB)`);

    return `/outputs/${jobId}_enhanced.png`;
  } finally {
    await page.close().catch(() => {});
  }
}

// ─── META AI ──────────────────────────────────────────
async function runMetaAI(jobId, imagePath, prompt) {
  log(jobId, "🚀 Opening Meta AI...");
  const page = await getSharedPage("meta");
  try {
    await page.goto("https://meta.ai", { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForTimeout(8000);

    const fileInput = await page.$('input[type="file"]');
    if (!fileInput) throw new Error("Not logged in to Meta AI");

    log(jobId, "📤 Uploading image...");
    await fileInput.setInputFiles(imagePath);
    log(jobId, "✅ Image uploaded");
    await page.mouse.click(200, 200);
    await page.waitForTimeout(12000);

    let promptBox = null;
    for (let i = 0; i < 30; i++) {
      let box = await page.$("textarea");
      if (box && await box.isVisible().catch(() => false)) { promptBox = box; break; }
      box = await page.$('div[contenteditable="true"]');
      if (box) { promptBox = box; break; }
      await page.waitForTimeout(2000);
    }
    if (!promptBox) throw new Error("Prompt box not found on Meta AI");
    await promptBox.fill(prompt);
    await promptBox.press("Enter");
    log(jobId, "⏳ Generating video (~3 min)...");

    let videoUrl = null;
    for (let i = 0; i < 72; i++) {
      await page.waitForTimeout(5000);
      const video = await page.$("video");
      if (video) {
        const src = await video.getAttribute("src");
        if (src && !src.startsWith("blob:")) { videoUrl = src; break; }
      }
      const links = await page.$$('a[href*=".mp4"], a[download]');
      for (const link of links) {
        const href = await link.getAttribute("href");
        if (href && href.includes(".mp4")) { videoUrl = href; break; }
      }
      if (videoUrl) break;
      if (i % 6 === 0) log(jobId, `⏳ Still generating... (${Math.round(i * 5 / 60)} min)`);
    }

    if (!videoUrl) {
      for (const btn of await page.$$("button, a")) {
        try {
          const txt = `${await btn.getAttribute("aria-label")||""} ${await btn.innerText().catch(()=>"")}`.toLowerCase();
          if (txt.includes("download")) {
            const dlPromise = page.waitForEvent("download", { timeout: 30000 });
            await btn.click();
            const dl = await dlPromise;
            const outPath = path.join(__dirname, "outputs", `${jobId}_video.mp4`);
            await dl.saveAs(outPath);
            log(jobId, "✅ Video downloaded");
            return `/outputs/${jobId}_video.mp4`;
          }
        } catch (_) {}
      }
      throw new Error("Video not found after 6 minutes");
    }

    log(jobId, "📥 Downloading video...");
    const outPath = path.join(__dirname, "outputs", `${jobId}_video.mp4`);
    const buf = await page.evaluate(async (url) => {
      const r = await fetch(url);
      const ab = await r.arrayBuffer();
      return Array.from(new Uint8Array(ab));
    }, videoUrl);
    fs.writeFileSync(outPath, Buffer.from(buf));
    log(jobId, "✅ Video saved");
    return `/outputs/${jobId}_video.mp4`;
  } finally {
    await page.close().catch(() => {});
  }
}

// ─── VIBES.AI ─────────────────────────────────────────
// Lay danh sach "tile" (anh hoac video) trong khu vuc noi dung chinh cua project,
// sap xep theo thu tu doc-hieu: hang tren truoc, trong 1 hang thi trai truoc.
// Bo qua sidebar ben trai (~240px) va cac icon nho (khong phai tile that).
async function getMediaTiles(page) {
  const handles = await page.$$("img, video");
  const withBox = [];
  for (const h of handles) {
    try {
      if (!(await h.isVisible())) continue;
      const box = await h.boundingBox();
      if (!box) continue;
      if (box.x < 80) continue; // sidebar (chi con icon rail khi o trong 1 project)
      if (box.width < 100 || box.height < 100) continue; // icon nho, khong phai tile that
      withBox.push({ handle: h, box });
    } catch (_) {}
  }
  withBox.sort((a, b) => {
    const rowDiff = Math.round(a.box.y / 50) - Math.round(b.box.y / 50);
    if (rowDiff !== 0) return rowDiff;
    return a.box.x - b.box.x;
  });
  // Dedup: nhieu web app render 2 the <img> chong len nhau cho CUNG 1 o
  // (vd anh placeholder mo + anh that) — chi giu 1 phan tu cho moi vi tri toa do.
  const deduped = [];
  for (const item of withBox) {
    const isDup = deduped.some(d => Math.abs(d.box.x - item.box.x) < 15 && Math.abs(d.box.y - item.box.y) < 15);
    if (!isDup) deduped.push(item);
  }
  return deduped.map(w => w.handle);
}

// Chup screenshot khi loi xay ra de debug — luu vao outputs/ de Hi xem duoc qua URL
async function debugScreenshot(page, jobId, label) {
  try {
    const fileName = `debug_${jobId}_${label}_${Date.now()}.png`;
    const outPath = path.join(__dirname, "outputs", fileName);
    await page.screenshot({ path: outPath, timeout: 5000 });
    log(jobId, `🖼️ Đã lưu ảnh debug: /outputs/${fileName}`);
  } catch (_) {}
}

// Sau khi mo editor 1 anh, day thumbnail cac anh trong project nam sat le trai (hep hon nhieu
// so voi tile trong grid — ~70-100px). Ham nay lay danh sach thumbnail do, sap theo tren->duoi.
//
// FIX: truoc day chi quet "img", nen khi 1 video (video0, video1...) da tao xong
// duoc render bang the <video> trong dai thumbnail nay, no bi BO SOT khoi mang ket qua.
// Vi cong thuc vi tri currentPos = i + videosGeneratedSoFar gia dinh moi video da tao
// CUNG chiem 1 o trong dai thumbnail (dung nhu thuc te: video moi luon bi chen len dau,
// giong het logic o getMediaTiles), nen phai quet ca "video" thi array moi dem du so o,
// tranh lech vi tri tu anh thu 2 tro di.
async function getEditorThumbnails(page) {
  const handles = await page.$$("img, video");
  const withBox = [];
  for (const h of handles) {
    try {
      if (!(await h.isVisible())) continue;
      const box = await h.boundingBox();
      if (!box) continue;
      if (box.x > 150) continue; // chi lay cot thumbnail sat trai
      if (box.y < 100) continue; // loai icon logo/nav sat mep tren (Vibes logo, badge BETA...)
      if (box.width < 40 || box.height < 40) continue; // icon UI thuong nho hon thumbnail that
      withBox.push({ handle: h, box });
    } catch (_) {}
  }
  withBox.sort((a, b) => a.box.y - b.box.y);
  // Dedup: cung ly do nhu getMediaTiles — tranh dem dup 1 o thumbnail thanh 2 phan tu.
  const deduped = [];
  for (const item of withBox) {
    const isDup = deduped.some(d => Math.abs(d.box.x - item.box.x) < 15 && Math.abs(d.box.y - item.box.y) < 15);
    if (!isDup) deduped.push(item);
  }
  return deduped.map(w => w.handle);
}

// Tim nut nam GAN GOC DUOI-PHAI cua 1 khung (vd: nut gui prompt hinh mui ten,
// luon nam o goc duoi ben phai o nhap "Describe how you want to animate...")
async function findButtonBottomRightOf(page, boxHandleOrLocator) {
  const box = await boxHandleOrLocator.boundingBox();
  if (!box) return null;
  const buttons = await page.$$("button");
  let best = null, bestScore = Infinity;
  for (const btn of buttons) {
    try {
      if (!(await btn.isVisible())) continue;
      const bbox = await btn.boundingBox();
      if (!bbox) continue;
      const withinY = bbox.y >= box.y - 5 && bbox.y <= box.y + box.height + 30;
      const rightHalf = bbox.x >= box.x + box.width * 0.5;
      if (withinY && rightHalf) {
        const dist = Math.abs((box.x + box.width) - (bbox.x + bbox.width));
        if (dist < bestScore) { bestScore = dist; best = btn; }
      }
    } catch (_) {}
  }
  return best;
}

// Nut download video nam ngay ben trai nut "Add to timeline" o goc tren phai.
async function findDownloadIconLeftOfAddToTimeline(page) {
  const addBtn = page.getByText("Add to timeline", { exact: false }).first();
  if (!(await addBtn.count())) return null;
  const box = await addBtn.boundingBox();
  if (!box) return null;
  const buttons = await page.$$("button");
  let best = null, bestDist = Infinity;
  for (const btn of buttons) {
    try {
      if (!(await btn.isVisible())) continue;
      const bbox = await btn.boundingBox();
      if (!bbox) continue;
      const sameRow = Math.abs(bbox.y - box.y) < 20;
      const isLeft = (bbox.x + bbox.width) <= box.x + 5;
      if (sameRow && isLeft) {
        const dist = box.x - (bbox.x + bbox.width);
        if (dist >= 0 && dist < bestDist) { bestDist = dist; best = btn; }
      }
    } catch (_) {}
  }
  return best;
}

// runVibesBatch: xu ly 1 project cho ca 1 batch —
// upload het anh thanh cong theo dung thu tu goc, mo editor tung anh (UI Vibes moi:
// click anh -> "Manual animate" -> nhap prompt -> gui -> cho xong -> download).
// Video moi tao ra bi CHEN LECH vi tri (giong logic UI cu), nen van can cong thuc:
//   vi tri hien tai cua anh goc thu i = i + so_video_da_tao_xong
async function runVibesBatch(groupId, tasks) {
  const firstJobId = tasks[0].jobId;
  log(firstJobId, "🚀 Opening Vibes.ai...");
  const page = await getSharedPage("vibes");

  try {
    await page.goto("https://vibes.ai/projects", { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForTimeout(4000);

    log(firstJobId, "📁 Tạo project mới...");
    let createBtn = page.getByText("Create new", { exact: false })
      .or(page.getByText("Tạo mới", { exact: false })).first();
    await createBtn.waitFor({ state: "visible", timeout: 20000 });
    await createBtn.click({ timeout: 15000 });
    await page.waitForURL(/\/projects\/[a-f0-9-]+/i, { timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(1500);

    // Nut "Upload media" (VI: co the la "Tai len phuong tien" / chua "Tai") —
    // tim button chua "Upload" hoac "Tai" nhung KHONG phai nut "Add from projects"/"Tu du an"
    let uploadBtn = null;
    const candidateBtns = await page.$$("button");
    for (const btn of candidateBtns) {
      try {
        if (!(await btn.isVisible())) continue;
        const txt = (await btn.innerText().catch(() => "")).toLowerCase();
        if (!txt) continue;
        if (txt.includes("add from") || txt.includes("từ dự án") || txt.includes("tu du an")) continue;
        if (txt.includes("upload") || txt.includes("tải")) { uploadBtn = btn; break; }
      } catch (_) {}
    }
    if (!uploadBtn) throw new Error("Không tìm thấy nút 'Upload media' trên Vibes.ai");
    await uploadBtn.click({ timeout: 15000 });
    await page.waitForTimeout(1000);

    let fileInput = null;
    for (let i = 0; i < 15; i++) {
      fileInput = await page.$('input[type="file"]');
      if (fileInput) break;
      await page.waitForTimeout(1000);
    }
    if (!fileInput) throw new Error("Không tìm thấy ô upload trên Vibes.ai");

    const imagePaths = tasks.map(t => t.imagePath);
    await fileInput.setInputFiles(imagePaths);
    await page.waitForTimeout(2000);

    let submitUploadBtn = page.getByText("Upload", { exact: false })
      .or(page.getByText("Tải lên", { exact: false })).last();
    await submitUploadBtn.click({ timeout: 15000 });
    log(firstJobId, `📤 Đang upload ${tasks.length} ảnh lên project...`);

    let tiles = [];
    for (let i = 0; i < 30; i++) {
      await page.waitForTimeout(2000);
      tiles = await getMediaTiles(page);
      if (tiles.length >= tasks.length) break;
    }
    if (tiles.length < tasks.length) {
      throw new Error(`Chỉ thấy ${tiles.length}/${tasks.length} ảnh sau khi upload lên Vibes.ai`);
    }
    log(firstJobId, `✅ Upload xong ${tiles.length} ảnh`);

    let videosGeneratedSoFar = 0; // video moi luon chen lech vi tri (giong logic UI cu) —
                                   // vi tri hien tai cua anh goc thu i = i + so video da tao xong

    for (let i = 0; i < tasks.length; i++) {
      const task = tasks[i];
      const currentPos = i + videosGeneratedSoFar;
      log(task.jobId, `🎬 Xử lý ảnh ${i + 1}/${tasks.length} (Vibes.ai, vị trí ${currentPos})...`);

      try {
        if (i === 0) {
          // Anh dau tien: click thang tu grid de mo editor
          tiles = await getMediaTiles(page);
          const tile = tiles[currentPos];
          if (!tile) throw new Error(`Không tìm thấy ảnh ở vị trí ${currentPos} trong project`);
          await tile.click({ timeout: 10000 });
          await page.waitForTimeout(1500);
        } else {
          // Cac anh sau: click vao thumbnail tuong ung trong dai ben trai editor
          // Cho them vai giay de UI on dinh sau khi video vua tao xong — tranh bat
          // trung trang thai tam thoi (video moi tao co the render du 1 phan tu
          // trong vai giay dau truoc khi UI gop lai dung so luong).
          await page.waitForTimeout(3000);
          const thumbs = await getEditorThumbnails(page);
          log(task.jobId, `🔍 Tìm thấy ${thumbs.length} thumbnail trong editor (cần vị trí ${currentPos})`);
          await debugScreenshot(page, task.jobId, `thumb_count_${thumbs.length}`);
          const thumb = thumbs[currentPos];
          if (!thumb) throw new Error(`Không tìm thấy thumbnail ở vị trí ${currentPos} trong editor (tổng ${thumbs.length})`);
          try {
            await thumb.click({ timeout: 10000 });
          } catch (clickErr) {
            // Click bi chan boi overlay (vd play-button icon khi hover video) —
            // thu lai voi force:true de click xuyen qua overlay thay vi fail luon.
            log(task.jobId, `⚠️ Click bị overlay chặn, thử lại (force)...`);
            await thumb.click({ timeout: 10000, force: true });
          }
          await page.waitForTimeout(1500);
        }

        // Bam "Manual animate"
        let manualBtn = page.getByText("Manual animate", { exact: false }).first();
        await manualBtn.waitFor({ state: "visible", timeout: 15000 });
        await manualBtn.click({ timeout: 10000 });
        await page.waitForTimeout(1000);

        // Nhap prompt vao o "Describe how you want to animate..."
        const promptBox = page.getByPlaceholder("Describe how you want to animate", { exact: false }).first();
        await promptBox.waitFor({ state: "visible", timeout: 15000 });
        await promptBox.fill(task.metaPrompt || "Turn this image into a cinematic TikTok video. Smooth motion. Luxury commercial style.");

        // Bam nut gui (mui ten len, goc duoi-phai cua o prompt)
        const submitBtn = await findButtonBottomRightOf(page, promptBox);
        if (!submitBtn) throw new Error("Không tìm thấy nút gửi prompt (mũi tên)");
        const urlBeforeSubmit = page.url(); // luu URL truoc khi gui, de so sanh phat hien URL doi
        await submitBtn.click({ timeout: 10000 });
        log(task.jobId, "⏳ Đang tạo hoạt ảnh (Vibes.ai, ~vài phút)...");

        // Cho den khi video tao xong. Uu tien phat hien qua URL doi sang /content/{id}
        // (dau hieu dang tin cay nhat, khong phu thuoc ngon ngu UI — vd khi UI tieng Anh
        // thi khong co badge "Đã tạo xong hoạt ảnh!" nhu ban tieng Viet).
        // Van giu check text lam phuong an du phong (ca tieng Viet lan tieng Anh).
        let done = false;
        for (let w = 0; w < 60; w++) {
          await page.waitForTimeout(4000);
          const urlChangedToContent = page.url() !== urlBeforeSubmit && page.url().includes("/content/");
          if (urlChangedToContent) { done = true; break; }
          const doneBadge = await page.$('text="Đã tạo xong hoạt ảnh!"')
            || await page.$('text=/[Đđ]ã tạo xong/i')
            || await page.$('text=/animation complete/i')
            || await page.$('text=/^done$/i');
          if (doneBadge) { done = true; break; }
          if (w > 0 && w % 5 === 0) log(task.jobId, `⏳ Vẫn đang tạo... (${w * 4}s)`);
        }
        if (!done) throw new Error("Video chưa tạo xong sau 4 phút (không thấy URL đổi hoặc badge hoàn thành)");
        await page.waitForTimeout(1000);

        // Bam nut download (icon canh "Add to timeline")
        const downloadBtn = await findDownloadIconLeftOfAddToTimeline(page);
        if (!downloadBtn) throw new Error("Không tìm thấy nút download video");
        const dlPromise = page.waitForEvent("download", { timeout: 30000 });
        await downloadBtn.click();
        const dl = await dlPromise;
        const outPath = path.join(__dirname, "outputs", `${task.jobId}_video.mp4`);
        await dl.saveAs(outPath);

        update(task.jobId, { step: "meta_done", videoUrl: `/outputs/${task.jobId}_video.mp4` });
        log(task.jobId, "🎉 Done! (Vibes.ai)");
        videosGeneratedSoFar++;

      } catch (err) {
        await debugScreenshot(page, task.jobId, "vibes_step_error");
        log(task.jobId, `❌ Vibes.ai lỗi: ${err.message}`);
        update(task.jobId, { step: "error", error: err.message });
      }
    }
  } catch (err) {
    await debugScreenshot(page, firstJobId, "batch_error");
    tasks.forEach(t => {
      if (jobs[t.jobId] && jobs[t.jobId].step !== "meta_done") {
        update(t.jobId, { step: "error", error: err.message });
      }
    });
    log(firstJobId, `❌ Vibes.ai batch lỗi: ${err.message}`);
  } finally {
    await page.close().catch(() => {});
  }
}

// ─── ROUTES ───────────────────────────────────────────

// Single
app.post("/start", upload.single("image"), (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No image" });
  const jobId = uuidv4();
  const geminiPrompt = req.body.geminiPrompt || "Turn this into a premium ecommerce product photo. Luxury background. Soft cinematic lighting. Ultra realistic.";
  const metaPrompt = req.body.metaPrompt || "Turn this image into a cinematic TikTok video. Smooth motion. Luxury commercial style.";
  // FIX: truyen prompt vao createJob ngay tu dau de luu lai trong jobs[jobId] —
  // can thiet cho flow Vibes.ai vi checkGeminiGroupDoneForVibes se doc lai
  // jobs[id].metaPrompt khi gom successTasks, thay vi bi mat prompt goc.
  createJob(jobId, req.file.originalname, 0, geminiPrompt, metaPrompt);
  log(jobId, "📋 Job queued");
  const provider = req.body.provider === "vibes" ? "vibes" : "meta";
  const skipGemini = req.body.skipGemini === "true";
  const groupId = jobId; // single job — groupId == jobId
  batchGroups[groupId] = [jobId];
  groupProvider[groupId] = provider;

  if (skipGemini) {
    const ext = path.extname(req.file.path) || ".jpg";
    const enhancedFileName = `${jobId}_enhanced${ext}`;
    const enhancedPath = path.join(__dirname, "outputs", enhancedFileName);
    fs.copyFileSync(req.file.path, enhancedPath);
    update(jobId, { step: "gemini_done", enhancedImage: `/outputs/${enhancedFileName}` });
    log(jobId, "⏭️ Bỏ qua Gemini — dùng ảnh gốc để tạo video");
    if (provider === "vibes") {
      checkGeminiGroupDoneForVibes(groupId);
    } else {
      metaQueue.push({ jobId, groupId, enhancedPath, metaPrompt });
      processMetaQueue();
    }
  } else {
    geminiQueue.push({ jobId, groupId, imagePath: req.file.path, geminiPrompt, metaPrompt });
    processGeminiQueue();
  }
  res.json({ jobId });
});

// Batch
app.post("/batch", upload.array("images", 20), (req, res) => {
  if (!req.files || req.files.length === 0) return res.status(400).json({ error: "No images" });
  const geminiPrompt = req.body.geminiPrompt || "Turn this into a premium ecommerce product photo. Luxury background. Soft cinematic lighting. Ultra realistic.";
  const metaPrompt = req.body.metaPrompt || "Turn this image into a cinematic TikTok video. Smooth motion. Luxury commercial style.";
  const provider = req.body.provider === "vibes" ? "vibes" : "meta";
  const skipGemini = req.body.skipGemini === "true";

  const groupId = uuidv4(); // ID chung cho ca batch nay
  groupProvider[groupId] = provider;
  const jobIds = req.files.map((file, i) => {
    const jobId = uuidv4();
    // FIX: truyen prompt vao createJob ngay tu dau — xem giai thich o route /start
    createJob(jobId, file.originalname, i, geminiPrompt, metaPrompt);
    log(jobId, `📋 Queued ${i+1}/${req.files.length} — ${file.originalname}`);

    if (skipGemini) {
      const ext = path.extname(file.path) || ".jpg";
      const enhancedFileName = `${jobId}_enhanced${ext}`;
      const enhancedPath = path.join(__dirname, "outputs", enhancedFileName);
      fs.copyFileSync(file.path, enhancedPath);
      update(jobId, { step: "gemini_done", enhancedImage: `/outputs/${enhancedFileName}` });
      log(jobId, "⏭️ Bỏ qua Gemini — dùng ảnh gốc để tạo video");
      if (provider === "meta") {
        metaQueue.push({ jobId, groupId, enhancedPath, metaPrompt });
      }
      // vibes: gom sau khi tao het jobIds, goi checkGeminiGroupDoneForVibes 1 lan ben duoi
    } else {
      geminiQueue.push({ jobId, groupId, imagePath: file.path, geminiPrompt, metaPrompt });
    }
    return jobId;
  });

  batchGroups[groupId] = jobIds;

  if (skipGemini) {
    if (provider === "vibes") checkGeminiGroupDoneForVibes(groupId);
    else processMetaQueue();
  } else {
    processGeminiQueue();
  }
  res.json({ jobIds, total: jobIds.length, provider });
});

// Batch status
app.post("/batch-status", (req, res) => {
  const { jobIds } = req.body;
  if (!jobIds) return res.status(400).json({ error: "No jobIds" });
  res.json(jobIds.map(id => jobs[id] || { id, step: "not_found" }));
});

// Download all as zip
app.post("/download-all", async (req, res) => {
  const { jobIds } = req.body;
  if (!jobIds) return res.status(400).json({ error: "No jobIds" });
  try {
    const archiver = require("archiver");
    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", "attachment; filename=videos.zip");
    const archive = archiver("zip");
    archive.pipe(res);
    jobIds.forEach((id, i) => {
      const job = jobs[id];
      if (job?.videoUrl) {
        const filePath = path.join(__dirname, job.videoUrl.replace("/outputs/", "outputs/"));
        if (fs.existsSync(filePath)) {
          const baseName = job.filename ? path.basename(job.filename, path.extname(job.filename)) : `product_${i+1}`;
          archive.file(filePath, { name: `${baseName}_video.mp4` });
        }
      }
    });
    await archive.finalize();
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/status/:id", (req, res) => {
  const job = jobs[req.params.id];
  if (!job) return res.status(404).json({ error: "Not found" });
  res.json(job);
});

app.get("/health", (req, res) => res.json({ status: "ok" }));

setInterval(() => {
  const cutoff = Date.now() - 3600000;
  Object.keys(jobs).forEach(id => { if (jobs[id].createdAt < cutoff) delete jobs[id]; });
}, 3600000);

app.listen(PORT, () => {
  console.log(`\n🚀 Pic2Vid server running on port ${PORT}`);
  console.log(`⚡ Gemini concurrency: ${GEMINI_CONCURRENCY} tabs | Meta AI: ${META_CONCURRENCY} tabs | Vibes.ai: ${VIBES_CONCURRENCY} tab`);
  if (IS_LOCAL) {
    console.log(`📂 Mode: LOCAL — pipeline overlap enabled`);
    // Hien thi tat ca account slots tim duoc
    const gSlots = getProfileSlots("gemini");
    const mSlots = getProfileSlots("meta");
    const vSlots = getProfileSlots("vibes");
    console.log(`   Gemini accounts: ${gSlots.length > 0 ? gSlots.map(s => s.profileDir).join(", ") : "❌ NONE — chay: node add-account.js gemini 1"}`);
    console.log(`   Meta AI accounts: ${mSlots.length > 0 ? mSlots.map(s => s.profileDir).join(", ") : "❌ NONE — chay: node add-account.js meta 1"}`);
    console.log(`   Vibes.ai accounts: ${vSlots.length > 0 ? vSlots.map(s => s.profileDir).join(", ") : "❌ NONE — chay: node add-account.js vibes 1"}`);

  } else {
    console.log(`☁️  Mode: CLOUD`);
    console.log(`   COOKIES_GEMINI: ${process.env.COOKIES_GEMINI ? "✅ set" : "❌ missing"}`);
    console.log(`   COOKIES_META:   ${process.env.COOKIES_META ? "✅ set" : "❌ missing"}`);
    console.log(`   COOKIES_VIBES:  ${process.env.COOKIES_VIBES ? "✅ set" : "❌ missing"}`);
  }
  console.log(`\n🌐 Open: http://localhost:${PORT}\n`);
});
