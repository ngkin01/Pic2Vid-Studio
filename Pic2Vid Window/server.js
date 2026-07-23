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
// Gemini queue → chờ cả batch xong Gemini → gom lại đẩy vào Vibes.ai
const geminiQueue = [];   // { jobId, imagePath, geminiPrompt, metaPrompt }
const vibesQueue = [];    // { groupId, tasks: [{jobId, imagePath}] } — 1 phan tu = 1 batch project

const GEMINI_CONCURRENCY = 3; // 3 tab Gemini song song
const VIBES_CONCURRENCY = 1;  // 1 account Vibes -> chay tuan tu
let geminiActive = 0;
let vibesActive = 0;

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
  if (vibesActive === 0 && sharedCtx.vibes) {
    await sharedCtx.vibes.close().catch(() => {});
    sharedCtx.vibes = null;
  }
  const gSlots = getProfileSlots("gemini");
  const vSlots = getProfileSlots("vibes");
  [...gSlots, ...vSlots].forEach(({ profileDir }) => clearBrowserCache(profileDir));
  delete batchGroups[groupId];
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
        log(task.jobId, "✅ Gemini done — chờ cả batch xong Gemini để qua Vibes.ai");
        checkGeminiGroupDoneForVibes(task.groupId);

      } catch (err) {
        const retryCount = (jobs[task.jobId]?.retryCount || 0);
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
            checkGeminiGroupDoneForVibes(task.groupId);
          }
        } else if (retryCount < 2) {
          update(task.jobId, { step: "queued", retryCount: retryCount + 1, error: null });
          log(task.jobId, `⚠️ Gemini failed (${err.message}) — retry ${retryCount + 1}/2...`);
          geminiQueue.push(task);
        } else {
          log(task.jobId, `❌ Gemini error after 2 retries: ${err.message}`);
          update(task.jobId, { step: "error", error: err.message });
          checkGeminiGroupDoneForVibes(task.groupId);
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
const currentSlot = { gemini: null, vibes: null };
// Shared contexts — 1 browser per service
const sharedCtx = { gemini: null, vibes: null };
const ctxLock   = { gemini: false, vibes: false };

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
  const cookieKeyMap = { gemini: "COOKIES_GEMINI", vibes: "COOKIES_VIBES" };
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

// ─── VIBES.AI ─────────────────────────────────────────
// Lay danh sach "tile" (anh hoac video) trong luoi chinh cua project (man hinh TRUOC KHI
// bam vao anh nao de mo editor). CONFIRMED qua Inspect Element thuc te tren Vibes.ai:
// moi o trong luoi la 1 phan tu co attribute rieng biet, on dinh, khong phu thuoc class/style:
//   data-analytics-id="creation_gallery.thumbnail_click"
// Dung thang attribute nay thay vi doan qua toa do/kich thuoc cua <img>/<video> ben trong
// (kieu cu tung gay dem sai/dedup nham y het loi da gap va sua o getEditorThumbnails).
async function getMediaTiles(page) {
  const handles = await page.$$('[data-analytics-id="creation_gallery.thumbnail_click"]');
  const withBox = [];
  for (const h of handles) {
    try {
      if (!(await h.isVisible())) continue;
      const box = await h.boundingBox();
      if (!box) continue;
      withBox.push({ handle: h, box });
    } catch (_) {}
  }
  // Sap theo thu tu doc-hieu (hang tren truoc, trong 1 hang thi trai truoc) — giu lam luoi
  // an toan phong khi DOM order khong khop thu tu hien thi (vd grid tu sap xep lai vi tri).
  withBox.sort((a, b) => {
    const rowDiff = Math.round(a.box.y / 50) - Math.round(b.box.y / 50);
    if (rowDiff !== 0) return rowDiff;
    return a.box.x - b.box.x;
  });
  return withBox.map(w => w.handle);
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

// getEditorThumbnails: dai thumbnail co the dai hon 1 man hinh khi batch nhieu anh (7+),
// nen phai CUON DAN TU TREN XUONG, chup tung doan roi gop lai — khong duoc nhay thang
// xuong day (se lam mat cac item o dau danh sach do bi cuon khuat len tren).
//
// CONFIRMED qua Inspect Element thuc te tren Vibes.ai (khong con doan mo qua toa do/
// overflow nhu cac ban truoc — nguyen nhan gay dem sai/dem 0 lien tuc):
// Moi o thumbnail trong dai la 1 <button class="w_60px h_60px ..."> kich thuoc co dinh
// 60x60px, ben trong luon chua dung 1 <img> hoac <video> dai dien cho anh goc / video da tao.
async function getEditorThumbnails(page) {
  const attr = `data-vibes-seen-${Date.now()}`;
  const collected = [];

  async function captureNewOnes() {
    const handles = await page.$$(`button.w_60px.h_60px:not([${attr}])`);
    for (const h of handles) {
      try {
        if (!(await h.isVisible())) continue;
        const box = await h.boundingBox();
        if (!box) continue;
        if (box.x > 200) continue; // chi lay cot thumbnail sat trai, phong khi co button 60x60 khac o cho khac tren trang
        const media = await h.$("img, video");
        if (!media) continue; // button dung kich thuoc nhung khong chua anh/video -> khong phai o thumbnail that
        await h.evaluate((el, a) => el.setAttribute(a, "1"), attr).catch(() => {});
        collected.push({ handle: h, box });
      } catch (_) {}
    }
  }

  await captureNewOnes();

  // Neu dai co danh sach ao/virtualized (chi render cac o dang hien tren man hinh —
  // thuong gap voi batch nhieu anh 7+, con it anh thi khong can) -> tim dung khung cuon
  // bang cach LEO LEN tu 1 button THAT da tim duoc (dang tin cay hon han so voi do mu
  // ca trang theo overflow/scrollHeight cua cac ban truoc, von hay tra ve sai khung hoac null).
  if (collected.length > 0) {
    const anchor = collected[0].handle;
    const containerHandle = await anchor.evaluateHandle(el => {
      let node = el.parentElement;
      while (node) {
        const s = getComputedStyle(node);
        if (
          (s.overflowY === "auto" || s.overflowY === "scroll" || s.overflowY === "hidden") &&
          node.scrollHeight > node.clientHeight + 5
        ) {
          return node;
        }
        node = node.parentElement;
      }
      return null;
    }).catch(() => null);
    const containerEl = containerHandle ? containerHandle.asElement() : null;

    if (containerEl) {
      // Co khung can cuon that (dai dai hon 1 man hinh) -> quet lai tu dau theo dung thu tu tren->duoi
      collected.length = 0;
      await page.evaluate(a => {
        document.querySelectorAll(`[${a}]`).forEach(el => el.removeAttribute(a));
      }, attr).catch(() => {});
      await containerEl.evaluate(el => { el.scrollTop = 0; }).catch(() => {});
      await page.waitForTimeout(300);
      await captureNewOnes();
      for (let step = 0; step < 20; step++) {
        const scrolled = await containerEl.evaluate(el => {
          const before = el.scrollTop;
          el.scrollTop = Math.min(el.scrollHeight, el.scrollTop + el.clientHeight * 0.8);
          return el.scrollTop !== before;
        }).catch(() => false);
        if (!scrolled) break;
        await page.waitForTimeout(300);
        await captureNewOnes();
      }
    }
    if (containerHandle) await containerHandle.dispose().catch(() => {});
  }

  // Don dep attribute tam de khong anh huong lan goi sau
  try {
    await page.evaluate((a) => {
      document.querySelectorAll(`[${a}]`).forEach(el => el.removeAttribute(a));
    }, attr);
  } catch (_) {}

  // collected da theo dung thu tu phat hien tren->duoi
  return collected.map(c => c.handle);
}

// Nut gui prompt (mui ten len, o goc duoi-phai o nhap prompt) — CONFIRMED qua Inspect
// Element: nut nay luon co aria-label="Animate", khong con can doan qua toa do nua.
async function findAnimateSubmitButton(page) {
  return await page.$('button[aria-label="Animate"]');
}

// Nut download video canh "Add to timeline" — CONFIRMED qua Inspect Element: nut nay
// luon co aria-label="Download", khong con can do toa do tuong doi voi "Add to timeline" nua.
async function findDownloadButton(page) {
  return await page.$('button[aria-label="Download"]');
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

    // Phat hien phien dang nhap het han / chua dang nhap: khi vao thang /projects ma
    // khong con session, Vibes.ai se DA nguoi dung ve trang landing (vd chi con "vibes.ai",
    // hien nut "Log In" to o goc phai) thay vi bao loi ro rang. Neu chay tiep se fail mo ho
    // o buoc tim nut "Tao moi" ben duoi (timeout 20s, thong bao khong nghia).
    // -> Kiem tra 2 tin hieu: (1) URL khong con chua /projects/ NUA sau redirect,
    //    (2) co nut "Log In" dang hien. Neu dung ca 2 -> dung lai, CHO nguoi dung tu dang
    //    nhap thu cong trong chinh cua so Chrome dang mo (toi da 5 phut), roi tu dong tiep tuc.
    const onLandingPage = !/\/projects/i.test(page.url());
    let loginBtn = page.getByText("Log In", { exact: false })
      .or(page.getByText("Đăng nhập", { exact: false })).first();
    let loginBtnVisible = await loginBtn.isVisible().catch(() => false);

    if (onLandingPage && loginBtnVisible) {
      // Vibes.ai da luu san thong tin dang nhap trong profile (khong yeu cau lai mat khau/OTP) —
      // chi can bam nut "Log In" la vao thang giao dien lam viec, KHONG can nguoi dung thao tac them.
      // Nen tu dong click thay vi dung lai cho nguoi dung, thu lai vai lan phong khi lan click dau
      // chua kip navigate hoac trang can render them 1 nhip.
      log(firstJobId, `🔐 Phát hiện Vibes.ai đang ở màn hình chưa đăng nhập — tự động bấm "Log In"...`);
      let reLoggedIn = false;
      for (let attempt = 0; attempt < 3 && !reLoggedIn; attempt++) {
        try { await loginBtn.click({ timeout: 8000 }); } catch (_) {}
        reLoggedIn = await page.waitForFunction(
          () => location.pathname.includes("/projects") || !document.body.innerText.includes("Log In"),
          null,
          { timeout: 20000, polling: 1000 }
        ).then(() => true).catch(() => false);
        if (!reLoggedIn) {
          await page.waitForTimeout(2000);
          loginBtn = page.getByText("Log In", { exact: false })
            .or(page.getByText("Đăng nhập", { exact: false })).first();
          loginBtnVisible = await loginBtn.isVisible().catch(() => false);
          if (!loginBtnVisible) break; // co the da vao duoc nhung chua kip khop dieu kien tren
        }
      }
      if (!reLoggedIn) {
        // Fallback an toan: neu tu dong click 3 lan van khong vao duoc (vd nut doi vi tri/ten,
        // hoac can them buoc xac thuc that su) — cho nguoi dung tu bam thu cong, toi da 5 phut,
        // thay vi fail luon.
        log(firstJobId, `⚠️ Tự động đăng nhập không thành công sau vài lần thử — vui lòng bấm "Log In" thủ công trên cửa sổ Chrome đang mở, job sẽ tự tiếp tục (chờ tối đa 5 phút)...`);
        reLoggedIn = await page.waitForFunction(
          () => location.pathname.includes("/projects") || !document.body.innerText.includes("Log In"),
          null,
          { timeout: 5 * 60 * 1000, polling: 2000 }
        ).then(() => true).catch(() => false);
        if (!reLoggedIn) {
          throw new Error("Hết thời gian chờ (5 phút) đăng nhập lại Vibes.ai — vui lòng đăng nhập thủ công rồi chạy lại job.");
        }
      }
      log(firstJobId, "✅ Đã vào được giao diện Vibes.ai — tiếp tục xử lý...");
      await page.goto("https://vibes.ai/projects", { waitUntil: "domcontentloaded", timeout: 60000 });
      await page.waitForTimeout(3000);
    }

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
          // Cac anh sau: click vao thumbnail tuong ung trong dai ben trai editor.
          // Tong so tile PHAI dung bang: tong so anh goc (khong doi) + so video da tao xong —
          // neu dem ra khac con so nay, nghia la UI chua kip render du (video vua xong
          // chua hien vao dai), phai cho them va dem lai, KHONG duoc chon dai neu sai so.
          const expectedCount = tasks.length + videosGeneratedSoFar;
          let thumbs = [];
          let matched = false;

          for (let round = 0; round < 2 && !matched; round++) {
            // round 0: thu dem binh thuong. round 1: neu round 0 that bai, RELOAD trang 1 lan
            // truoc khi thu lai — vi co truong hop Vibes bi lag/loi mang khien dai thumbnail
            // khong tu cap nhat du video moi da tao xong that (giao dien chinh van hien dung,
            // download van chay duoc, nhung dai thumbnail ben trai bi "dung" khong len tile moi).
            // Reload se ep trinh duyet lay lai dung trang thai moi nhat tu server Vibes.
            if (round === 1) {
              log(task.jobId, `🔄 Đếm thumbnail vẫn không khớp — reload lại trang Vibes để đồng bộ...`);
              await page.reload({ waitUntil: "domcontentloaded", timeout: 30000 }).catch(() => {});
              await page.waitForTimeout(3000);
            }
            for (let attempt = 0; attempt < 6; attempt++) {
              await page.waitForTimeout(attempt === 0 ? 3000 : 2500);
              thumbs = await getEditorThumbnails(page);
              if (thumbs.length === expectedCount) { matched = true; break; }
              log(task.jobId, `⏳ Đếm thumbnail chưa khớp (${thumbs.length}/${expectedCount} kỳ vọng), thử đếm lại...`);
            }
          }

          log(task.jobId, `🔍 Tìm thấy ${thumbs.length} thumbnail trong editor (cần vị trí ${currentPos}, kỳ vọng ${expectedCount})`);
          await debugScreenshot(page, task.jobId, `thumb_count_${thumbs.length}`);
          if (!matched) {
            throw new Error(`Số thumbnail không khớp kỳ vọng (${thumbs.length}/${expectedCount}) sau nhiều lần thử kể cả reload — dừng để tránh chọn nhầm ảnh`);
          }
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

        // Vibes doi khi tu bao loi tao video ("Không tạo được — Đã xảy ra lỗi. Vui lòng thử lại.")
        // — day la loi phia Vibes, khong phai loi selector. Can phat hien va TU DONG THU LAI
        // dung anh nay (khong bo qua, khong chuyen anh khac) toi da 3 lan.
        let animateSuccess = false;
        let lastAnimateErr = null;

        // Ham mo lai dung anh nay (click lai dung vi tri) — dung khi retry, vi co truong hop
        // lan thu truoc bi loi khien trang nhay nham sang che do "video" (nut Manual animate
        // bi disabled do khong con la anh nua), phai click lai thumbnail de quay ve dung trang thai.
        async function reopenImage() {
          try {
            if (i === 0) {
              const t = await getMediaTiles(page);
              const tl = t[currentPos];
              if (tl) await tl.click({ timeout: 8000 }).catch(() => {});
            } else {
              const th = await getEditorThumbnails(page);
              const tt = th[currentPos];
              if (tt) await tt.click({ timeout: 8000 }).catch(() => {});
            }
            await page.waitForTimeout(1200);
          } catch (_) {}
        }

        // Ham dung chung: cho toi da 1 phut de biet video xong hay Vibes bao loi that su.
        // failBadge duoc "debounce" — thay vi tin ngay lan dau thay, doi them 2s roi kiem tra
        // lai 1 lan nua, chi ket luan la loi that neu VAN CON thay (tranh bat trung 1 khoanh khac
        // thoang qua roi Vibes tu phuc hoi, gay ket luan sai la that bai trong khi thuc ra dang chay binh thuong).
        async function waitForVibesDone(urlBeforeSubmit) {
          let done = false;
          let vibesFailed = false;
          let elapsedMs = 0;
          const maxWaitMs = 60000; // 1 phut
          while (elapsedMs < maxWaitMs) {
            const interval = elapsedMs < 20000 ? 1500 : 4000;
            await page.waitForTimeout(interval);
            elapsedMs += interval;

            const urlChangedToContent = urlBeforeSubmit != null && page.url() !== urlBeforeSubmit && page.url().includes("/content/");
            if (urlChangedToContent) { done = true; break; }
            const doneBadge = await page.$('text="Đã tạo xong hoạt ảnh!"')
              || await page.$('text=/[Đđ]ã tạo xong/i')
              || await page.$('text=/animation complete/i')
              || await page.$('text=/^done$/i');
            if (doneBadge) { done = true; break; }

            const failBadge = await page.$('text=/[Kk]hông tạo được/i')
              || await page.$('text=/[Đđ]ã xảy ra lỗi\\. Vui lòng thử lại/i')
              || await page.$('text=/[Kk]hông có câu lệnh nào/i')
              || await page.$('text=/failed to (create|generate|animate)/i')
              || await page.$('text=/something went wrong/i')
              || await page.$('text=/an unexpected error occurred/i')
              || await page.$('text=/no (command|prompt) (provided|for)/i');
            if (failBadge) {
              // Debounce: doi 2s roi kiem tra lai, chi ket luan that bai neu VAN CON thay
              await page.waitForTimeout(2000);
              elapsedMs += 2000;
              const stillFailing = await page.$('text=/[Kk]hông tạo được/i')
                || await page.$('text=/[Đđ]ã xảy ra lỗi\\. Vui lòng thử lại/i')
                || await page.$('text=/[Kk]hông có câu lệnh nào/i')
                || await page.$('text=/failed to (create|generate|animate)/i')
                || await page.$('text=/something went wrong/i')
                || await page.$('text=/an unexpected error occurred/i')
                || await page.$('text=/no (command|prompt) (provided|for)/i');
              if (stillFailing) { vibesFailed = true; break; }
              // Neu bien mat roi thi coi nhu bao dong gia, tiep tuc vong lap cho binh thuong
            }
            if (elapsedMs % 20000 < interval) log(task.jobId, `⏳ Vẫn đang tạo... (${Math.round(elapsedMs / 1000)}s)`);
          }
          return { done, vibesFailed };
        }

        for (let animAttempt = 1; animAttempt <= 3 && !animateSuccess; animAttempt++) {
          try {
            if (animAttempt > 1) {
              log(task.jobId, `🔁 Vibes báo lỗi tạo video — thử lại lần ${animAttempt} cho ảnh này...`);
              // Dong banner loi neu con hien (khong bat buoc phai thanh cong)
              try {
                const closeX = await page.$('button:near(:text("Không tạo được"), 80)');
                if (closeX) await closeX.click({ timeout: 3000 }).catch(() => {});
              } catch (_) {}
              await page.waitForTimeout(1000);
              // Click lai dung thumbnail — phong khi lan truoc trang bi nhay sang che do
              // "video" lam nut Manual animate bi disabled/khong con o dung trang image nua.
              await reopenImage();
            }

            // Bam "Manual animate"
            let manualBtn = page.getByText("Manual animate", { exact: false }).first();
            await manualBtn.waitFor({ state: "visible", timeout: 15000 });
            await manualBtn.waitFor({ state: "attached", timeout: 3000 }).catch(() => {});
            const isEnabled = await manualBtn.isEnabled().catch(() => false);
            if (!isEnabled) {
              // Nut bi khoa co the vi 2 ly do khac nhau:
              // (1) Dang THAT SU generate tu lan submit truoc (khong phai loi that, chi la
              //     lan poll truoc bat nham 1 khoanh khac thoang qua) — badge "Đang tạo hoạt ảnh..." con hien.
              // (2) Trang bi ket sai trang (vd da la video roi) — khong co badge dang tao.
              // Chi coi la loi that (throw) o truong hop (2); truong hop (1) thi cho tiep binh thuong.
              const stillGenerating = await page.$('text=/[Đđ]ang tạo hoạt ảnh/i') || await page.$('text=/generating/i');
              if (stillGenerating) {
                log(task.jobId, `ℹ️ Nút bị khoá nhưng đang thực sự tạo video (không phải lỗi) — tiếp tục chờ...`);
                const { done, vibesFailed } = await waitForVibesDone(null);
                if (vibesFailed) throw new Error("Vibes.ai báo lỗi tạo video (Không tạo được)");
                if (!done) throw new Error("Video chưa tạo xong sau 1 phút (không thấy URL đổi hoặc badge hoàn thành)");
                animateSuccess = true;
                continue;
              }
              throw new Error("Nút 'Manual animate' đang bị khoá (trang có thể đang ở chế độ video) — sẽ mở lại ảnh và thử tiếp");
            }
            await manualBtn.click({ timeout: 8000 });
            await page.waitForTimeout(1000);

            // Nhap prompt vao o "Describe how you want to animate..."
            const promptBox = page.getByPlaceholder("Describe how you want to animate", { exact: false }).first();
            await promptBox.waitFor({ state: "visible", timeout: 15000 });
            await promptBox.fill(task.metaPrompt || "Turn this image into a cinematic TikTok video. Smooth motion. Luxury commercial style.");

            // Bam nut gui (mui ten len, goc duoi-phai cua o prompt)
            const submitBtn = await findAnimateSubmitButton(page);
            if (!submitBtn) throw new Error("Không tìm thấy nút gửi prompt (mũi tên)");
            const urlBeforeSubmit = page.url(); // luu URL truoc khi gui, de so sanh phat hien URL doi
            await submitBtn.click({ timeout: 10000 });
            log(task.jobId, "⏳ Đang tạo hoạt ảnh (Vibes.ai, ~vài phút)...");

            const { done, vibesFailed } = await waitForVibesDone(urlBeforeSubmit);
            if (vibesFailed) throw new Error("Vibes.ai báo lỗi tạo video (Không tạo được)");
            if (!done) throw new Error("Video chưa tạo xong sau 1 phút (không thấy URL đổi hoặc badge hoàn thành)");
            animateSuccess = true;
          } catch (animErr) {
            lastAnimateErr = animErr;
            log(task.jobId, `⚠️ Animate lần ${animAttempt} lỗi: ${animErr.message}`);
          }
        }
        if (!animateSuccess) throw lastAnimateErr || new Error("Animate thất bại sau nhiều lần thử");
        await page.waitForTimeout(1000);

        // Bam nut download (icon canh "Add to timeline")
        const outPath = path.join(__dirname, "outputs", `${task.jobId}_video.mp4`);
        let downloadOk = false;
        let lastDlErr = null;

        for (let attempt = 1; attempt <= 2 && !downloadOk; attempt++) {
          try {
            const downloadBtn = await findDownloadButton(page);
            if (!downloadBtn) throw new Error("Không tìm thấy nút download video");
            const dlPromise = page.waitForEvent("download", { timeout: 30000 });
            await downloadBtn.click();
            const dl = await dlPromise;
            // Log ten file goc de doi chieu — neu bi le/tro nham download cua job khac se thay ngay trong log
            log(task.jobId, `📥 Nhận download: ${dl.suggestedFilename()} (lần thử ${attempt})`);
            await dl.saveAs(outPath);

            // Xac minh file thuc su ton tai va co dung luong hop ly — khong tin mu quang
            // vao viec saveAs() khong nem loi, vi co the bi gan nham download cua job khac.
            const stat = await fs.promises.stat(outPath).catch(() => null);
            if (!stat || stat.size < 10000) {
              throw new Error(`File tải về bất thường (dung lượng ${stat ? stat.size : 0} bytes)`);
            }
            log(task.jobId, `✅ Video đã lưu, dung lượng ${(stat.size / 1024).toFixed(0)}KB`);
            downloadOk = true;
          } catch (err) {
            lastDlErr = err;
            log(task.jobId, `⚠️ Download lần ${attempt} lỗi: ${err.message}`);
            if (attempt < 2) await page.waitForTimeout(2000);
          }
        }
        if (!downloadOk) throw lastDlErr || new Error("Download thất bại sau 2 lần thử");

        update(task.jobId, { step: "meta_done", videoUrl: `/outputs/${task.jobId}_video.mp4` });
        log(task.jobId, "🎉 Done! (Vibes.ai)");
        videosGeneratedSoFar++;

        // Cho them 1.5s truoc khi qua job tiep theo, tranh su kien download con "vuong" lai
        // gay nham lan cho waitForEvent('download') cua job sau.
        await page.waitForTimeout(1500);

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
  const skipGemini = req.body.skipGemini === "true";
  const groupId = jobId; // single job — groupId == jobId
  batchGroups[groupId] = [jobId];

  if (skipGemini) {
    const ext = path.extname(req.file.path) || ".jpg";
    const enhancedFileName = `${jobId}_enhanced${ext}`;
    const enhancedPath = path.join(__dirname, "outputs", enhancedFileName);
    fs.copyFileSync(req.file.path, enhancedPath);
    update(jobId, { step: "gemini_done", enhancedImage: `/outputs/${enhancedFileName}` });
    log(jobId, "⏭️ Bỏ qua Gemini — dùng ảnh gốc để tạo video");
    checkGeminiGroupDoneForVibes(groupId);
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
  const skipGemini = req.body.skipGemini === "true";

  const groupId = uuidv4(); // ID chung cho ca batch nay
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
      // gom sau khi tao het jobIds, goi checkGeminiGroupDoneForVibes 1 lan ben duoi
    } else {
      geminiQueue.push({ jobId, groupId, imagePath: file.path, geminiPrompt, metaPrompt });
    }
    return jobId;
  });

  batchGroups[groupId] = jobIds;

  if (skipGemini) {
    checkGeminiGroupDoneForVibes(groupId);
  } else {
    processGeminiQueue();
  }
  res.json({ jobIds, total: jobIds.length, provider: "vibes" });
});

// Batch status
app.post("/batch-status", (req, res) => {
  const { jobIds } = req.body;
  if (!jobIds) return res.status(400).json({ error: "No jobIds" });
  res.json(jobIds.map(id => jobs[id] || { id, step: "not_found" }));
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
  console.log(`⚡ Gemini concurrency: ${GEMINI_CONCURRENCY} tabs | Vibes.ai: ${VIBES_CONCURRENCY} tab`);
  if (IS_LOCAL) {
    console.log(`📂 Mode: LOCAL — pipeline overlap enabled`);
    // Hien thi tat ca account slots tim duoc
    const gSlots = getProfileSlots("gemini");
    const vSlots = getProfileSlots("vibes");
    console.log(`   Gemini accounts: ${gSlots.length > 0 ? gSlots.map(s => s.profileDir).join(", ") : "❌ NONE — chay: node add-account.js gemini 1"}`);
    console.log(`   Vibes.ai accounts: ${vSlots.length > 0 ? vSlots.map(s => s.profileDir).join(", ") : "❌ NONE — chay: node add-account.js vibes 1"}`);

  } else {
    console.log(`☁️  Mode: CLOUD`);
    console.log(`   COOKIES_GEMINI: ${process.env.COOKIES_GEMINI ? "✅ set" : "❌ missing"}`);
    console.log(`   COOKIES_VIBES:  ${process.env.COOKIES_VIBES ? "✅ set" : "❌ missing"}`);
  }
  console.log(`\n🌐 Open: http://localhost:${PORT}\n`);
});