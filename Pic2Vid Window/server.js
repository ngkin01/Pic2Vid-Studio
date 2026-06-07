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
function createJob(id, filename, index) {
  jobs[id] = {
    id, step: "queued", logs: [],
    enhancedImage: null, videoUrl: null,
    error: null, createdAt: Date.now(),
    filename: filename || "", index: index || 0,
    retryCount: 0
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

const GEMINI_CONCURRENCY = 3; // 3 tab Gemini song song
const META_CONCURRENCY = 2;   // 2 tab Meta AI song song
let geminiActive = 0;
let metaActive = 0;

function processGeminiQueue() {
  while (geminiActive < GEMINI_CONCURRENCY && geminiQueue.length > 0) {
    const task = geminiQueue.shift();
    geminiActive++;
    update(task.jobId, { step: "gemini_running" });

    (async () => {
      try {
        const enhanced = await runGemini(task.jobId, task.imagePath, task.geminiPrompt);
        update(task.jobId, { step: "gemini_done", enhancedImage: enhanced });
        log(task.jobId, "✅ Gemini done — queued for Meta AI");
        if (geminiQueue.length === 0 && geminiActive <= 1) {
          clearBrowserCache("profile_gemini");
          closeSharedCtx("gemini");
        }

        const localPath = path.join(__dirname, enhanced.replace("/outputs/", "outputs/"));
        metaQueue.push({ jobId: task.jobId, enhancedPath: localPath, metaPrompt: task.metaPrompt });
        processMetaQueue();

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
          }
        } else if (retryCount < 2) {
          update(task.jobId, { step: "queued", retryCount: retryCount + 1, error: null });
          log(task.jobId, `⚠️ Gemini failed (${err.message}) — retry ${retryCount + 1}/2...`);
          geminiQueue.push(task);
        } else {
          log(task.jobId, `❌ Gemini error after 2 retries: ${err.message}`);
          update(task.jobId, { step: "error", error: err.message });
        }
        geminiActive--;
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
        if (metaQueue.length === 0 && metaActive <= 1) {
          clearBrowserCache("profile_meta");
          closeSharedCtx("meta");
        }

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
          }
        } else if (retryCount < 2) {
          update(task.jobId, { step: "queued", retryCount: retryCount + 1, error: null });
          log(task.jobId, `⚠️ Meta AI failed (${err.message}) — retry ${retryCount + 1}/2...`);
          metaQueue.push(task);
        } else {
          log(task.jobId, `❌ Meta AI error after 2 retries: ${err.message}`);
          update(task.jobId, { step: "error", error: err.message });
        }
        processMetaQueue();
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
// Tim tat ca profile slot co san
// Thu tu: profile_gemini (legacy slot 0) -> profile_gemini_1 -> profile_gemini_2 ...
function getProfileSlots(service) {
  const slots = [];
  // Luon check profile legacy truoc (slot 0) neu ton tai
  const legacy = path.join(__dirname, `profile_${service}`);
  if (fs.existsSync(legacy)) slots.push({ slot: 0, profileDir: `profile_${service}` });
  // Them cac slot co so: profile_gemini_1, profile_gemini_2, ...
  for (let i = 1; i <= 10; i++) {
    const p = path.join(__dirname, `profile_${service}_${i}`);
    if (fs.existsSync(p)) slots.push({ slot: i, profileDir: `profile_${service}_${i}` });
  }
  return slots;
}

// Track slot hien tai cho moi service
const currentSlot = { gemini: null, meta: null };
// Shared contexts — 1 browser per service
const sharedCtx = { gemini: null, meta: null };
const ctxLock   = { gemini: false, meta: false };

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
  currentSlot[service] = next;
  return true;
}

async function getSharedPage(service) {
  const cookieKey = service === "gemini" ? "COOKIES_GEMINI" : "COOKIES_META";

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
    return await sharedCtx[service].newPage();
  } finally {
    ctxLock[service] = false;
  }
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

// ─── ROUTES ───────────────────────────────────────────

// Single
app.post("/start", upload.single("image"), (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No image" });
  const jobId = uuidv4();
  createJob(jobId, req.file.originalname, 0);
  log(jobId, "📋 Job queued");
  const geminiPrompt = req.body.geminiPrompt || "Turn this into a premium ecommerce product photo. Luxury background. Soft cinematic lighting. Ultra realistic.";
  const metaPrompt = req.body.metaPrompt || "Turn this image into a cinematic TikTok video. Smooth motion. Luxury commercial style.";
  geminiQueue.push({ jobId, imagePath: req.file.path, geminiPrompt, metaPrompt });
  processGeminiQueue();
  res.json({ jobId });
});

// Batch
app.post("/batch", upload.array("images", 20), (req, res) => {
  if (!req.files || req.files.length === 0) return res.status(400).json({ error: "No images" });
  const geminiPrompt = req.body.geminiPrompt || "Turn this into a premium ecommerce product photo. Luxury background. Soft cinematic lighting. Ultra realistic.";
  const metaPrompt = req.body.metaPrompt || "Turn this image into a cinematic TikTok video. Smooth motion. Luxury commercial style.";

  const jobIds = req.files.map((file, i) => {
    const jobId = uuidv4();
    createJob(jobId, file.originalname, i);
    log(jobId, `📋 Queued ${i+1}/${req.files.length} — ${file.originalname}`);
    geminiQueue.push({ jobId, imagePath: file.path, geminiPrompt, metaPrompt });
    return jobId;
  });

  processGeminiQueue();
  res.json({ jobIds, total: jobIds.length });
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
  console.log(`⚡ Gemini concurrency: ${GEMINI_CONCURRENCY} tabs | Meta AI: ${META_CONCURRENCY} tabs`);
  if (IS_LOCAL) {
    console.log(`📂 Mode: LOCAL — pipeline overlap enabled`);
    // Hien thi tat ca account slots tim duoc
    const gSlots = getProfileSlots("gemini");
    const mSlots = getProfileSlots("meta");
    console.log(`   Gemini accounts: ${gSlots.length > 0 ? gSlots.map(s => s.profileDir).join(", ") : "❌ NONE — chay: node add-account.js gemini 1"}`);
    console.log(`   Meta AI accounts: ${mSlots.length > 0 ? mSlots.map(s => s.profileDir).join(", ") : "❌ NONE — chay: node add-account.js meta 1"}`);
  } else {
    console.log(`☁️  Mode: CLOUD`);
    console.log(`   COOKIES_GEMINI: ${process.env.COOKIES_GEMINI ? "✅ set" : "❌ missing"}`);
    console.log(`   COOKIES_META:   ${process.env.COOKIES_META ? "✅ set" : "❌ missing"}`);
  }
  console.log(`\n🌐 Open: http://localhost:${PORT}\n`);
});
