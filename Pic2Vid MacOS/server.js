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

// Concurrency settings
const GEMINI_CONCURRENCY = 3;  // Max 3 Gemini tabs song song
const META_CONCURRENCY = 2;    // Max 2 Meta AI tabs song song

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
    filename: filename || "", index: index || 0
  };
  return jobs[id];
}
function update(id, data) { if (jobs[id]) Object.assign(jobs[id], data); }
function log(id, msg) {
  if (jobs[id]) { jobs[id].logs.push(msg); console.log(`[${id.slice(0,8)}] ${msg}`); }
}

// ─── PARALLEL PIPELINE ────────────────────────────────
// Gemini: max 3 tabs song song
// Meta AI: 1 tab tuần tự (nhưng chạy overlap với Gemini)
const geminiQueue = [];   // { jobId, imagePath, geminiPrompt, metaPrompt }
const metaQueue = [];     // { jobId, enhancedPath, metaPrompt }

let geminiActive = 0;     // Số tab Gemini đang chạy
let metaActive = 0;       // Số tab Meta AI đang chạy

const MAX_RETRIES = 2; // Retry tối đa 2 lần (tổng 3 attempts)

function processGeminiQueue() {
  while (geminiActive < GEMINI_CONCURRENCY && geminiQueue.length > 0) {
    const task = geminiQueue.shift();
    task.retries = task.retries || 0;
    geminiActive++;
    update(task.jobId, { step: "gemini_running" });

    runGemini(task.jobId, task.imagePath, task.geminiPrompt)
      .then(enhanced => {
        update(task.jobId, { step: "gemini_done", enhancedImage: enhanced });
        const localPath = path.join(__dirname, enhanced.replace("/outputs/", "outputs/"));
        const enhancedSize = fs.existsSync(localPath) ? fs.statSync(localPath).size : 0;
        log(task.jobId, `✅ Gemini done — enhanced: ${(enhancedSize/1024).toFixed(0)}KB → Meta AI queue`);
        metaQueue.push({ jobId: task.jobId, enhancedPath: localPath, metaPrompt: task.metaPrompt, retries: 0 });
        processMetaQueue();
      })
      .catch(err => {
        if (task.retries < MAX_RETRIES) {
          task.retries++;
          log(task.jobId, `⚠️ Gemini failed (attempt ${task.retries}/${MAX_RETRIES+1}): ${err.message} — retrying...`);
          update(task.jobId, { step: "queued" });
          geminiQueue.unshift(task); // đưa lại đầu queue
        } else {
          log(task.jobId, `❌ Gemini error (all ${MAX_RETRIES+1} attempts failed): ${err.message}`);
          update(task.jobId, { step: "error", error: err.message });
        }
      })
      .finally(() => {
        geminiActive--;
        processGeminiQueue();
      });
  }
}

function processMetaQueue() {
  while (metaActive < META_CONCURRENCY && metaQueue.length > 0) {
    const task = metaQueue.shift();
    task.retries = task.retries || 0;
    metaActive++;
    update(task.jobId, { step: "meta_running" });

    runMetaAI(task.jobId, task.enhancedPath, task.metaPrompt)
      .then(video => {
        update(task.jobId, { step: "meta_done", videoUrl: video });
        log(task.jobId, "🎉 Done!");
      })
      .catch(err => {
        if (task.retries < MAX_RETRIES) {
          task.retries++;
          log(task.jobId, `⚠️ Meta AI failed (attempt ${task.retries}/${MAX_RETRIES+1}): ${err.message} — retrying...`);
          update(task.jobId, { step: "gemini_done" }); // revert to waiting for meta
          metaQueue.unshift(task);
        } else {
          log(task.jobId, `❌ Meta AI error (all ${MAX_RETRIES+1} attempts failed): ${err.message}`);
          update(task.jobId, { step: "error", error: err.message });
        }
        update(task.jobId, { step: "error", error: err.message });
      })
      .finally(() => {
        metaActive--;
        processMetaQueue(); // pick up next
      });
  }
}

// ─── BROWSER CONTEXT MANAGER ──────────────────────────
function loadCookieState(envKey) {
  const raw = process.env[envKey];
  if (!raw) return null;
  try { return JSON.parse(Buffer.from(raw, "base64").toString("utf8")); }
  catch { return null; }
}

// Shared browser contexts (local mode)
let geminiCtx = null;
let metaCtx = null;

async function getContext(profileDir, cookieEnvKey) {
  if (IS_LOCAL) {
    const isGemini = profileDir.includes("gemini");

    // Reuse existing context — nhưng kiểm tra còn sống không
    if (isGemini && geminiCtx) {
      try {
        await geminiCtx.pages(); // test if context is alive
        return { ctx: geminiCtx, shared: true };
      } catch {
        geminiCtx = null; // context died, recreate
      }
    }
    if (!isGemini && metaCtx) {
      try {
        await metaCtx.pages();
        return { ctx: metaCtx, shared: true };
      } catch {
        metaCtx = null;
      }
    }

    const ctx = await chromium.launchPersistentContext(
      path.join(__dirname, profileDir),
      {
        headless: false,
        channel: "chrome",
        acceptDownloads: true,
        viewport: { width: 1280, height: 800 },
        args: ["--disable-blink-features=AutomationControlled", "--window-size=1280,800", "--window-position=0,0"]
      }
    );

    if (isGemini) geminiCtx = ctx;
    else metaCtx = ctx;

    return { ctx, shared: true };
  } else {
    const cookieState = loadCookieState(cookieEnvKey);
    if (!cookieState) throw new Error(`${cookieEnvKey} not set`);
    const browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage","--disable-blink-features=AutomationControlled"]
    });
    const ctx = await browser.newContext({
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
      viewport: { width: 1280, height: 800 }
    });
    if (cookieState?.cookies?.length) await ctx.addCookies(cookieState.cookies);
    return { ctx, browser, shared: false };
  }
}

async function releaseContext(profileDir, ctxInfo) {
  if (!ctxInfo.shared) {
    await ctxInfo.ctx.close().catch(() => {});
    if (ctxInfo.browser) await ctxInfo.browser.close().catch(() => {});
  }
  // Shared contexts stay open — closed on server shutdown or idle timeout
}

// Pre-warm: mở browser context sẵn khi server start
async function prewarmContexts() {
  if (!IS_LOCAL) return;
  const geminiProfile = path.join(__dirname, "profile_gemini");
  const metaProfile = path.join(__dirname, "profile_meta");
  if (fs.existsSync(geminiProfile) && fs.existsSync(metaProfile)) {
    console.log("🔥 Pre-warming browser contexts...");
    try {
      await getContext("profile_gemini", "COOKIES_GEMINI");
      await getContext("profile_meta", "COOKIES_META");
      console.log("✅ Browser contexts ready");
    } catch (e) {
      console.log(`⚠️ Pre-warm failed: ${e.message}`);
    }
  }
}

// ─── GEMINI ───────────────────────────────────────────
async function runGemini(jobId, imagePath, prompt) {
  log(jobId, "🚀 Opening Gemini...");
  const ctxInfo = await getContext("profile_gemini", "COOKIES_GEMINI");
  const { ctx } = ctxInfo;
  const page = await ctx.newPage();
  try {
    await page.goto("https://gemini.google.com/app/new", { waitUntil: "domcontentloaded" });

    // Chờ prompt box xuất hiện thay vì wait cứng 8s
    await page.waitForSelector('[role="textbox"], textarea, div[contenteditable="true"]', { timeout: 15000 }).catch(() => {});
    await page.waitForTimeout(2000); // buffer nhỏ cho JS init

    if (page.url().includes("accounts.google.com")) throw new Error("Gemini session expired");

    log(jobId, "📤 Uploading image...");

    // Click nút upload (mat-badge selector ổn định)
    let uploadBtn = await page.$('button.mat-badge[aria-haspopup="menu"]');
    if (!uploadBtn) {
      uploadBtn = await page.$('button[aria-label*="upload" i], button[aria-label*="tải lên" i], button[aria-label*="Upload" i]');
    }
    if (uploadBtn) {
      await uploadBtn.click();
      // Chờ menu xuất hiện
      await page.waitForSelector('[role="menuitem"][aria-haspopup="dialog"]', { timeout: 5000 }).catch(() => {});
    }

    // Click menu item upload
    const uploadMenuItem = await page.$('[role="menuitem"][aria-haspopup="dialog"]');
    if (uploadMenuItem) {
      await uploadMenuItem.click();
      // Chờ file input xuất hiện
      await page.waitForSelector('input[type="file"]', { timeout: 5000 }).catch(() => {});
    }

    // Set file
    const fileInput = await page.$('input[type="file"]');
    if (!fileInput) throw new Error("Upload input not found on Gemini");
    await fileInput.setInputFiles(imagePath);
    log(jobId, "✅ Image uploaded");

    // Đóng menu + dismiss consent
    await page.keyboard.press("Escape");
    await page.waitForTimeout(500);

    for (const text of ["Đồng ý", "I agree", "Accept", "Got it", "OK"]) {
      const btn = await page.$(`button:has-text("${text}"), [role="button"]:has-text("${text}")`);
      if (btn && await btn.isVisible().catch(() => false)) {
        await btn.click();
        log(jobId, `📋 Dismissed: "${text}"`);
        break;
      }
    }

    // Chờ file được attach (thumbnail xuất hiện)
    await page.waitForTimeout(2000);

    // Tìm prompt box
    let promptBox = await page.$('[aria-label*="prompt" i][role="textbox"], [aria-label*="câu lệnh" i][role="textbox"]');
    if (!promptBox) promptBox = await page.$('div[contenteditable="true"]');
    if (!promptBox) promptBox = await page.$("textarea");
    if (!promptBox) {
      // Retry với wait
      await page.waitForSelector('[role="textbox"], textarea', { timeout: 10000 });
      promptBox = await page.$('[role="textbox"]') || await page.$("textarea");
    }
    if (!promptBox) throw new Error("Prompt box not found");

    // Snapshot ảnh hiện có trước khi gửi prompt
    const existingImgSrcs = new Set();
    const existingImgAreas = new Set();
    for (const img of await page.$$("img")) {
      try {
        const src = await img.getAttribute("src");
        if (src) existingImgSrcs.add(src);
        const box = await img.boundingBox();
        if (box) existingImgAreas.add(`${Math.round(box.width)}x${Math.round(box.height)}`);
      } catch (_) {}
    }

    await promptBox.fill(prompt);
    await promptBox.press("Enter");
    log(jobId, "⏳ Waiting for Gemini to generate...");

    // Poll ảnh MỚI (max 2.5 phút, poll mỗi 3s thay vì 5s)
    let largestImg = null;
    let largestArea = 0;
    for (let attempt = 0; attempt < 50; attempt++) {
      await page.waitForTimeout(3000);
      largestImg = null;
      largestArea = 0;
      for (const img of await page.$$("img")) {
        try {
          const src = await img.getAttribute("src");
          if (src && existingImgSrcs.has(src)) continue;
          const box = await img.boundingBox();
          // Ảnh generated phải có cả width VÀ height > 200px
          if (!box || box.width < 200 || box.height < 200) continue;
          const area = box.width * box.height;
          if (area > largestArea) {
            largestArea = area;
            largestImg = img;
          }
        } catch (_) {}
      }
      if (largestImg && largestArea > 80000) {
        log(jobId, `✅ Image generated (${(attempt+1)*3}s)`);
        break;
      }
      if (attempt % 10 === 0 && attempt > 0) log(jobId, `⏳ Still waiting... (${attempt*3}s)`);
      largestImg = null;
    }

    if (!largestImg) throw new Error("Generated image not found after 2.5 minutes");

    const imgBox = await largestImg.boundingBox();
    log(jobId, `📐 Image ${Math.round(imgBox.width)}x${Math.round(imgBox.height)}`);

    // Download ảnh — ưu tiên fetch src trực tiếp
    const imgSrc = await largestImg.getAttribute("src");
    const outPath = path.join(__dirname, "outputs", `${jobId}_enhanced.png`);

    let saved = false;

    // Method 1: fetch (works for http/data URLs)
    if (!saved && imgSrc && (imgSrc.startsWith("http") || imgSrc.startsWith("data:"))) {
      try {
        const imgBuffer = await page.evaluate(async (url) => {
          const r = await fetch(url);
          const ab = await r.arrayBuffer();
          return Array.from(new Uint8Array(ab));
        }, imgSrc);
        fs.writeFileSync(outPath, Buffer.from(imgBuffer));
        log(jobId, `✅ Image saved (fetch, ${(imgBuffer.length/1024).toFixed(0)}KB)`);
        saved = true;
      } catch (e) {
        log(jobId, `⚠️ Fetch failed: ${e.message.slice(0, 50)}`);
      }
    }

    // Method 2: canvas toDataURL (works for blob: and any visible image)
    if (!saved) {
      try {
        const base64 = await page.evaluate((imgEl) => {
          return new Promise((resolve, reject) => {
            const canvas = document.createElement("canvas");
            canvas.width = imgEl.naturalWidth || imgEl.width;
            canvas.height = imgEl.naturalHeight || imgEl.height;
            const ctx2d = canvas.getContext("2d");
            ctx2d.drawImage(imgEl, 0, 0);
            resolve(canvas.toDataURL("image/png").split(",")[1]);
          });
        }, largestImg);
        fs.writeFileSync(outPath, Buffer.from(base64, "base64"));
        const size = fs.statSync(outPath).size;
        log(jobId, `✅ Image saved (canvas, ${(size/1024).toFixed(0)}KB)`);
        saved = true;
      } catch (e) {
        log(jobId, `⚠️ Canvas failed: ${e.message.slice(0, 50)}`);
      }
    }

    // Method 3: screenshot element (last resort)
    if (!saved) {
      await largestImg.screenshot({ path: outPath });
      const size = fs.statSync(outPath).size;
      log(jobId, `✅ Image saved (screenshot, ${(size/1024).toFixed(0)}KB)`);
    }

    return `/outputs/${jobId}_enhanced.png`;
  } finally {
    await page.close().catch(() => {});
    await releaseContext("profile_gemini", ctxInfo);
  }
}

// ─── META AI ──────────────────────────────────────────
async function runMetaAI(jobId, imagePath, prompt) {
  log(jobId, "🚀 Opening Meta AI...");
  const ctxInfo = await getContext("profile_meta", "COOKIES_META");
  const { ctx } = ctxInfo;
  const page = await ctx.newPage();
  try {
    await page.goto("https://meta.ai", { waitUntil: "domcontentloaded", timeout: 60000 });

    // Chờ file input xuất hiện thay vì wait cứng 8s
    await page.waitForSelector('input[type="file"]', { timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(2000);

    const fileInput = await page.$('input[type="file"]');
    if (!fileInput) throw new Error("Not logged in to Meta AI");

    log(jobId, "📤 Uploading image...");
    const uploadSize = fs.existsSync(imagePath) ? fs.statSync(imagePath).size : 0;
    log(jobId, `📎 File: ${path.basename(imagePath)} (${(uploadSize/1024).toFixed(0)}KB)`);
    await fileInput.setInputFiles(imagePath);
    log(jobId, "✅ Image uploaded");
    await page.mouse.click(200, 200);

    // Chờ prompt box sẵn sàng thay vì wait cứng 12s
    await page.waitForSelector('textarea, div[contenteditable="true"]', { timeout: 15000 }).catch(() => {});
    await page.waitForTimeout(3000);

    let promptBox = await page.$("textarea");
    if (!promptBox || !await promptBox.isVisible().catch(() => false)) {
      promptBox = await page.$('div[contenteditable="true"]');
    }
    if (!promptBox) throw new Error("Prompt box not found on Meta AI");

    await promptBox.fill(prompt);
    await promptBox.press("Enter");
    log(jobId, "⏳ Generating video (~3 min)...");

    // Poll video (mỗi 3s thay vì 5s, max 6 phút)
    let videoUrl = null;
    for (let i = 0; i < 120; i++) {
      await page.waitForTimeout(3000);
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
      if (i % 20 === 0 && i > 0) log(jobId, `⏳ Still generating... (${Math.round(i*3/60)} min)`);
    }

    if (!videoUrl) {
      // Try download button
      for (const btn of await page.$$("button, a")) {
        try {
          const txt = `${await btn.getAttribute("aria-label")||""} ${await btn.innerText().catch(()=>"")}`.toLowerCase();
          if (txt.includes("download")) {
            const dlPromise = page.waitForEvent("download", { timeout: 20000 });
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
    await releaseContext("profile_meta", ctxInfo);
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

// Cleanup old jobs every hour
setInterval(() => {
  const cutoff = Date.now() - 3600000;
  Object.keys(jobs).forEach(id => { if (jobs[id].createdAt < cutoff) delete jobs[id]; });
}, 3600000);

// ─── START ────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`\n🚀 Pic2Vid server running on port ${PORT}`);
  console.log(`⚡ Gemini concurrency: ${GEMINI_CONCURRENCY} tabs | Meta AI: ${META_CONCURRENCY} tab`);
  if (IS_LOCAL) {
    console.log(`📂 Mode: LOCAL`);
    console.log(`   profile_gemini: ${fs.existsSync(path.join(__dirname, "profile_gemini")) ? "✅ found" : "❌ missing"}`);
    console.log(`   profile_meta:   ${fs.existsSync(path.join(__dirname, "profile_meta")) ? "✅ found" : "❌ missing"}`);
    await prewarmContexts();
  } else {
    console.log(`☁️  Mode: CLOUD`);
    console.log(`   COOKIES_GEMINI: ${process.env.COOKIES_GEMINI ? "✅ set" : "❌ missing"}`);
    console.log(`   COOKIES_META:   ${process.env.COOKIES_META ? "✅ set" : "❌ missing"}`);
  }
  console.log(`\n🌐 Open: http://localhost:${PORT}\n`);
});
