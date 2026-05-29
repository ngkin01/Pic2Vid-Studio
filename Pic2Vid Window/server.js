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
        if (retryCount < 2) {
          update(task.jobId, { step: "queued", retryCount: retryCount + 1, error: null });
          log(task.jobId, `⚠️ Gemini failed (${err.message}) — retry ${retryCount + 1}/2...`);
          geminiQueue.push(task);
        } else {
          log(task.jobId, `❌ Gemini error after 2 retries: ${err.message}`);
          update(task.jobId, { step: "error", error: err.message });
        }
      } finally {
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
        if (retryCount < 2) {
          update(task.jobId, { step: "queued", retryCount: retryCount + 1, error: null });
          log(task.jobId, `⚠️ Meta AI failed (${err.message}) — retry ${retryCount + 1}/2...`);
          metaQueue.push(task);
        } else {
          log(task.jobId, `❌ Meta AI error after 2 retries: ${err.message}`);
          update(task.jobId, { step: "error", error: err.message });
        }
      } finally {
        metaActive--;
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

// Shared contexts — 1 browser per service, nhiều tab trong đó
const sharedCtx = { gemini: null, meta: null };
const ctxLock   = { gemini: false, meta: false };

async function getSharedPage(service) {
  // service = "gemini" | "meta"
  const profileDir  = service === "gemini" ? "profile_gemini" : "profile_meta";
  const cookieKey   = service === "gemini" ? "COOKIES_GEMINI" : "COOKIES_META";

  // Chờ nếu đang có thread khác đang khởi tạo context
  while (ctxLock[service]) await new Promise(r => setTimeout(r, 200));

  // Nếu context đã có và còn sống → mở tab mới
  if (sharedCtx[service]) {
    try {
      const page = await sharedCtx[service].newPage();
      return page;
    } catch (_) {
      // Context chết → tạo lại
      sharedCtx[service] = null;
    }
  }

  // Tạo context mới
  ctxLock[service] = true;
  try {
    if (IS_LOCAL) {
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

// Clear cache của browser profile — CHỈ xóa cache, KHÔNG đụng cookies/session
function clearBrowserCache(profileDir) {
  // SAFE: chỉ xóa cache thuần, không đụng Network/Sessions/Cookies/Login Data
  const safeCacheFolders = [
    "Cache", "Cache_Data", "Code Cache", "GPUCache",
    "DawnCache", "ShaderCache", "blob_storage"
  ];
  const profilePath = path.join(__dirname, profileDir, "Default");
  if (!fs.existsSync(profilePath)) return;
  let cleared = 0;
  for (const folder of safeCacheFolders) {
    const folderPath = path.join(profilePath, folder);
    if (fs.existsSync(folderPath)) {
      try {
        fs.rmSync(folderPath, { recursive: true, force: true });
        cleared++;
      } catch (_) {}
    }
  }
  // Top-level cache only (safe)
  const topPath = path.join(__dirname, profileDir);
  for (const folder of ["Cache", "Code Cache", "GPUCache", "ShaderCache"]) {
    const folderPath = path.join(topPath, folder);
    if (fs.existsSync(folderPath)) {
      try { fs.rmSync(folderPath, { recursive: true, force: true }); cleared++; } catch (_) {}
    }
  }
  if (cleared > 0) console.log(`🧹 Cleared ${cleared} cache folders from ${profileDir}`);
}

// ─── GEMINI ───────────────────────────────────────────
async function runGemini(jobId, imagePath, prompt) {
  log(jobId, "🚀 Opening Gemini...");
  const page = await getSharedPage("gemini");
  try {
    await page.goto("https://gemini.google.com/app/new", { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(8000);
    if (page.url().includes("accounts.google.com")) throw new Error("Gemini session expired");

    log(jobId, "📤 Uploading image...");
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
    log(jobId, "✅ Image uploaded");
    await page.waitForTimeout(5000);

    let promptBox = null;
    for (let i = 0; i < 30; i++) {
      let box = await page.$("textarea");
      if (box && await box.isVisible().catch(() => false)) { promptBox = box; break; }
      box = await page.$('div[contenteditable="true"]');
      if (box) { promptBox = box; break; }
      await page.waitForTimeout(2000);
    }
    if (!promptBox) throw new Error("Prompt box not found");
    await promptBox.fill(prompt);
    await promptBox.press("Enter");

    log(jobId, "⏳ Waiting for Gemini to generate image...");

    // Poll tối đa 4 phút
    // Logic: chờ DOM stable 2 lần liên tiếp (10s không đổi) VÀ có ảnh > 20000px²
    let generatedImg = null;
    let lastHtml = 0;
    let stableCount = 0;
    let smallImageCount = 0;

    for (let attempt = 0; attempt < 18; attempt++) {
      await page.waitForTimeout(5000);

      // Check DOM stable — KHÔNG reset khi thấy ảnh nhỏ
      const currentHtml = await page.evaluate(() => document.body.innerHTML.length);
      if (currentHtml === lastHtml) {
        stableCount++;
      } else {
        stableCount = 0;
        lastHtml = currentHtml;
      }

      // Tìm ảnh cuối cùng visible có size hợp lý
      const allImgs = await page.$$("img");
      let candidate = null;
      let candidateArea = 0;
      for (let i = allImgs.length - 1; i >= 0; i--) {
        try {
          const box = await allImgs[i].boundingBox();
          const visible = await allImgs[i].isVisible().catch(() => false);
          if (visible && box && box.width > 100 && box.height > 100) {
            candidate = allImgs[i];
            candidateArea = box.width * box.height;
            break;
          }
        } catch (_) {}
      }

      if (candidate && candidateArea > 20000) {
        // Ảnh đủ lớn — nếu DOM đã stable thì dừng
        if (stableCount >= 1) {
          const cBox = await candidate.boundingBox().catch(() => null);
          generatedImg = candidate;
          log(jobId, `✅ Image found after ${(attempt+1)*5}s (${Math.round(cBox?.width||0)}x${Math.round(cBox?.height||0)})`);
          break;
        }
        // DOM chưa stable — chờ thêm
        log(jobId, `⏳ Image found but DOM still changing... (${(attempt+1)*5}s)`);
      } else if (candidate && candidateArea <= 20000) {
        // Ảnh nhỏ (thumbnail 112x112) — Gemini chưa xong, chờ tiếp
        smallImageCount++;
        if (smallImageCount % 6 === 0) {
          log(jobId, `⏳ Found image but too small (${Math.round(Math.sqrt(candidateArea))}px) — Gemini still generating... (${(attempt+1)*5}s)`);
        }
      } else {
        log(jobId, `⏳ Waiting for image... (${(attempt+1)*5}s)`);
      }
    }

    if (!generatedImg) throw new Error("Generated image not found after 1.5 minutes");

    const imgBox = await generatedImg.boundingBox().catch(() => null);
    if (!imgBox) throw new Error("Image element became stale after detection");
    log(jobId, `📐 Found image ${Math.round(imgBox.width)}x${Math.round(imgBox.height)}`);
    let largestImg = generatedImg;

    // Scroll ảnh vào viewport
    await generatedImg.scrollIntoViewIfNeeded().catch(() => {});
    await page.waitForTimeout(1000);

    // Hover vào giữa ảnh trước
    await page.mouse.move(imgBox.x + imgBox.width / 2, imgBox.y + imgBox.height / 2).catch(() => {});
    await page.waitForTimeout(1500);

    // Hover vào góc trên phải nơi có 3 nút toolbar
    await page.mouse.move(imgBox.x + imgBox.width - 40, imgBox.y + 40).catch(() => {});
    await page.waitForTimeout(2000);

    // Debug: log tất cả button visible trên trang
    const allVisibleBtns = await page.$$("button");
    const btnDebug = [];
    for (const btn of allVisibleBtns) {
      try {
        const visible = await btn.isVisible().catch(() => false);
        if (!visible) continue;
        const aria = await btn.getAttribute("aria-label") || "";
        const title = await btn.getAttribute("title") || "";
        const text = await btn.innerText().catch(() => "");
        const bBox = await btn.boundingBox().catch(() => null);
        btnDebug.push(`[${aria}|${title}|${text.slice(0,20)}] @ ${bBox ? Math.round(bBox.x)+','+Math.round(bBox.y) : 'no-box'}`);
      } catch (_) {}
    }
    log(jobId, `🔍 Visible buttons: ${btnDebug.slice(0,10).join(' | ')}`);

    // Tìm download button — thử nhiều cách
    let downloadBtn = null;

    // Cách 1: aria-label hoặc title chứa download
    for (const btn of await page.$$("button")) {
      try {
        const aria = (await btn.getAttribute("aria-label") || "").toLowerCase();
        const title = (await btn.getAttribute("title") || "").toLowerCase();
        if (aria.includes("download") || title.includes("download")) {
          const visible = await btn.isVisible().catch(() => false);
          if (visible) { downloadBtn = btn; break; }
        }
      } catch (_) {}
    }

    // Cách 2: toolbar hiện 3 nút — lấy nút cuối cùng (download là nút thứ 3)
    if (!downloadBtn) {
      // Tìm các button visible nằm trong vùng góc trên phải của ảnh
      const allBtns = await page.$$("button");
      const nearBtns = [];
      for (const btn of allBtns) {
        try {
          const bBox = await btn.boundingBox();
          if (!bBox) continue;
          const visible = await btn.isVisible().catch(() => false);
          if (!visible) continue;
          // Button nằm trong vùng toolbar của ảnh (góc trên phải)
          if (
            bBox.x > imgBox.x + imgBox.width * 0.5 &&
            bBox.y < imgBox.y + imgBox.height * 0.3 &&
            bBox.x < imgBox.x + imgBox.width + 20
          ) {
            nearBtns.push({ btn, x: bBox.x });
          }
        } catch (_) {}
      }
      // Sort theo x, lấy nút ngoài cùng phải (download)
      if (nearBtns.length > 0) {
        nearBtns.sort((a, b) => b.x - a.x);
        downloadBtn = nearBtns[0].btn;
        log(jobId, `🎯 Found toolbar button (${nearBtns.length} buttons near image)`);
      }
    }

    const outPath = path.join(__dirname, "outputs", `${jobId}_enhanced.png`);
    let saved = false;

    // Cách 0: lấy src ảnh rồi fetch trực tiếp — không cần click UI
    try {
      const imgSrc = await generatedImg.evaluate(el =>
        el.src || el.getAttribute("data-src") || (el.srcset || "").split(",")[0].trim().split(" ")[0] || ""
      ).catch(() => "");
      if (imgSrc && imgSrc.startsWith("http")) {
        const buf = await page.evaluate(async (url) => {
          const r = await fetch(url, { credentials: "include" });
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          const ab = await r.arrayBuffer();
          return Array.from(new Uint8Array(ab));
        }, imgSrc);
        fs.writeFileSync(outPath, Buffer.from(buf));
        log(jobId, "✅ Enhanced image saved (direct fetch)");
        saved = true;
      }
    } catch (e) {
      log(jobId, `⚠️ Direct fetch failed: ${e.message}`);
    }

    // Cách 1: click download button nếu tìm được
    if (!saved && downloadBtn) {
      try {
        log(jobId, "📥 Trying download button...");
        const dlPromise = page.waitForEvent("download", { timeout: 30000 });
        await downloadBtn.click();
        const dl = await dlPromise;
        await dl.saveAs(outPath);
        log(jobId, "✅ Enhanced image saved (download button)");
        saved = true;
      } catch (e) {
        log(jobId, `⚠️ Download button failed: ${e.message}`);
      }
    }

    if (!saved) throw new Error("Could not download enhanced image — all methods failed");
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
      if (i % 6 === 0) log(jobId, `⏳ Still generating... (${Math.round(i*5/60)} min)`);
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

  processGeminiQueue(); // kick off Gemini queue
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
    console.log(`   profile_gemini: ${fs.existsSync(path.join(__dirname, "profile_gemini")) ? "✅ found" : "❌ missing"}`);
    console.log(`   profile_meta:   ${fs.existsSync(path.join(__dirname, "profile_meta")) ? "✅ found" : "❌ missing"}`);
  } else {
    console.log(`☁️  Mode: CLOUD`);
    console.log(`   COOKIES_GEMINI: ${process.env.COOKIES_GEMINI ? "✅ set" : "❌ missing"}`);
    console.log(`   COOKIES_META:   ${process.env.COOKIES_META ? "✅ set" : "❌ missing"}`);
  }
  console.log(`\n🌐 Open: http://localhost:${PORT}\n`);
});
