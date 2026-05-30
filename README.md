<p align="center">
  <img src="banner.png" alt="Pic2Vid Studio Logo" width="700"/>
</p>

<h1 align="center">🎬 Pic2Vid Studio</h1>

<p align="center">
  Turn product photos into TikTok-style videos automatically — no editing skills or AI knowledge required.
</p>

<p align="center">
  <a href="https://ngkin01.github.io/Pic2Vid-Studio/">🌐 Live Demo</a>
  &nbsp;·&nbsp;
  <a href="#-quick-setup-windows">🪟 Windows Setup</a>
  &nbsp;·&nbsp;
  <a href="#-quick-setup-macos">🍎 macOS Setup</a>
  &nbsp;·&nbsp;
  <a href="#-troubleshooting">🛠️ Troubleshooting</a>
</p>

---

## ✨ Features

- 📸 **AI Photo Enhancement** — Upload a product image; Gemini AI automatically generates a cleaner, higher-quality version.
- 🎥 **Auto Video Generation** — The enhanced image is passed to Meta AI to produce a short-form TikTok-style video.
- ⚡ **Parallel Processing** — Multiple images processed simultaneously via concurrent browser automation (up to 3 Gemini + 2 Meta AI tabs at once).
- 🖥️ **Cross-Platform** — Native support for both Windows 10/11 and macOS.
- 🔒 **No API Keys Needed** — Works through your existing Google and Facebook accounts; no paid API access required.
- 🧠 **Beginner-Friendly UI** — Simple web interface, no technical knowledge needed.

---

## 🔧 How It Works

```
Your Product Photo
       │
       ▼
 ┌─────────────┐      browser automation
 │  Gemini AI  │ ◄──── (Playwright + your Google account)
 └─────────────┘
       │  Enhanced Image
       ▼
 ┌─────────────┐      browser automation
 │   Meta AI   │ ◄──── (Playwright + your Facebook account)
 └─────────────┘
       │
       ▼
  TikTok Video 🎬
```

Pic2Vid Studio runs a local **Express** server that uses **Playwright** to automate Chrome — it logs into Gemini and Meta AI on your behalf, submits your images, and downloads the results automatically. No cloud server, no API fees.

---

## 📦 Versions

| Version | Operating System |
|---|---|
| `Pic2Vid Windows` | Windows 10 / 11 |
| `Pic2Vid macOS` | macOS (Monterey or later recommended) |

---

## 🪟 Quick Setup (Windows)

### Requirements

- Windows 10 or 11
- A **Google account** (for Gemini AI)
- A **Facebook account** (for Meta AI)
- **Google Chrome** installed

### Installation Steps

**1.** Download and install the latest LTS version of Node.js
👉 https://nodejs.org

**2.** Extract the `Pic2Vid Windows` folder to your Desktop

**3.** Right-click `SETUP.bat` → select **Run as administrator**
*(First-time setup only — takes about 2–3 minutes)*

**4.** A Chrome window will open automatically:
   - Log into **Gemini AI** with your Google account → return to the terminal → press **Enter**
   - Log into **Meta AI** with your Facebook account → return to the terminal → press **Enter**

**5.** Setup complete! From now on, just double-click `START.bat` to launch the app.

> 💡 Keep the terminal window open while using the app. Closing it will stop the server.

### Create a Desktop Shortcut (optional)

1. Right-click `START.bat` → **Create shortcut**
2. Drag the shortcut to your Desktop
3. Double-click it anytime to launch Pic2Vid Studio

---

## 🍎 Quick Setup (macOS)

### Requirements

- macOS (Monterey or later recommended)
- A **Google account** (for Gemini AI)
- A **Facebook account** (for Meta AI)
- **Google Chrome** installed

### Installation Steps

**1.** Download and install the latest LTS version of Node.js
👉 https://nodejs.org

**2.** Extract the `Pic2Vid macOS` folder to your Desktop

**3.** Double-click `SETUP.command` to run first-time setup
*(If macOS blocks it: go to **System Settings → Privacy & Security** → click **Open Anyway**)*

**4.** A Chrome window will open automatically:
   - Log into **Gemini AI** with your Google account → return to the terminal → press **Enter**
   - Log into **Meta AI** with your Facebook account → return to the terminal → press **Enter**

**5.** Setup complete! From now on, just double-click `START.command` to launch the app.

> 💡 Keep the terminal window open while using the app. Closing it will stop the server.

---

## 📱 Daily Usage

1. Launch the app via `START.bat` (Windows) or `START.command` (macOS)
2. Your browser will open the Pic2Vid Studio web interface automatically
3. Upload one or more product photos
4. Click **Generate** — the app handles everything else
5. Download your enhanced image and generated video when done

---

## 🛠️ Troubleshooting

| Problem | Solution |
|---|---|
| `SETUP.bat` doesn't run | Right-click → **Run as administrator** |
| macOS blocks `SETUP.command` | Go to **System Settings → Privacy & Security → Open Anyway** |
| Chrome doesn't open during setup | Make sure Google Chrome is installed (not just another browser) |
| Login step fails / times out | Re-run `SETUP.bat` / `SETUP.command` to redo the login |
| App opens but no output is generated | Make sure you're still logged in to Gemini and Meta AI — re-run setup if needed |
| Port 3000 already in use | Close other apps using port 3000, or restart your computer |
| `node` not found after installing | Restart your terminal / computer after installing Node.js |

---

## 🛠️ Built With

| Technology | Role |
|---|---|
| [Node.js](https://nodejs.org) | Backend runtime |
| [Express](https://expressjs.com) | Local web server |
| [Playwright](https://playwright.dev) | Browser automation (Gemini & Meta AI) |
| [Multer](https://github.com/expressjs/multer) | Image upload handling |
| HTML / CSS / JavaScript | Frontend UI |
| [Gemini AI](https://gemini.google.com) | AI image enhancement |
| [Meta AI](https://www.meta.ai) | AI video generation |

---

## 👤 Author

**Tommy Nguyen**
GitHub: [@ngkin01](https://github.com/ngkin01)

---

## ⚠️ Disclaimer

This tool automates interaction with Gemini AI and Meta AI through your personal accounts via browser automation. Use responsibly and in accordance with the Terms of Service of each platform. The author is not responsible for any account restrictions resulting from use of this tool.

