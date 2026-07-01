# Outfeed Boards ETL Console

A high-performance, Cloudflare-native ETL pipeline and administrative dashboard designed for synchronizing Real Estate MLS data (TRREB, CRMLS, ARMLS) into a centralized Cloudflare D1 / Iceberg data lake.

## 🚀 Infrastructure Stack

- **Platform:** Cloudflare Workers (Edge Computing)
- **Router:** [Hono](https://hono.dev/) (Lightning-fast web framework)
- **Database:** Cloudflare D1 (`mls_data`)
- **Module System:** Vanilla ES Modules (`worker.mjs`) with native Text module bundling for HTML assets.
- **Automation:** Cloudflare Cron Triggers (Automated hourly syncs)

## 🎨 Design & Style Architecture

The frontend interface (`console.html`) is built to be a lightning-fast, zero-dependency administrative console that feels native, premium, and highly responsive.

### Aesthetic Principles
- **Finder-Style Layout:** Utilizes a 3-column, resizable layout inspired by macOS Finder to seamlessly navigate between Boards, Sections, and detailed Content Views.
- **Vanilla Excellence:** Built purely with HTML, CSS, and JS. No heavy front-end frameworks (React/Vue/Tailwind) are used, ensuring a 0kb JavaScript bundle overhead for the baseline UI.
- **Native Dark Mode:** Fully supports system-level `prefers-color-scheme` with tailored CSS variables, featuring carefully selected dark grays, muted borders, and Cloudflare Orange (`#f6821f`) accents in dark mode.
- **Typography:** Relies on the modern 'Inter' font family, gracefully falling back to system fonts (`-apple-system`, `BlinkMacSystemFont`) for a native OS feel.
- **Micro-Interactions:** Includes smooth hover transitions, scalable column resizers, status indicator pulses, and interactive chevron toggles for an engaging UX.

## ⚙️ Core Functionality

### 1. The ETL Pipeline
The core logic resides in `worker.mjs`. It performs a 3-step ETL (Extract, Transform, Load) process:
- Connects to the respective MLS Board's RESO Web API.
- Transforms the incoming payload into a standardized schema.
- Loads the normalized data into the `mls_data` Cloudflare D1 database.

### 2. Execution Triggers
- **Automated:** Runs passively via Cloudflare Cron Triggers (`0 * * * *` configured in `wrangler.jsonc`).
- **Manual:** Can be triggered on-demand via the UI dashboard which hits the `POST /interface/etl/run` endpoint.

### 3. API Endpoints & Security
The programmatic API surface is secured using **Bearer Token Authentication** injected via Cloudflare Secrets (`API_KEY`).
- `GET /` - Serves the administrative console UI.
- `POST /interface/etl/run` - Initiates an on-demand extraction for a specific board.
- `GET /interface/:boardId` - Retrieves a localized snapshot of the processed listings for a given board.

## 🛠️ Local Development

To run the console locally, ensure you have your `.dev.vars` configured with the necessary API keys, then simply run:

```bash
npm run dev
```

This will boot the Wrangler development server, bind your local D1 database, and serve the application at `http://localhost:8787`.
