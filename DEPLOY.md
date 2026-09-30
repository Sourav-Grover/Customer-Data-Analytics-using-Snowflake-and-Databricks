# Deploying the dashboards

## Static site (recommended — simplest, free, no server)

`site/index.html` is one self-contained file: the Gold data is embedded as JSON and the
charts are hand-rolled inline SVG. No build step, no framework, no external script.

### Option A — Netlify (drag and drop, fastest)
1. Go to https://app.netlify.com/drop
2. Drag the `site/` folder onto the page.
3. Netlify gives you a live URL in seconds. Rename the site under **Site settings** if
   you want a nicer subdomain.

### Option B — Netlify (connected to GitHub)
1. Push this repo to GitHub (see below).
2. In Netlify: **Add new site → Import an existing project → GitHub** → pick the repo.
3. Build command: leave blank. Publish directory: `site`.
   (`netlify.toml` in the repo root already sets `publish = "site"`.)
4. Deploy. Every push to `main` redeploys automatically.

### Option C — Vercel
1. Push this repo to GitHub.
2. In Vercel: **Add New → Project** → import the repo.
3. Framework preset: **Other**. Output directory: `site` (already set in `vercel.json`).
4. Deploy.

### Option D — GitHub Pages
1. Push this repo to GitHub.
2. Repo **Settings → Pages → Deploy from a branch** → branch `main`, folder `/site`.
3. Wait a minute, then open the URL GitHub shows you.

Regenerate the page any time the Gold data changes:
```bash
python local_run/export_gold_to_csv.py
python dashboard/build_static_site.py
```

## Streamlit dashboard (Streamlit Community Cloud)

1. Push this repo to GitHub.
2. Go to https://share.streamlit.io → **New app**.
3. Repository: this repo. Branch: `main`. Main file path: `dashboard/app.py`.
4. Click **Advanced settings** and add (only if you want Snowflake-connected mode):
   ```
   DATA_SOURCE = "snowflake"
   SNOWFLAKE_ACCOUNT = "..."
   SNOWFLAKE_USER = "..."
   SNOWFLAKE_PASSWORD = "..."
   ```
   Leave these unset (or `DATA_SOURCE = "csv"`) to run off the committed Gold CSVs in
   `dashboard/data/` — works with zero configuration.
5. Deploy. `dashboard/requirements.txt` is already pinned for reproducible builds.

## Push to GitHub

```bash
cd customer-analytics-snowflake-databricks
git init
git add .
git commit -m "Customer data analytics: Databricks + Snowflake capstone"
git branch -M main
git remote add origin https://github.com/<your-username>/customer-analytics-snowflake-databricks.git
git push -u origin main
```
