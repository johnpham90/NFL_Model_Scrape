# PFR Weekly Scraper Extension

Standalone Chrome Manifest V3 extension for manually scraping a Pro Football Reference NFL week in an observable RPA-style workflow.

## Load in Chrome

1. Open `chrome://extensions`.
2. Enable **Developer mode**.
3. Select **Load unpacked**.
4. Choose this `Chrome_Extension` directory.
5. Click the extension toolbar button, enter a season and week, and select an export format.

The extension navigates the active Chrome tab to the weekly schedule, reads the visible schedule, navigates to each box score in sequence, reads the visible page, and continues to the next game. Choose **Base stats + starters + snap counts** for the standard run, or choose **Drive details only** for the separate play-by-play run. JSON exports one bundle; CSV exports one file per category. The browser tab remains visible throughout the run.

## Convert JSON to Python-style Excel files

The standalone converter recreates the output layout used by `Scrape/scrape.py`:

```powershell
python .\Chrome_Extension\json_to_excel.py `
	"C:\Users\japha\Downloads\PFR_Weekly_Scraper\2025_Week1_PFR.json"
```

If the JSON was downloaded by the extension into the default Downloads folder, the path can be omitted. The newest downloaded PFR JSON is selected automatically:

```powershell
python .\Chrome_Extension\json_to_excel.py
```

By default, files are written under `C:\NFLStats\data\<season>\Week_<week>`. Use `--output-root` to choose another location:

```powershell
python .\Chrome_Extension\json_to_excel.py input.json --output-root "D:\NFLStats\data"
```

The converter copies the JSON into the same season/week folder, flattens `gameInfo`, renames JSON metadata to the Python scraper's column names, removes duplicate rows, and writes one `.xlsx` file per populated category.

PFR may rate-limit automated requests. Use a reasonable pace and only scrape data you are permitted to access.

## Roster snapshots

Choose **Season roster snapshot** in the extension. The visible tab visits all 32 team roster pages for the selected season and downloads `<season>_Roster.json` and `<season>_Roster.csv`.

Run the converter afterward:

```powershell
python .\Chrome_Extension\json_to_excel.py
```

Roster JSON is copied to `C:\NFLStats\data\<season>\<season>_Roster.json`, and the current snapshot is written to:

```text
C:\NFLStats\data\<season>\NFL_Rosters.xlsx
```

The workbook contains `Current_Roster`. Repeated runs update the current rows and print `Added`, `Removed`, `Team_Change`, and `Updated` records. Failed team pages do not cause removals for that team.
