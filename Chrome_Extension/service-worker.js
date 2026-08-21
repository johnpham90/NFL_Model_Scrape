import { csvBlobs, jsonBlob } from './exporter.js';

const BASE_URL = 'https://www.pro-football-reference.com';
const ROSTER_TEAMS = [
  ['crd', 'ARI'], ['atl', 'ATL'], ['rav', 'BAL'], ['buf', 'BUF'], ['car', 'CAR'], ['chi', 'CHI'], ['cin', 'CIN'], ['cle', 'CLE'],
  ['dal', 'DAL'], ['den', 'DEN'], ['det', 'DET'], ['gnb', 'GNB'], ['htx', 'HOU'], ['clt', 'IND'], ['jax', 'JAX'], ['kan', 'KAN'],
  ['rai', 'LVR'], ['sdg', 'LAC'], ['ram', 'LAR'], ['mia', 'MIA'], ['min', 'MIN'], ['nwe', 'NWE'], ['nor', 'NOR'], ['nyg', 'NYG'],
  ['nyj', 'NYJ'], ['phi', 'PHI'], ['pit', 'PIT'], ['sfo', 'SFO'], ['sea', 'SEA'], ['tam', 'TAM'], ['oti', 'TEN'], ['was', 'WAS']
].map(([slug, teamId]) => ({ slug, teamId }));
const initialState = { status: 'ready', message: 'Choose a season and week to begin.', current: 0, total: 0 };
let state = { ...initialState };
let run = null;

function broadcast(message) {
  state = { ...state, ...message };
  chrome.runtime.sendMessage({ type: 'progress', ...state }).catch(() => {});
}

function sendToTab(message) {
  return chrome.tabs.sendMessage(run.tabId, message);
}

async function navigate(url) {
  await chrome.tabs.update(run.tabId, { url });
}

async function waitBeforeNavigation(url) {
  const delay = 4500 + Math.floor(Math.random() * 2000);
  await new Promise((resolve) => setTimeout(resolve, delay));
  if (run) await navigate(url);
}

async function blobToDataUrl(blob) {
  const bytes = new Uint8Array(await blob.arrayBuffer());
  let binary = '';
  for (let offset = 0; offset < bytes.length; offset += 8192) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 8192));
  }
  return `data:${blob.type};base64,${btoa(binary)}`;
}

async function startRun(message) {
  if (run) throw new Error('A scrape is already running.');
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab?.id) throw new Error('No active browser tab is available.');
  if (message.mode === 'roster') {
    run = { ...message, tabId: tab.id, rosterTeams: ROSTER_TEAMS, roster: [], failures: [], index: 0 };
    broadcast({ status: 'running', message: `Opening the ${message.season} roster...`, current: 0, total: ROSTER_TEAMS.length });
    await navigate(`${BASE_URL}/teams/${ROSTER_TEAMS[0].slug}/${message.season}_roster.htm`);
    return;
  }
  run = { ...message, tabId: tab.id, schedule: [], data: emptyData(), failures: [], index: -1 };
  broadcast({ status: 'running', message: `Opening ${message.season}, Week ${message.week}...`, current: 0, total: 1 });
  await navigate(`${BASE_URL}/years/${message.season}/week_${message.week}.htm`);
}

function emptyData() {
  return { Game_Summary: [], Team_Stats: [], Drives: [], Rushing: [], Passing: [], Receiving: [], Defense: [], Returns: [], Kicking: [], Player_Offense: [], Player_Defense: [], ExpectedPoints: [], Starters: [], Snap_Counts: [], Drive_Details: [] };
}

function mergeData(target, source) {
  for (const [category, rows] of Object.entries(source)) target[category].push(...rows);
}

function dedupeData(data) {
  return Object.fromEntries(Object.entries(data).map(([category, rows]) => {
    const seen = new Set();
    return [category, rows.filter((row) => {
      const key = JSON.stringify(row);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })];
  }));
}

async function finishRun() {
  const data = dedupeData(run.data);
  data.Run_Info = [{ season: run.season, week: run.week, weekUrl: `${BASE_URL}/years/${run.season}/week_${run.week}.htm`, gamesFound: run.schedule.length, failures: run.failures }];
  const files = run.format === 'json'
    ? [{ name: `${run.season}_Week${run.week}_PFR.json`, blob: jsonBlob(data, run.season, run.week) }]
    : csvBlobs(data).map((item) => ({ name: `${run.season}_Week${run.week}_${item.name}`, blob: item.blob }));
  for (const file of files) {
    const url = await blobToDataUrl(file.blob);
    await chrome.downloads.download({ url, filename: `PFR_Weekly_Scraper/${file.name}`, saveAs: true });
  }
  const gamesFound = run.schedule.length;
  const failures = run.failures.length;
  run = null;
  broadcast({ status: 'complete', message: `${gamesFound} games processed. Downloads are starting.`, current: gamesFound, total: gamesFound, gamesFound, failures });
}

async function finishRosterRun() {
  const completedTeams = run.rosterTeams.filter((team) => !run.failures.some((failure) => failure.slug === team.slug));
  const bundle = {
    schemaVersion: 1,
    season: run.season,
    generatedAt: new Date().toISOString(),
    Roster: run.roster,
    Run_Info: [{ mode: 'roster', season: run.season, teamsFound: completedTeams.length, teamsExpected: run.rosterTeams.length, complete: completedTeams.length === run.rosterTeams.length, failures: run.failures }]
  };
  const json = new Blob([JSON.stringify(bundle, null, 2)], { type: 'application/json' });
  const csv = csvBlobs({ Roster: run.roster })[0];
  const files = [
    { name: `${run.season}_Roster.json`, blob: json },
    { name: `${run.season}_Roster.csv`, blob: csv.blob }
  ];
  for (const file of files) {
    await chrome.downloads.download({ url: await blobToDataUrl(file.blob), filename: `PFR_Weekly_Scraper/${file.name}`, saveAs: true });
  }
  const rosterCount = run.roster.length;
  const failures = run.failures.length;
  run = null;
  broadcast({ status: 'complete', message: `${rosterCount} roster players collected. Downloads are starting.`, current: ROSTER_TEAMS.length, total: ROSTER_TEAMS.length, gamesFound: rosterCount, failures });
}

async function handlePageReady(message, sender) {
  if (!run || sender.tab?.id !== run.tabId) return;
  if (message.rateLimited) {
    run = null;
    broadcast({ status: 'failed', message: 'PFR appears to have rate-limited this run.', error: 'Wait before trying again and use the built-in pacing.' });
    return;
  }
  if (run.mode === 'roster' && message.pageType === 'roster') {
    const team = run.rosterTeams[run.index];
    if (!team || !message.url.includes(`/teams/${team.slug}/`)) return;
    broadcast({ status: 'running', message: `Reading ${team.teamId} roster (${run.index + 1}/${run.rosterTeams.length})`, current: run.index, total: run.rosterTeams.length });
    await sendToTab({ type: 'read-roster', season: run.season, teamSlug: team.slug, teamId: team.teamId });
    return;
  }
  if (message.pageType === 'schedule' && run.index === -1) {
    broadcast({ status: 'running', message: 'Reading the visible weekly schedule...', current: 0, total: 1 });
    await sendToTab({ type: 'read-schedule', season: run.season, week: run.week });
    return;
  }
  if (message.pageType === 'boxscore' && run.index >= 0 && message.url === run.schedule[run.index].boxScoreUrl) {
    broadcast({ status: 'running', message: `Reading ${run.schedule[run.index].awayTeam} at ${run.schedule[run.index].homeTeam}`, current: run.index, total: run.schedule.length });
    await sendToTab({ type: 'read-boxscore', game: run.schedule[run.index], options: run.options });
  }
}

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === 'start') {
    startRun(message).then(() => sendResponse({ accepted: true })).catch((error) => sendResponse({ accepted: false, error: error.message }));
    return true;
  }
  if (message.type === 'cancel') {
    if (run) { run = null; broadcast({ status: 'cancelled', message: 'Scrape cancelled.', current: 0, total: 0 }); }
    sendResponse({ accepted: true });
    return true;
  }
  if (message.type === 'get-state') {
    sendResponse({ ...state, running: Boolean(run) });
    return true;
  }
  if (message.type === 'page-ready') {
    handlePageReady(message, sender).catch((error) => broadcast({ status: 'failed', message: error.message, error: error.message }));
    return false;
  }
  if (message.type === 'schedule-result') {
    if (!run || sender.tab?.id !== run.tabId) return false;
    run.schedule = message.games;
    if (!run.schedule.length) { broadcast({ status: 'failed', message: 'No games found on the visible schedule page.', error: 'No games found.' }); run = null; return false; }
    run.index = 0;
    broadcast({ status: 'running', message: `Found ${run.schedule.length} games. Navigating to the first box score...`, current: 0, total: run.schedule.length });
    waitBeforeNavigation(run.schedule[0].boxScoreUrl).catch((error) => broadcast({ status: 'failed', message: error.message, error: error.message }));
    return false;
  }
  if (message.type === 'roster-result') {
    if (!run || run.mode !== 'roster' || sender.tab?.id !== run.tabId) return false;
    run.roster.push(...message.records);
    run.index += 1;
    if (run.index >= run.rosterTeams.length) finishRosterRun().catch((error) => broadcast({ status: 'failed', message: error.message, error: error.message }));
    else {
      const team = run.rosterTeams[run.index];
      waitBeforeNavigation(`${BASE_URL}/teams/${team.slug}/${run.season}_roster.htm`).catch((error) => broadcast({ status: 'failed', message: error.message, error: error.message }));
    }
    return false;
  }
  if (message.type === 'roster-failed') {
    if (!run || run.mode !== 'roster' || sender.tab?.id !== run.tabId) return false;
    run.failures.push({ ...run.rosterTeams[run.index], error: message.error });
    run.index += 1;
    if (run.index >= run.rosterTeams.length) finishRosterRun().catch((error) => broadcast({ status: 'failed', message: error.message, error: error.message }));
    else {
      const team = run.rosterTeams[run.index];
      waitBeforeNavigation(`${BASE_URL}/teams/${team.slug}/${run.season}_roster.htm`).catch((error) => broadcast({ status: 'failed', message: error.message, error: error.message }));
    }
    return false;
  }
  if (message.type === 'boxscore-result') {
    if (!run || sender.tab?.id !== run.tabId) return false;
    mergeData(run.data, message.data);
    run.index += 1;
    if (run.index >= run.schedule.length) finishRun().catch((error) => broadcast({ status: 'failed', message: error.message, error: error.message }));
    else {
      broadcast({ status: 'running', message: `Navigating to game ${run.index + 1} of ${run.schedule.length}...`, current: run.index, total: run.schedule.length });
      waitBeforeNavigation(run.schedule[run.index].boxScoreUrl).catch((error) => broadcast({ status: 'failed', message: error.message, error: error.message }));
    }
    return false;
  }
  if (message.type === 'boxscore-failed') {
    if (!run || sender.tab?.id !== run.tabId) return false;
    run.failures.push({ ...run.schedule[run.index], error: message.error });
    run.index += 1;
    if (run.index >= run.schedule.length) finishRun().catch((error) => broadcast({ status: 'failed', message: error.message, error: error.message }));
    else waitBeforeNavigation(run.schedule[run.index].boxScoreUrl).catch((error) => broadcast({ status: 'failed', message: error.message, error: error.message }));
    return false;
  }
  if (message.type === 'page-error') {
    if (run && sender.tab?.id === run.tabId) {
      run = null;
      broadcast({ status: 'failed', message: 'The visible page could not be parsed.', error: message.error });
    }
    return false;
  }
  return false;
});
