const form = document.querySelector('#scrape-form');
const seasonInput = document.querySelector('#season');
const weekInput = document.querySelector('#week');
const formatInput = document.querySelector('#format');
const startButton = document.querySelector('#start');
const cancelButton = document.querySelector('#cancel');
const status = document.querySelector('#status');
const detail = document.querySelector('#detail');
const progressBar = document.querySelector('#progress');
const error = document.querySelector('#error');
const statusPanel = document.querySelector('.status');
const runTypeInput = document.querySelector('#run-type');

function setState(running) {
  startButton.disabled = running;
  cancelButton.hidden = !running;
  progressBar.hidden = !running;
  statusPanel.classList.toggle('running', running);
}

function setStatus(title, message) { status.textContent = title; detail.textContent = message; }

chrome.storage.local.get({ season: new Date().getFullYear(), week: 1, format: 'json', runType: 'standard' }).then((settings) => {
  seasonInput.value = settings.season;
  weekInput.value = settings.week;
  formatInput.value = settings.format;
  runTypeInput.value = settings.runType;
});

chrome.runtime.sendMessage({ type: 'get-state' }).then((currentState) => {
  if (!currentState?.running) return;
  setState(true);
  setStatus('Scraping', currentState.message);
  progressBar.max = currentState.total || 1;
  progressBar.value = currentState.current || 0;
});

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  error.hidden = true;
  const season = Number(seasonInput.value);
  const week = Number(weekInput.value);
  const format = formatInput.value;
  const runType = runTypeInput.value;
  if (runType === 'roster') {
    await chrome.storage.local.set({ season, week, format, runType });
    setState(true); setStatus('Starting', `Preparing the ${season} roster...`);
    const response = await chrome.runtime.sendMessage({ type: 'start', season, format, mode: 'roster' });
    if (!response?.accepted) { setState(false); error.textContent = response?.error || 'Could not start the roster scraper.'; error.hidden = false; }
    return;
  }
  const options = runType === 'drive-details'
    ? { base: false, players: false, driveDetails: true }
    : { base: true, players: true, driveDetails: false };
  if (!Number.isInteger(season) || season < 1920 || !Number.isInteger(week) || week < 1 || week > 22) {
    error.textContent = 'Enter a valid season and a week from 1 through 22.';
    error.hidden = false;
    return;
  }
  await chrome.storage.local.set({ season, week, format, runType });
  setState(true); setStatus('Starting', `Preparing ${season}, Week ${week}...`);
  const response = await chrome.runtime.sendMessage({ type: 'start', season, week, format, options });
  if (!response?.accepted) { setState(false); error.textContent = response?.error || 'Could not start the scraper.'; error.hidden = false; }
});

cancelButton.addEventListener('click', async () => { await chrome.runtime.sendMessage({ type: 'cancel' }); setStatus('Cancelling', 'The current request will finish before stopping.'); });

chrome.runtime.onMessage.addListener((message) => {
  if (message.type === 'progress') {
    setState(true); setStatus('Scraping', message.message); progressBar.max = message.total || 1; progressBar.value = message.current || 0;
  }
  if (message.type === 'complete') {
    setState(false); setStatus('Complete', `${message.gamesFound} games processed. Downloads are starting.`); progressBar.value = progressBar.max;
    if (message.failures.length) detail.textContent += ` ${message.failures.length} game(s) failed.`;
  }
  if (message.type === 'cancelled') { setState(false); setStatus('Cancelled', message.message); }
  if (message.type === 'failed') { setState(false); statusPanel.classList.add('failed'); setStatus('Failed', 'The scrape did not finish.'); error.textContent = message.error; error.hidden = false; }
});
