let parserModule;

async function parser() {
  parserModule ||= import(chrome.runtime.getURL('parser.js'));
  return parserModule;
}

function pageType() {
  if (/\/boxscores\/[^/]+\.htm$/.test(location.pathname)) return 'boxscore';
  if (/\/years\/\d{4}\/week_\d+\.htm$/.test(location.pathname)) return 'schedule';
  if (/\/teams\/[a-z0-9]+\/\d{4}_roster\.htm$/.test(location.pathname)) return 'roster';
  return 'other';
}

function looksRateLimited() {
  const pageText = document.body?.innerText?.toLowerCase() || '';
  return document.title.toLowerCase().includes('too many requests')
    || pageText.includes('429 too many requests')
    || pageText.includes('rate limit exceeded')
    || pageText.includes('temporarily blocked');
}

chrome.runtime.sendMessage({ type: 'page-ready', pageType: pageType(), url: location.href, rateLimited: looksRateLimited() });

chrome.runtime.onMessage.addListener((message) => {
  if (message.type === 'read-roster') {
    parser().then(({ parseRoster }) => {
      const records = parseRoster(document, message.season, message.teamSlug, message.teamId);
      chrome.runtime.sendMessage({ type: 'roster-result', records });
    }).catch((error) => chrome.runtime.sendMessage({ type: 'roster-failed', error: `Roster parser: ${error.message}` }));
  }
  if (message.type === 'read-schedule') {
    parser().then(({ parseSchedule }) => {
      const games = parseSchedule(document, message.season, message.week);
      chrome.runtime.sendMessage({ type: 'schedule-result', games });
    }).catch((error) => chrome.runtime.sendMessage({ type: 'page-error', error: `Schedule parser: ${error.message}` }));
  }
  if (message.type === 'read-boxscore') {
    parser().then(({ parseBoxScore }) => {
      const data = parseBoxScore(document, message.game, message.options);
      chrome.runtime.sendMessage({ type: 'boxscore-result', data });
    }).catch((error) => chrome.runtime.sendMessage({ type: 'boxscore-failed', error: `Box-score parser: ${error.message}` }));
  }
});