const TEAM_MAPPING = {
  Lions: 'DET', Packers: 'GNB', Dolphins: 'MIA', Jets: 'NYJ', Falcons: 'ATL', Vikings: 'MIN', Saints: 'NOR', Giants: 'NYG',
  Jaguars: 'JAX', Titans: 'TEN', Panthers: 'CAR', Eagles: 'PHI', Browns: 'CLE', Steelers: 'PIT', Raiders: 'LVR',
  Buccaneers: 'TAM', Cardinals: 'ARI', Seahawks: 'SEA', Bills: 'BUF', Rams: 'LAR', Bears: 'CHI', '49ers': 'SFO', Chiefs: 'KAN',
  Chargers: 'LAC', Bengals: 'CIN', Cowboys: 'DAL', Colts: 'IND', Ravens: 'BAL', Texans: 'HOU', Broncos: 'DEN', Commanders: 'WAS', Patriots: 'NWE'
};

export function parseHtml(html) {
  return new DOMParser().parseFromString(html, 'text/html');
}

function text(element) {
  return element?.textContent?.replace(/\s+/g, ' ').trim() || null;
}

function numberOrText(value) {
  const clean = value?.trim();
  if (!clean) return null;
  return /^-?\d+(\.\d+)?$/.test(clean) ? Number(clean) : clean;
}

function findTable(root, tableId) {
  if (!root) return null;
  const direct = root.querySelector(`#${tableId}`);
  if (direct) return direct;
  const walker = root.ownerDocument?.createTreeWalker(root, NodeFilter.SHOW_COMMENT);
  let comment;
  while (walker && (comment = walker.nextNode())) {
    if (!comment.data.toLowerCase().includes('table')) continue;
    const commentDoc = parseHtml(comment.data);
    const table = commentDoc.querySelector(`#${tableId}`);
    if (table) return table;
  }
  return null;
}

function teamFromRow(row) {
  const cell = row?.querySelector('td');
  return text(cell?.querySelector('a') || cell);
}

function scoreFromRow(row) {
  const cells = [...(row?.querySelectorAll('td') || [])];
  const scoreCell = row?.querySelector('td.right') || cells.at(-1);
  const value = text(scoreCell);
  return value && /^\d+$/.test(value) ? Number(value) : null;
}

export function parseSchedule(doc, season, week) {
  const games = [];
  for (const game of doc.querySelectorAll('div.game_summary')) {
    const rows = [...game.querySelectorAll('tr.winner, tr.loser, tr.draw')];
    if (rows.length < 2) continue;
    const awayTeam = teamFromRow(rows[0]);
    const homeTeam = teamFromRow(rows[1]);
    const date = text(game.querySelector('tr.date td, tr.date th'));
    const link = game.querySelector('td.gamelink a[href*="/boxscores/"]');
    if (!awayTeam || !homeTeam || !date || !link) continue;
    const url = new URL(link.getAttribute('href'), 'https://www.pro-football-reference.com').href;
    const awayRow = rows.find((row) => teamFromRow(row) === awayTeam);
    const homeRow = rows.find((row) => teamFromRow(row) === homeTeam);
    games.push({
      season: Number(season), week: Number(week), date, awayTeam, homeTeam,
      awayScore: scoreFromRow(awayRow),
      homeScore: scoreFromRow(homeRow),
      boxScoreUrl: url
    });
  }
  return games;
}

function parseGameInfo(doc) {
  const info = {};
  const wrapper = doc.querySelector('#all_game_info') || doc;
  const table = findTable(wrapper, 'game_info');
  for (const row of table?.querySelectorAll('tr') || []) {
    const label = text(row.querySelector('[data-stat="info"]'));
    const value = text(row.querySelector('[data-stat="stat"]'));
    if (label && value) info[label.replace(/[^A-Za-z0-9]+/g, '_').replace(/^_|_$/g, '')] = value;
  }
  const meta = text(doc.querySelector('.scorebox_meta'));
  const startTime = meta?.match(/Start Time:\s*(\d{1,2}:\d{2}\s*(?:am|pm)?)/i)?.[1]?.trim();
  if (startTime) info.Game_Time = startTime;
  return info;
}

function playerId(cell) {
  return cell?.getAttribute('data-append-csv') || cell?.querySelector('a[href*="/players/"]')?.getAttribute('href')?.match(/\/players\/[A-Z]\/([A-Za-z0-9]+)\.htm/)?.[1] || null;
}

function rowsFromTable(table, metadata, includePlayerId = false) {
  const rows = [];
  for (const row of table?.querySelectorAll('tbody tr') || []) {
    if (row.classList.contains('thead') || /Kick Returns|Punt Returns|Scoring|Punting/.test(text(row) || '')) continue;
    const record = { ...metadata };
    for (const cell of row.querySelectorAll('[data-stat]')) {
      const key = cell.getAttribute('data-stat');
      const value = text(cell);
      if (key && value) record[key] = value;
    }
    const playerCell = row.querySelector('[data-stat="player"]');
    if (includePlayerId && playerCell) record.playerid = playerId(playerCell);
    if (Object.keys(record).length > Object.keys(metadata).length) rows.push(record);
  }
  return rows;
}

function parseTeamStats(table, metadata) {
  const away = { team: metadata.awayTeam, ...metadata };
  const home = { team: metadata.homeTeam, ...metadata };
  for (const row of table?.querySelectorAll('tr') || []) {
    const key = text(row.querySelector('[data-stat="stat"]'))?.replace(/[ -]+/g, '_');
    const awayValue = text(row.querySelector('[data-stat="vis_stat"]'));
    const homeValue = text(row.querySelector('[data-stat="home_stat"]'));
    if (key && awayValue && homeValue) { away[key] = awayValue; home[key] = homeValue; }
  }
  return table ? [away, home] : [];
}

function parseDrives(table, team, metadata, isHome) {
  return rowsFromTable(table, { ...metadata, team, is_home: isHome });
}

const TEAM_IDS = {
  'Kansas City Chiefs': 'KAN', 'Los Angeles Chargers': 'LAC', 'Philadelphia Eagles': 'PHI', 'Dallas Cowboys': 'DAL',
  'New England Patriots': 'NWE', 'Las Vegas Raiders': 'LVR', 'Washington Commanders': 'WAS', 'New York Giants': 'NYG',
  'New York Jets': 'NYJ', 'Pittsburgh Steelers': 'PIT', 'Indianapolis Colts': 'IND', 'Miami Dolphins': 'MIA',
  'Jacksonville Jaguars': 'JAX', 'Carolina Panthers': 'CAR', 'New Orleans Saints': 'NOR', 'Arizona Cardinals': 'ARI',
  'Atlanta Falcons': 'ATL', 'Tampa Bay Buccaneers': 'TAM', 'Denver Broncos': 'DEN', 'Tennessee Titans': 'TEN',
  'Seattle Seahawks': 'SEA', 'San Francisco 49ers': 'SFO', 'Los Angeles Rams': 'LAR', 'Houston Texans': 'HOU',
  'Buffalo Bills': 'BUF', 'Baltimore Ravens': 'BAL', 'Chicago Bears': 'CHI', 'Minnesota Vikings': 'MIN',
  'Green Bay Packers': 'GNB', 'Detroit Lions': 'DET', 'Cincinnati Bengals': 'CIN', 'Cleveland Browns': 'CLE'
};

function parsePlayerTable(table, metadata, teamid, kind) {
  const rows = [];
  const fields = kind === 'starters'
    ? { pos: 'pos' }
    : { pos: 'pos', offense: 'off_num', off_pct: 'off_pct', defense: 'def_num', def_pct: 'def_pct', special_teams: 'st_num', st_pct: 'st_pct' };
  for (const row of table?.querySelectorAll('tbody tr, tr') || []) {
    if (row.classList.contains('thead') || row.querySelector('[scope="col"]')) continue;
    const playerCell = row.querySelector('[data-stat="player"]');
    if (!playerCell || !text(playerCell)) continue;
    const record = {
      player: text(playerCell),
      playerid: playerId(playerCell),
      teamid,
      hometeamid: metadata.homeTeamId,
      awayteamid: metadata.awayTeamId,
      season: metadata.season,
      week: metadata.week
    };
    for (const [source, target] of Object.entries(fields)) {
      const value = text(row.querySelector(`[data-stat="${source}"]`));
      if (value) record[target] = value;
    }
    rows.push(record);
  }
  return rows;
}

function parsePlayerTables(doc, game, metadata) {
  const output = { Starters: [], Snap_Counts: [] };
  const teams = [
    { prefix: 'vis', team: metadata.awayTeam, id: TEAM_IDS[metadata.awayTeam], home: false },
    { prefix: 'home', team: metadata.homeTeam, id: TEAM_IDS[metadata.homeTeam], home: true }
  ];
  for (const item of teams) {
    const teamMetadata = { ...metadata, homeTeamId: TEAM_IDS[metadata.homeTeam], awayTeamId: TEAM_IDS[metadata.awayTeam] };
    output.Starters.push(...parsePlayerTable(findTable(doc.querySelector(`#all_${item.prefix}_starters`), `${item.prefix}_starters`), teamMetadata, item.id, 'starters'));
    output.Snap_Counts.push(...parsePlayerTable(findTable(doc.querySelector(`#all_${item.prefix}_snap_counts`), `${item.prefix}_snap_counts`), teamMetadata, item.id, 'snap'));
  }
  return output;
}

function parsePlayText(detailText) {
  const detail = detailText || '';
  const lower = detail.toLowerCase();
  const result = { Play_Type: null, Primary_Player: null, Receiver: null, Sack_By: null, Run_Location: null, Run_Gap: null, Pass_Type: null, Pass_Location: null, Pass_Yards: null, Yards: null, Tackler: null, Tackler2: null, Defender: null, Result: null, Penalized_Player: null, Penalty_Yards: null, Penalty: null, Penalty_Accepted: null };
  const yards = lower.match(/(?:for|gains?|loses|punts)\s+(-?\d+)\s+yards?/i);
  if (yards) result.Yards = Number(yards[1]);
  if (lower.includes('penalty')) {
    result.Play_Type = 'Penalty';
    result.Penalty = detail;
    result.Penalty_Accepted = lower.includes('accepted') ? true : lower.includes('declined') ? false : null;
    result.Penalized_Player = detail.match(/penalty on ([A-Za-z .'-]+)/i)?.[1]?.trim() || null;
    result.Penalty_Yards = Number(lower.match(/(\d+) yards?/i)?.[1]) || null;
  } else if (/pass|incomplete|complete|sacked/i.test(lower)) {
    result.Play_Type = lower.includes('sacked') ? 'Sack' : 'Pass';
    if (yards) result.Pass_Yards = Number(yards[1]);
    result.Pass_Type = lower.includes('incomplete') ? 'Incomplete' : lower.includes('complete') ? 'Complete' : null;
    result.Receiver = detail.match(/(?:complete to|intended for) ([A-Za-z .'-]+)/i)?.[1]?.trim() || null;
  } else if (/kickoff|punt/i.test(lower)) {
    result.Play_Type = lower.includes('punt') ? 'Punt' : 'Kickoff';
  } else if (/field goal/i.test(lower)) {
    result.Play_Type = 'Field Goal';
    result.Field_Goal_Yards = Number(lower.match(/(\d+) yard field goal/)?.[1]) || null;
  } else if (/extra point/i.test(lower)) {
    result.Play_Type = 'Extra Point';
  } else if (/rush|runs|left tackle|right tackle|up the middle|scrambles/i.test(lower)) {
    result.Play_Type = 'Run';
    result.Run_Location = lower.includes('left') ? 'Left' : lower.includes('right') ? 'Right' : lower.includes('middle') ? 'Middle' : null;
    result.Run_Gap = lower.match(/(left|right) (end|tackle|guard)/i)?.[2]?.replace(/^./, (char) => char.toUpperCase()) || null;
  }
  result.Primary_Player = detail.match(/^([A-Za-z][A-Za-z .'-]+?)(?: pass| rush| runs| left| right| punts| kicks| scrambles| sacked)/i)?.[1]?.trim() || null;
  return result;
}

function parseDriveDetails(doc, game, metadata) {
  const wrapper = doc.querySelector('#div_pbp') || doc.querySelector('#all_pbp');
  const table = findTable(wrapper, 'pbp') || findTable(doc.querySelector('#all_pbp'), 'pbp');
  const rows = [];
  let quarter = '1';
  let overtime = 0;
  for (const row of table?.querySelectorAll('tr') || []) {
    const rowText = text(row) || '';
    if (rowText.toLowerCase().includes('overtime') && !row.querySelector('[data-stat="detail"]')) { overtime += 1; quarter = String(4 + overtime); continue; }
    const detailCell = row.querySelector('[data-stat="detail"]');
    if (!detailCell) continue;
    const quarterCell = text(row.querySelector('[data-stat="quarter"]'));
    if (/^[1-4]$/.test(quarterCell || '') && overtime === 0) quarter = quarterCell;
    const detail = text(detailCell);
    const parsed = parsePlayText(detail);
    rows.push({ Date: game.date, Season: game.season, Week: game.week, 'Away Team': game.awayTeam, 'Home Team': game.homeTeam,
      Game_Time: metadata.gameTime || null, Quarter: quarter, Time: text(row.querySelector('[data-stat="qtr_time_remain"]')),
      Down: text(row.querySelector('[data-stat="down"]')), ToGo: text(row.querySelector('[data-stat="yds_to_go"]')),
      Location: text(row.querySelector('[data-stat="location"]')), Detail: detail,
      ...parsed, EPB: Number.isNaN(Number(text(row.querySelector('[data-stat="exp_pts_before"]')))) ? null : Number(text(row.querySelector('[data-stat="exp_pts_before"]'))),
      EPA: Number.isNaN(Number(text(row.querySelector('[data-stat="exp_pts_after"]')))) ? null : Number(text(row.querySelector('[data-stat="exp_pts_after"]'))) });
  }
  return rows;
}

export function parseBoxScore(doc, game, options = { base: true, players: true, driveDetails: false }) {
  const metadata = { date: game.date, season: game.season, week: game.week, awayTeam: game.awayTeam, homeTeam: game.homeTeam, boxScoreUrl: game.boxScoreUrl };
  const data = { Game_Summary: options.base ? [{ ...game, gameInfo: parseGameInfo(doc) }] : [], Team_Stats: [], Drives: [], Rushing: [], Passing: [], Receiving: [], Defense: [], Returns: [], Kicking: [], Player_Offense: [], Player_Defense: [], ExpectedPoints: [], Starters: [], Snap_Counts: [], Drive_Details: [] };
  if (!options.base) return options.players || options.driveDetails ? { ...data, ...(options.players ? parsePlayerTables(doc, game, metadata) : {}), Drive_Details: options.driveDetails ? parseDriveDetails(doc, game, { ...metadata, gameTime: parseGameInfo(doc).Game_Time }) : [] } : data;
  data.Team_Stats = parseTeamStats(findTable(doc.querySelector('#all_team_stats'), 'team_stats'), metadata);
  const sections = {
    Rushing: 'rushing_advanced', Passing: 'passing_advanced', Receiving: 'receiving_advanced', Defense: 'defense_advanced',
    Returns: 'returns', Kicking: 'kicking', Player_Offense: 'player_offense', Player_Defense: 'player_defense'
  };
  for (const [category, tableId] of Object.entries(sections)) data[category] = rowsFromTable(findTable(doc.querySelector(`#all_${tableId}`), tableId), metadata, true);
  data.Drives = [
    ...parseDrives(findTable(doc.querySelector('#all_vis_drives'), 'vis_drives'), game.awayTeam, metadata, false),
    ...parseDrives(findTable(doc.querySelector('#all_home_drives'), 'home_drives'), game.homeTeam, metadata, true)
  ];
  const expected = rowsFromTable(findTable(doc.querySelector('#all_expected_points'), 'expected_points'), metadata);
  data.ExpectedPoints = expected.map((row) => row.team_name ? { ...row, teamid: TEAM_MAPPING[row.team_name] || row.team_name } : row);
  if (options.players) Object.assign(data, parsePlayerTables(doc, game, metadata));
  if (options.driveDetails) data.Drive_Details = parseDriveDetails(doc, game, { ...metadata, gameTime: parseGameInfo(doc).Game_Time });
  return data;
}

export function mergeData(target, source) {
  for (const [category, rows] of Object.entries(source)) target[category].push(...rows);
  return target;
}

export function dedupeData(data) {
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
