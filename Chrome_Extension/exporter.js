function csvEscape(value) {
  const text = value == null ? '' : String(value);
  return /[",\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

export function jsonBlob(data, season, week) {
  return new Blob([JSON.stringify({ season, week, generatedAt: new Date().toISOString(), ...data }, null, 2)], { type: 'application/json' });
}

export function csvBlobs(data) {
  return Object.entries(data).filter(([, rows]) => rows.length).map(([category, rows]) => {
    const keys = [...new Set(rows.flatMap((row) => Object.keys(row)))];
    const lines = [keys.map(csvEscape).join(',')];
    for (const row of rows) lines.push(keys.map((key) => csvEscape(row[key])).join(','));
    return { name: `${category}.csv`, blob: new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' }) };
  });
}
