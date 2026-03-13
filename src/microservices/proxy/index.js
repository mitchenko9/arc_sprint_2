const express = require('express');
const httpProxy = require('http-proxy');

const app = express();
const proxy = httpProxy.createProxyServer({});

const PORT = process.env.PORT || 8000;
const MONOLITH_URL = process.env.MONOLITH_URL || 'http://localhost:8080';
const MOVIES_SERVICE_URL = process.env.MOVIES_SERVICE_URL || 'http://localhost:8081';
const EVENTS_SERVICE_URL = process.env.EVENTS_SERVICE_URL || 'http://localhost:8082';
const GRADUAL_MIGRATION = process.env.GRADUAL_MIGRATION === 'true';
const MOVIES_MIGRATION_PERCENT = parseInt(process.env.MOVIES_MIGRATION_PERCENT || '100', 10);

proxy.on('error', (err, req, res) => {
  console.error(`[ошибка] Прокси: ${err.message}`);
  res.status(502).json({ error: 'Плохой шлюз', message: err.message });
});

function getMoviesTarget() {
  if (!GRADUAL_MIGRATION) {
    return MOVIES_SERVICE_URL;
  }

  return Math.random() * 100 < MOVIES_MIGRATION_PERCENT ? MOVIES_SERVICE_URL : MONOLITH_URL;
}

app.get('/health', (req, res) => {
  res.json({ status: true });
});

app.all('/api/movies*', (req, res) => {
  const target = getMoviesTarget();
  console.log(`[movies] ${req.method} ${req.url} → ${target}`);
  proxy.web(req, res, { target });
});

app.all('/api/events*', (req, res) => {
  console.log(`[events] ${req.method} ${req.url} → ${EVENTS_SERVICE_URL}`);
  proxy.web(req, res, { target: EVENTS_SERVICE_URL });
});

app.all('*', (req, res) => {
  console.log(`[monolith] ${req.method} ${req.url} → ${MONOLITH_URL}`);
  proxy.web(req, res, { target: MONOLITH_URL });
});

app.listen(PORT, () => {
  console.log(`Прокси-сервис запущен на порту ${PORT}`);
  console.log(`  Монолит: ${MONOLITH_URL}`);
  console.log(`  Сервис фильмов: ${MOVIES_SERVICE_URL}`);
  console.log(`  Сервис событий: ${EVENTS_SERVICE_URL}`);
  console.log(`  Постепенная миграция: ${GRADUAL_MIGRATION}`);
  console.log(`  Процент миграции фильмов: ${MOVIES_MIGRATION_PERCENT}%`);
});
