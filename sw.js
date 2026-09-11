self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('fetch', (event) => {
  // No caching — always use the live server
  return;
});

self.addEventListener('message', async (event) => {
  const data = event.data || {};

  if (data.type === 'SKIP_WAITING') {
    self.skipWaiting();
    return;
  }

  if (data.type === 'RAJA_SHOW_NOTIFICATION') {
    try {
      const payload = data.payload || {};

      await self.registration.showNotification(
        payload.title || 'RAJA AI BOT',
        payload.options || {}
      );
    } catch (_) {}
  }
});
