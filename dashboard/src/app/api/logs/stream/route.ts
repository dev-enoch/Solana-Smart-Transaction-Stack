import { NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  const lifecycleLogsPath = path.join(process.cwd(), '../lifecycle_logs.json');
  const operationalEventsPath = path.join(process.cwd(), '../operational_events.jsonl');

  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: 'connected' })}\n\n`));

      let lastLifecycleUpdate = 0;
      let lastOperationalUpdate = 0;

      const sendUpdate = (fileType: string) => {
        const now = Date.now();
        if (fileType === 'lifecycle_update') {
          if (now - lastLifecycleUpdate < 500) return;
          lastLifecycleUpdate = now;
        } else if (fileType === 'operational_update') {
          if (now - lastOperationalUpdate < 500) return;
          lastOperationalUpdate = now;
        }

        try {
          controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: fileType })}\n\n`));
        } catch (e) {
          // stream closed
        }
      };

      let lifecycleWatcher: fs.FSWatcher | null = null;
      let operationalWatcher: fs.FSWatcher | null = null;

      try {
        if (fs.existsSync(lifecycleLogsPath)) {
          lifecycleWatcher = fs.watch(lifecycleLogsPath, (eventType) => {
            if (eventType === 'change') sendUpdate('lifecycle_update');
          });
        }
        
        if (fs.existsSync(operationalEventsPath)) {
          operationalWatcher = fs.watch(operationalEventsPath, (eventType) => {
            if (eventType === 'change') sendUpdate('operational_update');
          });
        }
      } catch (err) {
        console.error('Watch error:', err);
      }

      request.signal.addEventListener('abort', () => {
        if (lifecycleWatcher) lifecycleWatcher.close();
        if (operationalWatcher) operationalWatcher.close();
      });
    }
  });

  return new NextResponse(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
    },
  });
}
