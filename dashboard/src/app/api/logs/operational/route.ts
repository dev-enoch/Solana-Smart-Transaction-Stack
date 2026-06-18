import { NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const page = parseInt(searchParams.get('page') || '1', 10);
    const limit = parseInt(searchParams.get('limit') || '15', 10);
    const search = (searchParams.get('search') || '').toLowerCase().trim();

    const envPath = path.join(process.cwd(), '../.env');
    let network = 'mainnet';
    try {
      if (fs.existsSync(envPath)) {
        const envContent = fs.readFileSync(envPath, 'utf8');
        const match = envContent.match(/^NETWORK=(.*)$/m);
        if (match && match[1].trim() === 'devnet') {
          network = 'devnet';
        }
      }
    } catch (e) {
      console.error('Failed to read .env:', e);
    }

    const logsPath = path.join(process.cwd(), '../operational_events.jsonl');
    let logs = [];
    if (fs.existsSync(logsPath)) {
      const fileContent = fs.readFileSync(logsPath, 'utf8');
      const lines = fileContent.split('\n').filter(line => line.trim());
      logs = lines.map(line => {
        try {
          return JSON.parse(line);
        } catch (e) {
          return null;
        }
      }).filter(Boolean);
    }

    if (search) {
      logs = logs.filter((l: any) => l.event === search);
    }

    logs.sort((a: any, b: any) => new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime());

    const totalEvents = logs.length;
    const submissions = logs.filter((l: any) => l.event === "bundle_submitted").length;
    const failures = logs.filter((l: any) => l.event === "failure_detected" || l.event === "submission_error").length;
    const retries = logs.filter((l: any) => l.event === "retry_queued").length;

    const stats = {
      operational: { totalEvents, submissions, failures, retries }
    };

    const startIndex = (page - 1) * limit;
    const endIndex = startIndex + limit;
    const sliced = logs.slice(startIndex, endIndex);

    return NextResponse.json({
      network,
      stats,
      data: sliced,
      totalItems: logs.length,
      totalPages: Math.ceil(logs.length / limit),
      currentPage: page
    });

  } catch (error) {
    console.error('API Error:', error);
    return NextResponse.json({ error: 'Internal Server Error' }, { status: 500 });
  }
}
