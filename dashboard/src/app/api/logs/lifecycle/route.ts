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

    const logsPath = path.join(process.cwd(), '../lifecycle_logs.json');
    let logs = [];
    if (fs.existsSync(logsPath)) {
      const fileContent = fs.readFileSync(logsPath, 'utf8');
      if (fileContent.trim()) {
        try {
          logs = JSON.parse(fileContent);
        } catch (e) {
          console.error("Error parsing JSON:", e);
        }
      }
    }

    if (search) {
      logs = logs.filter((l: any) => l.status === search);
    }

    logs.sort((a: any, b: any) => {
      const timeA = a.latency_finalized_ms ? 0 : 1; 
      const timeB = b.latency_finalized_ms ? 0 : 1;
      return timeB - timeA; 
    });

    const totalTx = logs.length;
    const finalized = logs.filter((l: any) => l.status === "finalized").length;
    const failed = logs.filter((l: any) => l.status === "failed").length;
    const totalTip = logs.reduce((acc: number, l: any) => acc + (l.tip_lamports || 0), 0);
    
    const stats = {
      lifecycle: { totalTx, finalized, failed, totalTip }
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
