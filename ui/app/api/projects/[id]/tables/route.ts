import { NextRequest, NextResponse } from 'next/server';
import { getPrisma } from '@/lib/db';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const prisma = getPrisma();
    
    const tables = await prisma.dc_table.findMany({
      where: {
        pid: BigInt(id),
      },
      orderBy: {
        table_alias: 'asc',
      },
    });
    
    // Convert BigInt to number for JSON serialization
    const converted = tables.map(table => ({
      ...table,
      tid: Number(table.tid),
      pid: Number(table.pid),
    }));
    
    return NextResponse.json(converted);
  } catch (error: any) {
    console.error('Error fetching tables:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

