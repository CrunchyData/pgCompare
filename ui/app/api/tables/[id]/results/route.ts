import { NextRequest, NextResponse } from 'next/server';
import { getPrisma } from '@/lib/db';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const { searchParams } = new URL(request.url);
    const latest = searchParams.get('latest') === 'true';
    
    const prisma = getPrisma();
    
    const results = await prisma.dc_result.findMany({
      where: {
        tid: BigInt(id),
      },
      orderBy: {
        compare_start: 'desc',
      },
      take: latest ? 1 : 20, // Just the latest or last 20 results
    });
    
    // Convert BigInt fields
    const converted = results.map(result => ({
      ...result,
      tid: result.tid ? Number(result.tid) : null,
      rid: result.rid ? result.rid.toString() : null,
    }));
    
    return NextResponse.json(converted);
  } catch (error: any) {
    console.error('Error fetching table results:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

