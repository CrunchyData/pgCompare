import { NextRequest, NextResponse } from 'next/server';
import { getPrisma } from '@/lib/db';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const prisma = getPrisma();
    
    const results = await prisma.dc_result.findMany({
      where: {
        tid: BigInt(id),
      },
      orderBy: {
        compare_start: 'desc',
      },
      take: 10,
    });
    
    // Convert BigInt fields to numbers for JSON serialization
    const converted = results.map(result => ({
      ...result,
      tid: result.tid ? Number(result.tid) : null,
      rid: result.rid ? Number(result.rid) : null,
    }));
    
    return NextResponse.json(converted);
  } catch (error: any) {
    console.error('Error fetching results:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

