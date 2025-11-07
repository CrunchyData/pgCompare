import { NextRequest, NextResponse } from 'next/server';
import { getPrisma } from '@/lib/db';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const prisma = getPrisma();
    
    // Get result to find the tid
    const result = await prisma.dc_result.findUnique({
      where: {
        cid: parseInt(id),
      },
      select: {
        tid: true,
      },
    });

    if (!result || !result.tid) {
      return NextResponse.json({ error: 'Result not found' }, { status: 404 });
    }

    // Get target rows for this comparison using raw query
    const targetRows = await prisma.$queryRaw`
      SELECT 
        pk, 
        pk_hash, 
        column_hash, 
        compare_result, 
        thread_nbr,
        table_name,
        batch_nbr
      FROM dc_target
      WHERE tid = ${result.tid}
      ORDER BY pk_hash
      LIMIT 1000
    `;
    
    return NextResponse.json(targetRows);
  } catch (error: any) {
    console.error('Error fetching target data:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

