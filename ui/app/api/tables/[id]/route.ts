import { NextRequest, NextResponse } from 'next/server';
import { getPrisma } from '@/lib/db';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const prisma = getPrisma();
    
    const table = await prisma.dc_table.findUnique({
      where: {
        tid: BigInt(id),
      },
    });
    
    if (!table) {
      return NextResponse.json({ error: 'Table not found' }, { status: 404 });
    }
    
    // Convert BigInt fields
    const result = {
      ...table,
      tid: Number(table.tid),
      pid: Number(table.pid),
    };
    
    return NextResponse.json(result);
  } catch (error: any) {
    console.error('Error fetching table:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const body = await request.json();
    const prisma = getPrisma();
    
    await prisma.dc_table.update({
      where: {
        tid: BigInt(id),
      },
      data: {
        enabled: body.enabled,
        batch_nbr: body.batch_nbr,
        parallel_degree: body.parallel_degree,
      },
    });
    
    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('Error updating table:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
