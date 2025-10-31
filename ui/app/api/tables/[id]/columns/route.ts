import { NextRequest, NextResponse } from 'next/server';
import { getPrisma } from '@/lib/db';

// Note: This endpoint returns data from dc_table_column
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const prisma = getPrisma();
    
    const columns = await prisma.dc_table_column.findMany({
      where: {
        tid: BigInt(id),
      },
    });
    
    // Convert BigInt fields
    const converted = columns.map(col => ({
      ...col,
      tid: Number(col.tid),
      column_id: Number(col.column_id),
    }));
    
    return NextResponse.json(converted);
  } catch (error: any) {
    console.error('Error fetching table columns:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const body = await request.json();
    const prisma = getPrisma();
    
    await prisma.dc_table_column.update({
      where: {
        column_id: BigInt(body.column_id),
      },
      data: {
        column_alias: body.column_alias,
        enabled: body.enabled,
      },
    });
    
    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('Error updating table column:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
