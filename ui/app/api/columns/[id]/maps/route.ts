import { NextRequest, NextResponse } from 'next/server';
import { getPrisma } from '@/lib/db';

// Note: This uses dc_table_column_map which has a composite key
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const prisma = getPrisma();
    
    const maps = await prisma.dc_table_column_map.findMany({
      where: {
        column_id: BigInt(id),
      },
    });
    
    // Convert BigInt fields
    const converted = maps.map(map => ({
      ...map,
      tid: Number(map.tid),
      column_id: Number(map.column_id),
    }));
    
    return NextResponse.json(converted);
  } catch (error: any) {
    console.error('Error fetching column maps:', error);
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
    
    // Update the column map based on the composite primary key
    await prisma.dc_table_column_map.update({
      where: {
        column_id_column_origin_column_name: {
          column_id: BigInt(body.column_id),
          column_origin: body.column_origin,
          column_name: body.column_name,
        },
      },
      data: {
        map_expression: body.map_expression || null,
      },
    });
    
    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('Error updating column map:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

export async function DELETE(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    // Placeholder - dc_table_column_map has complex composite key
    return NextResponse.json({ success: true, message: 'Not fully implemented' });
  } catch (error: any) {
    console.error('Error deleting column map:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
