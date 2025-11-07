import { NextRequest, NextResponse } from 'next/server';
import { getPrisma } from '@/lib/db';

// Note: This endpoint returns empty arrays for now
// The actual pgCompare schema uses dc_table_map differently than initially expected
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const prisma = getPrisma();
    
    const maps = await prisma.dc_table_map.findMany({
      where: {
        tid: BigInt(id),
      },
    });
    
    // Convert BigInt fields
    const converted = maps.map(map => ({
      ...map,
      tid: Number(map.tid),
    }));
    
    return NextResponse.json(converted);
  } catch (error: any) {
    console.error('Error fetching table maps:', error);
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
    
    // Update the table map based on the composite primary key
    await prisma.dc_table_map.update({
      where: {
        tid_dest_type_schema_name_table_name: {
          tid: BigInt(body.tid),
          dest_type: body.dest_type,
          schema_name: body.schema_name,
          table_name: body.table_name,
        },
      },
      data: {
        mod_column: body.mod_column || null,
        table_filter: body.table_filter || null,
      },
    });
    
    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('Error updating table map:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
