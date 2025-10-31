import { NextRequest, NextResponse } from 'next/server';
import { getPrisma } from '@/lib/db';

// Get all column maps for a table (for dropdown population)
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const prisma = getPrisma();
    
    const columnMaps = await prisma.dc_table_column_map.findMany({
      where: {
        tid: BigInt(id),
      },
      select: {
        column_name: true,
        column_origin: true,
      },
    });
    
    return NextResponse.json(columnMaps);
  } catch (error: any) {
    console.error('Error fetching column maps:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

