import { NextRequest, NextResponse } from 'next/server';
import { getPrisma } from '@/lib/db';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const prisma = getPrisma();
    
    console.log('Fetching current run for project:', id);
    
    // First, let's check if there are any results at all
    const latestRid: any[] = await prisma.$queryRaw`
      SELECT rid 
      FROM dc_result 
      ORDER BY compare_start DESC 
      LIMIT 1
    `;
    
    console.log('Latest RID:', latestRid);
    
    if (!latestRid || latestRid.length === 0) {
      console.log('No results found in dc_result table');
      return NextResponse.json([]);
    }
    
    const rid = latestRid[0].rid;
    console.log('Using RID:', rid);
    
    // Execute the query to get last/current run details
    const runDetails: any[] = await prisma.$queryRaw`
      SELECT 
        table_name, 
        status, 
        compare_start, 
        CAST(coalesce(compare_end, current_timestamp) - compare_start AS TEXT) as run_time, 
        source_cnt, 
        target_cnt, 
        equal_cnt, 
        missing_source_cnt, 
        missing_target_cnt, 
        not_equal_cnt
      FROM dc_result 
      WHERE rid = ${rid}
      AND tid IN (
        SELECT tid 
        FROM dc_table 
        WHERE pid = ${BigInt(id)}
      )
      ORDER BY compare_start DESC
    `;
    
    console.log('Run details found:', runDetails.length, 'rows');
    
    return NextResponse.json(runDetails);
  } catch (error: any) {
    console.error('Error fetching current run:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

