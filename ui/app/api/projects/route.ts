import { NextResponse } from 'next/server';
import { getPrisma } from '@/lib/db';

export async function GET() {
  try {
    const prisma = getPrisma();
    const projects = await prisma.dc_project.findMany({
      orderBy: {
        project_name: 'asc',
      },
      select: {
        pid: true,
        project_name: true,
        project_config: true,
      },
    });
    
    // Convert BigInt to number for JSON serialization
    const converted = projects.map(project => ({
      ...project,
      pid: Number(project.pid),
    }));
    
    return NextResponse.json(converted);
  } catch (error: any) {
    console.error('Error fetching projects:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

