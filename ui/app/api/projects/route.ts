import { NextRequest, NextResponse } from 'next/server';
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

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { project_name } = body;

    if (!project_name) {
      return NextResponse.json({ error: 'Project name is required' }, { status: 400 });
    }

    const prisma = getPrisma();
    const newProject = await prisma.dc_project.create({
      data: {
        project_name,
      },
      select: {
        pid: true,
        project_name: true,
        project_config: true,
      },
    });

    // Convert BigInt to number for JSON serialization
    const converted = {
      ...newProject,
      pid: Number(newProject.pid),
    };

    return NextResponse.json(converted);
  } catch (error: any) {
    console.error('Error creating project:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

