import { NextRequest, NextResponse } from 'next/server';
import { getPrisma } from '@/lib/db';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const prisma = getPrisma();
    
    const project = await prisma.dc_project.findUnique({
      where: {
        pid: BigInt(id),
      },
      select: {
        pid: true,
        project_name: true,
        project_config: true,
      },
    });
    
    if (!project) {
      return NextResponse.json({ error: 'Project not found' }, { status: 404 });
    }
    
    // Convert BigInt to string for JSON serialization
    const result = {
      ...project,
      pid: Number(project.pid),
    };
    
    return NextResponse.json(result);
  } catch (error: any) {
    console.error('Error fetching project:', error);
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
    
    const updateData: any = {};
    
    // Only update fields that are provided
    if (body.project_name !== undefined) {
      updateData.project_name = body.project_name;
    }
    
    if (body.project_config !== undefined) {
      updateData.project_config = body.project_config;
    }
    
    await prisma.dc_project.update({
      where: {
        pid: BigInt(id),
      },
      data: updateData,
    });
    
    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('Error updating project:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

