import { NextRequest, NextResponse } from 'next/server';
import { testConnection, initializePrisma } from '@/lib/db';

export async function POST(request: NextRequest) {
  try {
    const credentials = await request.json();
    
    console.log('Testing connection to:', credentials.host, credentials.database, credentials.schema);
    
    // Test connection
    await testConnection(credentials);
    
    // Initialize Prisma for subsequent requests
    initializePrisma(credentials);
    
    console.log('Login successful, Prisma initialized');
    
    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('Login failed:', error.message);
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 401 }
    );
  }
}

