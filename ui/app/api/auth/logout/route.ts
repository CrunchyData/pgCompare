import { NextResponse } from 'next/server';
import { closePrisma } from '@/lib/db';

export async function POST() {
  try {
    await closePrisma();
    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('Error during logout:', error);
    return NextResponse.json({ success: true }); // Still return success even if already closed
  }
}

