import { PrismaClient } from '@prisma/client';
import { DBCredentials } from './types';

// Extend global type for TypeScript
declare global {
  var prisma: PrismaClient | undefined;
  var dbCredentials: DBCredentials | undefined;
  var dbSchema: string | undefined;
}

// Use global variables to persist across hot reloads in development
let prisma = global.prisma;
let currentCredentials = global.dbCredentials;
let currentSchema = global.dbSchema || 'pgcompare';

export function initializePrisma(credentials: DBCredentials): PrismaClient {
  const databaseUrl = `postgresql://${credentials.user}:${encodeURIComponent(credentials.password)}@${credentials.host}:${credentials.port}/${credentials.database}?schema=${credentials.schema || 'pgcompare'}`;
  
  currentSchema = credentials.schema || 'pgcompare';
  currentCredentials = credentials;
  
  // Store in global
  global.dbCredentials = credentials;
  global.dbSchema = currentSchema;
  
  if (prisma) {
    prisma.$disconnect().catch(() => {
      // Ignore errors during disconnect
    });
  }

  prisma = new PrismaClient({
    datasources: {
      db: {
        url: databaseUrl,
      },
    },
    log: process.env.NODE_ENV === 'development' ? ['error', 'warn'] : ['error'],
  });

  // Store in global
  global.prisma = prisma;
  
  console.log('Prisma client initialized for schema:', currentSchema);
  return prisma;
}

export function getPrisma(): PrismaClient {
  // Try to get from module variable first
  if (prisma) {
    return prisma;
  }
  
  // Try to get from global
  if (global.prisma) {
    prisma = global.prisma;
    return prisma;
  }
  
  // Check if we have credentials to reinitialize
  const creds = currentCredentials || global.dbCredentials;
  if (creds) {
    console.log('Reinitializing Prisma from stored credentials');
    return initializePrisma(creds);
  }
  
  throw new Error('Database not initialized. Please login again.');
}

export function getSchema(): string {
  return currentSchema || global.dbSchema || 'pgcompare';
}

export function getCredentials(): DBCredentials | undefined {
  return currentCredentials || global.dbCredentials;
}

export async function testConnection(credentials: DBCredentials): Promise<boolean> {
  const databaseUrl = `postgresql://${credentials.user}:${encodeURIComponent(credentials.password)}@${credentials.host}:${credentials.port}/${credentials.database}?schema=${credentials.schema || 'pgcompare'}`;
  
  const testClient = new PrismaClient({
    datasources: {
      db: {
        url: databaseUrl,
      },
    },
  });

  try {
    await testClient.$connect();
    // Test a simple query
    await testClient.$queryRaw`SELECT 1`;
    await testClient.$disconnect();
    return true;
  } catch (error) {
    await testClient.$disconnect();
    throw error;
  }
}

export async function closePrisma(): Promise<void> {
  if (prisma) {
    await prisma.$disconnect();
    prisma = undefined;
    global.prisma = undefined;
  }
  
  currentCredentials = undefined;
  global.dbCredentials = undefined;
  currentSchema = 'pgcompare';
  global.dbSchema = undefined;
  
  console.log('Prisma client disconnected');
}
