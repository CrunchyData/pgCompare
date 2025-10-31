'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import NavigationTree from '@/components/NavigationTree';
import ProjectView from '@/components/ProjectView';
import TableView from '@/components/TableView';
import ThemeToggle from '@/components/ThemeToggle';
import { LogOut } from 'lucide-react';

export default function DashboardPage() {
  const router = useRouter();
  const [selectedProjectId, setSelectedProjectId] = useState<number | undefined>();
  const [selectedTableId, setSelectedTableId] = useState<number | undefined>();

  const handleProjectSelect = (projectId: number) => {
    setSelectedProjectId(projectId);
    setSelectedTableId(undefined);
  };

  const handleTableSelect = (tableId: number) => {
    setSelectedTableId(tableId);
  };

  const handleLogout = async () => {
    try {
      await fetch('/api/auth/logout', { method: 'POST' });
      router.push('/');
    } catch (error) {
      console.error('Logout error:', error);
      // Still redirect even if there's an error
      router.push('/');
    }
  };

  return (
    <div className="h-screen flex flex-col bg-gray-50 dark:bg-gray-900">
      {/* Header */}
      <header className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 px-6 py-4">
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">pgCompare</h1>
          <div className="flex items-center gap-3">
            <ThemeToggle />
            <button
              onClick={handleLogout}
              className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 rounded-lg transition-colors"
              title="Logout"
            >
              <LogOut className="h-4 w-4" />
              <span>Logout</span>
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Sidebar - Navigation Tree */}
        <aside className="w-64 bg-white dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700 overflow-y-auto">
          <NavigationTree
            onProjectSelect={handleProjectSelect}
            onTableSelect={handleTableSelect}
            selectedProjectId={selectedProjectId}
            selectedTableId={selectedTableId}
          />
        </aside>

        {/* Content Area */}
        <main className="flex-1 overflow-y-auto p-6">
          {selectedTableId ? (
            <TableView tableId={selectedTableId} />
          ) : selectedProjectId ? (
            <ProjectView projectId={selectedProjectId} />
          ) : (
            <div className="flex items-center justify-center h-full">
              <p className="text-gray-500 dark:text-gray-400 text-lg">
                Select a project or table from the navigation tree
              </p>
            </div>
          )}
        </main>
      </div>
    </div>
  );
}

