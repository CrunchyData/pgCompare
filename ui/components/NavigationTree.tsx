'use client';

import { useEffect, useState } from 'react';
import { ChevronRight, ChevronDown, Folder, Table, X, Plus } from 'lucide-react';
import { Project, Table as TableType, Result } from '@/lib/types';

interface NavigationTreeProps {
  onProjectSelect: (projectId: number) => void;
  onTableSelect: (tableId: number) => void;
  selectedProjectId?: number;
  selectedTableId?: number;
}

export default function NavigationTree({
  onProjectSelect,
  onTableSelect,
  selectedProjectId,
  selectedTableId,
}: NavigationTreeProps) {
  const [projects, setProjects] = useState<Project[]>([]);
  const [expandedProjects, setExpandedProjects] = useState<Set<number>>(new Set());
  const [projectTables, setProjectTables] = useState<Map<number, TableType[]>>(new Map());
  const [tableStatuses, setTableStatuses] = useState<Map<number, boolean>>(new Map()); // tid -> hasIssues
  const [loading, setLoading] = useState(true);
  const [creatingProject, setCreatingProject] = useState(false);
  const [newProjectName, setNewProjectName] = useState('');

  useEffect(() => {
    loadProjects();
  }, []);

  const loadProjects = async () => {
    try {
      const response = await fetch('/api/projects');
      const data = await response.json();
      
      // Check if response is an error
      if (!response.ok || data.error) {
        console.error('Failed to load projects:', data.error || 'Unknown error');
        setProjects([]);
        return;
      }
      
      // Ensure data is an array
      if (Array.isArray(data)) {
        setProjects(data);
      } else {
        console.error('Invalid data format received:', data);
        setProjects([]);
      }
    } catch (error) {
      console.error('Failed to load projects:', error);
      setProjects([]);
    } finally {
      setLoading(false);
    }
  };

  const toggleProject = async (projectId: number) => {
    const newExpanded = new Set(expandedProjects);
    
    if (newExpanded.has(projectId)) {
      newExpanded.delete(projectId);
    } else {
      newExpanded.add(projectId);
      // Load tables if not already loaded
      if (!projectTables.has(projectId)) {
        try {
          const response = await fetch(`/api/projects/${projectId}/tables`);
          const tables = await response.json();
          setProjectTables(new Map(projectTables.set(projectId, tables)));
          
          // Load status for each table
          for (const table of tables) {
            loadTableStatus(table.tid);
          }
        } catch (error) {
          console.error('Failed to load tables:', error);
        }
      }
    }
    
    setExpandedProjects(newExpanded);
  };

  const loadTableStatus = async (tableId: number) => {
    try {
      const response = await fetch(`/api/tables/${tableId}/results?latest=true`);
      const data = await response.json();
      
      if (data && data.length > 0) {
        const latestResult = data[0];
        const hasIssues = (latestResult.not_equal_cnt || 0) > 0 || 
                          (latestResult.missing_source_cnt || 0) > 0 || 
                          (latestResult.missing_target_cnt || 0) > 0;
        setTableStatuses(prev => new Map(prev).set(tableId, hasIssues));
      }
    } catch (error) {
      console.error('Failed to load table status:', error);
    }
  };

  const handleCreateProject = async () => {
    if (!newProjectName.trim()) {
      alert('Please enter a project name');
      return;
    }

    try {
      const response = await fetch('/api/projects', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ project_name: newProjectName.trim() }),
      });

      if (!response.ok) {
        throw new Error('Failed to create project');
      }

      const newProject = await response.json();
      setProjects([...projects, newProject]);
      setNewProjectName('');
      setCreatingProject(false);
      
      // Select the newly created project
      onProjectSelect(newProject.pid);
    } catch (error) {
      console.error('Failed to create project:', error);
      alert('Failed to create project');
    }
  };

  if (loading) {
    return (
      <div className="p-4">
        <div className="animate-pulse space-y-2">
          <div className="h-8 bg-gray-200 dark:bg-gray-700 rounded"></div>
          <div className="h-8 bg-gray-200 dark:bg-gray-700 rounded"></div>
          <div className="h-8 bg-gray-200 dark:bg-gray-700 rounded"></div>
        </div>
      </div>
    );
  }

  if (projects.length === 0) {
    return (
      <div className="p-4">
        <p className="text-sm text-gray-500 dark:text-gray-400">
          No projects found. Check your database connection and schema.
        </p>
      </div>
    );
  }

  return (
    <div className="p-4 space-y-1">
      {/* Add New Project Button */}
      {!creatingProject ? (
        <button
          onClick={() => setCreatingProject(true)}
          className="w-full flex items-center gap-2 px-2 py-2 mb-2 text-sm text-blue-600 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 rounded"
        >
          <Plus className="h-4 w-4" />
          New Project
        </button>
      ) : (
        <div className="mb-2 p-2 border border-blue-500 rounded bg-blue-50 dark:bg-blue-900/20">
          <input
            type="text"
            value={newProjectName}
            onChange={(e) => setNewProjectName(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                handleCreateProject();
              } else if (e.key === 'Escape') {
                setCreatingProject(false);
                setNewProjectName('');
              }
            }}
            placeholder="Project name..."
            autoFocus
            className="w-full px-2 py-1 text-sm border border-gray-300 dark:border-gray-600 rounded mb-2 dark:bg-gray-700 dark:text-white"
          />
          <div className="flex gap-2">
            <button
              onClick={handleCreateProject}
              className="flex-1 px-2 py-1 text-xs bg-blue-600 text-white rounded hover:bg-blue-700"
            >
              Create
            </button>
            <button
              onClick={() => {
                setCreatingProject(false);
                setNewProjectName('');
              }}
              className="flex-1 px-2 py-1 text-xs bg-gray-300 dark:bg-gray-600 text-gray-700 dark:text-gray-200 rounded hover:bg-gray-400 dark:hover:bg-gray-500"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {/* Projects List */}
      {projects.map((project) => {
        const isExpanded = expandedProjects.has(project.pid);
        const tables = projectTables.get(project.pid) || [];
        const isSelected = selectedProjectId === project.pid;

        return (
          <div key={project.pid}>
            <div
              className={`flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 ${
                isSelected ? 'bg-blue-50 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400' : ''
              }`}
            >
              <button
                onClick={() => toggleProject(project.pid)}
                className="p-0.5 hover:bg-gray-200 dark:hover:bg-gray-600 rounded"
              >
                {isExpanded ? (
                  <ChevronDown className="h-4 w-4" />
                ) : (
                  <ChevronRight className="h-4 w-4" />
                )}
              </button>
              <Folder className="h-4 w-4" />
              <span
                onClick={() => onProjectSelect(project.pid)}
                className="flex-1 text-sm"
              >
                {project.project_name} <span className="text-xs text-gray-500 dark:text-gray-400">({project.pid})</span>
              </span>
            </div>

            {isExpanded && (
              <div className="ml-6 mt-1 space-y-1">
                {tables.map((table) => {
                  const isTableSelected = selectedTableId === table.tid;
                  const hasIssues = tableStatuses.get(table.tid) || false;
                  return (
                    <div
                      key={table.tid}
                      onClick={() => onTableSelect(table.tid)}
                      className={`flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 ${
                        isTableSelected
                          ? 'bg-blue-50 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400'
                          : ''
                      }`}
                    >
                      <Table className="h-4 w-4" />
                      <span className="text-sm flex-1">{table.table_alias}</span>
                      {hasIssues && (
                        <X className="h-4 w-4 text-red-600 dark:text-red-400" />
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

