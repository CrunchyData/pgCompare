'use client';

import { useEffect, useState } from 'react';
import { Activity, Clock, CheckCircle, XCircle, RefreshCw } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';

interface CurrentRunRow {
  table_name: string;
  status: string | null;
  compare_start: Date | null;
  run_time: any; // PostgreSQL interval
  source_cnt: number | null;
  target_cnt: number | null;
  equal_cnt: number | null;
  missing_source_cnt: number | null;
  missing_target_cnt: number | null;
  not_equal_cnt: number | null;
}

interface ProjectCurrentRunPanelProps {
  projectId: number;
}

export default function ProjectCurrentRunPanel({ projectId }: ProjectCurrentRunPanelProps) {
  const [runData, setRunData] = useState<CurrentRunRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [autoRefreshInterval, setAutoRefreshInterval] = useState<number>(10000); // Default 10 seconds

  useEffect(() => {
    loadCurrentRun();
    
    // Auto-refresh based on selected interval
    if (autoRefreshInterval > 0) {
      const interval = setInterval(loadCurrentRun, autoRefreshInterval);
      return () => clearInterval(interval);
    }
  }, [projectId, autoRefreshInterval]);

  const loadCurrentRun = async (isManualRefresh = false) => {
    if (isManualRefresh) {
      setRefreshing(true);
    }
    
    try {
      console.log('Loading current run for project:', projectId);
      const response = await fetch(`/api/projects/${projectId}/current-run`);
      const data = await response.json();
      
      console.log('Current run response:', { ok: response.ok, data });
      
      // Check if response is an error or not an array
      if (!response.ok || data.error || !Array.isArray(data)) {
        console.error('Failed to load current run:', data.error || 'Invalid data format');
        setRunData([]);
        return;
      }
      
      console.log('Setting run data:', data.length, 'rows');
      setRunData(data);
    } catch (error) {
      console.error('Failed to load current run:', error);
      setRunData([]);
    } finally {
      setLoading(false);
      if (isManualRefresh) {
        setRefreshing(false);
      }
    }
  };

  const handleRefresh = () => {
    loadCurrentRun(true);
  };

  const handleIntervalChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const value = parseInt(e.target.value);
    setAutoRefreshInterval(value);
    
    // Save preference to localStorage
    localStorage.setItem('currentRunRefreshInterval', value.toString());
  };

  // Load saved interval preference on mount
  useEffect(() => {
    const savedInterval = localStorage.getItem('currentRunRefreshInterval');
    if (savedInterval) {
      setAutoRefreshInterval(parseInt(savedInterval));
    }
  }, []);

  // Format PostgreSQL interval to readable string
  const formatRunTime = (interval: any): string => {
    if (!interval) return 'N/A';
    
    // Handle different interval formats
    if (typeof interval === 'string') {
      // Parse PostgreSQL interval format (e.g., "00:01:23.456789")
      const parts = interval.split(':');
      if (parts.length >= 3) {
        const hours = parseInt(parts[0]);
        const minutes = parseInt(parts[1]);
        const seconds = parseFloat(parts[2]);
        
        if (hours > 0) {
          return `${hours}h ${minutes}m ${Math.round(seconds)}s`;
        } else if (minutes > 0) {
          return `${minutes}m ${Math.round(seconds)}s`;
        } else {
          return `${seconds.toFixed(1)}s`;
        }
      }
    }
    
    return String(interval);
  };

  if (loading) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
          <Activity className="h-5 w-5" />
          Current/Last Run
        </h3>
        <div className="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
      </div>
    );
  }

  if (runData.length === 0) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
          <Activity className="h-5 w-5" />
          Current/Last Run
        </h3>
        <p className="text-sm text-gray-500 dark:text-gray-400 text-center py-8">
          No run data available
        </p>
      </div>
    );
  }

  // Check if any table is currently running
  const hasRunningTables = runData.some(row => !row.status || row.status === 'running');

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white flex items-center gap-2">
            <Activity className="h-5 w-5" />
            Current/Last Run
            {hasRunningTables && (
              <span className="ml-2 px-2 py-1 text-xs bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200 rounded animate-pulse">
                In Progress
              </span>
            )}
          </h3>
        </div>
        <div className="flex items-center gap-3">
          {runData[0]?.compare_start && (
            <span className="text-sm text-gray-500 dark:text-gray-400 flex items-center gap-1">
              <Clock className="h-4 w-4" />
              Started {formatDistanceToNow(new Date(runData[0].compare_start), { addSuffix: true })}
            </span>
          )}
          <div className="flex items-center gap-2">
            <label htmlFor="refresh-interval" className="text-sm text-gray-600 dark:text-gray-400">
              Auto-refresh:
            </label>
            <select
              id="refresh-interval"
              value={autoRefreshInterval}
              onChange={handleIntervalChange}
              className="px-2 py-1 text-sm border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
            >
              <option value="0">Off</option>
              <option value="5000">5 sec</option>
              <option value="10000">10 sec</option>
              <option value="30000">30 sec</option>
              <option value="60000">1 min</option>
              <option value="120000">2 min</option>
              <option value="300000">5 min</option>
            </select>
          </div>
          <button
            onClick={handleRefresh}
            disabled={refreshing}
            className="flex items-center gap-2 px-3 py-1.5 text-sm bg-gray-100 hover:bg-gray-200 dark:bg-gray-700 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 rounded transition-colors disabled:opacity-50"
            title="Refresh data"
          >
            <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
            {refreshing ? 'Refreshing...' : 'Refresh'}
          </button>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 dark:bg-gray-700">
            <tr>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300">Table</th>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300">Status</th>
              <th className="px-3 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Run Time</th>
              <th className="px-3 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Source</th>
              <th className="px-3 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Target</th>
              <th className="px-3 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Equal</th>
              <th className="px-3 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Not Equal</th>
              <th className="px-3 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Missing</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
            {runData.map((row, index) => {
              const hasIssues = (row.not_equal_cnt || 0) > 0 || 
                                (row.missing_source_cnt || 0) > 0 || 
                                (row.missing_target_cnt || 0) > 0;
              const isRunning = !row.status || row.status === 'running';
              const missingTotal = (row.missing_source_cnt || 0) + (row.missing_target_cnt || 0);

              return (
                <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                  <td className="px-3 py-2 font-medium">{row.table_name}</td>
                  <td className="px-3 py-2">
                    {isRunning ? (
                      <span className="px-2 py-1 rounded text-xs bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200 flex items-center gap-1 w-fit">
                        <Activity className="h-3 w-3 animate-spin" />
                        Running
                      </span>
                    ) : hasIssues ? (
                      <span className="px-2 py-1 rounded text-xs bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200 flex items-center gap-1 w-fit">
                        <XCircle className="h-3 w-3" />
                        Issues
                      </span>
                    ) : (
                      <span className="px-2 py-1 rounded text-xs bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200 flex items-center gap-1 w-fit">
                        <CheckCircle className="h-3 w-3" />
                        Complete
                      </span>
                    )}
                  </td>
                  <td className="px-3 py-2 text-right text-gray-600 dark:text-gray-400">
                    {formatRunTime(row.run_time)}
                  </td>
                  <td className="px-3 py-2 text-right">{row.source_cnt?.toLocaleString() || 0}</td>
                  <td className="px-3 py-2 text-right">{row.target_cnt?.toLocaleString() || 0}</td>
                  <td className="px-3 py-2 text-right text-green-600 dark:text-green-400">
                    {row.equal_cnt?.toLocaleString() || 0}
                  </td>
                  <td className="px-3 py-2 text-right">
                    <span className={row.not_equal_cnt ? 'text-red-600 dark:text-red-400 font-medium' : ''}>
                      {row.not_equal_cnt?.toLocaleString() || 0}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-right">
                    <span className={missingTotal ? 'text-orange-600 dark:text-orange-400 font-medium' : ''}>
                      {missingTotal.toLocaleString()}
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Summary Stats */}
      {runData.length > 0 && (
        <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            <div className="text-center p-2 bg-gray-50 dark:bg-gray-700 rounded">
              <p className="text-xs text-gray-600 dark:text-gray-400">Tables</p>
              <p className="text-lg font-bold text-gray-900 dark:text-white">{runData.length}</p>
            </div>
            <div className="text-center p-2 bg-blue-50 dark:bg-blue-900/20 rounded">
              <p className="text-xs text-gray-600 dark:text-gray-400">Total Source</p>
              <p className="text-lg font-bold text-blue-600 dark:text-blue-400">
                {runData.reduce((sum, row) => sum + (row.source_cnt || 0), 0).toLocaleString()}
              </p>
            </div>
            <div className="text-center p-2 bg-green-50 dark:bg-green-900/20 rounded">
              <p className="text-xs text-gray-600 dark:text-gray-400">Total Equal</p>
              <p className="text-lg font-bold text-green-600 dark:text-green-400">
                {runData.reduce((sum, row) => sum + (row.equal_cnt || 0), 0).toLocaleString()}
              </p>
            </div>
            <div className="text-center p-2 bg-red-50 dark:bg-red-900/20 rounded">
              <p className="text-xs text-gray-600 dark:text-gray-400">Total Not Equal</p>
              <p className="text-lg font-bold text-red-600 dark:text-red-400">
                {runData.reduce((sum, row) => sum + (row.not_equal_cnt || 0), 0).toLocaleString()}
              </p>
            </div>
            <div className="text-center p-2 bg-orange-50 dark:bg-orange-900/20 rounded">
              <p className="text-xs text-gray-600 dark:text-gray-400">Total Missing</p>
              <p className="text-lg font-bold text-orange-600 dark:text-orange-400">
                {runData.reduce((sum, row) => sum + (row.missing_source_cnt || 0) + (row.missing_target_cnt || 0), 0).toLocaleString()}
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

