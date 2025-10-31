'use client';

import { useEffect, useState } from 'react';
import { Save, TrendingUp, BarChart3, AlertTriangle, CheckCircle, XCircle } from 'lucide-react';
import { Project, Result } from '@/lib/types';
import { LineChart, Line, BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { formatDistanceToNow } from 'date-fns';
import CompareDetailsModal from './CompareDetailsModal';

interface ProjectViewProps {
  projectId: number;
}

export default function ProjectView({ projectId }: ProjectViewProps) {
  const [project, setProject] = useState<Project | null>(null);
  const [results, setResults] = useState<Result[]>([]);
  const [configData, setConfigData] = useState<Array<{ key: string; value: string }>>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [editingName, setEditingName] = useState(false);
  const [projectName, setProjectName] = useState('');
  const [selectedResult, setSelectedResult] = useState<Result | null>(null);
  const [showModal, setShowModal] = useState(false);

  useEffect(() => {
    loadProject();
    loadResults();
  }, [projectId]);

  const loadProject = async () => {
    try {
      const response = await fetch(`/api/projects/${projectId}`);
      const data = await response.json();
      setProject(data);
      setProjectName(data.project_name);
      
      // Convert JSON config to key-value pairs
      const config = data.project_config || {};
      const configArray = Object.entries(config).map(([key, value]) => ({
        key,
        value: typeof value === 'object' ? JSON.stringify(value) : String(value),
      }));
      setConfigData(configArray);
    } catch (error) {
      console.error('Failed to load project:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadResults = async () => {
    try {
      const response = await fetch(`/api/projects/${projectId}/results`);
      const data = await response.json();
      setResults(data);
    } catch (error) {
      console.error('Failed to load results:', error);
    }
  };

  const handleConfigChange = (index: number, field: 'key' | 'value', value: string) => {
    const newConfig = [...configData];
    newConfig[index][field] = value;
    setConfigData(newConfig);
  };

  const addConfigRow = () => {
    setConfigData([...configData, { key: '', value: '' }]);
  };

  const removeConfigRow = (index: number) => {
    setConfigData(configData.filter((_, i) => i !== index));
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      // Convert key-value pairs back to JSON object
      const configObject: Record<string, any> = {};
      configData.forEach(({ key, value }) => {
        if (key) {
          try {
            configObject[key] = JSON.parse(value);
          } catch {
            configObject[key] = value;
          }
        }
      });

      await fetch(`/api/projects/${projectId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          project_name: projectName,
          project_config: configObject 
        }),
      });

      // Update local state
      if (project) {
        setProject({ ...project, project_name: projectName });
      }
      setEditingName(false);

      alert('Configuration saved successfully');
    } catch (error) {
      console.error('Failed to save configuration:', error);
      alert('Failed to save configuration');
    } finally {
      setSaving(false);
    }
  };

  const handleResultClick = (result: Result) => {
    const hasIssues = (result.not_equal_cnt || 0) > 0 || 
                      (result.missing_source_cnt || 0) > 0 || 
                      (result.missing_target_cnt || 0) > 0;
    
    if (hasIssues) {
      setSelectedResult(result);
      setShowModal(true);
    }
  };

  if (loading) {
    return <div className="p-4">Loading...</div>;
  }

  if (!project) {
    return <div className="p-4">Project not found</div>;
  }

  const lastResult = results[0];

  // Prepare chart data using actual pgCompare schema fields
  const chartData = results.slice(0, 10).reverse().map((result) => ({
    timestamp: result.compare_start ? new Date(result.compare_start).toLocaleDateString() : 'N/A',
    equal: result.equal_cnt || 0,
    notEqual: result.not_equal_cnt || 0,
    missingSource: result.missing_source_cnt || 0,
    missingTarget: result.missing_target_cnt || 0,
  }));

  // Calculate overall project statistics
  const totalCompares = results.length;
  const successfulCompares = results.filter(r => 
    (r.not_equal_cnt || 0) === 0 && 
    (r.missing_source_cnt || 0) === 0 && 
    (r.missing_target_cnt || 0) === 0
  ).length;
  const failedCompares = totalCompares - successfulCompares;

  // Pie chart data for last compare
  const lastComparePieData = lastResult ? [
    { name: 'Equal', value: lastResult.equal_cnt || 0, color: '#10b981' },
    { name: 'Not Equal', value: lastResult.not_equal_cnt || 0, color: '#ef4444' },
    { name: 'Missing Source', value: lastResult.missing_source_cnt || 0, color: '#f59e0b' },
    { name: 'Missing Target', value: lastResult.missing_target_cnt || 0, color: '#8b5cf6' },
  ].filter(item => item.value > 0) : [];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <div className="flex items-center justify-between">
          <div className="flex-1">
            {editingName ? (
              <input
                type="text"
                value={projectName}
                onChange={(e) => setProjectName(e.target.value)}
                onBlur={() => setEditingName(false)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    setEditingName(false);
                    handleSave();
                  } else if (e.key === 'Escape') {
                    setProjectName(project?.project_name || '');
                    setEditingName(false);
                  }
                }}
                autoFocus
                className="text-2xl font-bold text-gray-900 dark:text-white bg-transparent border-b-2 border-blue-500 focus:outline-none w-full"
              />
            ) : (
              <h2 
                onClick={() => setEditingName(true)}
                className="text-2xl font-bold text-gray-900 dark:text-white cursor-pointer hover:text-blue-600 dark:hover:text-blue-400"
                title="Click to edit"
              >
                {projectName}
              </h2>
            )}
            <p className="text-sm text-gray-500 dark:text-gray-400">Project ID: {project?.pid}</p>
          </div>
        </div>
      </div>

      {/* Configuration Editor */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Configuration</h3>
          <button
            onClick={handleSave}
            disabled={saving}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
          >
            <Save className="h-4 w-4" />
            {saving ? 'Saving...' : 'Save'}
          </button>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-200 dark:border-gray-700">
                <th className="text-left py-2 px-4 text-sm font-medium text-gray-700 dark:text-gray-300">Key</th>
                <th className="text-left py-2 px-4 text-sm font-medium text-gray-700 dark:text-gray-300">Value</th>
                <th className="w-20"></th>
              </tr>
            </thead>
            <tbody>
              {configData.map((item, index) => (
                <tr key={index} className="border-b border-gray-100 dark:border-gray-700">
                  <td className="py-2 px-4">
                    <input
                      type="text"
                      value={item.key}
                      onChange={(e) => handleConfigChange(index, 'key', e.target.value)}
                      className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded dark:bg-gray-700 dark:text-white"
                    />
                  </td>
                  <td className="py-2 px-4">
                    <input
                      type="text"
                      value={item.value}
                      onChange={(e) => handleConfigChange(index, 'value', e.target.value)}
                      className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded dark:bg-gray-700 dark:text-white"
                    />
                  </td>
                  <td className="py-2 px-4">
                    <button
                      onClick={() => removeConfigRow(index)}
                      className="text-red-600 hover:text-red-700 dark:text-red-400"
                    >
                      Remove
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <button
          onClick={addConfigRow}
          className="mt-4 px-4 py-2 text-sm text-blue-600 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 rounded"
        >
          + Add Row
        </button>
      </div>

      {/* Last Run Summary */}
      {lastResult && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
            <TrendingUp className="h-5 w-5" />
            Last Run Summary
          </h3>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
            <div className="p-4 bg-gray-50 dark:bg-gray-700 rounded">
              <p className="text-sm text-gray-600 dark:text-gray-400">Status</p>
              <p className="text-xl font-bold flex items-center gap-2">
                {((lastResult.not_equal_cnt || 0) > 0 || 
                  (lastResult.missing_source_cnt || 0) > 0 || 
                  (lastResult.missing_target_cnt || 0) > 0) ? (
                  <>
                    <XCircle className="h-5 w-5 text-red-600" />
                    <span className="text-red-600">Out of Sync</span>
                  </>
                ) : (
                  <>
                    <CheckCircle className="h-5 w-5 text-green-600" />
                    <span className="text-green-600">In Sync</span>
                  </>
                )}
              </p>
            </div>
            <div className="p-4 bg-gray-50 dark:bg-gray-700 rounded">
              <p className="text-sm text-gray-600 dark:text-gray-400">Table</p>
              <p className="text-lg font-bold text-gray-900 dark:text-white">{lastResult.table_name || 'N/A'}</p>
            </div>
            <div className="p-4 bg-green-50 dark:bg-green-900/20 rounded">
              <p className="text-sm text-gray-600 dark:text-gray-400">Equal Rows</p>
              <p className="text-2xl font-bold text-green-600 dark:text-green-400">
                {(lastResult.equal_cnt || 0).toLocaleString()}
              </p>
            </div>
            <div className="p-4 bg-red-50 dark:bg-red-900/20 rounded">
              <p className="text-sm text-gray-600 dark:text-gray-400">Not Equal</p>
              <p className="text-2xl font-bold text-red-600 dark:text-red-400">
                {(lastResult.not_equal_cnt || 0).toLocaleString()}
              </p>
            </div>
            <div className="p-4 bg-orange-50 dark:bg-orange-900/20 rounded">
              <p className="text-sm text-gray-600 dark:text-gray-400">Missing Rows</p>
              <p className="text-2xl font-bold text-orange-600 dark:text-orange-400">
                {((lastResult.missing_source_cnt || 0) + (lastResult.missing_target_cnt || 0)).toLocaleString()}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Charts */}
      {results.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Overview Statistics */}
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Project Statistics</h3>
            <div className="space-y-4">
              <div className="flex items-center justify-between p-3 bg-blue-50 dark:bg-blue-900/20 rounded">
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Total Compares</span>
                <span className="text-xl font-bold text-blue-600 dark:text-blue-400">{totalCompares}</span>
              </div>
              <div className="flex items-center justify-between p-3 bg-green-50 dark:bg-green-900/20 rounded">
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Successful</span>
                <span className="text-xl font-bold text-green-600 dark:text-green-400">{successfulCompares}</span>
              </div>
              <div className="flex items-center justify-between p-3 bg-red-50 dark:bg-red-900/20 rounded">
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">With Issues</span>
                <span className="text-xl font-bold text-red-600 dark:text-red-400">{failedCompares}</span>
              </div>
              <div className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-700 rounded">
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Success Rate</span>
                <span className="text-xl font-bold text-gray-900 dark:text-white">
                  {totalCompares > 0 ? Math.round((successfulCompares / totalCompares) * 100) : 0}%
                </span>
              </div>
            </div>
          </div>

          {/* Last Compare Breakdown */}
          {lastResult && lastComparePieData.length > 0 && (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Last Compare Breakdown</h3>
              <ResponsiveContainer width="100%" height={250}>
                <PieChart>
                  <Pie
                    data={lastComparePieData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, percent }: any) => `${name}: ${(percent * 100).toFixed(0)}%`}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {lastComparePieData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Row Comparison History */}
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 lg:col-span-2">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
              <BarChart3 className="h-5 w-5" />
              Row Comparison History (Last 10 Runs)
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="timestamp" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="equal" fill="#10b981" name="Equal Rows" />
                <Bar dataKey="notEqual" fill="#ef4444" name="Not Equal Rows" />
                <Bar dataKey="missingSource" fill="#f59e0b" name="Missing in Source" />
                <Bar dataKey="missingTarget" fill="#8b5cf6" name="Missing in Target" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Historical Compare Outcomes */}
      {results.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Historical Compare Outcomes</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 dark:bg-gray-700">
                <tr>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300">Date</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300">Table</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300">Status</th>
                  <th className="px-4 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Equal</th>
                  <th className="px-4 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Not Equal</th>
                  <th className="px-4 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Missing</th>
                  <th className="px-4 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Duration</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
                {results.map((result) => {
                  const hasIssues = (result.not_equal_cnt || 0) > 0 || 
                                    (result.missing_source_cnt || 0) > 0 || 
                                    (result.missing_target_cnt || 0) > 0;
                  const duration = result.compare_start && result.compare_end
                    ? Math.round((new Date(result.compare_end).getTime() - new Date(result.compare_start).getTime()) / 1000)
                    : null;
                  const missingTotal = (result.missing_source_cnt || 0) + (result.missing_target_cnt || 0);

                  return (
                    <tr 
                      key={result.cid} 
                      onClick={() => handleResultClick(result)}
                      className={`hover:bg-gray-50 dark:hover:bg-gray-700 ${
                        hasIssues ? 'cursor-pointer' : ''
                      }`}
                      title={hasIssues ? 'Click to view details' : ''}
                    >
                      <td className="px-4 py-2">
                        {result.compare_start 
                          ? formatDistanceToNow(new Date(result.compare_start), { addSuffix: true })
                          : 'N/A'}
                      </td>
                      <td className="px-4 py-2 font-medium">{result.table_name || 'N/A'}</td>
                      <td className="px-4 py-2">
                        <span className={`px-2 py-1 rounded text-xs flex items-center gap-1 w-fit ${
                          hasIssues
                            ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
                            : 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                        }`}>
                          {hasIssues ? (
                            <>
                              <XCircle className="h-3 w-3" />
                              Out of Sync
                            </>
                          ) : (
                            <>
                              <CheckCircle className="h-3 w-3" />
                              In Sync
                            </>
                          )}
                        </span>
                      </td>
                      <td className="px-4 py-2 text-right text-green-600 dark:text-green-400">
                        {result.equal_cnt?.toLocaleString() || 0}
                      </td>
                      <td className="px-4 py-2 text-right">
                        <span className={result.not_equal_cnt ? 'text-red-600 dark:text-red-400 font-medium' : ''}>
                          {result.not_equal_cnt?.toLocaleString() || 0}
                        </span>
                      </td>
                      <td className="px-4 py-2 text-right">
                        <span className={missingTotal ? 'text-orange-600 dark:text-orange-400 font-medium' : ''}>
                          {missingTotal.toLocaleString()}
                        </span>
                      </td>
                      <td className="px-4 py-2 text-right text-gray-600 dark:text-gray-400">
                        {duration ? `${duration}s` : 'N/A'}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
      
      {selectedResult && (
        <CompareDetailsModal
          result={selectedResult}
          isOpen={showModal}
          onClose={() => {
            setShowModal(false);
            setSelectedResult(null);
          }}
        />
      )}
    </div>
  );
}

