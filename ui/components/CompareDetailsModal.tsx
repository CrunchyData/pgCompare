'use client';

import { useEffect, useState } from 'react';
import { X, AlertTriangle } from 'lucide-react';
import { Result } from '@/lib/types';

interface CompareDetailsModalProps {
  result: Result;
  isOpen: boolean;
  onClose: () => void;
}

interface SourceTargetRow {
  pk: any;
  pk_hash: string | null;
  column_hash: string | null;
  compare_result: string | null;
  thread_nbr: number | null;
}

export default function CompareDetailsModal({ result, isOpen, onClose }: CompareDetailsModalProps) {
  const [sourceRows, setSourceRows] = useState<SourceTargetRow[]>([]);
  const [targetRows, setTargetRows] = useState<SourceTargetRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<'not_equal' | 'missing_source' | 'missing_target'>('not_equal');

  useEffect(() => {
    if (isOpen) {
      loadCompareDetails();
    }
  }, [isOpen, result.cid]);

  const loadCompareDetails = async () => {
    setLoading(true);
    try {
      console.log('Loading compare details for cid:', result.cid);
      
      const [sourceResponse, targetResponse] = await Promise.all([
        fetch(`/api/results/${result.cid}/source`),
        fetch(`/api/results/${result.cid}/target`)
      ]);
      
      if (!sourceResponse.ok) {
        console.error('Source response error:', await sourceResponse.text());
      }
      if (!targetResponse.ok) {
        console.error('Target response error:', await targetResponse.text());
      }
      
      const sourceData = await sourceResponse.json();
      const targetData = await targetResponse.json();
      
      console.log('Source data:', sourceData);
      console.log('Target data:', targetData);
      
      setSourceRows(Array.isArray(sourceData) ? sourceData : []);
      setTargetRows(Array.isArray(targetData) ? targetData : []);
    } catch (error) {
      console.error('Failed to load compare details:', error);
    } finally {
      setLoading(false);
    }
  };

  if (!isOpen) return null;

  // Filter rows based on compare_result
  const notEqualSource = sourceRows.filter(row => row.compare_result === 'n'); // Different
  const notEqualTarget = targetRows.filter(row => row.compare_result === 'n');
  const missingInTarget = sourceRows.filter(row => row.compare_result === 'm'); // Source only
  const missingInSource = targetRows.filter(row => row.compare_result === 'm'); // Target only

  const renderTable = (rows: SourceTargetRow[], title: string) => (
    <div className="mb-6">
      <h4 className="text-md font-semibold text-gray-900 dark:text-white mb-3">{title}</h4>
      {rows.length === 0 ? (
        <p className="text-sm text-gray-500 dark:text-gray-400">No rows found</p>
      ) : (
        <div className="overflow-x-auto max-h-96 overflow-y-auto">
          <table className="w-full text-sm border-collapse">
            <thead className="bg-gray-50 dark:bg-gray-700 sticky top-0">
              <tr>
                <th className="px-3 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600">
                  Primary Key
                </th>
                <th className="px-3 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600">
                  PK Hash
                </th>
                <th className="px-3 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600">
                  Column Hash
                </th>
                <th className="px-3 py-2 text-center text-xs font-medium text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600">
                  Thread
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
              {rows.map((row, index) => (
                <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                  <td className="px-3 py-2 text-xs font-mono border border-gray-200 dark:border-gray-600">
                    {typeof row.pk === 'object' ? JSON.stringify(row.pk) : row.pk}
                  </td>
                  <td className="px-3 py-2 text-xs font-mono border border-gray-200 dark:border-gray-600 truncate max-w-xs">
                    {row.pk_hash || 'N/A'}
                  </td>
                  <td className="px-3 py-2 text-xs font-mono border border-gray-200 dark:border-gray-600 truncate max-w-xs">
                    {row.column_hash || 'N/A'}
                  </td>
                  <td className="px-3 py-2 text-xs text-center border border-gray-200 dark:border-gray-600">
                    {row.thread_nbr || '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-7xl w-full max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="border-b border-gray-200 dark:border-gray-700 px-6 py-4 flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-orange-600" />
              Compare Details - {result.table_name}
            </h3>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              {result.compare_start ? new Date(result.compare_start).toLocaleString() : 'N/A'}
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-500 dark:hover:text-gray-300"
          >
            <X className="h-6 w-6" />
          </button>
        </div>

        {/* Summary Stats */}
        <div className="px-6 py-4 bg-gray-50 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700">
          <div className="grid grid-cols-3 gap-4">
            <div className="text-center">
              <p className="text-sm text-gray-600 dark:text-gray-400">Not Equal Rows</p>
              <p className="text-2xl font-bold text-red-600 dark:text-red-400">
                {result.not_equal_cnt?.toLocaleString() || 0}
              </p>
            </div>
            <div className="text-center">
              <p className="text-sm text-gray-600 dark:text-gray-400">Missing in Source</p>
              <p className="text-2xl font-bold text-orange-600 dark:text-orange-400">
                {result.missing_source_cnt?.toLocaleString() || 0}
              </p>
            </div>
            <div className="text-center">
              <p className="text-sm text-gray-600 dark:text-gray-400">Missing in Target</p>
              <p className="text-2xl font-bold text-purple-600 dark:text-purple-400">
                {result.missing_target_cnt?.toLocaleString() || 0}
              </p>
            </div>
          </div>
        </div>

        {/* Tabs */}
        <div className="border-b border-gray-200 dark:border-gray-700 px-6">
          <div className="flex gap-4">
            <button
              onClick={() => setActiveTab('not_equal')}
              className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                activeTab === 'not_equal'
                  ? 'border-red-600 text-red-600 dark:text-red-400'
                  : 'border-transparent text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200'
              }`}
            >
              Not Equal ({notEqualSource.length})
            </button>
            <button
              onClick={() => setActiveTab('missing_target')}
              className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                activeTab === 'missing_target'
                  ? 'border-orange-600 text-orange-600 dark:text-orange-400'
                  : 'border-transparent text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200'
              }`}
            >
              Missing in Target ({missingInTarget.length})
            </button>
            <button
              onClick={() => setActiveTab('missing_source')}
              className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                activeTab === 'missing_source'
                  ? 'border-purple-600 text-purple-600 dark:text-purple-400'
                  : 'border-transparent text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200'
              }`}
            >
              Missing in Source ({missingInSource.length})
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {loading ? (
            <div className="text-center py-8">Loading compare details...</div>
          ) : (
            <>
              {activeTab === 'not_equal' && (
                <div className="space-y-6">
                  {renderTable(notEqualSource, 'Source Rows (Different)')}
                  {renderTable(notEqualTarget, 'Target Rows (Different)')}
                  {notEqualSource.length === 0 && notEqualTarget.length === 0 && (
                    <p className="text-sm text-gray-500 dark:text-gray-400 text-center py-8">
                      No differing rows found
                    </p>
                  )}
                </div>
              )}

              {activeTab === 'missing_target' && (
                <>
                  {renderTable(missingInTarget, 'Rows in Source but Missing in Target')}
                </>
              )}

              {activeTab === 'missing_source' && (
                <>
                  {renderTable(missingInSource, 'Rows in Target but Missing in Source')}
                </>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}

