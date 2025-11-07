'use client';

import { useEffect, useState } from 'react';
import { Result } from '@/lib/types';
import { formatDistanceToNow } from 'date-fns';
import CompareDetailsModal from './CompareDetailsModal';

interface TableResultsPanelProps {
  tableId: number;
}

export default function TableResultsPanel({ tableId }: TableResultsPanelProps) {
  const [results, setResults] = useState<Result[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedResult, setSelectedResult] = useState<Result | null>(null);
  const [showModal, setShowModal] = useState(false);

  useEffect(() => {
    loadResults();
  }, [tableId]);

  const loadResults = async () => {
    try {
      const response = await fetch(`/api/tables/${tableId}/results`);
      const data = await response.json();
      setResults(data);
    } catch (error) {
      console.error('Failed to load table results:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleRowClick = (result: Result) => {
    const hasIssues = (result.not_equal_cnt || 0) > 0 || 
                      (result.missing_source_cnt || 0) > 0 || 
                      (result.missing_target_cnt || 0) > 0;
    
    if (hasIssues) {
      setSelectedResult(result);
      setShowModal(true);
    }
  };

  if (loading) {
    return <div className="p-4">Loading results...</div>;
  }

  return (
    <>
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Compare History</h3>
        
        {results.length === 0 ? (
          <p className="text-sm text-gray-500 dark:text-gray-400">No comparison results found</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 dark:bg-gray-700">
                <tr>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300">Date</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300">Status</th>
                  <th className="px-4 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Equal</th>
                  <th className="px-4 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Not Equal</th>
                  <th className="px-4 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Missing Source</th>
                  <th className="px-4 py-2 text-right text-xs font-medium text-gray-700 dark:text-gray-300">Missing Target</th>
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

                  return (
                    <tr 
                      key={result.cid} 
                      onClick={() => handleRowClick(result)}
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
                      <td className="px-4 py-2">
                        <span className={`px-2 py-1 rounded text-xs ${
                          hasIssues
                            ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
                            : result.status === 'completed'
                            ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                            : 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'
                        }`}>
                          {hasIssues ? 'Out of Sync' : result.status || 'Unknown'}
                        </span>
                      </td>
                      <td className="px-4 py-2 text-right">{result.equal_cnt?.toLocaleString() || 0}</td>
                      <td className="px-4 py-2 text-right">
                        <span className={result.not_equal_cnt ? 'text-red-600 dark:text-red-400 font-medium' : ''}>
                          {result.not_equal_cnt?.toLocaleString() || 0}
                        </span>
                      </td>
                      <td className="px-4 py-2 text-right">
                        <span className={result.missing_source_cnt ? 'text-orange-600 dark:text-orange-400 font-medium' : ''}>
                          {result.missing_source_cnt?.toLocaleString() || 0}
                        </span>
                      </td>
                      <td className="px-4 py-2 text-right">
                        <span className={result.missing_target_cnt ? 'text-orange-600 dark:text-orange-400 font-medium' : ''}>
                          {result.missing_target_cnt?.toLocaleString() || 0}
                        </span>
                      </td>
                      <td className="px-4 py-2 text-right">
                        {duration ? `${duration}s` : 'N/A'}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

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
    </>
  );
}

