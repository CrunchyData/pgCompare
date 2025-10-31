'use client';

import { useEffect, useState } from 'react';
import { Edit2 } from 'lucide-react';
import { TableColumn } from '@/lib/types';
import TableColumnMapModal from './TableColumnMapModal';

interface TableColumnPanelProps {
  tableId: number;
}

export default function TableColumnPanel({ tableId }: TableColumnPanelProps) {
  const [columns, setColumns] = useState<TableColumn[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedColumn, setSelectedColumn] = useState<TableColumn | null>(null);
  const [showModal, setShowModal] = useState(false);

  useEffect(() => {
    loadColumns();
  }, [tableId]);

  const loadColumns = async () => {
    try {
      const response = await fetch(`/api/tables/${tableId}/columns`);
      const data = await response.json();
      setColumns(data);
    } catch (error) {
      console.error('Failed to load table columns:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleColumnClick = (column: TableColumn) => {
    setSelectedColumn(column);
    setShowModal(true);
  };

  if (loading) {
    return <div className="p-4">Loading columns...</div>;
  }

  return (
    <>
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Table Columns</h3>
        
        {columns.length === 0 ? (
          <p className="text-sm text-gray-500 dark:text-gray-400">No columns found</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 dark:bg-gray-700">
                <tr>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300">Column Alias</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300">Enabled</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-700 dark:text-gray-300">Action</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
                {columns.map((column) => (
                  <tr key={column.column_id} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                    <td className="px-4 py-2 font-medium">
                      {column.column_alias}
                    </td>
                    <td className="px-4 py-2">
                      <span className={`px-2 py-1 rounded text-xs ${
                        column.enabled 
                          ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                          : 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'
                      }`}>
                        {column.enabled ? 'Yes' : 'No'}
                      </span>
                    </td>
                    <td className="px-4 py-2">
                      <button
                        onClick={() => handleColumnClick(column)}
                        className="text-blue-600 hover:text-blue-700 dark:text-blue-400 flex items-center gap-1"
                      >
                        <Edit2 className="h-3 w-3" />
                        View Details
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {selectedColumn && (
        <TableColumnMapModal
          column={selectedColumn}
          isOpen={showModal}
          onClose={() => {
            setShowModal(false);
            setSelectedColumn(null);
          }}
        />
      )}
    </>
  );
}

