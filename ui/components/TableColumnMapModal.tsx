'use client';

import { useEffect, useState } from 'react';
import { X, Save } from 'lucide-react';
import { TableColumn, TableColumnMap } from '@/lib/types';

interface TableColumnMapModalProps {
  column: TableColumn;
  isOpen: boolean;
  onClose: () => void;
}

export default function TableColumnMapModal({ column, isOpen, onClose }: TableColumnMapModalProps) {
  const [maps, setMaps] = useState<TableColumnMap[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (isOpen) {
      loadMaps();
    }
  }, [isOpen, column.column_id]);

  const loadMaps = async () => {
    setLoading(true);
    try {
      const response = await fetch(`/api/columns/${column.column_id}/maps`);
      const data = await response.json();
      setMaps(data);
    } catch (error) {
      console.error('Failed to load column maps:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleMapExpressionChange = (index: number, value: string) => {
    const newMaps = [...maps];
    newMaps[index] = { ...newMaps[index], map_expression: value || undefined };
    setMaps(newMaps);
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      // Save each map
      for (const map of maps) {
        await fetch(`/api/columns/${column.column_id}/maps`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            tid: map.tid,
            column_id: map.column_id,
            column_origin: map.column_origin,
            column_name: map.column_name,
            map_expression: map.map_expression,
          }),
        });
      }
      alert('Column mappings saved successfully');
    } catch (error) {
      console.error('Failed to save column mappings:', error);
      alert('Failed to save column mappings');
    } finally {
      setSaving(false);
    }
  };

  if (!isOpen) return null;

  // Group by column_origin (source/target)
  const sourceColumns = maps.filter(m => m.column_origin === 'source');
  const targetColumns = maps.filter(m => m.column_origin === 'target');

  const renderColumnGroup = (columnList: TableColumnMap[], origin: string, startIndex: number) => (
    <div className="flex-1">
      <h4 className="text-md font-semibold text-gray-900 dark:text-white mb-3 capitalize">
        {origin} Columns
      </h4>
      <div className="space-y-3">
        {columnList.map((col, localIndex) => {
          const globalIndex = startIndex + localIndex;
          return (
            <div key={`${origin}-${localIndex}`} className="border border-gray-200 dark:border-gray-700 rounded p-4">
              <div className="space-y-3">
                {/* Header Section */}
                <div className="flex justify-between items-start">
                  <div>
                    <p className="font-medium text-gray-900 dark:text-white">{col.column_name}</p>
                    <p className="text-sm text-gray-500 dark:text-gray-400">{col.data_type}</p>
                  </div>
                  <span className={`px-2 py-1 rounded text-xs ${
                    col.supported 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                      : 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
                  }`}>
                    {col.supported ? 'Supported' : 'Not Supported'}
                  </span>
                </div>

                {/* Column Details Grid */}
                <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-xs bg-gray-50 dark:bg-gray-900/30 p-3 rounded">
                  <div>
                    <span className="text-gray-600 dark:text-gray-400">Column ID: </span>
                    <span className="font-medium">{col.column_id}</span>
                  </div>
                  <div>
                    <span className="text-gray-600 dark:text-gray-400">Map Type: </span>
                    <span className="font-medium">{col.map_type}</span>
                  </div>
                  {col.data_class && (
                    <div>
                      <span className="text-gray-600 dark:text-gray-400">Data Class: </span>
                      <span className="font-medium">{col.data_class}</span>
                    </div>
                  )}
                  {col.data_length && (
                    <div>
                      <span className="text-gray-600 dark:text-gray-400">Data Length: </span>
                      <span className="font-medium">{col.data_length}</span>
                    </div>
                  )}
                  {col.data_class === 'numeric' && col.number_precision != null && (
                    <div>
                      <span className="text-gray-600 dark:text-gray-400">Precision: </span>
                      <span className="font-medium">{col.number_precision}</span>
                    </div>
                  )}
                  {col.data_class === 'numeric' && col.number_scale != null && (
                    <div>
                      <span className="text-gray-600 dark:text-gray-400">Scale: </span>
                      <span className="font-medium">{col.number_scale}</span>
                    </div>
                  )}
                </div>

                {/* Map Expression - Editable */}
                <div>
                  <label className="block text-xs text-gray-600 dark:text-gray-400 mb-1">
                    Map Expression:
                  </label>
                  <textarea
                    value={col.map_expression || ''}
                    onChange={(e) => handleMapExpressionChange(globalIndex, e.target.value)}
                    className="w-full px-2 py-1 bg-white dark:bg-gray-700 border border-gray-300 dark:border-gray-600 rounded text-xs font-mono"
                    rows={3}
                    placeholder="Enter SQL expression..."
                  />
                </div>

                {/* Column Flags */}
                <div className="flex flex-wrap gap-3 text-xs">
                  {col.column_nullable && (
                    <span className="text-gray-600 dark:text-gray-400">✓ Nullable</span>
                  )}
                  {col.column_primarykey && (
                    <span className="text-blue-600 dark:text-blue-400">🔑 Primary Key</span>
                  )}
                  {col.preserve_case && (
                    <span className="text-gray-600 dark:text-gray-400">✓ Preserve Case</span>
                  )}
                </div>
              </div>
            </div>
          );
        })}
        {columnList.length === 0 && (
          <p className="text-sm text-gray-500 dark:text-gray-400">No {origin} columns found</p>
        )}
      </div>
    </div>
  );

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-6xl w-full max-h-[80vh] overflow-y-auto m-4">
        <div className="sticky top-0 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 px-6 py-4 flex items-center justify-between z-10">
          <div>
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Column Details</h3>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              {column.column_alias}
            </p>
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={handleSave}
              disabled={saving}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
            >
              <Save className="h-4 w-4" />
              {saving ? 'Saving...' : 'Save'}
            </button>
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-gray-500 dark:hover:text-gray-300"
            >
              <X className="h-6 w-6" />
            </button>
          </div>
        </div>

        <div className="p-6">
          {loading ? (
            <div className="text-center py-8">Loading column mappings...</div>
          ) : (
            <div className="flex gap-6">
              {renderColumnGroup(sourceColumns, 'source', 0)}
              {renderColumnGroup(targetColumns, 'target', sourceColumns.length)}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

