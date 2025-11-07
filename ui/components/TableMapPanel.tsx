'use client';

import { useEffect, useState } from 'react';
import { TableMap, TableColumnMap } from '@/lib/types';
import { Save } from 'lucide-react';

interface TableMapPanelProps {
  tableId: number;
}

export default function TableMapPanel({ tableId }: TableMapPanelProps) {
  const [maps, setMaps] = useState<TableMap[]>([]);
  const [availableColumns, setAvailableColumns] = useState<TableColumnMap[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    loadMaps();
    loadAvailableColumns();
  }, [tableId]);

  const loadMaps = async () => {
    try {
      const response = await fetch(`/api/tables/${tableId}/maps`);
      const data = await response.json();
      setMaps(data);
    } catch (error) {
      console.error('Failed to load table maps:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadAvailableColumns = async () => {
    try {
      // Get all columns for this table to populate the dropdown
      const response = await fetch(`/api/tables/${tableId}/all-columns`);
      const data = await response.json();
      setAvailableColumns(data);
    } catch (error) {
      console.error('Failed to load available columns:', error);
    }
  };

  const handleFieldChange = (index: number, field: 'table_filter' | 'mod_column', value: string) => {
    const newMaps = [...maps];
    newMaps[index] = { ...newMaps[index], [field]: value || undefined };
    setMaps(newMaps);
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      // Save each map
      for (const map of maps) {
        await fetch(`/api/tables/${tableId}/maps`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            tid: map.tid,
            dest_type: map.dest_type,
            schema_name: map.schema_name,
            table_name: map.table_name,
            mod_column: map.mod_column,
            table_filter: map.table_filter,
          }),
        });
      }
      alert('Table mappings saved successfully');
    } catch (error) {
      console.error('Failed to save table mappings:', error);
      alert('Failed to save table mappings');
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return <div className="p-4">Loading table maps...</div>;
  }

  const sourceMaps = maps.filter(map => map.dest_type === 'source');
  const targetMaps = maps.filter(map => map.dest_type === 'target');

  const renderMapTable = (mapList: TableMap[], destType: string, startIndex: number) => (
    <div className="flex-1">
      <h4 className="text-md font-semibold text-gray-900 dark:text-white mb-3 capitalize">
        {destType}
      </h4>
      <div className="space-y-4">
        {mapList.map((map, localIndex) => {
          const globalIndex = startIndex + localIndex;
          // Get columns for this dest_type from availableColumns
          const columnsForDestType = availableColumns.filter(col => col.column_origin === destType);
          
          return (
            <div key={`${map.dest_type}-${localIndex}`} className="border border-gray-200 dark:border-gray-700 rounded p-4">
              <div className="space-y-3">
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Schema Name
                    </label>
                    <div className="px-3 py-2 bg-gray-50 dark:bg-gray-700 rounded text-sm">
                      {map.schema_name}
                    </div>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Table Name
                    </label>
                    <div className="px-3 py-2 bg-gray-50 dark:bg-gray-700 rounded text-sm">
                      {map.table_name}
                    </div>
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Parallel Hash Column
                  </label>
                  <input
                    type="text"
                    list={`columns-${destType}-${globalIndex}`}
                    value={map.mod_column || ''}
                    onChange={(e) => handleFieldChange(globalIndex, 'mod_column', e.target.value)}
                    className="w-full px-3 py-2 bg-white dark:bg-gray-700 border border-gray-300 dark:border-gray-600 rounded text-sm"
                    placeholder="Select or enter column name..."
                  />
                  <datalist id={`columns-${destType}-${globalIndex}`}>
                    {columnsForDestType.map((col, idx) => (
                      <option key={idx} value={col.column_name} />
                    ))}
                  </datalist>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Table Filter
                  </label>
                  <textarea
                    value={map.table_filter || ''}
                    onChange={(e) => handleFieldChange(globalIndex, 'table_filter', e.target.value)}
                    className="w-full px-3 py-2 bg-white dark:bg-gray-700 border border-gray-300 dark:border-gray-600 rounded text-sm font-mono"
                    rows={3}
                    placeholder="Enter SQL WHERE clause..."
                  />
                </div>

                <div className="flex gap-4 text-sm">
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={map.schema_preserve_case || false}
                      readOnly
                      className="w-4 h-4"
                    />
                    <span className="text-gray-700 dark:text-gray-300">Preserve Schema Case</span>
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={map.table_preserve_case || false}
                      readOnly
                      className="w-4 h-4"
                    />
                    <span className="text-gray-700 dark:text-gray-300">Preserve Table Case</span>
                  </label>
                </div>
              </div>
            </div>
          );
        })}
        {mapList.length === 0 && (
          <p className="text-sm text-gray-500 dark:text-gray-400">No {destType} mappings found</p>
        )}
      </div>
    </div>
  );

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Table Mappings</h3>
        <button
          onClick={handleSave}
          disabled={saving}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
        >
          <Save className="h-4 w-4" />
          {saving ? 'Saving...' : 'Save'}
        </button>
      </div>
      <div className="flex gap-6">
        {renderMapTable(sourceMaps, 'source', 0)}
        {renderMapTable(targetMaps, 'target', sourceMaps.length)}
      </div>
    </div>
  );
}

