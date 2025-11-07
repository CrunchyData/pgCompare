'use client';

import { useEffect, useState } from 'react';
import { Save } from 'lucide-react';
import { Table } from '@/lib/types';
import TableMapPanel from './TableMapPanel';
import TableColumnPanel from './TableColumnPanel';
import TableResultsPanel from './TableResultsPanel';

interface TableViewProps {
  tableId: number;
}

export default function TableView({ tableId }: TableViewProps) {
  const [table, setTable] = useState<Table | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    loadTable();
  }, [tableId]);

  const loadTable = async () => {
    try {
      const response = await fetch(`/api/tables/${tableId}`);
      const data = await response.json();
      setTable(data);
    } catch (error) {
      console.error('Failed to load table:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleFieldChange = (field: keyof Table, value: any) => {
    if (table) {
      setTable({ ...table, [field]: value });
    }
  };

  const handleSave = async () => {
    if (!table) return;

    setSaving(true);
    try {
      await fetch(`/api/tables/${tableId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          enabled: table.enabled,
          batch_nbr: table.batch_nbr,
          parallel_degree: table.parallel_degree,
        }),
      });

      alert('Table settings saved successfully');
    } catch (error) {
      console.error('Failed to save table:', error);
      alert('Failed to save table settings');
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return <div className="p-4">Loading...</div>;
  }

  if (!table) {
    return <div className="p-4">Table not found</div>;
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-2xl font-bold text-gray-900 dark:text-white">{table.table_alias}</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400">Table ID: {table.tid}</p>
      </div>

      {/* Table Information Panel */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Table Settings</h3>
          <button
            onClick={handleSave}
            disabled={saving}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
          >
            <Save className="h-4 w-4" />
            {saving ? 'Saving...' : 'Save'}
          </button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Table ID (read-only)
            </label>
            <input
              type="text"
              value={table.tid}
              readOnly
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded bg-gray-50 dark:bg-gray-700 dark:text-white"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Table Alias (read-only)
            </label>
            <input
              type="text"
              value={table.table_alias}
              readOnly
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded bg-gray-50 dark:bg-gray-700 dark:text-white"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Enabled
            </label>
            <input
              type="checkbox"
              checked={table.enabled}
              onChange={(e) => handleFieldChange('enabled', e.target.checked)}
              className="w-5 h-5"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Batch Number
            </label>
            <input
              type="number"
              value={table.batch_nbr}
              onChange={(e) => handleFieldChange('batch_nbr', parseInt(e.target.value))}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded dark:bg-gray-700 dark:text-white"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Parallel Degree
            </label>
            <input
              type="number"
              value={table.parallel_degree}
              onChange={(e) => handleFieldChange('parallel_degree', parseInt(e.target.value))}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded dark:bg-gray-700 dark:text-white"
            />
          </div>
        </div>
      </div>

      {/* Table Map Panel */}
      <TableMapPanel tableId={tableId} />

      {/* Table Column Panel */}
      <TableColumnPanel tableId={tableId} />

      {/* Table Results Panel */}
      <TableResultsPanel tableId={tableId} />
    </div>
  );
}
