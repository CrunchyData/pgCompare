# pgCompare UI - Application Summary

## Overview
A modern Next.js-based web application for viewing and editing pgCompare configuration data with an intuitive interface, real-time data editing, and comprehensive visualization capabilities.

## Created Files Structure

### Core Application Files
```
ui/
├── app/
│   ├── layout.tsx                          # Root layout with theme provider
│   ├── page.tsx                            # Login page
│   ├── dashboard/
│   │   └── page.tsx                        # Main dashboard with navigation tree
│   └── api/                                # API routes for database operations
│       ├── auth/
│       │   └── login/route.ts              # Authentication endpoint
│       ├── projects/
│       │   ├── route.ts                    # List all projects
│       │   └── [id]/
│       │       ├── route.ts                # Get/update single project
│       │       ├── results/route.ts        # Get project results
│       │       └── tables/route.ts         # Get project tables
│       ├── tables/
│       │   └── [id]/
│       │       ├── route.ts                # Get/update table
│       │       ├── maps/route.ts           # Table mappings CRUD
│       │       └── columns/route.ts        # Table columns CRUD
│       └── columns/
│           └── [id]/
│               └── maps/route.ts           # Column mappings CRUD
│
├── components/
│   ├── ThemeProvider.tsx                   # Theme context provider
│   ├── ThemeToggle.tsx                     # Light/dark mode toggle button
│   ├── NavigationTree.tsx                  # Sidebar project/table navigation
│   ├── ProjectView.tsx                     # Project config editor & results
│   ├── TableView.tsx                       # Table settings editor
│   ├── TableMapPanel.tsx                   # Source/target table mappings
│   ├── TableColumnPanel.tsx                # Source/target columns display
│   └── ColumnMapModal.tsx                  # Modal for column map editing
│
├── lib/
│   ├── types.ts                            # TypeScript type definitions
│   ├── db.ts                               # PostgreSQL connection utilities
│   └── storage.ts                          # Local storage utilities
│
├── .gitignore                              # Git ignore file
├── tsconfig.json                           # TypeScript configuration
├── package.json                            # Dependencies and scripts
└── README.md                               # Documentation

```

## Key Features Implemented

### 1. Authentication
- **Login Page** (`app/page.tsx`)
  - PostgreSQL connection form
  - Credential validation
  - Auto-saves last used credentials (except password) to local storage
  - Error handling with user feedback

### 2. Navigation & Layout
- **Dashboard** (`app/dashboard/page.tsx`)
  - Split layout with sidebar and content area
  - Responsive design
  - Theme toggle in header

- **Navigation Tree** (`components/NavigationTree.tsx`)
  - Hierarchical project → tables structure
  - Expandable/collapsible projects
  - Dynamic table loading
  - Visual selection indicators

### 3. Project Management
- **Project View** (`components/ProjectView.tsx`)
  - Editable JSON configuration as key-value table
  - Add/remove configuration rows
  - Save changes to database
  - Last run summary with key metrics
  - **Charts & Visualizations:**
    - Bar chart: Matched vs Mismatched rows over time
    - Line chart: Duration trend analysis
  - Real-time data from `dc_result` table

### 4. Table Management
- **Table View** (`components/TableView.tsx`)
  - Editable fields: enabled, batch_nbr, parallel_degree
  - Read-only fields: tid, table_alias
  - Save functionality

- **Table Map Panel** (`components/TableMapPanel.tsx`)
  - Side-by-side source/target display
  - Editable mappings:
    - Map Type
    - Schema Name
    - Table Name
    - Where Clause
    - Column List
  - Individual save buttons per mapping

- **Table Column Panel** (`components/TableColumnPanel.tsx`)
  - Tabular source/target column display
  - Editable: column_name, data_type, is_nullable
  - "Map" button to open column mapping modal

### 5. Column Mapping
- **Column Map Modal** (`components/ColumnMapModal.tsx`)
  - Full-screen modal overlay
  - View/edit/delete column mappings
  - Add new attribute-value pairs
  - Save changes to `dc_table_column_map`

### 6. Theme System
- **Theme Provider** (`components/ThemeProvider.tsx`)
  - Context-based theme management
  - Persists preference to local storage
  - SSR-safe implementation

- **Theme Toggle** (`components/ThemeToggle.tsx`)
  - One-click light/dark mode switch
  - Animated icon transition
  - Positioned in header

### 7. API Layer
Complete RESTful API routes for all CRUD operations:
- Projects: GET (all), GET (single), PUT (update config)
- Results: GET (by project)
- Tables: GET (by project), GET (single), PUT (update)
- Table Maps: GET, POST, PUT
- Table Columns: GET, PUT
- Column Maps: GET, POST, PUT, DELETE

## Database Schema Expected

The application connects to these PostgreSQL tables:

1. **dc_project**
   - project_id (PK)
   - project_name
   - project_config (JSON)

2. **dc_result**
   - result_id (PK)
   - pid (FK → dc_project)
   - run_timestamp
   - status
   - total_tables, tables_compared, tables_matched, tables_mismatched
   - total_rows, matched_rows, mismatched_rows
   - duration_seconds

3. **dc_table**
   - tid (PK)
   - pid (FK → dc_project)
   - table_name, table_alias
   - enabled, batch_nbr, parallel_degree

4. **dc_table_map**
   - map_id (PK)
   - tid (FK → dc_table)
   - dest_type ('source' | 'target')
   - map_type, schema_name, table_name
   - where_clause, column_list

5. **dc_table_column**
   - column_id (PK)
   - tid (FK → dc_table)
   - dest_type ('source' | 'target')
   - column_name, ordinal_position, data_type
   - is_nullable, column_key

6. **dc_table_column_map**
   - map_id (PK)
   - column_id (FK → dc_table_column)
   - map_attribute, map_value

## Technology Stack

- **Framework**: Next.js 16 (App Router)
- **Language**: TypeScript 5
- **Styling**: Tailwind CSS 4
- **Database**: PostgreSQL (via node-postgres)
- **Charts**: Recharts 3.3
- **Icons**: Lucide React
- **React**: 19.2

## Getting Started

1. **Install dependencies:**
   ```bash
   cd ui
   npm install
   ```

2. **Run development server:**
   ```bash
   npm run dev
   ```
   Open http://localhost:3000

3. **Login:**
   - Enter PostgreSQL connection details
   - Credentials (except password) are saved locally

4. **Navigate:**
   - Select projects from sidebar
   - Expand projects to view tables
   - Click items to view/edit

5. **Build for production:**
   ```bash
   npm run build
   npm start
   ```

## Design Decisions

1. **Client-Side Database Connection**: Pool initialized on login, maintained for session
2. **Local Storage**: Theme and credentials (not password) persisted locally
3. **Split View**: Source/Target displayed side-by-side for easy comparison
4. **Modal for Column Maps**: Separate modal to avoid cluttering table view
5. **Individual Save Buttons**: Per-entity saves to give users control
6. **Real-time Charts**: Visualizations update based on latest run data
7. **Dark Mode First**: Both themes carefully designed for accessibility

## Security Considerations

- Passwords never stored in local storage
- Database credentials maintained in memory only
- API routes validate inputs
- Error messages don't expose sensitive data

## Future Enhancements (Not Implemented)

Potential improvements:
- Session management with JWT
- Batch operations for multiple tables
- Export/import configurations
- Advanced filtering and search
- Audit log for changes
- Validation rules for configurations
- Diff view for comparing changes
- WebSocket for real-time updates

## Browser Compatibility

Tested and supported:
- Chrome 90+
- Firefox 88+
- Safari 14+
- Edge 90+

## Notes

- Application requires PostgreSQL database with pgCompare schema
- All database operations use prepared statements (SQL injection protection)
- Theme preference persists across sessions
- Responsive design adapts to mobile/tablet/desktop

