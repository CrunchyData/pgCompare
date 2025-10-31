# Quick Start Guide

## Prerequisites
- Node.js 18 or higher
- npm (comes with Node.js)
- PostgreSQL database with pgCompare schema

## Installation

1. Navigate to the UI directory:
```bash
cd /Users/bpace/app/gitecto/pgCompare/ui
```

2. Dependencies are already installed. If needed, run:
```bash
npm install
```

## Running the Application

### Development Mode
```bash
npm run dev
```
The application will start at: http://localhost:3000

### Production Mode
```bash
npm run build
npm start
```

## First Time Usage

1. **Open Browser**: Navigate to http://localhost:3000

2. **Login Screen**: You'll see the database connection form

3. **Enter Connection Details**:
   - Host: `localhost` (or your PostgreSQL host)
   - Port: `5432` (default PostgreSQL port)
   - Database: Your pgCompare database name
   - User: Your PostgreSQL username
   - Password: Your PostgreSQL password

4. **Connect**: Click the "Connect" button

5. **Success**: If credentials are correct, you'll be redirected to the dashboard

## Using the Application

### Navigation
- **Left Sidebar**: Shows all projects
- **Expand Project**: Click the arrow icon to see tables
- **Select Item**: Click on project or table name to view details

### Project View
- **Edit Configuration**: Modify JSON config in table format
- **Add Row**: Click "+ Add Row" for new config entries
- **Save**: Click "Save" button to persist changes
- **View Results**: See charts showing comparison history

### Table View
- **Edit Settings**: Modify enabled, batch_nbr, parallel_degree
- **Table Mappings**: Edit source and target mappings separately
- **Column Mappings**: Click "Map" button on any column to edit column-specific mappings

### Theme Toggle
- Click the sun/moon icon in the top-right corner
- Theme preference is saved automatically

## Common Operations

### Edit Project Configuration
1. Select project from sidebar
2. Edit values in the table
3. Click "Save"

### Edit Table Settings
1. Select table from sidebar
2. Modify editable fields
3. Click "Save" in the table settings panel

### Edit Column Mappings
1. Select table from sidebar
2. Scroll to "Table Columns" section
3. Click "Map" button on desired column
4. Add/edit/delete mappings in modal
5. Click "Save" for each mapping

### View Comparison Results
1. Select project from sidebar
2. Scroll to "Last Run Summary" section
3. View charts showing historical trends

## Troubleshooting

### Cannot Connect to Database
- Verify PostgreSQL is running
- Check host, port, and credentials
- Ensure database exists and has pgCompare schema
- Check firewall settings

### No Projects Appear
- Verify `dc_project` table has data
- Check browser console for errors
- Verify database connection is active

### Theme Not Switching
- Clear browser cache and local storage
- Refresh the page
- Check browser console for errors

### Charts Not Displaying
- Ensure `dc_result` table has data for the project
- Verify data format matches expected schema
- Check browser console for errors

## File Structure
```
ui/
├── app/              # Next.js pages and API routes
├── components/       # React components
├── lib/             # Utilities and types
├── package.json     # Dependencies
└── README.md        # Full documentation
```

## Available Scripts
- `npm run dev` - Start development server (with hot reload)
- `npm run build` - Build for production
- `npm start` - Start production server
- `npm run lint` - Run ESLint

## Browser Support
- Chrome 90+
- Firefox 88+
- Safari 14+
- Edge 90+

## Data Persistence
- **Theme**: Saved in browser local storage
- **Credentials**: Last used (except password) saved in local storage
- **Database Changes**: Immediately saved to PostgreSQL

## Security Notes
- Password is never stored locally
- Database connection maintained in server memory
- Always use secure connections in production
- Consider using environment variables for sensitive data

## Next Steps
- Read full documentation in README.md
- Review APPLICATION_SUMMARY.md for technical details
- Customize theme in tailwind.config.ts if needed
- Add validation rules as needed for your use case

## Support
For issues or questions:
1. Check the logs in browser console
2. Review APPLICATION_SUMMARY.md for architecture details
3. Verify database schema matches expected structure
4. Check that all required tables exist and have proper permissions

