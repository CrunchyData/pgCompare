# Release Notes - pgCompare v0.5.0

**Release Date:** November 2024

---

## 🎉 Major Features

### 1. **Snowflake Support**
- **NEW**: Full support for Snowflake as both source and target database
- Added `SnowflakeHelper` class for Snowflake-specific operations
- Implemented Snowflake SQL constants and query patterns
- Platform-specific metadata handling for Snowflake

### 2. **SQL Fix Generation**
- **NEW**: Automatic generation of SQL statements to fix data discrepancies
- Generate INSERT, UPDATE, and DELETE statements for out-of-sync rows
- Support for complex primary key scenarios with proper escaping

### 3. **Web UI (Preview)**
- **NEW**: Modern web-based user interface built with Next.js and React
- Real-time comparison monitoring and status updates
- Project and table management through intuitive UI
- Visual representation of comparison results
- Dark/Light theme support
- RESTful API backend for UI integration

---

## 🚀 Performance Improvements

### Multi-Threading Enhancements
- Optimized thread synchronization with `ThreadSync` improvements
- Better queue management for data loading threads
- Enhanced observer thread coordination
- Reduced thread contention and improved throughput

### Database Operations
- Improved batch processing with configurable batch sizes
- Optimized fetch sizes for large result sets
- Better connection pooling and resource management
- Reduced database round-trips through batching

### Memory Management
- More efficient CachedRowSet usage
- Improved cleanup of database resources
- Better handling of large data sets

---

## 🐛 Bug Fixes

### Data Comparison
- **Fixed**: Equal count calculation bug in comparison results
- **Fixed**: Primary key escaping for special characters and quotes
- **Fixed**: Handling of NULL values in comparisons
- **Fixed**: Case sensitivity issues in identifier handling

### Threading
- **Fixed**: Race conditions in thread synchronization
- **Fixed**: Proper cleanup of thread resources
- **Fixed**: Observer thread coordination issues

### Database Operations
- **Fixed**: Connection leaks in error scenarios
- **Fixed**: Transaction management in batch operations
- **Fixed**: Proper handling of platform-specific SQL syntax

---

## 📚 Documentation

### New Documentation
- **NEW**: `ARCHITECTURE.md` - Comprehensive architecture documentation (1,200+ lines)
  - Complete class hierarchy and method signatures
  - Detailed call flow diagrams
  - Component dependency graphs
  - Design pattern documentation
- **NEW**: `ui/APPLICATION_SUMMARY.md` - UI application overview
- **NEW**: `ui/QUICKSTART.md` - Quick start guide for UI
- **NEW**: `ui/README.md` - UI-specific documentation

### Updated Documentation
- **Enhanced**: `README.md` - Updated with v0.5.0 features and examples
- Improved inline code documentation
- Better Javadoc coverage

---

## 🗺️ Migration Guide

### For Users

#### Command Line Usage
Pass the action as the first arguement which replaces the previous version that used the `--action` arguement. 
```bash
# Discovery still works the same
java -jar pgcompare.jar discover --project 1

# Comparison still works the same
java -jar pgcompare.jar compare --project 1 --batch 1
```

#### New Web UI
To use the new web UI:
```bash
cd ui
npm install
npm run dev
```
Then navigate to `http://localhost:3000`

---

## 📝 Notes

### System Requirements
- Java 17 or higher
- For UI: Node.js 18+ and npm

### Supported Databases
- PostgreSQL (all versions)
- MySQL / MariaDB
- Microsoft SQL Server
- Oracle Database
- IBM DB2
- **NEW**: Snowflake

### Known Limitations
- UI is in preview/beta state
- Snowflake support is new and may have edge cases

---

## 🔗 Links

- [Architecture Documentation](ARCHITECTURE.md)
- [UI Quick Start](ui/QUICKSTART.md)
- [Main README](README.md)

---

For questions, issues, or contributions, please refer to the project repository.

