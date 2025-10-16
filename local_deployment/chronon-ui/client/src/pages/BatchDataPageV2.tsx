import { useState, useMemo, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Database, Table as TableIcon, ChevronLeft, ChevronRight, Search } from "lucide-react";

// Import PrimeReact CSS directly
import "primereact/resources/themes/lara-dark-teal/theme.css";
import "primereact/resources/primereact.min.css";
import "primeicons/primeicons.css";

interface DatabasesResponse {
  databases: string[];
}

interface TablesResponse {
  database: string;
  tables: string[];
}

interface TableSchema {
  name: string;
  type: string;
}

interface SampleDataResponse {
  database: string;
  table: string;
  data: Record<string, any>[];
  table_schema: TableSchema[];
  row_count: number;
  limit: number;
  offset: number;
}

export default function BatchDataPageV2() {
  const [selectedDatabase, setSelectedDatabase] = useState<string>("");
  const [selectedTable, setSelectedTable] = useState<string>("");
  const [page, setPage] = useState(0);
  const pageSize = 10;

  // Initialize selection from URL params (?db=...&table=...)
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const db = params.get("db") || "";
    const table = params.get("table") || "";
    if (db) setSelectedDatabase(db);
    if (table) setSelectedTable(table);
    setPage(0);
  }, []);

  // Fetch databases
  const { data: databasesData, isLoading: isLoadingDatabases } = useQuery<DatabasesResponse>({
    queryKey: ["/v1/spark-data/databases"],
  });

  // Fetch tables for selected database
  const { data: tablesData, isLoading: isLoadingTables } = useQuery<TablesResponse>({
    queryKey: [`/v1/spark-data/databases/${selectedDatabase}/tables`],
    enabled: !!selectedDatabase,
  });

  // Fetch sample data
  const { 
    data: sampleData, 
    isLoading: isLoadingSample,
    error: sampleError 
  } = useQuery<SampleDataResponse>({
    queryKey: [
      `/v1/spark-data/databases/${selectedDatabase}/tables/${selectedTable}/sample?limit=${pageSize}&offset=${page * pageSize}`
    ],
    enabled: !!selectedDatabase && !!selectedTable,
  });

  // Reset table selection when database changes
  const handleDatabaseChange = (database: string) => {
    setSelectedDatabase(database);
    setSelectedTable("");
    setPage(0);
  };

  // Reset page when table changes
  const handleTableChange = (table: string) => {
    setSelectedTable(table);
    setPage(0);
  };

  const totalPages = useMemo(() => {
    if (!sampleData) return 0;
    return Math.ceil(sampleData.row_count / pageSize);
  }, [sampleData]);

  const canGoNext = page < totalPages - 1;
  const canGoPrev = page > 0;

  return (
    <div className="h-full w-full overflow-y-auto">
      <div className="max-w-[1400px] mx-auto p-6 flex flex-col gap-6">
        {/* Header */}
        <Card>
          <CardHeader>
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-md bg-node-batch/20">
                <Database className="h-6 w-6 text-node-batch" />
              </div>
              <div>
                <CardTitle>Batch Data Explorer V2</CardTitle>
                <CardDescription>Browse databases and tables with improved layout</CardDescription>
              </div>
            </div>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            {/* Database and Table Selection - Fixed width container */}
            <div className="flex gap-4 items-end">
              <div className="flex-1">
                <label className="text-sm font-medium mb-2 block">Database</label>
                <Select 
                  value={selectedDatabase} 
                  onValueChange={handleDatabaseChange}
                  disabled={isLoadingDatabases}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={isLoadingDatabases ? "Loading..." : "Select a database"} />
                  </SelectTrigger>
                  <SelectContent>
                    {databasesData?.databases.map((db) => (
                      <SelectItem key={db} value={db}>
                        {db}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="flex-1">
                <label className="text-sm font-medium mb-2 block">Table</label>
                <Select 
                  value={selectedTable} 
                  onValueChange={handleTableChange}
                  disabled={!selectedDatabase || isLoadingTables}
                >
                  <SelectTrigger>
                    <SelectValue 
                      placeholder={
                        !selectedDatabase 
                          ? "Select a database first" 
                          : isLoadingTables 
                          ? "Loading..." 
                          : "Select a table"
                      } 
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {tablesData?.tables.map((table) => (
                      <SelectItem key={table} value={table}>
                        {table}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <Button 
                onClick={() => setPage(0)}
                disabled={!selectedTable || isLoadingSample}
                className="gap-2"
              >
                <Search className="h-4 w-4" />
                Sample Data
              </Button>
            </div>

            {/* Table Info */}
            {selectedDatabase && selectedTable && sampleData && (
              <div className="flex gap-2 items-center text-sm text-muted-foreground">
                <TableIcon className="h-4 w-4" />
                <span>
                  <strong>{selectedDatabase}.{selectedTable}</strong> 
                  {" · "}
                  {sampleData.row_count.toLocaleString()} total rows
                </span>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Data Display */}
        {sampleError && (
          <Alert variant="destructive">
            <AlertDescription>
              Failed to load sample data: {sampleError instanceof Error ? sampleError.message : "Unknown error"}
            </AlertDescription>
          </Alert>
        )}

        {isLoadingSample && selectedTable && (
          <Card>
            <CardContent className="p-6 text-center text-muted-foreground">
              Loading sample data...
            </CardContent>
          </Card>
        )}

        {sampleData && sampleData.data.length > 0 && (
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="text-lg">Sample Data</CardTitle>
                  <CardDescription>
                    Showing rows {page * pageSize + 1} - {Math.min((page + 1) * pageSize, sampleData.row_count)} of {sampleData.row_count.toLocaleString()}
                  </CardDescription>
                </div>
                <div className="flex gap-2 items-center">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setPage(p => p - 1)}
                    disabled={!canGoPrev}
                  >
                    <ChevronLeft className="h-4 w-4" />
                  </Button>
                  <span className="text-sm text-muted-foreground min-w-[80px] text-center">
                    Page {page + 1} of {totalPages}
                  </span>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setPage(p => p + 1)}
                    disabled={!canGoNext}
                  >
                    <ChevronRight className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              {/* Table container with fixed width and horizontal scroll */}
              <div className="w-full overflow-x-auto border rounded-md">
                <DataTable
                  value={sampleData.data}
                  scrollable
                  scrollHeight="500px"
                  className="min-w-full bg-transparent text-foreground custom-datatable"
                  tableStyle={{ minWidth: "max-content" }}
                >
                  {sampleData.table_schema.map((col) => (
                    <Column
                      key={col.name}
                      field={col.name}
                      header={
                        <div className="flex flex-col gap-1.5">
                          <span className="text-sm font-medium text-white">{col.name}</span>
                          <Badge variant="outline" className="w-fit text-[10px] font-normal">{col.type}</Badge>
                        </div>
                      }
                      body={(row: Record<string, any>) => (
                        row[col.name] !== null && row[col.name] !== undefined
                          ? String(row[col.name])
                          : <span className="text-muted-foreground italic">null</span>
                      )}
                      style={{ minWidth: "15rem" }}
                      headerClassName="h-auto py-3 bg-zinc-900 text-zinc-50 border-zinc-800"
                      bodyClassName="font-mono text-xs text-zinc-100"
                    />
                  ))}
                </DataTable>
              </div>
              {/* Localized styling to improve contrast/visibility */}
              <style>{`
                .custom-datatable .p-datatable-wrapper { background: transparent; }
                .custom-datatable .p-datatable-table { border-collapse: separate; border-spacing: 0; }
                .custom-datatable .p-datatable-thead > tr > th {
                  background-color: #0a0a0a; /* near-black for strong contrast */
                  color: #f5f5f5; /* bright header text */
                  border-bottom: 1px solid #27272a;
                  border-right: 1px solid #27272a;
                  position: sticky; top: 0; z-index: 1;
                }
                .custom-datatable .p-datatable-thead > tr > th:last-child { border-right: none; }
                .custom-datatable .p-datatable-tbody > tr > td {
                  border-top: 1px solid #27272a;
                  border-right: 1px solid #27272a;
                  color: #e5e7eb; /* zinc-200 */
                }
                .custom-datatable .p-datatable-tbody > tr > td:last-child { border-right: none; }
                .custom-datatable .p-datatable-tbody > tr:nth-child(even) > td { background-color: rgba(255,255,255,0.03); }
                .custom-datatable .p-datatable-tbody > tr:hover > td { background-color: rgba(59,130,246,0.08); }
              `}</style>
            </CardContent>
          </Card>
        )}

        {sampleData && sampleData.data.length === 0 && (
          <Card>
            <CardContent className="p-6 text-center text-muted-foreground">
              No data found in this table
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}

