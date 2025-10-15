import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ScrollArea, ScrollBar } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Database, Table as TableIcon, ChevronLeft, ChevronRight, Search } from "lucide-react";

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

export default function BatchDataPage() {
  const [selectedDatabase, setSelectedDatabase] = useState<string>("");
  const [selectedTable, setSelectedTable] = useState<string>("");
  const [page, setPage] = useState(0);
  const pageSize = 10;

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
    <div className="h-full w-full p-6 flex flex-col gap-6 overflow-x-hidden min-w-0">
      {/* Header */}
      <Card>
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-md bg-node-batch/20">
              <Database className="h-6 w-6 text-node-batch" />
            </div>
            <div>
              <CardTitle>Batch Data Explorer</CardTitle>
              <CardDescription>
                Browse and sample data from your databases and tables
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {/* Database and Table Selection */}
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
        <Card className="flex-1 flex flex-col min-h-0">
          <CardHeader className="flex-none">
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
          <CardContent className="flex-1 min-h-0 overflow-y-auto overflow-x-hidden">
            <div className="border rounded-md w-full min-w-0 max-w-full">
              <ScrollArea className="w-full max-w-full">
                <div className="min-w-max">
                  <Table className="w-full">
                <TableHeader className="bg-muted/80">
                  <TableRow className="hover:bg-muted/50">
                    {sampleData.table_schema.map((col) => (
                      <TableHead key={col.name} className="font-semibold h-auto py-3">
                        <div className="flex flex-col gap-1.5">
                          <span className="text-foreground text-sm">{col.name}</span>
                          <Badge variant="secondary" className="w-fit text-xs font-normal">
                            {col.type}
                          </Badge>
                        </div>
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {sampleData.data.map((row, idx) => (
                    <TableRow key={idx}>
                      {sampleData.table_schema.map((col) => (
                        <TableCell key={col.name} className="font-mono text-xs">
                          {row[col.name] !== null && row[col.name] !== undefined
                            ? String(row[col.name])
                            : <span className="text-muted-foreground italic">null</span>
                          }
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
                  </Table>
                </div>
                <ScrollBar orientation="horizontal" />
              </ScrollArea>
            </div>
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
  );
}
