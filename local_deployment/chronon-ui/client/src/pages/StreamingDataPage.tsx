import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import {
    Alert,
    AlertDescription,
    AlertTitle,
} from "@/components/ui/alert";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Activity, CheckCircle2, XCircle, RefreshCw, Download, RotateCcw } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import "primereact/resources/themes/lara-dark-teal/theme.css";
import "primereact/resources/primereact.min.css";
import "primeicons/primeicons.css";

interface HealthResponse {
    status: string;
    kinesis_client: string;
    endpoint_url: string;
    region: string;
    error?: string;
}

interface StreamsResponse {
    streams: string[];
    stream_count: number;
}

interface ProduceRecordRequest {
    data: Record<string, unknown>;
    partition_key: string;
}

interface ProduceRequestPayload {
    records: ProduceRecordRequest[];
}

interface ProduceResponse {
    status: string;
    message: string;
    records_pushed?: number;
    error?: string;
}

interface ConsumedRecord {
    partition_key: string;
    sequence_number: string;
    approximate_arrival: string;
    data: Record<string, unknown>;
}

interface ReadRecordsResponse {
    stream_name: string;
    records: ConsumedRecord[];
    record_count: number;
}

interface RecreateStreamRequest {
    shard_count: number;
    enforce_consumer_deletion: boolean;
}

interface RecreateStreamResponse {
    status: string;
    message: string;
}

type IteratorType = "TRIM_HORIZON" | "LATEST";

const DEFAULT_RECORD_TEMPLATE = `[
  {
    "data": {
      "id": 1,
      "message": "Hello"
    },
    "partition_key": "key1"
  }
]`;

export default function StreamingDataPage() {
    const [selectedStream, setSelectedStream] = useState<string>("");
    const [recordsInput, setRecordsInput] = useState<string>(DEFAULT_RECORD_TEMPLATE);
    const [iteratorType, setIteratorType] = useState<IteratorType>("TRIM_HORIZON");
    const [limit, setLimit] = useState<string>("10");
    const { toast } = useToast();

    const {
        data: healthData,
        isLoading: isLoadingHealth,
        error: healthError,
        refetch: refetchHealth,
    } = useQuery<HealthResponse>({
        queryKey: ["kinesis-health"],
        queryFn: async () => {
            const res = await apiRequest("GET", "/v1/kinesis/health");
            return (await res.json()) as HealthResponse;
        },
        refetchInterval: 10000, // Refetch every 10 seconds
    });

    const {
        data: streamsData,
        isLoading: isLoadingStreams,
        error: streamsError,
        refetch: refetchStreams,
    } = useQuery<StreamsResponse>({
        queryKey: ["kinesis-streams"],
        queryFn: async () => {
            const res = await apiRequest("GET", "/v1/kinesis/streams");
            return (await res.json()) as StreamsResponse;
        },
        enabled: healthData?.status === "healthy",
    });

    const { data: produceResponse, isPending: isProducing, mutateAsync } = useMutation<
        ProduceResponse,
        Error,
        { stream: string; payload: ProduceRequestPayload }
    >({
        mutationFn: async ({ stream, payload }) => {
            const res = await apiRequest("POST", `/v1/kinesis/${stream}/records`, payload);
            return (await res.json()) as ProduceResponse;
        },
    });

    const { data: readResponse, isPending: isReading, mutateAsync: readRecords } = useMutation<
        ReadRecordsResponse,
        Error,
        { stream: string; iteratorType: IteratorType; limit: number }
    >({
        mutationFn: async ({ stream, iteratorType, limit }) => {
            const res = await apiRequest("GET", `/v1/kinesis/${stream}/records?iterator_type=${iteratorType}&limit=${limit}`);
            return (await res.json()) as ReadRecordsResponse;
        },
    });

    const { isPending: isRecreating, mutateAsync: recreateStream } = useMutation<
        RecreateStreamResponse,
        Error,
        { stream: string; payload: RecreateStreamRequest }
    >({
        mutationFn: async ({ stream, payload }) => {
            const res = await apiRequest("DELETE", `/v1/kinesis/${stream}/records`, payload);
            return (await res.json()) as RecreateStreamResponse;
        },
    });

    const handleStreamSelect = (value: string) => {
        setSelectedStream(value);
    };

    const handleProduce = async () => {
        if (!selectedStream) {
            toast({
                title: "No stream selected",
                description: "Please select a stream first.",
                variant: "destructive",
            });
            return;
        }

        let parsed: unknown;
        try {
            parsed = JSON.parse(recordsInput);
        } catch (err) {
            toast({
                title: "Invalid JSON",
                description: "Please provide valid JSON for the records.",
                variant: "destructive",
            });
            return;
        }

        if (!Array.isArray(parsed)) {
            toast({
                title: "Invalid structure",
                description: "Records must be provided as a JSON array.",
                variant: "destructive",
            });
            return;
        }

        const records = parsed as unknown[];
        const validatedRecords: ProduceRecordRequest[] = [];
        const errors: string[] = [];

        records.forEach((record, idx) => {
            if (typeof record !== "object" || record === null) {
                errors.push(`Record at index ${idx} is not an object.`);
                return;
            }

            const rec = record as Record<string, unknown>;
            if (!rec.data || typeof rec.data !== "object") {
                errors.push(`Record at index ${idx} is missing valid 'data' field.`);
                return;
            }

            const partitionKey = rec.partition_key 
                ? String(rec.partition_key).trim() 
                : `default-key-${idx}`;

            validatedRecords.push({
                data: rec.data as Record<string, unknown>,
                partition_key: partitionKey,
            });
        });

        if (errors.length > 0) {
            toast({
                title: "Validation errors",
                description: errors.join(" "),
                variant: "destructive",
            });
            return;
        }

        if (validatedRecords.length === 0) {
            toast({
                title: "No records to send",
                description: "Please provide at least one valid record.",
                variant: "destructive",
            });
            return;
        }

        try {
            const result = await mutateAsync({
                stream: selectedStream,
                payload: { records: validatedRecords },
            });

            toast({
                title: "Records produced successfully",
                description: result.message,
            });
        } catch (err) {
            const message = err instanceof Error ? err.message : "Unknown error";
            toast({
                title: "Failed to produce records",
                description: message,
                variant: "destructive",
            });
        }
    };

    const handleReadRecords = async () => {
        if (!selectedStream) {
            toast({
                title: "No stream selected",
                description: "Please select a stream first.",
                variant: "destructive",
            });
            return;
        }

        const limitNum = parseInt(limit, 10);
        if (isNaN(limitNum) || limitNum < 1 || limitNum > 10000) {
            toast({
                title: "Invalid limit",
                description: "Limit must be a number between 1 and 10000.",
                variant: "destructive",
            });
            return;
        }

        try {
            const result = await readRecords({
                stream: selectedStream,
                iteratorType,
                limit: limitNum,
            });

            toast({
                title: "Records fetched successfully",
                description: `Retrieved ${result.record_count} records from ${selectedStream}`,
            });
        } catch (err) {
            const message = err instanceof Error ? err.message : "Unknown error";
            toast({
                title: "Failed to read records",
                description: message,
                variant: "destructive",
            });
        }
    };

    const handleRecreateStream = async () => {
        if (!selectedStream) {
            toast({
                title: "No stream selected",
                description: "Please select a stream first.",
                variant: "destructive",
            });
            return;
        }

        try {
            const result = await recreateStream({
                stream: selectedStream,
                payload: {
                    shard_count: 1,
                    enforce_consumer_deletion: false,
                },
            });

            toast({
                title: "Stream recreated successfully",
                description: result.message,
            });
        } catch (err) {
            const message = err instanceof Error ? err.message : "Unknown error";
            toast({
                title: "Failed to recreate stream",
                description: message,
                variant: "destructive",
            });
        }
    };

    const isHealthy = healthData?.status === "healthy";

    const renderHealthStatus = () => {
        if (isLoadingHealth) {
            return (
                <Alert>
                    <Activity className="h-4 w-4 animate-spin" />
                    <AlertTitle>Checking health...</AlertTitle>
                </Alert>
            );
        }

        if (healthError) {
            return (
                <Alert variant="destructive">
                    <XCircle className="h-4 w-4" />
                    <AlertTitle>Health check failed</AlertTitle>
                    <AlertDescription>
                        {healthError instanceof Error ? healthError.message : "Unknown error"}
                    </AlertDescription>
                </Alert>
            );
        }

        if (healthData?.status === "unhealthy") {
            return (
                <Alert variant="destructive">
                    <XCircle className="h-4 w-4" />
                    <AlertTitle>Streaming backend is unhealthy</AlertTitle>
                    <AlertDescription>
                        {healthData.error || "The Kinesis client is not initialized properly."}
                    </AlertDescription>
                </Alert>
            );
        }

        if (isHealthy) {
            return (
                <Alert className="border-green-500 bg-green-500/10">
                    <CheckCircle2 className="h-4 w-4 text-green-500" />
                    <AlertTitle className="text-green-500">Streaming backend is healthy</AlertTitle>
                    <AlertDescription className="text-sm text-muted-foreground mt-1">
                        <div>Endpoint: <strong>{healthData.endpoint_url}</strong></div>
                        <div>Region: <strong>{healthData.region}</strong></div>
                        <div>Client: <strong>{healthData.kinesis_client}</strong></div>
                    </AlertDescription>
                </Alert>
            );
        }

        return null;
    };

    return (
        <div className="h-full w-full overflow-y-auto">
            <div className="max-w-[1200px] mx-auto p-6 flex flex-col gap-6">
                <Card>
                    <CardHeader>
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-3">
                                <div className="p-2 rounded-md bg-purple-500/20">
                                    <Activity className="h-6 w-6 text-purple-500" />
                                </div>
                                <div>
                                    <CardTitle>Streaming Data Explorer</CardTitle>
                                    <CardDescription>
                                        Interact with local Kinesis streams for real-time data processing.
                                    </CardDescription>
                                </div>
                            </div>
                            <Button 
                                variant="outline" 
                                size="sm" 
                                onClick={() => {
                                    refetchHealth();
                                    refetchStreams();
                                }}
                            >
                                <RefreshCw className="mr-2 h-4 w-4" /> Refresh
                            </Button>
                        </div>
                    </CardHeader>
                    <CardContent className="flex flex-col gap-6">
                        {/* Health Status */}
                        <div className="flex flex-col gap-2">
                            <span className="text-sm font-medium text-muted-foreground">
                                Backend Status
                            </span>
                            {renderHealthStatus()}
                        </div>

                        {/* Stream Selection - only show if healthy */}
                        {isHealthy && (
                            <>
                                <div className="flex flex-col gap-3">
                                    <label className="text-sm font-medium">Stream</label>
                                    <Select
                                        value={selectedStream}
                                        onValueChange={handleStreamSelect}
                                        disabled={isLoadingStreams}
                                    >
                                        <SelectTrigger>
                                            <SelectValue
                                                placeholder={
                                                    isLoadingStreams
                                                        ? "Loading streams..."
                                                        : streamsData?.streams.length
                                                            ? "Select a stream"
                                                            : "No streams found"
                                                }
                                            />
                                        </SelectTrigger>
                                        <SelectContent>
                                            {streamsData?.streams.map((stream) => (
                                                <SelectItem key={stream} value={stream}>
                                                    {stream}
                                                </SelectItem>
                                            ))}
                                        </SelectContent>
                                    </Select>

                                    {streamsError && (
                                        <Alert variant="destructive">
                                            <AlertTitle>Failed to load streams</AlertTitle>
                                            <AlertDescription>
                                                {streamsError instanceof Error ? streamsError.message : "Unknown error"}
                                            </AlertDescription>
                                        </Alert>
                                    )}

                                    {streamsData && streamsData.streams.length === 0 && (
                                        <Alert>
                                            <AlertTitle>No streams available</AlertTitle>
                                            <AlertDescription>
                                                No Kinesis streams found in the current region.
                                            </AlertDescription>
                                        </Alert>
                                    )}
                                </div>

                                {/* Produce Messages - only show if stream is selected */}
                                {selectedStream && (
                                    <>
                                        <div className="flex flex-col gap-2 rounded-md border p-4 bg-muted/20">
                                            <div className="flex items-center gap-2 mb-2">
                                                <Badge variant="outline" className="text-sm">
                                                    {selectedStream}
                                                </Badge>
                                            </div>

                                            <div className="flex flex-col gap-2">
                                                <label className="text-sm font-medium">
                                                    Records to Produce (JSON Array)
                                                </label>
                                                <Textarea
                                                    value={recordsInput}
                                                    onChange={(event) => setRecordsInput(event.target.value)}
                                                    rows={8}
                                                    className="font-mono text-xs"
                                                    placeholder={DEFAULT_RECORD_TEMPLATE}
                                                />
                                                <span className="text-xs text-muted-foreground">
                                                    Provide an array of records. Each record should have a 'data' object
                                                    and optionally a 'partition_key' string.
                                                </span>
                                            </div>

                                            <Button
                                                className="mt-2 self-end"
                                                onClick={handleProduce}
                                                disabled={isProducing}
                                            >
                                                {isProducing ? "Producing..." : "Produce Messages"}
                                            </Button>

                                            {/* Produce Response - inline */}
                                            {produceResponse && (
                                                <Alert 
                                                    variant={produceResponse.status === "success" ? "default" : "destructive"}
                                                    className={produceResponse.status === "success" ? "border-green-500 bg-green-500/10 mt-4" : "mt-4"}
                                                >
                                                    {produceResponse.status === "success" ? (
                                                        <CheckCircle2 className="h-4 w-4 text-green-500" />
                                                    ) : (
                                                        <XCircle className="h-4 w-4" />
                                                    )}
                                                    <AlertTitle>
                                                        {produceResponse.status === "success" ? "Success" : "Error"}
                                                    </AlertTitle>
                                                    <AlertDescription>
                                                        <div>{produceResponse.message}</div>
                                                        {produceResponse.records_pushed !== undefined && (
                                                            <div className="mt-1">
                                                                Records pushed: <strong>{produceResponse.records_pushed}</strong>
                                                            </div>
                                                        )}
                                                        {produceResponse.error && (
                                                            <div className="mt-1 text-destructive">
                                                                Error: {produceResponse.error}
                                                            </div>
                                                        )}
                                                    </AlertDescription>
                                                </Alert>
                                            )}
                                        </div>
                                    </>
                                )}
                            </>
                        )}
                    </CardContent>
                </Card>

                {/* Stream Records Card - Always visible when stream is selected */}
                {isHealthy && selectedStream && (
                    <Card>
                        <CardHeader>
                            <div className="flex items-center justify-between">
                                <div className="flex items-center gap-3">
                                    <div>
                                        <CardTitle className="text-lg">Stream Records</CardTitle>
                                        <CardDescription>
                                            {readResponse
                                                ? `Showing ${readResponse.record_count} record${readResponse.record_count !== 1 ? 's' : ''} from ${readResponse.stream_name}`
                                                : `Read messages from ${selectedStream}`
                                            }
                                        </CardDescription>
                                    </div>
                                </div>
                                <Button
                                    variant="outline"
                                    size="sm"
                                    onClick={handleRecreateStream}
                                    disabled={isRecreating}
                                    className="gap-2 bg-orange-500 hover:bg-orange-600 text-white border-orange-500 hover:border-orange-600"
                                >
                                    <RotateCcw className="h-4 w-4" />
                                    {isRecreating ? "Recreating..." : "Recreate Stream"}
                                </Button>
                            </div>
                        </CardHeader>
                        <CardContent className="flex flex-col gap-4">
                            {/* Read Controls */}
                            <div className="flex gap-4 items-end rounded-md border p-4 bg-muted/20">
                                <div className="flex-1">
                                    <label className="text-sm font-medium block mb-2">
                                        Iterator Type
                                    </label>
                                    <Select
                                        value={iteratorType}
                                        onValueChange={(value) => setIteratorType(value as IteratorType)}
                                    >
                                        <SelectTrigger>
                                            <SelectValue />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="TRIM_HORIZON">
                                                TRIM_HORIZON (oldest)
                                            </SelectItem>
                                            <SelectItem value="LATEST">
                                                LATEST (newest)
                                            </SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>

                                <div className="flex-1">
                                    <label className="text-sm font-medium block mb-2">
                                        Limit
                                    </label>
                                    <Input
                                        type="number"
                                        value={limit}
                                        onChange={(e) => setLimit(e.target.value)}
                                        min={1}
                                        max={10000}
                                        placeholder="10"
                                    />
                                </div>

                                <Button
                                    onClick={handleReadRecords}
                                    disabled={isReading}
                                    className="gap-2"
                                >
                                    <Download className="h-4 w-4" />
                                    {isReading ? "Reading..." : "Read Records"}
                                </Button>
                            </div>

                            {/* Records Table */}
                            {readResponse && readResponse.records.length > 0 && (
                                <div className="w-full overflow-x-auto border rounded-md">
                                    <DataTable
                                        value={readResponse.records}
                                        scrollable
                                        scrollHeight="500px"
                                        className="min-w-full bg-transparent text-foreground custom-datatable"
                                        tableStyle={{ minWidth: "max-content" }}
                                    >
                                        <Column
                                            field="partition_key"
                                            header="Partition Key"
                                            style={{ minWidth: "12rem" }}
                                            headerClassName="h-auto py-3"
                                            bodyClassName="font-mono text-xs"
                                        />
                                        <Column
                                            field="sequence_number"
                                            header="Sequence Number"
                                            style={{ minWidth: "20rem" }}
                                            headerClassName="h-auto py-3"
                                            bodyClassName="font-mono text-xs"
                                            body={(row: ConsumedRecord) => (
                                                <span className="text-xs">{row.sequence_number}</span>
                                            )}
                                        />
                                        <Column
                                            field="approximate_arrival"
                                            header="Arrival Time"
                                            style={{ minWidth: "12rem" }}
                                            headerClassName="h-auto py-3"
                                            bodyClassName="font-mono text-xs"
                                            body={(row: ConsumedRecord) => (
                                                <span>{new Date(row.approximate_arrival).toLocaleString()}</span>
                                            )}
                                        />
                                        <Column
                                            field="data"
                                            header="Data"
                                            style={{ minWidth: "20rem" }}
                                            headerClassName="h-auto py-3"
                                            bodyClassName="font-mono text-xs"
                                            body={(row: ConsumedRecord) => {
                                                const data = row.data;
                                                if (data === null || data === undefined) {
                                                    return <span className="text-muted-foreground italic">null</span>;
                                                }
                                                if (typeof data === "object") {
                                                    return <pre className="whitespace-pre-wrap">{JSON.stringify(data, null, 2)}</pre>;
                                                }
                                                return String(data);
                                            }}
                                        />
                                    </DataTable>
                                    {/* Localized styling for light mode table */}
                                    <style>{`
                                        .custom-datatable .p-datatable-wrapper { background: white; }
                                        .custom-datatable .p-datatable-table { border-collapse: separate; border-spacing: 0; }
                                        .custom-datatable .p-datatable-thead > tr > th {
                                          background-color: #e5e7eb;
                                          color: #4b5563;
                                          border-bottom: 1px solid #d1d5db;
                                          border-right: 1px solid #d1d5db;
                                          position: sticky; top: 0; z-index: 1;
                                        }
                                        .custom-datatable .p-datatable-thead > tr > th:last-child { border-right: none; }
                                        .custom-datatable .p-datatable-tbody > tr > td {
                                          border-top: 1px solid #e5e7eb;
                                          border-right: 1px solid #e5e7eb;
                                          color: #000000;
                                          background-color: #ffffff;
                                          padding: 0.375rem 0.75rem;
                                          line-height: 1.75;
                                        }
                                        .custom-datatable .p-datatable-tbody > tr > td:last-child { border-right: none; }
                                        .custom-datatable .p-datatable-tbody > tr:nth-child(even) > td { background-color: #f9fafb; }
                                        .custom-datatable .p-datatable-tbody > tr:hover > td { background-color: #eff6ff; }
                                    `}</style>
                                </div>
                            )}

                            {readResponse && readResponse.records.length === 0 && (
                                <Alert>
                                    <AlertDescription>
                                        No records found in the stream
                                    </AlertDescription>
                                </Alert>
                            )}

                            {!readResponse && (
                                <Alert>
                                    <AlertDescription>
                                        Configure the options above and click "Read Records" to fetch messages from the stream.
                                    </AlertDescription>
                                </Alert>
                            )}
                        </CardContent>
                    </Card>
                )}
            </div>
        </div>
    );
}

