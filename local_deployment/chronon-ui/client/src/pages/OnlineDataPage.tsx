import { useEffect, useMemo, useRef, useState } from "react";
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
    ToggleGroup,
    ToggleGroupItem,
} from "@/components/ui/toggle-group";
import {
    Alert,
    AlertDescription,
    AlertTitle,
} from "@/components/ui/alert";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import "primereact/resources/themes/lara-dark-teal/theme.css";
import "primereact/resources/primereact.min.css";
import "primeicons/primeicons.css";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Database, Globe, RefreshCw } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { apiRequest, fetcherRequest } from "@/lib/queryClient";

interface OnlineConf {
    name: string;
    conf_type: "group_by" | "join";
    primary_keys: string[];
}

interface FetcherRequestPayload {
    name: string;
    keys: Record<string, string>;
}

interface FetcherResponse {
    name: string;
    keys: Record<string, unknown>;
    values?: Record<string, unknown> | null;
    message: string;
}

type OnlineDataKind = "group_by" | "join";

const KEY_STORAGE_KEY = "online-data-page:lastKeys";

function loadStoredKeys(kind: OnlineDataKind, dataset: string | undefined, primaryKeys: string[]) {
    try {
        const raw = localStorage.getItem(KEY_STORAGE_KEY);
        if (!raw) return Object.fromEntries(primaryKeys.map((k) => [k, ""]));
        const parsed = JSON.parse(raw) as Record<string, Record<string, string>>;
        const datasetKey = `${kind}:${dataset ?? ""}`;
        const stored = parsed[datasetKey] ?? {};
        return Object.fromEntries(primaryKeys.map((key) => [key, stored[key] ?? ""]));
    } catch (err) {
        console.warn("Failed to load stored keys", err);
        return Object.fromEntries(primaryKeys.map((k) => [k, ""]));
    }
}

function persistStoredKeys(kind: OnlineDataKind, dataset: string, keys: Record<string, string>) {
    try {
        const raw = localStorage.getItem(KEY_STORAGE_KEY);
        const parsed = raw ? (JSON.parse(raw) as Record<string, Record<string, string>>) : {};
        parsed[`${kind}:${dataset}`] = keys;
        localStorage.setItem(KEY_STORAGE_KEY, JSON.stringify(parsed));
    } catch (err) {
        console.warn("Failed to persist keys", err);
    }
}

export default function OnlineDataPage() {
    const [dataKind, setDataKind] = useState<OnlineDataKind>("group_by");
    const [selectedDataset, setSelectedDataset] = useState<string>("");
    const [requestPayload, setRequestPayload] = useState<FetcherRequestPayload | null>(null);
    const [keysInput, setKeysInput] = useState<string>("{}");
    const { toast } = useToast();
    const skipNextReset = useRef(false);

    useEffect(() => {
        if (skipNextReset.current) {
            skipNextReset.current = false;
            return;
        }

        setSelectedDataset("");
        setKeysInput("{}");
    }, [dataKind]);

    const {
        data: datasets,
        isLoading: isLoadingConfigs,
        error: listError,
        refetch: refetchConfigs,
    } = useQuery<OnlineConf[]>({
        queryKey: ["online-confs", dataKind],
        queryFn: async () => {
            const res = await apiRequest("GET", `/v1/graph/list_confs?conf_type=${dataKind}`);
            return (await res.json()) as OnlineConf[];
        },
    });

    const selectedConfig = useMemo(
        () => datasets?.find((conf) => conf.name === selectedDataset) ?? null,
        [datasets, selectedDataset],
    );

    useEffect(() => {
        const params = new URLSearchParams(window.location.search);
        const typeParam = params.get("type");
        const datasetParam = params.get("dataset");

        if (typeParam === "group_by" || typeParam === "join") {
            if (typeParam !== dataKind) {
                skipNextReset.current = true;
                setDataKind(typeParam);
            }
        }

        if (datasetParam) {
            setSelectedDataset(datasetParam);
        }
    }, []);

    useEffect(() => {
        const params = new URLSearchParams(window.location.search);
        params.set("type", dataKind);

        if (selectedDataset) {
            params.set("dataset", selectedDataset);
        } else {
            params.delete("dataset");
        }

        const newRelativePathQuery = `${window.location.pathname}?${params.toString()}`;
        window.history.replaceState(null, "", newRelativePathQuery);
    }, [dataKind, selectedDataset]);

    useEffect(() => {
        if (!selectedConfig) {
            setKeysInput("{}");
            return;
        }

        const storedKeys = loadStoredKeys(dataKind, selectedConfig.name, selectedConfig.primary_keys);
        const templateEntries = selectedConfig.primary_keys.map((key) => [
            key,
            storedKeys[key] && storedKeys[key].trim().length > 0
                ? storedKeys[key]
                : "ENTER_VALUE_HERE",
        ]);
        const template = Object.fromEntries(templateEntries);
        setKeysInput(JSON.stringify(template, null, 2));
    }, [selectedConfig, dataKind]);

    const { data: latestResponse, isPending: isFetching, mutateAsync } = useMutation<
        FetcherResponse,
        Error,
        FetcherRequestPayload
    >({
        mutationFn: async (payload) => {
            const endpoint = dataKind === "join" ? "/api/v1/join" : "/api/v1/groupby";
            return await fetcherRequest<FetcherResponse>("POST", endpoint, payload);
        },
    });

    const handleToggleChange = (value: string) => {
        if (!value) return;
        setDataKind(value as OnlineDataKind);
    };

    const handleDatasetSelect = (value: string) => {
        setSelectedDataset(value);
    };

    const handleFetch = async () => {
        if (!selectedConfig) return;

        let parsed: unknown;
        try {
            parsed = JSON.parse(keysInput);
        } catch (err) {
            toast({
                title: "Invalid JSON",
                description: "Please provide valid JSON for the primary keys.",
                variant: "destructive",
            });
            return;
        }

        if (parsed === null || Array.isArray(parsed) || typeof parsed !== "object") {
            toast({
                title: "Invalid structure",
                description: "Key values must be provided as a JSON object.",
                variant: "destructive",
            });
            return;
        }

        const record = parsed as Record<string, unknown>;
        const normalized: Record<string, string> = {};
        const missing: string[] = [];

        selectedConfig.primary_keys.forEach((key) => {
            const value = record[key];
            if (value === undefined || value === null) {
                missing.push(key);
                return;
            }

            const stringValue = String(value).trim();
            if (!stringValue || stringValue === "ENTER_VALUE_HERE") {
                missing.push(key);
                return;
            }

            normalized[key] = stringValue;
        });

        if (missing.length > 0) {
            toast({
                title: "Missing key values",
                description: `Provide values for: ${missing.join(", ")}.`,
                variant: "destructive",
            });
            return;
        }

        // Keep input formatted
        setKeysInput(JSON.stringify({ ...record, ...normalized }, null, 2));

        const payload: FetcherRequestPayload = {
            name: selectedConfig.name,
            keys: normalized,
        };

        setRequestPayload(payload);

        try {
            persistStoredKeys(dataKind, selectedConfig.name, normalized);
            await mutateAsync(payload);
            toast({
                title: "Fetch successful",
                description: `Online data retrieved for ${selectedConfig.name}`,
            });
        } catch (err) {
            const message = err instanceof Error ? err.message : "Unknown error";
            toast({
                title: "Failed to fetch data",
                description: message,
                variant: "destructive",
            });
        }
    };

    const renderValuesTable = () => {
        if (!latestResponse) return null;

        const valuesEntries = Object.entries(latestResponse.values ?? {});

        if (valuesEntries.length === 0) {
            return (
                <Alert>
                    <AlertTitle>No data</AlertTitle>
                    <AlertDescription>
                        The fetcher returned no values for the provided keys.
                    </AlertDescription>
                </Alert>
            );
        }

        return (
            <Card>
                <CardHeader>
                    <CardTitle className="text-lg">Online Feature Values</CardTitle>
                    <CardDescription>
                        Results returned for the provided lookup keys.
                    </CardDescription>
                </CardHeader>
                <CardContent>
                    <div className="w-full overflow-x-auto border rounded-md">
                        <DataTable
                            value={valuesEntries.map(([feature, value]) => ({ feature, value }))}
                            scrollable
                            scrollHeight="400px"
                            className="min-w-full bg-transparent text-foreground custom-datatable"
                            tableStyle={{ minWidth: "max-content" }}
                        >
                            <Column
                                field="feature"
                                header="Feature"
                                style={{ minWidth: "12rem" }}
                                headerClassName="h-auto py-3"
                                bodyClassName="font-mono text-xs"
                            />
                            <Column
                                field="value"
                                header="Value"
                                style={{ minWidth: "18rem" }}
                                headerClassName="h-auto py-3"
                                bodyClassName="font-mono text-xs"
                                body={(rowData) => {
                                    const value = rowData.value;
                                    if (value === null || value === undefined) {
                                        return <span className="text-muted-foreground italic">null</span>;
                                    }
                                    if (typeof value === "object") {
                                        if (Array.isArray(value)) {
                                            return <span className="whitespace-nowrap">{JSON.stringify(value)}</span>;
                                        }
                                        return <pre className="whitespace-pre-wrap">{JSON.stringify(value, null, 2)}</pre>;
                                    }
                                    return String(value);
                                }}
                            />
                        </DataTable>
                    </div>
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
                </CardContent>
            </Card>
        );
    };

    return (
        <div className="h-full w-full overflow-y-auto">
            <div className="max-w-[1200px] mx-auto p-6 flex flex-col gap-6">
                <Card>
                    <CardHeader>
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-3">
                                <div className="p-2 rounded-md bg-sky-500/20">
                                    <Globe className="h-6 w-6 text-sky-500" />
                                </div>
                                <div>
                                    <CardTitle>Online Data Explorer</CardTitle>
                                    <CardDescription>
                                        Fetch real-time online features from Chronon.
                                    </CardDescription>
                                </div>
                            </div>
                            <Button variant="outline" size="sm" onClick={() => refetchConfigs()}>
                                <RefreshCw className="mr-2 h-4 w-4" /> Refresh
                            </Button>
                        </div>
                    </CardHeader>
                    <CardContent className="flex flex-col gap-6">
                        <div className="flex flex-col gap-2">
                            <span className="text-sm font-medium text-muted-foreground">
                                Online data type
                            </span>
                            <ToggleGroup
                                type="single"
                                value={dataKind}
                                onValueChange={handleToggleChange}
                                className="justify-start"
                            >
                                <ToggleGroupItem value="group_by" aria-label="Group by">
                                    Group By
                                </ToggleGroupItem>
                                <ToggleGroupItem value="join" aria-label="Join">
                                    Join
                                </ToggleGroupItem>
                            </ToggleGroup>
                        </div>

                        <div className="flex flex-col gap-3">
                            <label className="text-sm font-medium">Online Dataset</label>
                            <Select
                                value={selectedDataset}
                                onValueChange={handleDatasetSelect}
                                disabled={isLoadingConfigs}
                            >
                                <SelectTrigger>
                                    <SelectValue
                                        placeholder={
                                            isLoadingConfigs
                                                ? "Loading datasets..."
                                                : datasets?.length
                                                    ? "Select an online dataset"
                                                    : "No datasets found"
                                        }
                                    />
                                </SelectTrigger>
                                <SelectContent>
                                    {datasets?.map((conf) => (
                                        <SelectItem key={conf.name} value={conf.name}>
                                            {conf.name}
                                        </SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>

                            {listError && (
                                <Alert variant="destructive">
                                    <AlertTitle>Failed to load datasets</AlertTitle>
                                    <AlertDescription>
                                        {listError instanceof Error ? listError.message : "Unknown error"}
                                    </AlertDescription>
                                </Alert>
                            )}
                        </div>

                        {selectedConfig && (
                            <div className="flex flex-col gap-2 rounded-md border p-4 bg-muted/20">
                                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                                    <Database className="h-4 w-4" />
                                    <span>
                                        <strong>{selectedConfig.name}</strong>
                                        {" · "}
                                        {dataKind === "join" ? "Join" : "Group By"}
                                    </span>
                                </div>
                                <div className="flex flex-wrap gap-2">
                                    {selectedConfig.primary_keys.map((key) => (
                                        <Badge key={key} variant="outline">
                                            {key}
                                        </Badge>
                                    ))}
                                    {selectedConfig.primary_keys.length === 0 && (
                                        <span className="text-xs text-muted-foreground">
                                            No primary keys defined.
                                        </span>
                                    )}
                                </div>
                                <div className="flex flex-col gap-2 mt-4">
                                    <label className="text-sm font-medium">Primary key values (JSON)</label>
                                    <Textarea
                                        value={keysInput}
                                        onChange={(event) => setKeysInput(event.target.value)}
                                        rows={selectedConfig.primary_keys.length * 2 || 4}
                                        className="font-mono text-xs"
                                    />
                                    <span className="text-xs text-muted-foreground">
                                        Provide values for all required keys. Additional keys are ignored.
                                    </span>
                                </div>
                                <Button
                                    className="mt-4 self-start"
                                    onClick={handleFetch}
                                    disabled={isFetching || selectedConfig.primary_keys.length === 0}
                                >
                                    {isFetching ? "Fetching..." : "Fetch Online Data"}
                                </Button>
                            </div>
                        )}
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader>
                        <CardTitle>Latest Response</CardTitle>
                        <CardDescription>
                            Results from the most recent fetcher request.
                        </CardDescription>
                    </CardHeader>
                    <CardContent className="flex flex-col gap-4">
                        {requestPayload && (
                            <div className="text-xs text-muted-foreground">
                                <div>Dataset: <strong>{requestPayload.name}</strong></div>
                                <div className="mt-1">
                                    Keys:
                                    <pre className="mt-1 whitespace-pre-wrap rounded border bg-muted/40 p-2">
                                        {JSON.stringify(requestPayload.keys, null, 2)}
                                    </pre>
                                </div>
                            </div>
                        )}

                        {isFetching && (
                            <Alert>
                                <AlertDescription>Fetching online data...</AlertDescription>
                            </Alert>
                        )}

                        {!isFetching && !latestResponse && (
                            <Alert>
                                <AlertTitle>No fetch performed yet</AlertTitle>
                                <AlertDescription>
                                    Select a dataset and configure keys to fetch online data.
                                </AlertDescription>
                            </Alert>
                        )}

                        {!isFetching && latestResponse && (
                            <div className="flex flex-col gap-4">
                                {renderValuesTable()}
                                <Alert variant="default">
                                    <AlertDescription>{latestResponse.message}</AlertDescription>
                                </Alert>
                            </div>
                        )}
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}

