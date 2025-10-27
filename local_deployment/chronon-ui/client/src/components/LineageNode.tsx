import { Handle, Position } from "reactflow";
import type { GraphNode } from "@/shared/schema";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { useLocation } from "wouter";
import { Loader2, Trash2 } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

interface LineageNodeProps {
  data: GraphNode & { label: string };
}

const getNodeColor = (typeVisual: string) => {
  switch (typeVisual) {
    case "batch-data":
      return "node-batch";
    case "online-data":
      return "node-online";
    case "conf":
      return "node-conf";
    case "streaming-data":
      return "node-streaming";
    default:
      return "node-batch";
  }
};

export function LineageNode({ data }: LineageNodeProps) {
  const [hovering, setHovering] = useState(false);
  const [showDateDialog, setShowDateDialog] = useState(false);
  const [dateValue, setDateValue] = useState("");
  const [pendingAction, setPendingAction] = useState<string | null>(null);
  const [dateError, setDateError] = useState<string | null>(null);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const colorClass = getNodeColor(data.type_visual);
  const { toast } = useToast();
  const [, setLocation] = useLocation();
  const queryClient = useQueryClient();

  const deleteTableMutation = useMutation({
    mutationFn: async () => {
      const res = await apiRequest("POST", `/v1/actions/delete-table?table_name=${encodeURIComponent(data.name)}`, null);

      if (!res.ok) {
        const errorText = await res.text();
        throw new Error(errorText || `HTTP ${res.status}`);
      }

      return await res.json();
    },
    onSuccess: () => {
      toast({
        title: "Table deleted",
        description: `Successfully deleted table ${data.name}`,
      });
    },
    onError: (error: Error) => {
      toast({
        title: "Delete failed",
        description: error.message || `Failed to delete table ${data.name}`,
        variant: "destructive",
      });
    },
    onSettled: () => {
      // Re-fetch graph data after deletion completes
      queryClient.invalidateQueries({ queryKey: ["/v1/graph/graph_data"] });
    },
  });

  const executeActionMutation = useMutation({
    mutationFn: async ({ action, ds }: { action: string; ds?: string }) => {
      try {
        // Special handling for conf-group_by nodes
        if ((data.type === "conf-group_by" || data.type === "conf-join" || data.type === "upload-group_by") && data.config_file_path) {
          const res = await apiRequest("POST", "/v1/actions/run-spark-job", {
            conf_path: data.config_file_path,
            ds: ds || "2023-12-01", // Use provided date or fallback
            mode: action,
          });

          if (!res.ok) {
            const errorText = await res.text();
            throw new Error(errorText || `HTTP ${res.status}`);
          }

          return await res.json();
        }

        // Default behavior for other node types
        const res = await apiRequest("POST", "/node_action", { nodeName: data.name, action });

        if (!res.ok) {
          const errorText = await res.text();
          throw new Error(errorText || `HTTP ${res.status}`);
        }

        return await res.json();
      } catch (error) {
        console.error("Action execution error:", error);
        throw error;
      }
    },
    onSuccess: (result, variables) => {
      if (result.status === "failed") {
        toast({
          title: "Action failed",
          description: result.message || `Failed to execute ${variables.action} on ${data.name}`,
          variant: "destructive",
        });
      } else {
        toast({
          title: "Action executed",
          description: result.message || `Successfully executed ${variables.action} on ${data.name}`,
        });
      }
    },
    onError: (error: Error, variables) => {
      toast({
        title: "Action failed",
        description: error.message || `Failed to execute ${variables.action} on ${data.name}`,
        variant: "destructive",
      });
    },
    onSettled: () => {
      // Re-fetch graph data after mutation completes (success or error)
      queryClient.invalidateQueries({ queryKey: ["/v1/graph/graph_data"] });
    },
  });

  const validateDateFormat = (date: string): boolean => {
    // Validate YYYY-MM-DD format
    const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
    if (!dateRegex.test(date)) {
      return false;
    }

    // Validate it's a real date
    const parsedDate = new Date(date);
    if (isNaN(parsedDate.getTime())) {
      return false;
    }

    // Validate the format matches the parsed date (catches invalid dates like 2025-13-45)
    const [year, month, day] = date.split('-').map(Number);
    return (
      parsedDate.getFullYear() === year &&
      parsedDate.getMonth() === month - 1 &&
      parsedDate.getDate() === day
    );
  };

  const handleDateSubmit = () => {
    if (!dateValue) {
      setDateError("Date is required");
      return;
    }

    if (!validateDateFormat(dateValue)) {
      setDateError("Invalid date format. Please use YYYY-MM-DD");
      return;
    }

    if (pendingAction) {
      executeActionMutation.mutate({ action: pendingAction, ds: dateValue });
      setShowDateDialog(false);
      setDateValue("");
      setPendingAction(null);
      setDateError(null);
    }
  };

  const handleAction = (action: string) => {
    if (action === "show-online-data" && data.type_visual === "conf") {
      const params = new URLSearchParams();

      if (data.type.includes("group_by")) {
        params.set("type", "group_by");
      } else if (data.type.includes("join")) {
        params.set("type", "join");
      } else {
        toast({
          title: "Unsupported configuration",
          description: `Could not determine online data type for ${data.name}`,
          variant: "destructive",
        });
        return;
      }

      params.set("dataset", data.name);
      setLocation(`/online-data?${params.toString()}`);
      return;
    }

    // Deep link to Batch Data when raw-data or backfill-group_by node with "show" action is clicked
    if ((data.type === "raw-data" || data.type === "backfill-group_by" || data.type === "upload-group_by" || data.type === "backfill-join") && action === "show" && data.exists) {
      const dotIndex = data.name.indexOf(".");
      if (dotIndex > 0 && dotIndex < data.name.length - 1) {
        const db = data.name.substring(0, dotIndex);
        const table = data.name.substring(dotIndex + 1);
        // Open in new window/tab
        window.open(`/batch-data?db=${encodeURIComponent(db)}&table=${encodeURIComponent(table)}`, '_blank');
      } else {
        toast({
          title: "Invalid node name",
          description: `Could not parse database/table from ${data.name}`,
          variant: "destructive",
        });
      }
      return;
    }

    // Show date dialog for actions that require a date parameter
    if ((data.type === "conf-group_by" || data.type === "conf-join" || data.type === "upload-group_by") && data.config_file_path) {
      setPendingAction(action);
      setShowDateDialog(true);
      return;
    }

    // Execute action without date for other node types
    executeActionMutation.mutate({ action });
  };

  return (
    <div
      className="relative"
      onMouseEnter={() => setHovering(true)}
      onMouseLeave={() => setHovering(false)}
    >
      <Handle type="target" position={Position.Left} className="!bg-border" />

      <div
        className={`
          min-w-[200px] px-4 py-3 rounded-md
          ${data.exists ? `bg-${colorClass} border-2 border-${colorClass}` : `bg-transparent border-2 border-dashed border-${colorClass}`}
          ${hovering ? 'shadow-lg' : ''}
          transition-all
        `}
        style={{
          backgroundColor: data.exists
            ? `hsl(var(--chart-${data.type_visual === 'batch-data' ? '1' : data.type_visual === 'online-data' ? '2' : data.type_visual === 'streaming-data' ? '4' : '3'}) / 0.15)`
            : 'transparent',
          borderColor: `hsl(var(--chart-${data.type_visual === 'batch-data' ? '1' : data.type_visual === 'online-data' ? '2' : data.type_visual === 'streaming-data' ? '4' : '3'}))`,
          borderStyle: data.exists ? 'solid' : 'dashed',
        }}
        data-testid={`node-${data.name}`}
      >
        <div className="font-mono text-xs font-bold text-foreground mb-1">
          {data.name}
        </div>
        <Badge
          variant="outline"
          className="text-xs font-normal mb-2"
          data-testid={`badge-${data.type}`}
        >
          {data.type}
        </Badge>

        {hovering && ((data.actions?.length ?? 0) > 0 || data.type_visual === "batch-data") && (
          <div className="mt-2">
            <div className="text-xs text-muted-foreground mb-1">Actions:</div>
            <div className="flex gap-2">
              {data.actions && data.actions.map((action) => (
                <Button
                  key={action}
                  size="sm"
                  variant="secondary"
                  onClick={() => handleAction(action)}
                  disabled={!data.exists || executeActionMutation.isPending}
                  className="h-6 text-xs"
                  data-testid={`button-${action}-${data.name}`}
                >
                  {action}
                </Button>
              ))}
              {data.type_visual === "batch-data" && data.exists && (
                <Button
                  size="sm"
                  variant="outline"
                  onClick={(e) => {
                    e.stopPropagation();
                    setShowDeleteDialog(true);
                  }}
                  disabled={deleteTableMutation.isPending}
                  className="h-6 text-xs px-2 bg-destructive/20 hover:bg-destructive/30 text-destructive border-destructive/30"
                  data-testid={`button-delete-${data.name}`}
                  title="Delete table"
                >
                  <Trash2 className="h-3 w-3" />
                </Button>
              )}
            </div>
          </div>
        )}
      </div>

      {(executeActionMutation.isPending || deleteTableMutation.isPending) && (
        <div className="absolute bottom-1 right-1" data-testid="node-loading-indicator">
          <Loader2 className="h-4 w-4 animate-spin text-foreground/60" />
        </div>
      )}

      <Handle type="source" position={Position.Right} className="!bg-border" />

      <Dialog open={showDateDialog} onOpenChange={(open) => {
        setShowDateDialog(open);
        if (!open) {
          setDateValue("");
          setPendingAction(null);
          setDateError(null);
        }
      }}>
        <DialogContent className="sm:max-w-[425px]">
          <DialogHeader>
            <DialogTitle>Enter Date</DialogTitle>
            <DialogDescription>
              Please enter a date for the action "{pendingAction}" in YYYY-MM-DD format.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="date">
                Date (YYYY-MM-DD)
              </Label>
              <Input
                id="date"
                type="text"
                placeholder="2025-11-01"
                value={dateValue}
                onChange={(e) => {
                  setDateValue(e.target.value);
                  setDateError(null);
                }}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    handleDateSubmit();
                  }
                }}
                className={dateError ? "border-red-500" : ""}
              />
              {dateError && (
                <p className="text-sm text-red-500">{dateError}</p>
              )}
            </div>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setShowDateDialog(false);
                setDateValue("");
                setPendingAction(null);
                setDateError(null);
              }}
            >
              Cancel
            </Button>
            <Button onClick={handleDateSubmit}>
              Execute
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <DialogContent className="sm:max-w-[425px]">
          <DialogHeader>
            <DialogTitle>Delete Table</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete the table <strong>{data.name}</strong>? This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setShowDeleteDialog(false)}
              disabled={deleteTableMutation.isPending}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={() => {
                deleteTableMutation.mutate();
                setShowDeleteDialog(false);
              }}
              disabled={deleteTableMutation.isPending}
            >
              {deleteTableMutation.isPending ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Deleting...
                </>
              ) : (
                'Delete'
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
