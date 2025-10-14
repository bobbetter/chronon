import { Handle, Position } from "reactflow";
import type { GraphNode } from "@/shared/schema";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";

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
    default:
      return "node-batch";
  }
};

export function LineageNode({ data }: LineageNodeProps) {
  const [hovering, setHovering] = useState(false);
  const colorClass = getNodeColor(data.type_visual);
  const { toast } = useToast();

  const executeActionMutation = useMutation({
    mutationFn: async (action: string) => {
      const res = await apiRequest("POST", "/node_action", { nodeName: data.name, action });
      return await res.json();
    },
    onSuccess: (result, action) => {
      if (result.status === "failed") {
        toast({
          title: "Action failed",
          description: result.message || `Failed to execute ${action} on ${data.name}`,
          variant: "destructive",
        });
      } else {
        toast({
          title: "Action executed",
          description: result.message || `Successfully executed ${action} on ${data.name}`,
        });
      }
    },
    onError: (error, action) => {
      toast({
        title: "Action failed",
        description: `Failed to execute ${action} on ${data.name}`,
        variant: "destructive",
      });
    },
  });

  const handleAction = (action: string) => {
    executeActionMutation.mutate(action);
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
            ? `hsl(var(--chart-${data.type_visual === 'batch-data' ? '1' : data.type_visual === 'online-data' ? '2' : '3'}) / 0.15)`
            : 'transparent',
          borderColor: `hsl(var(--chart-${data.type_visual === 'batch-data' ? '1' : data.type_visual === 'online-data' ? '2' : '3'}))`,
          borderStyle: data.exists ? 'solid' : 'dashed',
        }}
        data-testid={`node-${data.name}`}
      >
        <div className="font-mono text-xs text-foreground mb-1">
          {data.name}
        </div>
        <Badge
          variant="outline"
          className="text-xs mb-2"
          data-testid={`badge-${data.type}`}
        >
          {data.type}
        </Badge>

        {hovering && data.actions && data.actions.length > 0 && (
          <div className="flex gap-2 mt-2">
            {data.actions.map((action) => (
              <Button
                key={action}
                size="sm"
                variant="secondary"
                onClick={() => handleAction(action)}
                disabled={executeActionMutation.isPending}
                className="h-6 text-xs"
                data-testid={`button-${action}-${data.name}`}
              >
                {action}
              </Button>
            ))}
          </div>
        )}
      </div>

      <Handle type="source" position={Position.Right} className="!bg-border" />
    </div>
  );
}
