import { useMemo, useEffect, useCallback, useState } from "react";
import ReactFlow, {
  Node,
  Edge,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  MarkerType,
  Panel,
  NodeChange,
  applyNodeChanges,
  type ReactFlowInstance,
} from "reactflow";
import "reactflow/dist/style.css";
import type { GraphData, GraphNode, GraphEdge } from "@/shared/schema";
import { LineageNode } from "./LineageNode";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { RefreshCw } from "lucide-react";
import { useTeam } from "@/context/TeamContext";
import { useComputeEngine } from "@/context/ComputeEngineContext";

interface LineageGraphProps {
  data?: GraphData;
}

const nodeTypes = {
  lineageNode: LineageNode,
};

export function LineageGraph({ data: initialData }: LineageGraphProps) {
  const queryClient = useQueryClient();
  const { selectedTeam } = useTeam();
  const { computeEngine } = useComputeEngine();
  const { data: graphData, isLoading, error, refetch, isFetching } = useQuery<GraphData>({
    queryKey: ["/v1/graph", selectedTeam, `graph_data?compute_engine=${computeEngine}`],
    initialData: initialData,
    enabled: !!selectedTeam,
  });
  const [rfInstance, setRfInstance] = useState<ReactFlowInstance | null>(null);

  // Load hidden types from localStorage on mount
  const [hiddenTypes, setHiddenTypes] = useState<Set<string>>(() => {
    try {
      const saved = localStorage.getItem('lineage-graph-hidden-types');
      return saved ? new Set(JSON.parse(saved)) : new Set();
    } catch {
      return new Set();
    }
  });

  // Load hidePending from localStorage on mount
  const [hidePending, setHidePending] = useState(() => {
    try {
      const saved = localStorage.getItem('lineage-graph-hide-pending');
      return saved ? JSON.parse(saved) : false;
    } catch {
      return false;
    }
  });

  const handleRefresh = () => {
    refetch();
  };

  const toggleTypeVisibility = useCallback((typeVisual: string) => {
    setHiddenTypes((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(typeVisual)) {
        newSet.delete(typeVisual);
      } else {
        newSet.add(typeVisual);
      }

      // Save to localStorage
      try {
        localStorage.setItem('lineage-graph-hidden-types', JSON.stringify(Array.from(newSet)));
      } catch (error) {
        console.warn('Failed to save hidden types:', error);
      }

      return newSet;
    });
  }, []);

  const toggleHidePending = useCallback(() => {
    setHidePending((prev: boolean) => {
      const newValue = !prev;

      // Save to localStorage
      try {
        localStorage.setItem('lineage-graph-hide-pending', JSON.stringify(newValue));
      } catch (error) {
        console.warn('Failed to save hide pending state:', error);
      }

      return newValue;
    });
  }, []);

  // Load saved node positions from localStorage
  const loadNodePositions = useCallback((): Record<string, { x: number; y: number }> => {
    try {
      const saved = localStorage.getItem('lineage-graph-node-positions');
      return saved ? JSON.parse(saved) : {};
    } catch {
      return {};
    }
  }, []);

  // Save node positions to localStorage
  const saveNodePositions = useCallback((nodes: Node[]) => {
    try {
      const positions: Record<string, { x: number; y: number }> = {};
      nodes.forEach((node) => {
        positions[node.id] = { x: node.position.x, y: node.position.y };
      });
      localStorage.setItem('lineage-graph-node-positions', JSON.stringify(positions));
    } catch (error) {
      console.warn('Failed to save node positions:', error);
    }
  }, []);

  const computedNodes: Node[] = useMemo(() => {
    if (!graphData) return [];

    // Load saved positions
    const savedPositions = loadNodePositions();

    // Analyze each node's dependencies
    const nodeCategories = graphData.nodes.map((node: GraphNode) => {
      const isSource = graphData.edges.some((edge: GraphEdge) => edge.source === node.name);
      const isTarget = graphData.edges.some((edge: GraphEdge) => edge.target === node.name);

      if (isSource && isTarget) return 'middle';
      if (isSource && !isTarget) return 'left';
      if (!isSource && isTarget) return 'right';
      return 'isolated';
    });

    // Group nodes by category
    const leftNodes = graphData.nodes.filter((_: GraphNode, i: number) => nodeCategories[i] === 'left');
    const middleNodes = graphData.nodes.filter((_: GraphNode, i: number) => nodeCategories[i] === 'middle');
    const rightNodes = graphData.nodes.filter((_: GraphNode, i: number) => nodeCategories[i] === 'right');
    const isolatedNodes = graphData.nodes.filter((_: GraphNode, i: number) => nodeCategories[i] === 'isolated');

    const nodes: Node[] = [];
    const verticalSpacing = 150;
    const horizontalSpacing = 400;

    // Position left nodes (only downstream)
    leftNodes.forEach((node: GraphNode, index: number) => {
      const savedPos = savedPositions[node.name];
      nodes.push({
        id: node.name,
        type: "lineageNode",
        position: savedPos || { x: 50, y: 50 + index * verticalSpacing },
        data: { ...node, label: node.name },
        hidden: hiddenTypes.has(node.type_visual) || (hidePending && !node.exists),
      });
    });

    // Position middle nodes (both upstream and downstream)
    middleNodes.forEach((node: GraphNode, index: number) => {
      const savedPos = savedPositions[node.name];
      nodes.push({
        id: node.name,
        type: "lineageNode",
        position: savedPos || { x: 50 + horizontalSpacing, y: 50 + index * verticalSpacing },
        data: { ...node, label: node.name },
        hidden: hiddenTypes.has(node.type_visual) || (hidePending && !node.exists),
      });
    });

    // Position right nodes (only upstream)
    rightNodes.forEach((node: GraphNode, index: number) => {
      const savedPos = savedPositions[node.name];
      nodes.push({
        id: node.name,
        type: "lineageNode",
        position: savedPos || { x: 50 + horizontalSpacing * 2, y: 50 + index * verticalSpacing },
        data: { ...node, label: node.name },
        hidden: hiddenTypes.has(node.type_visual) || (hidePending && !node.exists),
      });
    });

    // Position isolated nodes (no dependencies) at bottom middle
    isolatedNodes.forEach((node: GraphNode, index: number) => {
      const baseY = Math.max(
        leftNodes.length,
        middleNodes.length,
        rightNodes.length
      ) * verticalSpacing + 100;

      const savedPos = savedPositions[node.name];
      nodes.push({
        id: node.name,
        type: "lineageNode",
        position: savedPos || { x: 50 + horizontalSpacing, y: baseY + index * verticalSpacing },
        data: { ...node, label: node.name },
        hidden: hiddenTypes.has(node.type_visual) || (hidePending && !node.exists),
      });
    });

    return nodes;
  }, [graphData, loadNodePositions, hiddenTypes, hidePending]);

  const computedEdges: Edge[] = useMemo(() => {
    if (!graphData) return [];

    return graphData.edges.map((edge: GraphEdge) => ({
      id: `${edge.source}-${edge.target}`,
      source: edge.source,
      target: edge.target,
      type: edge.exists ? "default" : "default",
      style: {
        strokeWidth: 2,
        stroke: edge.exists
          ? "hsl(var(--foreground) / 0.3)"
          : "hsl(var(--foreground) / 0.3)",
        strokeDasharray: edge.exists ? "0" : "8 4",
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: "hsl(var(--foreground) / 0.3)",
      },
      data: { exists: edge.exists },
    }));
  }, [graphData]);

  const [nodes, setNodes] = useNodesState(computedNodes);
  const [edges, setEdges] = useEdgesState(computedEdges);

  useEffect(() => {
    if (graphData) console.debug('[graph] loaded', { nodes: graphData.nodes?.length, edges: graphData.edges?.length });
  }, [graphData]);

  useEffect(() => {
    setNodes(computedNodes);
    setEdges(computedEdges);
    // Auto-fit view when graph data updates
    if (rfInstance && computedNodes.length > 0) {
      try {
        rfInstance.fitView({ padding: 0.2 });
      } catch {
        // Ignore fitView errors during initialization
      }
    }
  }, [computedNodes, computedEdges, setNodes, setEdges, rfInstance]);

  const onNodesChange = useCallback(
    (changes: NodeChange[]) => {
      setNodes((nds) => {
        const updatedNodes = applyNodeChanges(changes, nds);

        // Save positions after drag operations
        const hasDragChanges = changes.some(
          (change) => change.type === 'position' && change.dragging === false
        );
        if (hasDragChanges) {
          saveNodePositions(updatedNodes);
        }

        return updatedNodes;
      });
    },
    [setNodes, saveNodePositions]
  );

  if (isLoading) {
    return (
      <div className="h-full w-full flex items-center justify-center">
        <div className="text-muted-foreground">Loading graph...</div>
      </div>
    );
  }

  if (error) {
    // Check if it's a 404 error (team has no graph data)
    const is404 = error instanceof Error && error.message.startsWith("404:");

    if (is404) {
      return (
        <div className="h-full w-full flex items-center justify-center p-8">
          <div className="text-center max-w-md">
            <div className="text-muted-foreground text-lg">
              No feature lineage available for Team:{" "}
              <span className="font-semibold text-foreground">{selectedTeam}</span>.
            </div>
            <div className="text-muted-foreground mt-2">
              Add Python configuration files and run compile command.
            </div>
          </div>
        </div>
      );
    }

    return (
      <div className="h-full w-full flex items-center justify-center">
        <div className="text-red-500">Failed to load graph</div>
      </div>
    );
  }

  if (graphData && graphData.nodes?.length === 0 && graphData.edges?.length === 0) {
    return (
      <div className="h-full w-full flex items-center justify-center">
        <div className="text-muted-foreground">No graph data</div>
      </div>
    );
  }

  return (
    <div className="h-full w-full" data-testid="graph-container">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        nodesDraggable={true}
        fitView
        onInit={(instance) => setRfInstance(instance)}
        className="bg-background"
      >
        <Background />
        <Controls className="bg-card border-border" />
        <Panel position="top-right" className="flex items-center gap-4 bg-card/80 backdrop-blur-sm p-4 rounded-md border border-border">
          <div className="flex items-center gap-2">
            <div className="w-8 h-0.5 bg-foreground/30" />
            <span className="text-xs text-muted-foreground">Exists</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-8 h-0.5 border-t-2 border-dashed border-foreground/30" />
            <span className="text-xs text-muted-foreground">Pending</span>
          </div>
          <div className="h-6 w-px bg-border" />
          <Button
            size="sm"
            variant="ghost"
            onClick={handleRefresh}
            disabled={isFetching}
            className="h-7 px-2"
            data-testid="graph-refresh-button"
            title="Refresh graph data"
          >
            <RefreshCw className={`h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
          </Button>
        </Panel>
        <Panel position="bottom-left" className="text-xs text-muted-foreground bg-card/80 backdrop-blur-sm p-2 rounded-md border border-border">
          {`Nodes: ${nodes.filter(n => !n.hidden).length}/${nodes.length} · Edges: ${edges.length}`}
        </Panel>
        <Panel position="top-left" className="flex gap-2">
          <Badge
            variant="outline"
            className={`cursor-pointer transition-opacity ${hiddenTypes.has('batch-data')
              ? 'opacity-40 line-through'
              : 'bg-node-batch/20 border-node-batch'
              }`}
            onClick={() => toggleTypeVisibility('batch-data')}
            data-testid="legend-batch-data"
          >
            Batch Data
          </Badge>
          <Badge
            variant="outline"
            className={`cursor-pointer transition-opacity ${hiddenTypes.has('online-data')
              ? 'opacity-40 line-through'
              : 'bg-node-online/20 border-node-online'
              }`}
            onClick={() => toggleTypeVisibility('online-data')}
            data-testid="legend-online-data"
          >
            Online Data
          </Badge>
          <Badge
            variant="outline"
            className={`cursor-pointer transition-opacity ${hiddenTypes.has('streaming-data')
              ? 'opacity-40 line-through'
              : 'bg-node-streaming/20 border-node-streaming'
              }`}
            onClick={() => toggleTypeVisibility('streaming-data')}
            data-testid="legend-streaming-data"
          >
            Streaming Data
          </Badge>
          <Badge
            variant="outline"
            className={`cursor-pointer transition-opacity ${hiddenTypes.has('conf')
              ? 'opacity-40 line-through'
              : 'bg-node-conf/20 border-node-conf'
              }`}
            onClick={() => toggleTypeVisibility('conf')}
            data-testid="legend-conf"
          >
            Configuration
          </Badge>
          <div className="h-6 w-px bg-border" />
          <Badge
            variant="outline"
            className={`cursor-pointer transition-opacity ${hidePending
              ? 'opacity-40 line-through'
              : 'bg-muted/20 border-muted-foreground'
              }`}
            onClick={toggleHidePending}
            data-testid="legend-hide-pending"
          >
            Hide Pending
          </Badge>
        </Panel>
      </ReactFlow>
    </div>
  );
}
