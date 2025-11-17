import { z } from "zod";

export interface GraphNode {
    name: string;
    type: string;
    type_visual: "batch-data" | "online-data" | "conf" | "streaming-data";
    exists: boolean;
    actions?: string[];
    label?: string;
    config_file_path?: string;
}

export interface GraphEdge {
    source: string;
    target: string;
    type: string;
    exists: boolean;
}

export interface GraphData {
    nodes: GraphNode[];
    edges: GraphEdge[];
}

export interface NodeAction {
    nodeName: string;
    action: string;
    timestamp: string;
    status: "pending" | "success" | "failed";
    message?: string;
}

export const nodeActionSchema = z.object({
    nodeName: z.string(),
    action: z.string(),
});

export type NodeActionRequest = z.infer<typeof nodeActionSchema>;

