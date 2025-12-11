import * as React from "react";

export type ComputeEngine = "local" | "remote";

export const COMPUTE_ENGINE_OPTIONS: { value: ComputeEngine; label: string }[] = [
    { value: "local", label: "Local" },
    { value: "remote", label: "Remote" },
];

// Actions that are allowed when compute engine is set to "remote"
export const REMOTE_ALLOWED_ACTIONS = ["backfill", "pre-compute-upload", "upload-to-kv"] as const;
export type RemoteAllowedAction = (typeof REMOTE_ALLOWED_ACTIONS)[number];

export function isActionAllowedForRemote(action: string): boolean {
    return REMOTE_ALLOWED_ACTIONS.includes(action as RemoteAllowedAction);
}

type ComputeEngineContextType = {
    computeEngine: ComputeEngine;
    setComputeEngine: (engine: ComputeEngine) => void;
};

const ComputeEngineContext = React.createContext<ComputeEngineContextType | null>(null);

export function useComputeEngine() {
    const context = React.useContext(ComputeEngineContext);
    if (!context) {
        throw new Error("useComputeEngine must be used within a ComputeEngineProvider");
    }
    return context;
}

export function ComputeEngineProvider({ children }: { children: React.ReactNode }) {
    const [computeEngine, setComputeEngine] = React.useState<ComputeEngine>("local");

    const value = React.useMemo(
        () => ({
            computeEngine,
            setComputeEngine,
        }),
        [computeEngine]
    );

    return <ComputeEngineContext.Provider value={value}>{children}</ComputeEngineContext.Provider>;
}

