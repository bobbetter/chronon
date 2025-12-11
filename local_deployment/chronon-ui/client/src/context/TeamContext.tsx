import * as React from "react";
import { useQuery } from "@tanstack/react-query";

// Type for the environment configuration from team metadata (env.common)
export type TeamEnvConfig = Record<string, string>;

// Type for the full team metadata response structure
type TeamMetadataResponse = {
    executionInfo?: {
        env?: {
            common?: TeamEnvConfig;
        };
    };
};

type TeamContextType = {
    selectedTeam: string | null;
    setSelectedTeam: (team: string) => void;
    teamEnvConfig: TeamEnvConfig | null;
    isLoadingEnvConfig: boolean;
    envConfigError: Error | null;
};

const TeamContext = React.createContext<TeamContextType | null>(null);

export function useTeam() {
    const context = React.useContext(TeamContext);
    if (!context) {
        throw new Error("useTeam must be used within a TeamProvider");
    }
    return context;
}

export function TeamProvider({ children }: { children: React.ReactNode }) {
    const [selectedTeam, setSelectedTeam] = React.useState<string | null>(null);

    // Fetch team metadata when a team is selected
    const {
        data: metadata,
        isLoading: isLoadingEnvConfig,
        error: envConfigError,
    } = useQuery<TeamMetadataResponse>({
        queryKey: ["/v1/teams/metadata", selectedTeam],
        enabled: !!selectedTeam,
    });

    // Extract env.common from the metadata
    const teamEnvConfig = React.useMemo(() => {
        return metadata?.executionInfo?.env?.common ?? null;
    }, [metadata]);

    const value = React.useMemo(
        () => ({
            selectedTeam,
            setSelectedTeam,
            teamEnvConfig,
            isLoadingEnvConfig,
            envConfigError: envConfigError as Error | null,
        }),
        [selectedTeam, teamEnvConfig, isLoadingEnvConfig, envConfigError]
    );

    return <TeamContext.Provider value={value}>{children}</TeamContext.Provider>;
}

