import { useQuery } from "@tanstack/react-query";

export function useTeamMetadata(teamName: string | null) {
  return useQuery<Record<string, unknown>>({
    queryKey: ["/v1/teams/metadata", teamName],
    enabled: !!teamName,
  });
}

