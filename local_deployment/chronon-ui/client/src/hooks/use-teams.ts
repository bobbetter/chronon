import { useQuery } from "@tanstack/react-query";

export function useTeams() {
  return useQuery<string[]>({
    queryKey: ["/v1/teams"],
  });
}

