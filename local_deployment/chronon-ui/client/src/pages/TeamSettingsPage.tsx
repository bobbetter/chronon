import { useTeam } from "@/context/TeamContext";
import { useTeamMetadata } from "@/hooks/use-team-metadata";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AlertCircle, Users } from "lucide-react";

function JsonDisplay({ data, level = 0 }: { data: unknown; level?: number }) {
  if (data === null) {
    return <span className="text-muted-foreground italic">null</span>;
  }

  if (typeof data === "string") {
    return <span className="text-emerald-600 dark:text-emerald-400">"{data}"</span>;
  }

  if (typeof data === "number" || typeof data === "boolean") {
    return <span className="text-amber-600 dark:text-amber-400">{String(data)}</span>;
  }

  if (Array.isArray(data)) {
    if (data.length === 0) {
      return <span className="text-muted-foreground">[]</span>;
    }
    return (
      <div className="ml-4">
        {data.map((item, index) => (
          <div key={index} className="flex">
            <span className="text-muted-foreground mr-2">{index}:</span>
            <JsonDisplay data={item} level={level + 1} />
          </div>
        ))}
      </div>
    );
  }

  if (typeof data === "object") {
    const entries = Object.entries(data as Record<string, unknown>);
    if (entries.length === 0) {
      return <span className="text-muted-foreground">{"{}"}</span>;
    }
    return (
      <div className={level > 0 ? "ml-4 border-l border-border pl-4" : ""}>
        {entries.map(([key, value]) => (
          <div key={key} className="py-1">
            <span className="text-sky-600 dark:text-sky-400 font-medium">{key}</span>
            <span className="text-muted-foreground">: </span>
            {typeof value === "object" && value !== null ? (
              <JsonDisplay data={value} level={level + 1} />
            ) : (
              <JsonDisplay data={value} level={level + 1} />
            )}
          </div>
        ))}
      </div>
    );
  }

  return <span className="text-muted-foreground">{String(data)}</span>;
}

export default function TeamSettingsPage() {
  const { selectedTeam } = useTeam();
  const { data: metadata, isLoading, error } = useTeamMetadata(selectedTeam);

  if (!selectedTeam) {
    return (
      <div className="p-6">
        <Alert>
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>No Team Selected</AlertTitle>
          <AlertDescription>
            Please select a team from the sidebar to view its settings.
          </AlertDescription>
        </Alert>
      </div>
    );
  }

  return (
    <div className="p-6 h-full overflow-auto">
      <div className="max-w-4xl mx-auto space-y-6">
        <div className="flex items-center gap-3">
          <Users className="h-8 w-8 text-primary" />
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Team Settings</h1>
            <p className="text-muted-foreground">
              Configuration and metadata for team:{" "}
              <span className="font-semibold text-foreground">{selectedTeam}</span>
            </p>
          </div>
        </div>

        {isLoading ? (
          <Card>
            <CardHeader>
              <Skeleton className="h-6 w-48" />
            </CardHeader>
            <CardContent className="space-y-3">
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-3/4" />
              <Skeleton className="h-4 w-5/6" />
              <Skeleton className="h-4 w-2/3" />
            </CardContent>
          </Card>
        ) : error ? (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Error Loading Metadata</AlertTitle>
            <AlertDescription>
              {error instanceof Error ? error.message : "Failed to load team metadata"}
            </AlertDescription>
          </Alert>
        ) : metadata ? (
          <Card>
            <CardHeader>
              <CardTitle>Team Metadata</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="font-mono text-sm bg-muted/50 rounded-lg p-4 overflow-auto">
                <JsonDisplay data={metadata} />
              </div>
            </CardContent>
          </Card>
        ) : (
          <Alert>
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>No Metadata</AlertTitle>
            <AlertDescription>
              No metadata available for this team.
            </AlertDescription>
          </Alert>
        )}
      </div>
    </div>
  );
}

