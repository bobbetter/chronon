import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Database } from "lucide-react";

export default function BatchDataPage() {
  return (
    <div className="h-full w-full p-6">
      <Card className="max-w-2xl mx-auto mt-20">
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-md bg-node-batch/20">
              <Database className="h-6 w-6 text-node-batch" />
            </div>
            <div>
              <CardTitle>Batch Data</CardTitle>
              <CardDescription>
                Batch data processing and management
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">
            This page will display batch data processing information, tables, and controls.
            Coming soon...
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
