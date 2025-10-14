import { LineageGraph } from "@/components/LineageGraph";

export default function LineagePage() {
  return (
    <div className="h-full w-full flex flex-col min-h-0">
      <div className="flex-1 min-h-0">
        <LineageGraph />
      </div>
    </div>
  );
}
