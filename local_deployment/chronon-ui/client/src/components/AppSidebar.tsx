import { useEffect } from "react";
import { GitBranch, Database, Globe, Activity, Users, Settings, Cpu } from "lucide-react";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarSeparator,
} from "@/components/ui/sidebar";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Link, useLocation } from "wouter";
import { useTeams } from "@/hooks/use-teams";
import { useTeam } from "@/context/TeamContext";
import { useComputeEngine, COMPUTE_ENGINE_OPTIONS, ComputeEngine } from "@/context/ComputeEngineContext";

const dataToolsItems = [
  {
    title: "Lineage",
    url: "/",
    icon: GitBranch,
  },
  {
    title: "Batch Data",
    url: "/batch-data",
    icon: Database,
  },
  {
    title: "Online Data",
    url: "/online-data",
    icon: Globe,
  },
  {
    title: "Streaming Data",
    url: "/streaming-data",
    icon: Activity,
  },
];

const settingsItems = [
  {
    title: "Team",
    url: "/settings/team",
    icon: Settings,
  },
];

export function AppSidebar() {
  const [location] = useLocation();
  const { data: teams, isLoading: isLoadingTeams } = useTeams();
  const { selectedTeam, setSelectedTeam } = useTeam();
  const { computeEngine, setComputeEngine } = useComputeEngine();

  // Auto-select first team when teams are loaded
  useEffect(() => {
    if (teams && teams.length > 0 && !selectedTeam) {
      setSelectedTeam(teams[0]);
    }
  }, [teams, selectedTeam, setSelectedTeam]);

  return (
    <Sidebar>
      <SidebarHeader>
        <SidebarGroup className="p-0">
          <SidebarGroupLabel className="flex items-center gap-2">
            <Users className="h-4 w-4" />
            Team
          </SidebarGroupLabel>
          <SidebarGroupContent>
            {isLoadingTeams ? (
              <Skeleton className="h-9 w-full" />
            ) : (
              <Select
                value={selectedTeam ?? undefined}
                onValueChange={setSelectedTeam}
              >
                <SelectTrigger
                  className="w-full"
                  data-testid="team-selector"
                >
                  <SelectValue placeholder="Select a team" />
                </SelectTrigger>
                <SelectContent>
                  {teams?.map((team) => (
                    <SelectItem key={team} value={team}>
                      {team}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarHeader>
      <SidebarSeparator />
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Data Tools</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {dataToolsItems.map((item) => (
                <SidebarMenuItem key={item.title}>
                  <SidebarMenuButton asChild isActive={location === item.url}>
                    <Link href={item.url} data-testid={`link-${item.title.toLowerCase().replace(" ", "-")}`}>
                      <item.icon className="h-5 w-5" />
                      <span className="text-base">{item.title}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
        <SidebarGroup>
          <SidebarGroupLabel>Settings</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {settingsItems.map((item) => (
                <SidebarMenuItem key={item.title}>
                  <SidebarMenuButton asChild isActive={location === item.url}>
                    <Link href={item.url} data-testid={`link-settings-${item.title.toLowerCase().replace(" ", "-")}`}>
                      <item.icon className="h-5 w-5" />
                      <span className="text-base">{item.title}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
        <SidebarGroup className="p-0 px-2">
          <SidebarGroupLabel className="flex items-center gap-2 text-base font-medium text-sidebar-foreground">
            <Cpu className="h-4 w-4" />
            Compute Engine
          </SidebarGroupLabel>
          <SidebarGroupContent>
            <Select
              value={computeEngine}
              onValueChange={(value) => setComputeEngine(value as ComputeEngine)}
            >
              <SelectTrigger
                className="w-full"
                data-testid="compute-engine-selector"
              >
                <SelectValue placeholder="Select compute engine" />
              </SelectTrigger>
              <SelectContent>
                {COMPUTE_ENGINE_OPTIONS.map((option) => (
                  <SelectItem key={option.value} value={option.value}>
                    {option.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  );
}
