import * as React from "react";

type ActionDateContextType = {
    prevActionDate: string | null;
    setPrevActionDate: (date: string) => void;
};

const ActionDateContext = React.createContext<ActionDateContextType | null>(null);

export function useActionDate() {
    const context = React.useContext(ActionDateContext);
    if (!context) {
        throw new Error("useActionDate must be used within an ActionDateProvider");
    }
    return context;
}

export function ActionDateProvider({ children }: { children: React.ReactNode }) {
    const [prevActionDate, setPrevActionDate] = React.useState<string | null>(null);

    const value = React.useMemo(
        () => ({
            prevActionDate,
            setPrevActionDate,
        }),
        [prevActionDate]
    );

    return <ActionDateContext.Provider value={value}>{children}</ActionDateContext.Provider>;
}

