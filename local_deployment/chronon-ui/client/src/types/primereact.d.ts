declare module "primereact/datatable" {
  import * as React from "react";

  export interface DataTableProps extends React.HTMLAttributes<HTMLDivElement> {
    value?: any[];
    scrollable?: boolean;
    scrollHeight?: string;
    tableStyle?: React.CSSProperties;
    className?: string;
  }

  export class DataTable extends React.Component<DataTableProps> {}
}

declare module "primereact/column" {
  import * as React from "react";

  export interface ColumnProps {
    field?: string;
    header?: React.ReactNode;
    body?: (rowData: any) => React.ReactNode;
    style?: React.CSSProperties;
    headerClassName?: string;
    bodyClassName?: string;
  }

  export class Column extends React.Component<ColumnProps> {}
}


