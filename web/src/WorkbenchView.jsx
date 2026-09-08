import React from "react";
import { ReactFlowProvider } from "@xyflow/react";
import GraphWorkbench from "./GraphWorkbench.jsx";

/** 只在定义编排或运行详情中加载图引擎和其独立画布状态。 */
export default function WorkbenchView(props) {
  return (
    <ReactFlowProvider>
      <GraphWorkbench {...props} />
    </ReactFlowProvider>
  );
}
