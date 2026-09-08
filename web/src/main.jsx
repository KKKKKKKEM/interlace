import React from "react";
import { createRoot } from "react-dom/client";
import { createHashRouter, RouterProvider } from "react-router-dom";
import "@xyflow/react/dist/style.css";
import {
  Layout,
  ProjectList,
  ProjectDetail,
  DefinitionDetail,
  NotFound,
} from "./Management.jsx";
import "./tokens.css";
import "./style.css";
import "./management.css";

const router = createHashRouter([
  {
    element: <Layout />,
    children: [
      { path: "/", element: <ProjectList /> },
      { path: "/projects", element: <ProjectList /> },
      { path: "/projects/:projectId", element: <ProjectDetail /> },
      {
        path: "/projects/:projectId/definitions/:definitionName",
        element: <DefinitionDetail />,
      },
      { path: "*", element: <NotFound /> },
    ],
  },
]);

createRoot(document.getElementById("root")).render(
  <RouterProvider router={router} />,
);
