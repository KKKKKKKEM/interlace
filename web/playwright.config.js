import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  timeout: 30000,
  use: {
    baseURL: process.env.INTERLACE_TEST_URL || "http://127.0.0.1:8765",
    viewport: { width: 1440, height: 960 },
  },
  webServer: {
    command:
      "uv run --extra service python examples/service.py --port 8765 --database :memory:",
    cwd: "..",
    url: "http://127.0.0.1:8765",
    reuseExistingServer: false,
  },
});
