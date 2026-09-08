import { defineConfig } from "vite";

export default defineConfig({
  base: "./",
  build: { outDir: "../interlace/service/static", emptyOutDir: true },
});
