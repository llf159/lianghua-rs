import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    target: ["chrome107", "edge107", "firefox104", "safari16.0"],
    rolldownOptions: {
      output: {
        codeSplitting: {
          includeDependenciesRecursively: false,
          groups: [{
            debugName: "app",
            name(id) {
              if (id.includes("/src/pages/desktop/")) {
                const pageName = id.split("/src/pages/desktop/")[1]?.split(".")[0];
                return pageName ? `page-${pageName}` : null;
              }
              if (!id.includes("node_modules")) {
                return null;
              }
              if (id.includes("react") || id.includes("scheduler")) {
                return "vendor-react";
              }
              if (id.includes("@tauri-apps")) {
                return "vendor-tauri";
              }
              return null;
            },
          }],
        },
      },
    },
  },
  server: {
    watch: {
      ignored: ["**/src-tauri/gen/**", "**/src-tauri/target/**", "**/target/**"],
    },
  },
});
