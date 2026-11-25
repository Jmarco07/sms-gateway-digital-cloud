import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// `npm run dev` talks to a gateway on localhost:8000. In the Docker image, nginx does the
// same job and also adds the admin key (see nginx.conf.template).
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        rewrite: (path) => path.replace(/^\/api/, "/admin"),
        headers: { "X-Admin-Key": process.env.ADMIN_KEY ?? "dev-admin-key" },
      },
    },
  },
});
