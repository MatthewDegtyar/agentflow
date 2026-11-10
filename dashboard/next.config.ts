import type { NextConfig } from "next";

// The dashboard is UI only; every /api/* call is proxied to the agentflow runtime.
const RUNTIME = process.env.AGENTFLOW_API ?? "http://127.0.0.1:7070";

const nextConfig: NextConfig = {
  async rewrites() {
    return [{ source: "/api/:path*", destination: `${RUNTIME}/api/:path*` }];
  },
};

export default nextConfig;
