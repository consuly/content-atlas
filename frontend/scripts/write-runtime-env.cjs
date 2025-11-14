const fs = require("fs");
const path = require("path");

const apiUrl =
  process.env.VITE_API_URL || process.env.API_URL || "http://localhost:8000";

const targetPath = path.join(__dirname, "..", "dist", "runtime-env.js");

const config = {
  VITE_API_URL: apiUrl,
};

const fileContents = `window.__CONTENT_ATLAS_RUNTIME_CONFIG__ = ${JSON.stringify(
  config
)};`;

fs.writeFileSync(targetPath, fileContents);
