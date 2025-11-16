const fs = require("fs");
const path = require("path");

const apiUrl =
  process.env.VITE_API_URL || process.env.API_URL || "http://localhost:8000";
const maxUploadSizeMb =
  process.env.VITE_MAX_UPLOAD_SIZE_MB ||
  process.env.UPLOAD_MAX_FILE_SIZE_MB ||
  100;

const targetPath = path.join(__dirname, "..", "dist", "runtime-env.js");

const config = {
  VITE_API_URL: apiUrl,
  VITE_MAX_UPLOAD_SIZE_MB: maxUploadSizeMb,
};

const fileContents = `window.__CONTENT_ATLAS_RUNTIME_CONFIG__ = ${JSON.stringify(
  config
)};`;

fs.writeFileSync(targetPath, fileContents);
