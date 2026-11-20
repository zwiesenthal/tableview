// Builds TableView.app, signed with a real certificate when the login keychain
// has one (Developer ID first, then Apple Development). Ad-hoc signatures change
// on every build, and macOS then asks for your password before the new build
// may read saved database passwords; a stable signature keeps "Always Allow".
// Set APPLE_SIGNING_IDENTITY to choose the certificate yourself.
import { execFileSync, spawnSync } from "node:child_process";

let identity = process.env.APPLE_SIGNING_IDENTITY?.trim();
if (!identity) {
  let listing = "";
  try {
    listing = execFileSync("security", ["find-identity", "-v", "-p", "codesigning"], { encoding: "utf8" });
  } catch {}
  const ids = [...listing.matchAll(/^\s*\d+\)\s+([0-9A-F]{40})\s+"(.+)"$/gm)].map((m) => ({ sha: m[1], name: m[2] }));
  const pick = ["Developer ID Application:", "Apple Development:"].map((p) => ids.find((i) => i.name.startsWith(p))).find(Boolean);
  if (pick) {
    identity = pick.sha;
    console.log(`Signing with ${pick.name}`);
  } else {
    console.log("No code-signing certificate found, so the build is ad-hoc signed (macOS may ask for Keychain access after rebuilds).");
  }
}
const env = { ...process.env, ...(identity ? { APPLE_SIGNING_IDENTITY: identity } : {}) };
const r = spawnSync("npx", ["tauri", "build", "--bundles", "app", ...process.argv.slice(2)], { stdio: "inherit", env });
process.exit(r.status ?? 1);
