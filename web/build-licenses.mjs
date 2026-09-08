/** 将工作台运行依赖的原始许可证随构建产物一起分发。 */
import fs from "node:fs";
import path from "node:path";

const lock = JSON.parse(fs.readFileSync("package-lock.json", "utf8"));
const sections = [];
for (const [location, metadata] of Object.entries(lock.packages)) {
  if (!location || metadata.dev) continue;
  const manifest = JSON.parse(
    fs.readFileSync(path.join(location, "package.json"), "utf8"),
  );
  const files = fs
    .readdirSync(location)
    .filter((name) => /^licen[sc]e(?:\.|$)/i.test(name));
  if (!files.length) throw new Error(`Missing license: ${manifest.name}`);
  const contents = files
    .map((name) => fs.readFileSync(path.join(location, name), "utf8"))
    .join("\n");
  sections.push(
    `${manifest.name} ${manifest.version}\n${"=".repeat(60)}\n${contents}`,
  );
}
fs.writeFileSync(
  "../interlace/service/static/THIRD_PARTY_LICENSES.txt",
  sections.join("\n\n"),
);
