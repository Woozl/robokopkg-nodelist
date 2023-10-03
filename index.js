import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import readline from "readline";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const fileStream = fs.createReadStream(path.join(__dirname, "./nodes.jsonl"));
const writeStream = fs.createWriteStream(path.join(__dirname, "nodes.txt"), {
  flags: 'a',
});

const rl = readline.createInterface({
  input: fileStream,
  crlfDelay: Infinity,
});

for await (const line of rl) {
  const lineJson = JSON.parse(line);
  if (lineJson.id === undefined) continue;
  writeStream.write(`\n${lineJson.id}`)
}
