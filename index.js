import fs from "fs";
import path from "path";
import readline from "readline";
import { cwd } from "process";

const BATCH_SIZE = process.env.BATCH_SIZE ?? 1000;
const NAME_RESOLVER = process.env.NAME_RESOLVER ?? "https://name-resolution-sri-dev.apps.renci.org";

const [, , inputFileRel, outputDirRel] = process.argv;
const inputFilePath = path.resolve(cwd(), inputFileRel);
const outputDirPath = path.resolve(cwd(), outputDirRel);

const inputFileStream = fs.createReadStream(inputFilePath);
const inputSize = fs.statSync(inputFilePath).size
const nodelist = readline.createInterface({
  input: inputFileStream,
  crlfDelay: Infinity,
});

fs.mkdirSync(outputDirPath, { recursive: true });
const errorFilePath = path.join(outputDirPath, "error.txt");
if (fs.existsSync(errorFilePath)) fs.unlinkSync(errorFilePath);
const errorLog = fs.createWriteStream(errorFilePath, { flags: "a+" });

// Map<string, fs.WriteStream>
const fsMap = new Map();

const sleep = async (delay) =>
  new Promise((res) => setTimeout(() => res, delay));

const processBatch = async (nodes, batchStartIndex, bytesReadSoFar) => {
  const t0 = performance.now();
  
  // parse nodes here so we don't have to do it twice
  const parsedNodes = nodes.map(node => JSON.parse(node));
  
  // request node synonyms from nameres for this batch
  const synonymsList = await fetch(`${NAME_RESOLVER}/reverse_lookup`, {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      curies: parsedNodes.map(({ id }) => id)
    })
  }).then(res => res.json());
  
  nodes.forEach((node) => {
    try {
      const {
        id,
        name,
        category,
      } = JSON.parse(node);
      const synonyms = synonymsList[id];
      if (!Array.isArray(synonyms)) throw new Error(`Node ${node} did not have synonyms`)

      const mainCat = category[0].replace("biolink:", "");
      let categoryFile = fsMap.get(mainCat);
      if (categoryFile === undefined) {
        fsMap.set(
          mainCat,
          fs.createWriteStream(path.join(outputDirPath, `${mainCat}.txt`))
        );
        categoryFile = fsMap.get(mainCat);
      }

      // use Set to get rid of duplicates
      const nameList = Array.from(new Set([name, ...synonyms]));

      const outputJson = {
        curie: id,
        names: nameList,
        types: category.map((c) => c.replace("biolink:", "")),
        preferred_name: name,
        shortest_name_length: nameList.reduce((smallest, cur) => (
          cur.length < smallest ? cur.length : smallest
        ), Infinity),
      };

      categoryFile.write(`${JSON.stringify(outputJson)}\n`);
    } catch (e) {
      errorLog.write(`${node}\n${e}\n\n\n`);
    }
  });

  console.log(`[${((bytesReadSoFar / inputSize) * 100).toFixed(2)}%] [${(performance.now() - t0).toFixed(1)}ms] Finished processing batch of nodes ${batchStartIndex} - ${batchStartIndex + nodes.length}`)
};

let t0 = performance.now();
const progStart = t0;
let batch = [];
let index = 0;
let bytesReadSoFar = 0;
for await (const node of nodelist) {
  bytesReadSoFar += Buffer.byteLength(node) + 1;
  batch.push(node);
  if (index % BATCH_SIZE === 0 && index !== 0) {
    await processBatch(batch, index - BATCH_SIZE, bytesReadSoFar);
    batch = [];
  }
  index += 1;
}
await processBatch(batch, index - BATCH_SIZE, bytesReadSoFar); // process remaining

console.log(`Job took ${(performance.now() - progStart / 1000).toFixed(2)} seconds.`)