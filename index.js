import fs from "fs";
import path from "path";
import readline from "readline";
import fetch from 'node-fetch';
import { cwd } from "process";

const BATCH_SIZE = process.env.BATCH_SIZE ?? 1000;
const NAME_RESOLVER = process.env.NAME_RESOLVER ?? "https://name-resolution-sri-dev.apps.renci.org";

const [, , inputFileRel, outputDirRel, startLineNum] = process.argv;
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

// promisified stream write
const write = (stream, data) => new Promise((res, rej) => {
  stream.write(data, 'utf-8', (err) => { err ? rej(err) : res() })
})

const getHumanReadableTime = (ms) => {
  const h = Math.floor(ms / 3_600_000).toString().padStart(2, '0');
  const m = Math.floor((ms % 3_600_000) / 60_000).toString().padStart(2, '0');
  const s = Math.floor((ms % 60_000) / 1_000).toString().padStart(2, '0');
  return `${h}:${m}:${s}`
}

const sleep = async (delay) =>
  new Promise((res) => setTimeout(() => res(), delay));

const reverseLookup = async (nodes) => await fetch(`${NAME_RESOLVER}/reverse_lookup`, {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      curies: nodes.map(({ id }) => id)
    })
  });

const processBatch = async (nodes, batchStartIndex, bytesReadSoFar) => {
  const t0 = performance.now();
  
  // parse nodes here so we don't have to do it twice
  const parsedNodes = nodes.map(node => JSON.parse(node));
  
  // request node synonyms from nameres for this batch
  // will attempt to fetch 10 times (with 1 sec delay) before writing an error log and moving on 
  let synonymsList = null;
  let currentAttempt = 0;
  do {
    let text;
    
    // try to lookup from nameres
    try {
      text = await reverseLookup(parsedNodes).then(res => res.text());
    } catch (e) {
      await write(errorLog, `Error on nameres batch fetch starting at line ${batchStartIndex}\n${text}\n${e}\n\n\n`);
      return;
    }

    // try to get JSON. If it's something else, try again
    try {
      synonymsList = JSON.parse(text)
    } catch (_) {
      currentAttempt += 1;
      await sleep(1000)
    }
  } while (synonymsList === null && currentAttempt < 10);

  if (synonymsList === null) {
    await write(errorLog, `Error parsing response for batch starting at line ${batchStartIndex}\n\n\n`);
    return;
  }
  
  nodes.forEach(async (node) => {
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
          fs.createWriteStream(path.join(outputDirPath, `${mainCat}.txt`), { flags: 'a+' })
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

      await write(categoryFile, `${JSON.stringify(outputJson)}\n`);
    } catch (e) {
      await write(errorLog, `${node}\n${e}\n\n\n`);
    }
  });

  console.log(`[${getHumanReadableTime(performance.now() - progStart)}] [${((bytesReadSoFar / inputSize) * 100).toFixed(2)}%] [${(performance.now() - t0).toFixed(1)}ms] Finished processing batch of nodes ${batchStartIndex} - ${batchStartIndex + nodes.length}`)
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
    if (!startLineNum || index >= parseInt(startLineNum))
      await processBatch(batch, index - BATCH_SIZE, bytesReadSoFar);
    batch = [];
  }
  index += 1;
}
await processBatch(batch, index - BATCH_SIZE, bytesReadSoFar); // process remaining

console.log(`Job took ${(performance.now() - progStart / 1000).toFixed(2)} seconds.`)