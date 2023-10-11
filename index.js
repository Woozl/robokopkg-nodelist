import fs from "fs";
import path from "path";
import readline from "readline";
import fetch from 'node-fetch';
import { cwd } from "process";

const BATCH_SIZE = process.env.BATCH_SIZE ?? 1000;
const NAME_RESOLVER = process.env.NAME_RESOLVER ?? "https://name-resolution-sri-dev.apps.renci.org";
const IGNORE_LIST = ["biolink:SequenceVariant"] // if the node contains any of these categories, skip

// Get three inputs from the command line args:
// - the input file containing the node list as jsonl
// - the directory to put the output files
// - (optional) the line number of the input file to start at
// The file and directory are processed relative to the users current working directory
const [, , inputFileRel, outputDirRel, startLineNum] = process.argv;
const inputFilePath = path.resolve(cwd(), inputFileRel);
const outputDirPath = path.resolve(cwd(), outputDirRel);

// create the stream for the input file, get it's size in bytes, and
// set up a readline interface for a line iterator
const inputFileStream = fs.createReadStream(inputFilePath);
const inputSize = fs.statSync(inputFilePath).size
const nodelist = readline.createInterface({
  input: inputFileStream,
  crlfDelay: Infinity,
});

// get the path to the specified output dir, or create it if it doesn't exist
// inside the output dir, create an error log file "error.txt", delete the file if
// it already exists from a previous run
fs.mkdirSync(outputDirPath, { recursive: true });
const errorFilePath = path.join(outputDirPath, "error.txt");
if (fs.existsSync(errorFilePath)) fs.unlinkSync(errorFilePath);
const errorLog = fs.createWriteStream(errorFilePath, { flags: "a+" });

// This map used to map the category name ("NamedThing", etc), to a file stream handler
// It is empty right now, but is filled as we come across new top-level categories
// Map<string, fs.WriteStream>
const fsMap = new Map();

// The default node fs.write is callback-based, this wrapper fn promisifies it 
// can be awaited
const write = (stream, data) => new Promise((res, rej) => {
  stream.write(data, 'utf-8', (err) => { err ? rej(err) : res() })
})

// # milliseconds -> HH:MM:SS
const getHumanReadableTime = (ms) => {
  const h = Math.floor(ms / 3_600_000).toString().padStart(2, '0');
  const m = Math.floor((ms % 3_600_000) / 60_000).toString().padStart(2, '0');
  const s = Math.floor((ms % 60_000) / 1_000).toString().padStart(2, '0');
  return `${h}:${m}:${s}`
}

// variable async delay (milliseconds)
const sleep = async (delay) =>
  new Promise((res) => setTimeout(() => res(), delay));

// reverse lookup a batch of nodes to get their synonyms
// `nodes` must be an array of objects with key `id` corresponding to a valid CURIE string
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

/**
 * this fn performs the logic and lookup on a group of lines from the nodelist file
 * @param {{ id: string, name: string, category: string[] }[]} nodes 
 * @param {number} batchStartIndex the index/line number where this batch starts, used for the
 * human-readable output format and error logs
 * @param {number} batchEndIndex the index/line number where this batch ends
 * @param {number} bytesReadSoFar number of bytes read from the nodelist file up til this line, used 
 * for the percentage complete text
 */
const processBatch = async (nodes, batchStartIndex, batchEndIndex, bytesReadSoFar) => {
  const t0 = performance.now();
  
  // request node synonyms from nameres for this batch
  // will attempt to fetch 10 times (with 0.5 sec delay) before writing an error log and moving on 
  let synonymsList = null;
  let currentAttempt = 0;
  let text;
  do {
    
    // try to lookup from nameres
    try {
      text = await reverseLookup(nodes).then(res => res.text());
    } catch (e) {
      await write(errorLog, `Error on nameres batch fetch from lines ${batchStartIndex}-${batchEndIndex}\n${text}\n${e}\n\n\n`);
      console.error(`Batch from lines ${batchStartIndex}-${batchEndIndex} failed!`)
      return;
    }

    // try to get JSON. If it fails to parse, try again with a 0.5 sec delay
    try {
      synonymsList = JSON.parse(text)
    } catch (_) {
      currentAttempt += 1;
      await sleep(500)
    }
  } while (synonymsList === null && currentAttempt < 10);

  // the synonym list will be null if it could not be properly fetched
  // in that case, write to log and return so the next line can be processed
  if (synonymsList === null) {
    await write(errorLog, `Error parsing response for batch starting from lines ${batchStartIndex}-${batchEndIndex}\n${text}\n\n\n`);
    console.error(`Batch from lines ${batchStartIndex}-${batchEndIndex} failed!`)
    return;
  }
  
  // for every node in this batch, rewrite the field names to the nameres-compliant json format
  // and add the synonyms to the name array
  nodes.forEach(async ({
    id,
    name,
    category,
  }) => {
    try {
      const synonyms = synonymsList[id];
      if (!Array.isArray(synonyms)) throw new Error(`Node ${node} did not have synonyms`)

      // this gets an output file stream corresponding to the category if it exists, or
      // creates a new one if it doesn't exist, using fsMap
      const mainCat = category[0].replace("biolink:", "");
      let categoryFile = fsMap.get(mainCat);
      if (categoryFile === undefined) {
        fsMap.set(
          mainCat,
          // the output streams are set to append/create (a+). If you run the script twice, it 
          // will append nodes onto the previous run's list. TODO: break out into command line arg?
          fs.createWriteStream(path.join(outputDirPath, `${mainCat}.txt`), { flags: 'a+' })
        );
        categoryFile = fsMap.get(mainCat);
      }

      // concat name and synonyms, convert to a Set to remove duplicates, then convert back
      // into an array
      const nameList = Array.from(new Set([name, ...synonyms]));

      // the output payload to be written to a line in the appropriate category output file
      const outputJson = {
        curie: id,
        names: nameList,
        types: category.map((c) => c.replace("biolink:", "")),
        preferred_name: name,
        shortest_name_length: nameList.reduce((smallest, cur) => (
          cur.length < smallest ? cur.length : smallest
        ), Infinity),
      };

      // wait for the write to the output file to finish before moving on to avoid weird file
      // race conditions
      await write(categoryFile, `${JSON.stringify(outputJson)}\n`);
    } catch (e) {
      // if anything went wrong, write the node and the error thrown
      // keep in mind the network call to nameres has it's errors handled seperately
      // TODO: make this error handling more fine-tuned
      await write(errorLog, `${{ id, name, category }}\n${e}\n\n\n`);
    }
  });

  // print some human-readable log output:
  // [HH:MM:SS since prog start] [percentage complete] [time the batch took to complete] the line numbers/node range that were completed by this batch
  console.log(`[${getHumanReadableTime(performance.now() - progStart)}] [${((bytesReadSoFar / inputSize) * 100).toFixed(2)}%] [${(performance.now() - t0).toFixed(1)}ms] Finished processing batch of nodes ${batchStartIndex} - ${batchEndIndex}`)
};

const progStart = performance.now();
let batch = [];
let batchStartIndex = 0;
let index = 0;
let bytesReadSoFar = 0;
// main loop runs linearly through node list. It collects batches `BATCH_SIZE` long (1000 by default)
// in the `batch` array (filtered by the ignore list), which it then sends to the processBatch fn. 
// It also keeps track of the number of bytes that have been read so far and the line/node number
for await (const nodeStr of nodelist) {
  bytesReadSoFar += Buffer.byteLength(nodeStr) + 1;
  const node = JSON.parse(nodeStr);

  // if this node includes a category on the IGNORE_LIST, skip over it **AND**
  // if the command line arg was set, skip processing until we get to that line number
  if(
    !node.category.some((category) => IGNORE_LIST.includes(category)) &&
    (!startLineNum || index >= startLineNum)
  ) {
    batch.push(node);

    if(batch.length === BATCH_SIZE) {
      await processBatch(batch, batchStartIndex, index, bytesReadSoFar)
      batch = [];
      batchStartIndex = index + 1;
    }
  }

  index += 1;
}
await processBatch(batch, batchStartIndex, index, bytesReadSoFar); // process remaining

console.log(`Job took ${(performance.now() - progStart / 1000).toFixed(2)} seconds.`)