import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import readline from "readline";
import { cwd } from "process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const [,, inputFileRel, outputDirRel] = process.argv;
const inputFilePath = path.resolve(cwd(), inputFileRel)
const outputDirPath = path.resolve(cwd(), outputDirRel)

const inputSize = fs.statSync(inputFilePath).size
let bytesReadSoFar = 0;
const inputFileStream = fs.createReadStream(inputFilePath);
const rl = readline.createInterface({
  input: inputFileStream,
  crlfDelay: Infinity,
});

const errorFilePath = path.join(outputDirPath, "error.txt");
if(fs.existsSync(errorFilePath)) fs.unlinkSync(errorFilePath);
const errorLog = fs.createWriteStream(errorFilePath, { flags: 'a+' });

// Map<string, fs.WriteStream>
const fsMap = new Map();

let t0 = performance.now();
for await (const line of rl) {
  bytesReadSoFar += Buffer.byteLength(line) + 1;
  const t1 = performance.now();
  if (t1 - t0 > 500) { // every 0.5 second
    console.log(`${((bytesReadSoFar / inputSize) * 100).toFixed(2)}%`)
    t0 = performance.now();
  }
  
  try {
    const {
      id,
      name,
      category,
      // equivalent_identifiers,
    } = JSON.parse(line);
    
    const mainCat = category[0].replace("biolink:", "");
    let categoryFile = fsMap.get(mainCat);
    if (categoryFile === undefined) {
      fsMap.set(mainCat, fs.createWriteStream(path.join(__dirname, '/out/', `${mainCat}.txt`)))
      categoryFile = fsMap.get(mainCat);
    }

    const outputJson = {
      curie: id,
      names: [name],
      types: category.map((c) => c.replace("biolink:", "")),
      preferred_name: name,
      shortest_name_length: name.length,
    }

    categoryFile.write(`${JSON.stringify(outputJson)}\n`)
  }
  catch (e) {
    errorLog.write(`${line}\n${e}\n\n\n`)
  }
}
