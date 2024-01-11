// Worker Thread
import { workerData, parentPort } from 'node:worker_threads';
import { open } from 'node:fs/promises';

// Worker thread 
const { fileName, start, end } = workerData;

const file = await open(fileName, 'r');
const encoding = 'utf8';
// const highWaterMark = (8 * 64) * 1024; // 64 * 1024 by default. Now 1mb
// 0x1fffffe8 = 536870888 possible max value
const highWaterMark = 64 * 1024;

let fileLines;
if (end) {
    fileLines = file.readLines({ highWaterMark, encoding, start, end });
} else {
    fileLines = file.readLines({ highWaterMark, encoding, start });
}

const hm = new Map();

for await (const line of fileLines) {
    processLine(line);
}
// await Promise.all(fileLines.map(processLine));

parentPort.postMessage(hm);

fileLines.close();
await file.close();

/**
 * LOGIC
 */

// process line
function processLine(data) {
    const [key, value] = data.split(';');
    if (!key || !value) return; // TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    const iv = +value;
    const hmItem = hm.get(key);
    if (hmItem) {
        hm.set(key, {
            min: Math.min(hmItem.min, iv),
            max: Math.max(hmItem.max, iv),
            sum: hmItem.sum + iv,
            total: hmItem.total + 1,
        });
    } else {
        hm.set(key, {
            min: iv,
            max: iv,
            sum: iv,
            total: 1,
        });
    }
}
