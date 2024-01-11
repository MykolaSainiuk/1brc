// Worker Thread
import { workerData, parentPort } from 'node:worker_threads';
import { createReadStream } from 'node:fs';

// Worker thread
const { fd, start, end } = workerData;

const encoding = 'utf8';
// const highWaterMark = (8 * 64) * 1024; // 64 * 1024 by default. Now 1mb
// 0x1fffffe8 = 536870888 possible max value
const highWaterMark = 64 * 1024;

const rs = createReadStream(null, {
    fd,
    encoding,
    start,
    end: end || undefined,
    highWaterMark,
    autoClose: false,
});

const hm = new Map();

let leftover = '';
for await (const content of rs) {
    const lines = content.split('\n');
    const l = lines.length;
    const ll = l - 1;
    for (let i = 0, line; i < l; i++) {
        if (i > 0 && i < ll) {
            line = lines[i];
        } else if (i === 0) {
            line = leftover ? leftover + lines[0] : lines[0];
        } else if (i === ll) {
            leftover = lines[i];
            break;
        }

        const [key, value] = line.split(';');
        key && value && processLine(key, value);
    }
}

if (leftover) {
    const [key, value] = leftover.split(';');
    processLine(key, value);
}

parentPort.postMessage(hm);

/**
 * LOGIC
 */

// process line
function processLine(key, value) {
    // const [key, value] = data.split(';');
    // if (!key || !value) return; // unlikely
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
