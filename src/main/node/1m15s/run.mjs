// Worker Thread
import { workerData, parentPort } from 'node:worker_threads';
import { createReadStream } from 'node:fs';

// Worker thread
const { fd, start, end } = workerData;

const encoding = 'utf8';
const highWaterMark = 8 * 64 * 1024; // 64 * 1024 by default. Now 1mb
// 0x1fffffe8 = 536870888 possible max value
// const highWaterMark = 64 * 1024;

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
    for (let i = 0, line; i < l; i++) {
        if (i === 0 && leftover) {
            line = leftover + lines[0];
        } else if (i === l - 1) {
            leftover = lines[i];
            break;
        } else {
            line = lines[i];
        }

        processLine(line);
    }
}

if (leftover) {
    processLine(leftover);
}

parentPort.postMessage(hm);

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
