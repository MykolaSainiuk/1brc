import { Worker } from 'node:worker_threads';
import { open } from 'node:fs/promises';
import { close } from 'node:fs';

const fileName = process.argv[2];

const chunkSize = ~~(0.45 * 1024 * 1024 * 1024); // 0.45 bcz must be less than 0x1fffffe8
const maxOldGenerationSizeMb = ~~(~~(chunkSize / 1024 / 1024) * 1.5);

const fh = await open(fileName, 'r');
const fileStat = await fh.stat();
const numberOfWorkers = ~~(fileStat.size / chunkSize) + 1;
console.debug('numberOfWorkers', numberOfWorkers);
// const numberOfWorkers = 13;

// Main Thread

const workerFilePath = './src/main/node/worker.mjs';
const workers = [];

for (let i = 0; i < numberOfWorkers; i++) {
    const worker = new Worker(workerFilePath, {
        trackUnmanagedFds: false,
        workerData: {
            fd: fh.fd,
            start: i * chunkSize,
            end: i + 1 === numberOfWorkers ? 0 : (i + 1) * chunkSize,
        },
        stdin: false,
        stdout: false,
        stderr: false,
        resourceLimits: {
            maxOldGenerationSizeMb,
        },
    });
    workers.push(worker);
}

const subMaps = await Promise.all(
    workers.map(
        (w) =>
            new Promise((resolve) =>
                w.once('message', (subMap) => resolve(subMap)),
            ),
    ),
);

const sortedKeysSet = new Set();
for (const sm of subMaps) {
    for (const key of sm.keys()) {
        sortedKeysSet.add(key);
    }
}

const sortedKeys = [...sortedKeysSet].sort();
for (const key of sortedKeys) {
    let val = null;
    for (const sm of subMaps) {
        if (!sm.has(key)) continue;
        if (!val) {
            val = sm.get(key);
            continue;
        }
        const subMapItem = sm.get(key);
        val = {
            min: Math.min(val.min, subMapItem.min),
            max: Math.max(val.max, subMapItem.max),
            sum: val.sum + subMapItem.sum,
            total: val.total + subMapItem.total,
        };
    }

    // print result
    process.stdout.write(
        `${key}=${val.min}/${(val.sum / val.total).toFixed(1)}/${val.max}, `,
    );
}

process.stdout.end('\n');

close(fh.fd, () => {
    for (const w of workers) {
        w.terminate();
    }
});
