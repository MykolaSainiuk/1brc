import { stat } from 'node:fs/promises';
import { finished } from 'node:stream/promises';
import { Worker, isMainThread } from 'node:worker_threads';

const fileName = process.argv[2];
const workers = [];

const chunkSize = ~~(0.005 * 1024 * 1024 * 1024); // 2gb

const fileStat = await stat(fileName);
const maxChunksAmount = ~~(fileStat.size / chunkSize) + 1; // amount of workers
// const maxChunksAmount = 12 * 2;

if (!isMainThread) {
    throw new Error('This file should be run in the main thread');
}

// Main Thread

console.debug(
    'fileStat.size',
    fileStat.size,
    'maxChunksAmount',
    maxChunksAmount,
);

const workerFilePath = './src/main/node/worker.mjs';
for (let i = 0; i < maxChunksAmount; i++) {
    const worker = new Worker(workerFilePath, {
        workerData: {
            fileName,
            start: i * chunkSize,
            end: (i + 1) * chunkSize,
        },
        resourceLimits: {
            maxOldGenerationSizeMb: ~~(chunkSize / 1024 / 1024),
        },
    });
    workers.push(worker);
}

await Promise.all(workers.map((w) => finished(w.stdout)));

console.debug('sharedMap', sharedMap);

// print result
const hm = sharedMap;
[...hm.keys()].sort().forEach((key) => {
    const hmItem = hm.get(key);
    process.stdout.write(
        `${key}=${hmItem.min}/${(hmItem.sum / hmItem.total).toFixed(1)}/${
            hmItem.max
        }, `,
    );
});
process.stdout.end('\n');
