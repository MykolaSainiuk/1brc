import { Worker } from 'node:worker_threads';
import { stat } from 'node:fs/promises';

const fileName = process.argv[2];

const chunkSize = ~~(1 * 1024 * 1024 * 1024); // 512mb
// const maxOldGenerationSizeMb = ~~(~~(chunkSize / 1024 / 1024) * 1.5);

const fileStat = await stat(fileName);
const numberOfWorkers = ~~(fileStat.size / chunkSize) + 1;
console.debug('numberOfWorkers', numberOfWorkers);
// const numberOfWorkers = 13;

// Main Thread

const workerFilePath = './src/main/node/worker.mjs';
const workers = [];

for (let i = 0; i < numberOfWorkers; i++) {
    const worker = new Worker(workerFilePath, {
        workerData: {
            fileName,
            start: i * chunkSize,
            end: i + 1 === numberOfWorkers ? 0 : (i + 1) * chunkSize,
        },
        stdin: false,
        stdout: false,
        stderr: false,
        // resourceLimits: {
        //     maxOldGenerationSizeMb,
        // },
    });
    workers.push(worker);
}

const subMaps = await Promise.all(
    workers.map(
        (w) =>
            new Promise((resolve) => {
                w.once('message', (subMap) => resolve(subMap));
            }),
    ),
);
for (const w of workers) {
    w.terminate();
}

const sortedKeysSet = new Set();
for (const sm of subMaps) {
    for (const key of sm.keys()) {
        sortedKeysSet.add(key);
    }
}

const sortedKeys = [...sortedKeysSet].sort();
// const mergedMap = new Map();

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
        `${key}=${val.min}/${(val.sum / val.total).toFixed(1)}/${
            val.max
        }, `,
    );
}
process.stdout.end('\n');

// // print result
// for (const key of sortedKeys) {
//     const hmItem = mergedMap.get(key);
//     process.stdout.write(
//         `${key}=${hmItem.min}/${(hmItem.sum / hmItem.total).toFixed(1)}/${
//             hmItem.max
//         }, `,
//     );
// }
// process.stdout.end('\n');

// /**
//  * LOGIC
//  */
// // merge partial maps

// function mergeMaps(subMaps) {
//     const sortedKeysSet = new Set();
//     for (const sm of subMaps) {
//         for (const key of sm.keys()) {
//             sortedKeysSet.add(key);
//         }
//     }

//     const sortedKeys = [...sortedKeysSet].sort();
//     const mergedMap = new Map();

//     for (const key of sortedKeys) {
//         for (const sm of subMaps) {
//             if (!sm.has(key)) continue;
//             if (!mergedMap.has(key)) {
//                 mergedMap.set(key, sm.get(key));
//                 continue;
//             }
//             const subMapItem = sm.get(key);
//             const mapItem = mergedMap.get(key);
//             mergedMap.set(key, {
//                 min: Math.min(mapItem.min, subMapItem.min),
//                 max: Math.max(mapItem.max, subMapItem.max),
//                 sum: mapItem.sum + subMapItem.sum,
//                 total: mapItem.total + subMapItem.total,
//             });
//         }
//     }

//     return { mergedMap, sortedKeys };
// }
