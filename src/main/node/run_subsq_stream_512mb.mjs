import { createReadStream } from 'node:fs';
import { finished } from 'node:stream/promises';

const fileName = process.argv[2];
const hm = new Map();

//  512mb is default? limit
const chunkSize = 0x1fffffe7; // 0x1fffffe8 is max size
// const chunkSize = 1 * 1024 * 1024 * 1024; // 2gb - ERR_STRING_TOO_LONG

const rs = createReadStream(fileName, {
    encoding: 'utf8',
    highWaterMark: chunkSize,
});

// for await (const hugeChunk of rs) {
//     processText(hugeChunk.toString());
// }

rs.on('data', processText);

// process lines
function processText(data) {
    console.log('processText for chunk of size: ', data.length);

    const lines = data.split('\n');
    for (const line of lines) {
        const [key, value] = line.split(';');
        if (!key || !value) continue;
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
}

await finished(rs);

rs.close();
rs.removeAllListeners();

// print result

[...hm.keys()].sort().forEach((key) => {
    const hmItem = hm.get(key);
    process.stdout.write(
        `${key}=${hmItem.min}/${(hmItem.sum / hmItem.total).toFixed(1)}/${
            hmItem.max
        }, `,
    );
});
process.stdout.end('\n');

// const stepSize = 0.2e10;
// const iterationN = 12;
// const chunkSize = ~~(stepSize / iterationN);

// // const readStreams = [];
// for (let i = 0; i < iterationN; i++) {
//     const rs = forkReadStream(i * chunkSize, (i + 1) * chunkSize);
//     // readStreams.push(rs);
//     console.log(`start ${i * chunkSize} ${i * chunkSize + chunkSize}`);
// }

// // process read stream
// function forkReadStream(from, to) {
//     const rs =  createReadStream(fileName, {
//         encoding: 'utf8',
//         start: from,
//         end: to,
//     });
//     processText(rs);
//     rs.close();
// }
