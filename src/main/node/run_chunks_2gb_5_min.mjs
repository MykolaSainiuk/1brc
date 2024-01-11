import { open, stat } from 'node:fs/promises';

const fileName = process.argv[2];
const hm = new Map();
const chunkSize = 2 * 1024 * 1024 * 1024; // 521mb - ERR_STRING_TOO_LONG

const fileStat = await stat(fileName);
const maxChunksAmount = ~~(fileStat.size / chunkSize) + 1;
// const maxChunksAmount = 12 * 2;

console.debug('fileStat.size', fileStat.size, 'maxChunksAmount', maxChunksAmount);

const file = await open(fileName, 'r');
const promises = [];
for (let i = 0; i < maxChunksAmount; i++) {
    promises.push(
        (async (idx) => {
            const fileLines = file.readLines({
                encoding: 'utf8',
                start: idx * chunkSize,
                end: (idx + 1) * chunkSize,
            });

            for await (const line of fileLines) {
                processLine(line);
            }
            // await Promise.all(fileLines.map(processLine));

            await fileLines.close();
        })(i),
    );
}

await Promise.all(promises);
await file.close();


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
