import { readFileSync } from 'node:fs';

const fileName = process.argv[2];
const hm = new Map();

// all in RAM

const data = readFileSync(fileName, { encoding: 'utf8' });

const lines = data.split('\n');
for (const line of lines) {
    const [key, value] = line.split(';');
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

[...hm.keys()].sort().forEach((key) => {
    const hmItem = hm.get(key);
    process.stdout.write(
        `${key}=${hmItem.min}/${(hmItem.sum / hmItem.total).toFixed(1)}/${
            hmItem.max
        }, `,
    );
});
process.stdout.end('\n');
