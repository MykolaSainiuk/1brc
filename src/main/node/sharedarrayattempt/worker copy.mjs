// Worker Thread
import { workerData, isMainThread } from 'node:worker_threads';
import { open } from 'node:fs/promises';

if (isMainThread) {
    throw new Error('This file should NOT be run in the main thread');
}

// Access the shared object and lock passed from the main thread
const { sharedMap, lock, fileName, start, end } = workerData;
sharedMap.set('test', 'test');

const file = await open(fileName, 'r');

let fileLines;
try {
    fileLines = file.readLines({
        encoding: 'utf8',
        start,
        end,
    });
} catch {
    fileLines = file.readLines({
        encoding: 'utf8',
        start,
    });
}

for await (const line of fileLines) {
    acquireLock();

    processLine(line);

    releaseLock();
}
// await Promise.all(fileLines.map(processLine));

fileLines.close();
await file.close();

// Function to acquire the lock
function acquireLock() {
    while (Atomics.compareExchange(lock, 0, 0, 1) !== 0) {
        // Wait for the lock to be released
        Atomics.wait(lock, 0, 0, 10); // Sleep for a short duration
    }
}

// Function to release the lock
function releaseLock() {
    Atomics.store(lock, 0, 0);
    Atomics.notify(lock, 0, 1);
}

/**
 * LOGIC
 */

// process line
function processLine(data) {
    const [key, value] = data.split(';');
    if (!key || !value) return; // TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    const iv = +value;
    const hm = sharedMap;
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
