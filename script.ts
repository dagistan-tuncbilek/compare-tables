import {PrismaClient} from '@prisma/client'
import {createReadStream} from "fs";
import * as readline from "readline";

const prisma = new PrismaClient();
const chunkSize = 100000; //Default 100000
const limit = 3000000; // Default 3000000


async function main() {
    // First read and store files one by one, then test app.
    await clearTables();
    await readFileAndStore('./files/cdpSanitized.txt', 'cdpSanitized');
    await readFileAndStore('./files/dal_ticketList.txt', 'ticketList');
    await compareCounts();

    // Prepare results table
    const resultsTableCount = await prisma.results.count();
    if (resultsTableCount){
        console.log('Results table is not empty. Please truncate the table.');
        return;
    }
    let skip = 0;
    let stop = false;
    while (!stop) {
        const rows = await compareRecordsAndSaveDB(limit, skip);
        skip = skip + limit;
        if (rows < limit) stop = true;
    }
    console.log(`Total unmatched rows: ${await prisma.results.count()}`);

    // Prepare ComparedResults table
    skip = 0;
    stop = false;
    const tempLimit = limit / 3;
    while (!stop) {
        const rows = await finalizeAndPrepareComparedResults(tempLimit, skip);
        skip = skip + tempLimit;
        if (rows < tempLimit) stop = true;
    }
    console.log('Compared table prepared. ALL FINISHED!!!');
}

async function clearTables() {
    await prisma.$queryRaw`TRUNCATE TABLE results;`;
    await prisma.$queryRaw`TRUNCATE TABLE comparedresults;`;
    await prisma.$queryRaw`ALTER TABLE results AUTO_INCREMENT = 1;`;
    await prisma.$queryRaw`TRUNCATE TABLE cdpsanitized;`;
    await prisma.$queryRaw`TRUNCATE TABLE ticketlist;`;
    console.log('Database tables truncated')
}

async function readFileAndStore(filepath: string, table: 'cdpSanitized' | 'ticketList') {
    console.log('Reading: ' + filepath);
    const ticketLists = await prisma[table].count();
    console.log('Table is not empty: ' + table);
    if (ticketLists) return;
    return new Promise((resolve, reject) => {
        const readStream = createReadStream(filepath);
        const rl = readline.createInterface({
            input: readStream,
            crlfDelay: Infinity // To handle different line ending formats
        });
        const data: { id: string, value: string }[] = [];
        rl.on('line', (line: string) => {
            const [id, value] = line
                .replace('{', '')
                .replace('}', '')
                .replace(/\s+/g, ' ')
                .split(' ');
            data.push({id, value});
        });

        rl.on('close', async () => {
            // File reading is complete
            console.log('File reading complete. Now writing to database...');
            const chunkSize = 10000;
            for (let i = 0; i < data.length; i += chunkSize) {
                const chunk = data.slice(i, i + chunkSize);
                await prisma[table].createMany({data: chunk});
                console.log(`Records saved: ${i} to ${i + chunkSize}`);
            }
            resolve(true);
        });

        rl.on('error', (err) => {
            console.log(err);
            reject(err);
        })
    });
}

async function compareCounts() {
    const ticketLists = await prisma.ticketList.count();
    const cdpSanitized = await prisma.cdpSanitized.count();
    console.log(ticketLists === cdpSanitized
        ? `Counts compare: Passed. Total ${ticketLists} records each`
        : `Counts compare: Fails.  Difference : ${Math.abs(ticketLists - cdpSanitized)}`
    );
}

async function compareRecordsAndSaveDB(limit: number, skip: number) {
    // const resultFile = './results-1.json';
    const unmatchedRows: { id: string, value: string }[] = await prisma.$queryRaw`
        SELECT *
        FROM (SELECT ticketlist.id, ticketlist.value
              FROM ticketlist
              UNION ALL
              SELECT cdpsanitized.id, cdpsanitized.value
              FROM cdpsanitized) t
        GROUP BY id, value
        HAVING COUNT(*) = 1
            LIMIT ${skip} , ${limit};
    `;

    // If values in the columns involved in the comparison are identical, no row returns.
    if (unmatchedRows.length) {
        console.log(`Some rows are not matched. Total: ${unmatchedRows.length}`);
        for (let i = 0; i < unmatchedRows.length; i += chunkSize) {
            const chunk = unmatchedRows.slice(i, i + chunkSize);
            await prisma.results.createMany({
                data: chunk.map(row => ({
                    value: row.value,
                    modelId: row.id
                }))
            });
            console.log(`Results saved: ${i + skip} to ${i + chunkSize + skip}`);
        }
    } else {
        console.log(`All records are identical`);
    }
    return unmatchedRows.length;
}

async function finalizeAndPrepareComparedResults(limit: number, skip: number) {
    const comparedResults: {
        id: string,
        ticketListValue?: string,
        cdpSanitizedValue?: string
    }[] = await prisma.$queryRaw`
        SELECT DISTINCT results.modelId AS id,
               cdpsanitized.value AS cdpSanitizedValue,
               ticketlist.value   AS ticketListValue
        FROM results
                 LEFT JOIN cdpsanitized
                            ON results.modelId = cdpsanitized.id
                 LEFT JOIN ticketlist
                            ON results.modelId = ticketlist.id
            LIMIT ${skip}, ${limit};
    `;

    for (let i = 0; i < comparedResults.length; i += chunkSize) {
        const chunk = comparedResults.slice(i, i + chunkSize);
        await prisma.comparedResults.createMany({data: chunk});
        console.log(`ComparedResults saved: ${i + skip} to ${i + chunkSize + skip}`);
    }
    return comparedResults.length;
}

main()
    .then(async () => {
        await prisma.$disconnect()
    })
    .catch(async (e) => {
        console.error(e)
        await prisma.$disconnect()
        process.exit(1)
    })
