import {PrismaClient} from '@prisma/client'
import {createReadStream} from "fs";
import * as readline from "readline";

const prisma = new PrismaClient();
const chunkSize = 10;
let limit = 30;

async function clearTables() {
    await prisma.$queryRaw`TRUNCATE TABLE results;`;
    await prisma.$queryRaw`ALTER TABLE results AUTO_INCREMENT = 1;`;
    await prisma.$queryRaw`TRUNCATE TABLE cdpsanitized;`;
    await prisma.$queryRaw`TRUNCATE TABLE ticketlist;`;
    console.log('Database tables truncated')
}

async function main() {
    // First read and store files one by one, then test app.
    await clearTables();
    await readFileAndStore('./files/cdpSanitized.txt', 'cdpSanitized');
    await readFileAndStore('./files/dal_ticketList.txt', 'ticketList');
    await compareCounts();
    const resultsTableCount = await prisma.results.count();
    if (resultsTableCount){
        console.log('Results table is not empty. Please truncate the table.');
        return;
        // await prisma.results.deleteMany({});
    }

    let skip = 0;
    while (limit !== 0) {
        const rows = await compareRecordsAndSaveDB(limit, skip);
        skip = skip + limit;
        if (rows < limit) limit = 0;
    }
    console.log(`Total unmatched rows: ${await prisma.results.count()}`);
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
            LIMIT ${skip}
             , ${limit};
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

async function compareCounts() {
    const ticketLists = await prisma.ticketList.count();
    const cdpSanitized = await prisma.cdpSanitized.count();
    console.log(ticketLists === cdpSanitized
        ? `Counts compare: Passed. Total ${ticketLists} records each`
        : `Counts compare: Fails.  Difference : ${Math.abs(ticketLists - cdpSanitized)}`
    );
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
    })

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
