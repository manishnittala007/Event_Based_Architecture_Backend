const ZongJi = require('zongji');
const mysql = require('mysql');
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const app = express();
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

const port = 3000;
let clients = [];

const connection = mysql.createConnection({
    host: 'staging-prod-db.crhg7zleeuhf.ap-south-1.rds.amazonaws.com',
    user: 'hakate',
    password: '}*w;caq[&Na75HtbamFKbc+[zU&ns8H-',
    database: 'edwisely_college'
});

const zongji = new ZongJi({
    host: 'staging-prod-db.crhg7zleeuhf.ap-south-1.rds.amazonaws.com',
    user: 'hakate',
    password: '}*w;caq[&Na75HtbamFKbc+[zU&ns8H-'
});

zongji.on('binlog', async function (evt) {
    if (evt.getEventName() === 'query') {
        if (evt.query !== 'BEGIN') {
            // Extracting table name and other details from the query
            const parsedData = await extractDetailsFromQuery(evt.query);

            // Send the parsed data to all clients
            await sendEventsToAll(parsedData);
        }
    }
});

async function extractDetailsFromQuery(query) {
    console.log('Processing query:', query);

    let id = null;
    let name = null;
    let description = null;
    let table = null;

    if (query.startsWith('UPDATE')) {
        const match = query.match(/UPDATE `(\w+)`\.`(\w+)` SET `(\w+)` = '([^']*)' WHERE \(`(\w+)` = '([^']*)'\)/);
        if (match) {
            table = match[2];
            const setField = match[3];
            const setValue = match[4];
            id = match[6];
            
            // Fetch both name and description using the ID
            const result = await getNameById(table, id);
            if (result) {
                name = result.name;
                description = result.description;
            }

            console.log('Table:', table);
            console.log('ID:', id);
            console.log('Name:', name);
            console.log('Description:', description);
        }
    }

    console.log('Parsed data:', { query, id, name, description, table });
    return { query, id, name, description, table };
}

async function getNameById(table, id) {
  return new Promise((resolve, reject) => {
      const query = `SELECT name, description FROM \`${table}\` WHERE id = ?`;
      connection.query(query, [id], (error, results) => {
          if (error) {
              console.error('Error fetching name and description:', error);
              return reject(error);
          }
          if (results.length > 0) {
              resolve({ name: results[0].name, description: results[0].description });
          } else {
              resolve(null);
          }
      });
  });
}

zongji.start({
    includeEvents: ['query']
});

process.on('SIGINT', function () {
    console.log('Got SIGINT.');
    zongji.stop();
    process.exit();
});

app.listen(port, () => {
    console.log(`Example app listening on port ${port}`);
});

// This is for client addition
function eventsHandler(request, response, next) {
    const headers = {
        'Content-Type': 'text/event-stream',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache'
    };
    response.writeHead(200, headers);

    // Fetch initial top 3 rows from the questionnaire table
    fetchInitialRows().then(initialRows => {
        response.write(`data: ${JSON.stringify(initialRows)}\n\n`);
    });

    const clientId = Date.now();

    const newClient = {
        id: clientId,
        response
    };

    clients.push(newClient);

    request.on('close', () => {
        console.log(`${clientId} Connection closed`);
        clients = clients.filter(client => client.id !== clientId);
    });
}

async function fetchInitialRows() {
    return new Promise((resolve, reject) => {
        const query = `SELECT * FROM questionnaire LIMIT 3`;
        connection.query(query, (error, results) => {
            if (error) {
                console.error('Error fetching initial rows:', error);
                return reject(error);
            }
            resolve(results);
        });
    });
}

function sendEventsToAll(newQueries) {
    clients.forEach(client => client.response.write(`data: ${JSON.stringify(newQueries)}\n\n`));
}

app.get('/events', eventsHandler);
