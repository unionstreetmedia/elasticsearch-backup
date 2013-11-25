var backup = require('./backup.js'),
    prom = require('./promiscuous-config.js');

backup.run({
    host: process.argv[2],
    port: process.argv[3],
    filePath: process.argv[4],
    index: process.argv[5],
    type: process.argv[6]
});
