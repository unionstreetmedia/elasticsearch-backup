var backup = require('./backup.js'), prom = require('./promiscuous-config.js');
backup.run({
  filePath: process.argv[2] || 'temp',
  index: process.argv[3],
  type: process.argv[4]
});
