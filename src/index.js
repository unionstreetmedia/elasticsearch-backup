var backup = require('./backup.js'),
    prom = require('./promiscuous-config.js');

options = {};
maybe(options, 'host', process.argv[2]);
maybe(options, 'port', process.argv[3]);
maybe(options, 'filePath', process.argv[4]);
maybe(options, 'index', process.argv[5]);
maybe(options, 'type', process.argv[6]);

backup.run(options);

function maybe (object, name, value) {
    if (value) {
        object[name] = value;
    }
    return object;
}

