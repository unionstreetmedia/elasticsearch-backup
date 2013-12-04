'use strict';

//Standard Library
var fs = require('fs');

//Internal
var util = require('./util.js'),
    Client = require('./client.js');

//External
var prom = require('promiscuous-tool'),
    _ = require('lodash'),
    byline = require('byline');

module.exports = unpack;

//Find the latest tar
function latest (filePath) {
    return _.last(_.sortBy(fs.readdirSync(filePath)));
}

//Apply tar.gz if missing
function tarExtension (file) {
    return file.substr(-7) === '.tar.gz' ? file : file + '.tar.gz';
}

//Get correct version name
function version (filePath, name) {
    return tarExtension(name === 'latest' ? latest(filePath) : name);
}

//Find index directories
function getDirectories (filePath) {
    return prom((fulfill, reject) => fs.readdir(filePath, (err, files) => {
        if (err) {
            return reject(err);
        }
        //filter out files with extensions
        fulfill(files.filter(file => file.indexOf('.') == -1));
    }));
}

function getESPath (filePath) {
    return filePath.split('/').slice(2);
}

//[id]/[index]/settings.json
function putSettings (client, filePath) {
    var [index] = getESPath(filePath);
    return client.put({
        index,
        body: fs.readFileSync(filePath + '/settings.json').toString()
    })
}

//[id]/[index]/[type]/mapping.json
function buildTypes (client, filePath) {
    return getDirectories(filePath)
        .then(types => prom.all(_.map(types, type => prom.sequence([
            _.partial(putMapping, client, filePath + '/' + type),
            _.partial(bulkDocuments, client, filePath + '/' + type)
        ]))));
}

function putMapping (client, filePath) {
    var [index, type] = getESPath(filePath);
    return client.put({
        index,
        type,
        path: '_mapping',
        body: fs.readFileSync(filePath + '/mapping.json').toString()
    });
}

//[id]/[index]/[type]/documents.json
function bulkDocuments (client, filePath) {
    filePath = filePath + '/documents.json';
    return prom((fulfill, reject) => {
        var maxLines = 2000,
            buffer = [],
            promises = [],
            send = function () {
                if (buffer.length) {
                    promises.push(client.post({
                        path: '_bulk',
                        body: buffer.join('\n')
                    }).then(x => x, util.log));
                    buffer = [];
                }
            };
        if (fs.existsSync(filePath)) {
            byline(fs.createReadStream(filePath))
                .on('data', line => {
                    if (buffer.length == maxLines) {
                        send();
                    }
                    buffer.push(line.toString());
                })
                .on('end', () => {
                    send();
                    fulfill(promises);
                });
        } else {
            fulfill('no documents');
        }
    });
}

//Repopulate single index
function buildIndex (client, filePath, index) {
    filePath = filePath + '/' + index;
    return putSettings(client, filePath)
        .then(_.partial(buildTypes, client, filePath));
}

//Repopulate indexes
function buildIndexes (client, filePath) {
    return getDirectories(filePath)
        .then((indexes) => prom.all(_.map(indexes, _.partial(buildIndex, client, filePath))))
}

//Extract and populate cluster from tar.gz
function unpack ({host = 'localhost', port = 9200, filePath = 'temp', name = 'latest', rebuild = true}) {
    var client = new Client({host, port}),
        promise = util.extract(filePath + '/' + version(filePath, name));

    if (rebuild) {
        promise = promise.then(_.partial(buildIndexes, client));
    }
    
    return promise.then(data => console.log(data), util.errorHandler);
}
