'use strict';

//Standard Library
var fs = require('fs');

//Internal
var Client = require('./client.js'),
    util = require('./util.js');

//External
var prom = require('promiscuous-tool'),
    _ = require('lodash');

module.exports = pack;

//Curried function for writing data to a file stream
function writeDocuments (fileStream) {
    return data => {
        return util.promiseWriteToFileStream(fileStream, _.map(data.hits.hits, JSON.stringify).join('\n'));
    };
}

//Curried document getter
function documentGetter ({client, index, type, sortBy = '_Created'}) {
    return (start, size) => client.get({
        index: index,
        type: type,
        path: '_search',
        body: {
            query: {
                "match_all": {}
            },
            start: start,
            size: size,
            sortBy: sortBy
        }
    });
}

function backupDocuments ({docGetter, fileStream, start = 0, size = 100}) {
    return docGetter(start, size)
        .then(data => prom.sequence([
            writeDocuments(fileStream),
            data => {
                if (start + size < data.hits.total) {
                    return backupDocuments({
                        docGetter,
                        fileStream,
                        size,
                        start: start + size
                    });
                } else {
                    return util.promiseEndFile(fileStream);
                }
            }], data));
}

function writeMappingBackup (fileStream, mapping) {
    return util.promiseWriteToFileStream(fileStream, JSON.stringify(mapping))
        .then(() => util.promiseEndFile(fileStream));
}

//Retrieve mappings from index or type
function mappings (client, index, type) {
    return client.get({
            index,
            type,
            path: '_mapping'
        }).then(response => !type ? response[index] : response[type]);
}

//Create file path string from base path, index and type
function backupPath (path, index, type) {
    return path + '/' + index + '_' + type + '_';
}

//Create both document and mapping file paths
function filePaths (path, index, type) {
    var base = backupPath(path, index, type);
    return [base + 'documents.json', base + 'mapping.json'];
}

function backupType ({client, index, type, filePath}) {
    var [docFileName, mappingFileName] = filePaths(filePath, index, type);
    if (!fs.existsSync(filePath)) {
        fs.mkdirSync(filePath);
    }
    return mappings(client, index, type)
        .then(mapping => prom.join(
            writeMappingBackup(fs.createWriteStream(mappingFileName, {flags: 'w'}), mapping),
            backupDocuments({
                docGetter: documentGetter({
                    client,
                    index,
                    type
                }),
                fileStream: fs.createWriteStream(docFileName, {flags: 'w'})})))
        .then(() => {
            return [docFileName, mappingFileName];
        });
}

//Get Index Settings
function indexSettings (client, index) {
    return client.get({
        index,
        path: '_settings'}).then(response => response[index]);
}

function indexWriteBackup (filePath, index, data) {
    return util.promiseWriteToFileStream(
            fs.createWriteStream(filePath + '/' + index + '_settings.json', {flags: 'w'}),
            JSON.stringify(data));
}

function backupIndex (client, index, filePath) {
    if (!fs.existsSync(filePath)) {
        fs.mkdirSync(filePath);
    }
    return indexSettings(client, index)
        .then(data => indexWriteBackup(filePath, index, data))
        //Backup types if they exist
        .then(() => mappings(client, index))
        .then(mappings => _.keys(mappings).length
            ? prom.all(_.map(mappings, (data, type) => backupType({
                client,
                index,
                type,
                filePath})))
            : [] )
        .then(_.flatten); //pretty up those deeply nested file paths.
}

//Retrieve indices from cluster status
function indicesFromStatus (status) {
    return _.keys(status.indices);
}

//Retrieve the status of the cluster
function clusterStatus (client) {
    return client.get({
            path: '_status'
        });
}

function backupCluster (client, filePath) {
    return clusterStatus(client)
        .then(indicesFromStatus)
        .then(indices => prom.all(_.map(indices, name => backupIndex(client, name, filePath))))
        .then(_.flatten); //pretty up those deeply nested file paths.
}

//Generate backup tar.gz
function pack ({host = 'localhost', port = 9200, index, type, filePath = 'temp'}) {
    var client = new Client({host, port});

    //append timestamp for unique id
    filePath += '/' + new Date().getTime();

    return (() => { 
        if (index && type) {
            return backupType({client, index, type, filePath});
        } else if (index) {
            return backupIndex(client, index, filePath);
        } else {
            return backupCluster(client, filePath);
        }
    }()).then(files => (process.stdout.write('\n' + files.join('\n')), filePath))
        .then(util.compress)
        .then(util.rmdirR, util.errorHandler);
}
