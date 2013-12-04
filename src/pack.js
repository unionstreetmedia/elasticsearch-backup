'use strict';

//Standard Library
var fs = require('fs');

//Internal
var Client = require('./client.js'),
    util = require('./util.js');

//External
var prom = require('promiscuous-tool'),
    _ = require('lodash');

//Globals
var THROTTLE = 50;

module.exports = pack;

//Curried function for writing data to a file stream
function writeDocuments (fileStream) {
    return data => util.promiseWriteToFileStream(
        fileStream,
        _.map(data.hits.hits,
            data => JSON.stringify({
                index: {
                    _index: data._index,
                    _type: data._type,
                    _id: data._id,
                }
            }) + '\n' + JSON.stringify(data._source)).join('\n'));
}

//Curried document getter
function documentScroller (client, scrollID) {
    return () => client.get({
        path: '_search/scroll?scroll=' + (THROTTLE + 100) + 'm&scroll_id=' + scrollID
    }).then(data => {
        //Reassign scrollID
        scrollID = data['_scroll_id'];
        return data;
    });
}

//Kick off a scroll search
function startScroll ({client, index, type, size = 100}) {
    return client.get({
        index: index,
        type: type,
        path: '_search?search_type=scan&scroll=' + (THROTTLE + 100) + 'm&size=' + size,
        body: {
            query: {
                "match_all": {}
            }
        }
    }).then(data => documentScroller(client, data['_scroll_id']));
}

function backupDocuments ({docScroller, fileStream}) {
    return docScroller()
        .then(data => prom.sequence([
            writeDocuments(fileStream),
            data => {
                if (data.hits.hits.length) {
                    process.stdout.write('\rwriting ' + fileStream.path + ' : ' + fileStream.bytesWritten + '\r');
                    return prom.delay(THROTTLE, () => backupDocuments({
                        docScroller,
                        fileStream
                    }));
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
        }).then(response => type == null ? response[index] : response);
}

function backupType ({client, index, type, filePath}) {
    filePath = filePath + '/' + type;

    if (!fs.existsSync(filePath)) {
        fs.mkdirSync(filePath);
    }

    var docFileName = filePath + '/documents.json',
        mappingFileName = filePath + '/mapping.json';

    return mappings(client, index, type)
        .then(mapping => prom.join(
            writeMappingBackup(fs.createWriteStream(mappingFileName, {flags: 'w'}), mapping),
            startScroll({
                    client,
                    index,
                    type
            }).then(docScroller => backupDocuments({
                docScroller,
                fileStream: fs.createWriteStream(docFileName, {flags: 'w'})
            }))
        ))
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

function indexWriteSettings (filePath, index, data) {
    var innerSettings = {};
    innerSettings[index] = {
        'number_of_shards': data.settings['index.number_of_shards'],
        'number_of_replicas': data.settings['index.number_of_replicas']
    };
    return util.promiseWriteToFileStream(
        fs.createWriteStream(filePath + '/settings.json', {flags: 'w'}),
        JSON.stringify({settings: innerSettings}));
}

function createIndexDir (filePath, index) {
    filePath = filePath + '/' + index;
    if (!fs.existsSync(filePath)) {
        fs.mkdirSync(filePath);
    }
    return filePath;
}

function backupIndex (client, index, filePath) {
    //Store index data in single directory
    filePath = createIndexDir(filePath, index);
    return indexSettings(client, index)
        .then(data => indexWriteSettings(filePath, index, data))
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
function pack ({host = 'localhost', port = 9200, index, type, filePath = 'temp', no_compress = false}) {
    var client = new Client({host, port});

    //append timestamp for unique id
    filePath += '/' + new Date().getTime();

    //Make route directory
    if (!fs.existsSync(filePath)) {
        fs.mkdirSync(filePath);
    }

    var promise = (() => { 
        if (index && type) {
            filePath = createIndexDir(filePath, index);
            return backupType({client, index, type, filePath});
        } else if (index) {
            return backupIndex(client, index, filePath);
        } else {
            return backupCluster(client, filePath);
        }
    }()).then(files => (process.stdout.write('\n' + files.join('\n')), filePath));

    //Compress and archive the data
    if (!no_compress) {
        promise = promise.then(util.compress)
        .then(util.rmdirR, util.errorHandler);
    }

    return promise;
}
