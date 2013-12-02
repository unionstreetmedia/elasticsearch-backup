'use strict';

//Standard Library
var fs = require('fs');

//Internal
var util = require('./util.js'),
    Client = require('./client.js');

//External
var prom = require('promiscuous-tool'),
    _ = require('lodash');

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
function getIndexDirectories (filePath) {
    return prom((fulfill, reject) => fs.readdir(filePath, (err, files) => {
        if (err) {
            return reject(err);
        }
        fulfill(files);
    }));
}

//[id]/[index]/settings.json
function postSettings (client, filePath) {
    return filePath;
    return client.post({
        index: filePath.substring(filePath.lastIndexOf('/')),
        body: fs.readFileSync(filePath + '/settings.json')
    });
}

//[id]/[index]/[type]_mapping.json
function putMappings (client, filePath) {
    return filePath;
}

//[id]/[index]/[type]_documents.json
function bulkDocuments (client, filePath) {
    return filePath;
}

//Repopulate single index
function buildIndex (client, filePath, index) {
    filePath = filePath + '/' + index;
    return postSettings(client, filePath);/*
        .then(putMappings)
        .then(bulkDocuments)*/
}

//Repopulate indexes
function buildIndexes (client, filePath) {
    return getIndexDirectories(filePath)
        .then(_.partialRight(_.map, _.partial(buildIndex, client, filePath)))
}

//Extract and populate cluster from tar.gz
function unpack ({host, port, filePath = 'temp', name = 'latest'}) {
    var client = new Client({host, port});
    return util.extract(filePath + '/' + version(filePath, name))
        //.then(_.partial(buildIndexes, client))
        .then(data => console.log(data), util.errorHandler);
}
