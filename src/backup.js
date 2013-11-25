'use strict';

//Standard Library
var zlib = require('zlib'),
    fs = require('fs');

//Internal
var Client = require('./client.js');

//External
var prom = require('promiscuous-tool'),
    _ = require('lodash'),
    fstream = require('fstream'),
    tar = require('tar');

module.exports.pack = pack;
module.exports.unpack = unpack;

//Write to file stream and return promise
function promiseWriteToFileStream (fileStream, data) {
    return prom((fulfill, reject) => {
        process.stdout.write('\rwriting to ' + fileStream.path + ' : ' + fileStream.bytesWritten + ' bytes');
        if (fileStream.write(data)) {
            fulfill(fileStream);
        } else {
            fileStream.once('drain', () => fulfill(fileStream));
        }
    });
}

//End file stream and return promise
function promiseEndFile (fileStream) {
    return prom((fulfill, reject) => {
        fileStream.once('finish', fulfill);
        fileStream.end();
    });
}

//Curried function for writing data to a file stream
function writeDocuments (fileStream) {
    return data => {
        return promiseWriteToFileStream(fileStream, _.map(data.hits.hits, JSON.stringify).join('\n'));
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
                    return promiseEndFile(fileStream);
                }
            }], data));
}

function writeMappingBackup (fileStream, mapping) {
    return promiseWriteToFileStream(fileStream, JSON.stringify(mapping))
        .then(() => promiseEndFile(fileStream));
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

function backupIndex (client, index, filePath) {
    return mappings(client, index)
        //Backup type if it exists
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

function compress (filePath) {
    return prom((fulfill, reject) => {
        //tar and gzip the directory
        if (fs.existsSync(filePath)) {
            fstream.Reader({path: filePath, type: 'Directory'})
                .pipe(tar.Pack())
                .pipe(zlib.Gzip())
                .pipe(fstream.Writer(filePath + '.tar.gz'))
                .on('error', reject)
                .on('close', () => {
                    process.stdout.write('\ncompressed to ' + filePath + '.tar.gz \n');
                    fulfill(filePath);
                });
        } else {
            process.stdout.write('\nNo file to compressed to ' + filePath + '.tar.gz \n');
            fulfill(filePath);
        }
    });
}

function extract (filePath, version) {
    return prom((fulfill, reject) => {
        var file = filePath + '/' + version + '.tar.gz';
        //tar and gzip the directory
        if (fs.existsSync(file)) {
            fstream.Reader({path: file, type: 'file'})
                .pipe(zlib.Gunzip())
                .pipe(tar.Extract({path: filePath}))
                .on('error', reject)
                .on('close', () => {
                    process.stdout.write('\nextracted to ' + filePath + '\n');
                    fulfill(filePath);
                });
        } else {
            process.stdout.write('\nNo file to extract to ' + filePath + '\n');
            fulfill(filePath);
        }
    });
}

//Recursively delete a directory
function rmdirR (path) {
    if (fs.existsSync(path)) {
        fs.readdirSync(path).forEach(function(file){
            var curPath = path + "/" + file;
            if (fs.lstatSync(curPath).isDirectory()) { // recurse
                rmdirR(curPath);
            } else { // delete file
                fs.unlinkSync(curPath);
            }
        });
        fs.rmdirSync(path);
    }
}

function errorHandler (error) {
    if (error.errno === 34 && error.code === 'ENOENT') {
        console.log('Target directory does not exist');
    } else {
        console.log(error);
    }
    return error
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
        .then(compress)
        .then(rmdirR, errorHandler);
}

//Extract and populate cluster from tar.gz
function unpack ({host, port, filePath = 'temp', version}) {
    return prom((fullfill, reject) => extract(filePath, version));
}
