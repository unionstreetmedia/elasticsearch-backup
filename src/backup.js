var http = require('http'),
    prom = require('promiscuous-tool'),
    _ = require('lodash'),
    fs = require('fs'),
    fstream = require('fstream'),
    zlib = require('zlib'),
    tar = require('tar');

exports.run = backup;

process.on('uncaughtException',function(e) {
    //Got additional debugging
    console.log("Caught unhandled exception: " + e);
    console.log(" ---> : " + e.stack);
});

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

//Return function for writing data to a file stream
function writeDocuments (fileStream) {
    return data => {
        return promiseWriteToFileStream(fileStream, _.map(data.hits.hits, JSON.stringify).join('\n'));
    };
}

//Document Backup
function documents ({client, index, type, start = 0, size = 100, sortBy = '_Created', fileStream}) {
    return client.get({
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
        }).then(data => prom.sequence([
            writeDocuments(fileStream),
            (data) => {
                if (start + size < data.hits.total) {
                    return documents({
                        client: client,
                        index: index,
                        type: type,
                        start: start + size,
                        size: size,
                        sortBy: sortBy,
                        fileStream: fileStream
                    });
                } else {
                    return promiseEndFile(fileStream);
                }
            }], data));
}

//Write mapping backup files
function writeMappingBackup ({fileStream, index, name, mapping}) {
    return promiseWriteToFileStream(fileStream, JSON.stringify(mapping))
        .then(() => promiseEndFile(fileStream));
}

function mappings ({client, index, type = null}) {
   return client.get({
            index: index,
            type: type,
            path: '_mapping'
        }).then(response => {
            if (!type) {
                return response[index];
            }
            return response[type];
        });
}

//Type Backup
function backupType ({client, index, type, filePath}) {
    var fileBase = filePath + '/' + index + '_' + type + '_',
        docFileName = fileBase + 'documents.json',
        mappingFileName = fileBase + 'mapping.json';
    fs.mkdirSync(filePath);
    return mappings({
            client: client,
            index: index,
            type: type})
        .then(mapping => prom.join(
            writeMappingBackup({
                index: index,
                name: type,
                mapping: mapping,
                fileStream: fs.createWriteStream(mappingFileName, {flags: 'w'})}),
            documents({
                client: client,
                index: index,
                type: type,
                fileStream: fs.createWriteStream(docFileName, {flags: 'w'})})))
        .then(() => {
            return [docFileName, mappingFileName];
        });
}

//Index Backup
function backupIndex (client, index, filePath) {
    return mappings({
            client: client,
            index: index})
        .then(mappings => prom.all(_.map(mappings, (data, name) => backupType({
                    client: client,
                    index: index,
                    type: name,
                    filePath: filePath}))))
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

//Cluster Backup
function backupCluster (client, filePath) {
    return clusterStatus(client)
        .then(indicesFromStatus)
        .then(indices => prom.all(_.map(indices, name => backupIndex(client, name, filePath))))
        .then(_.flatten); //pretty up those deeply nested file paths.
}

//Main function
function backup ({host = 'localhost', port = 9200, index, type, filePath = 'temp'}) {
    var client = new Client({host: host, port: port});

    //append timestamp for unique id
    filePath += '/' + new Date().getTime();

    return (() => { 
        if (index && type) {
            return backupType({client: client, index: index, type: type, filePath: filePath});
        } else if (index) {
            return backupIndex(client, index, filePath);
        } else {
            return backupCluster(client, filePath);
        }
    }()).then((files) => prom((fulfill, reject) => {
        //Files written
        process.stdout.write('\n' + files.join('\n'));

        //tar and gzip the directory
        fstream.Reader({path: filePath, type: 'Directory'})
            .pipe(tar.Pack())
            .pipe(zlib.Gzip())
            .pipe(fstream.Writer(filePath + '.tar.gz'))
            .on('error', reject)
            .on('close', () => {
                //delete temp files
                rmdirR(filePath);

                process.stdout.write('\ncompressed to ' + filePath + '.tar.gz \n');
                fulfill();
            });
    })).then(() => {}, error => console.log(error));
}

// Elasticsearch client
function Client ({host = 'localhost', port = 9200}) {
    this.host = host;
    this.port = port;
}

Client.prototype.get = function ({index = null, type = null, path, body = null}) {
    var path = [index, type, path].filter(val => val).join('/');
    return prom((fulfill, reject) => {
        var request = http.request({
            host: this.host,
            port: this.port,
            path: path,
            method: 'get',
            headers: {
                'Content-Type': 'application/json'
            }
        }, response => {
            if (response.statusCode == 200) {
                var data = '';
                response.on('data', chunk => data += chunk)
                    .once('error', error => reject(error))
                    .once('end', () => fulfill(JSON.parse(data)));
            } else {
                fail(response);
            }
        });
        if (body) {
            request.write(JSON.stringify(body));
        }
        request.once('error', error => reject(error));
        request.end();
    });
}

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
