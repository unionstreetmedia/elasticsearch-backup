'use strict';
var prom = require('promiscuous-tool'), Client = require('./client.js'), _ = require('lodash'), fs = require('fs'), fstream = require('fstream'), zlib = require('zlib'), tar = require('tar');
exports.run = backup;
function promiseWriteToFileStream(fileStream, data) {
  return prom((function(fulfill, reject) {
    process.stdout.write('\rwriting to ' + fileStream.path + ' : ' + fileStream.bytesWritten + ' bytes');
    if (fileStream.write(data)) {
      fulfill(fileStream);
    } else {
      fileStream.once('drain', (function() {
        return fulfill(fileStream);
      }));
    }
  }));
}
function promiseEndFile(fileStream) {
  return prom((function(fulfill, reject) {
    fileStream.once('finish', fulfill);
    fileStream.end();
  }));
}
function writeDocuments(fileStream) {
  return (function(data) {
    return promiseWriteToFileStream(fileStream, _.map(data.hits.hits, JSON.stringify).join('\n'));
  });
}
function documentGetter($__0) {
  var client = $__0.client, index = $__0.index, type = $__0.type, sortBy = "sortBy"in $__0 ? $__0.sortBy: '_Created';
  return (function(start, size) {
    return client.get({
      index: index,
      type: type,
      path: '_search',
      body: {
        query: {"match_all": {}},
        start: start,
        size: size,
        sortBy: sortBy
      }
    });
  });
}
function backupDocuments($__1) {
  var docGetter = $__1.docGetter, fileStream = $__1.fileStream, start = "start"in $__1 ? $__1.start: 0, size = "size"in $__1 ? $__1.size: 100;
  return docGetter(start, size).then((function(data) {
    return prom.sequence([writeDocuments(fileStream), (function(data) {
      if (start + size < data.hits.total) {
        return backupDocuments({
          docGetter: docGetter,
          fileStream: fileStream,
          size: size,
          start: start + size
        });
      } else {
        return promiseEndFile(fileStream);
      }
    })], data);
  }));
}
function writeMappingBackup(fileStream, mapping) {
  return promiseWriteToFileStream(fileStream, JSON.stringify(mapping)).then((function() {
    return promiseEndFile(fileStream);
  }));
}
function mappings(client, index, type) {
  return client.get({
    index: index,
    type: type,
    path: '_mapping'
  }).then((function(response) {
    return !type ? response[index]: response[type];
  }));
}
function backupPath(path, index, type) {
  return path + '/' + index + '_' + type + '_';
}
function filePaths(path, index, type) {
  var base = backupPath(path, index, type);
  return [base + 'documents.json', base + 'mapping.json'];
}
function backupType($__2) {
  var client = $__2.client, index = $__2.index, type = $__2.type, filePath = $__2.filePath;
  var $__3 = filePaths(filePath, index, type), docFileName = $__3[0], mappingFileName = $__3[1];
  fs.mkdirSync(filePath);
  return mappings(client, index, type).then((function(mapping) {
    return prom.join(writeMappingBackup(fs.createWriteStream(mappingFileName, {flags: 'w'}), mapping), backupDocuments({
      docGetter: documentGetter({
        client: client,
        index: index,
        type: type
      }),
      fileStream: fs.createWriteStream(docFileName, {flags: 'w'})
    }));
  })).then((function() {
    return [docFileName, mappingFileName];
  }));
}
function backupIndex(client, index, filePath) {
  return mappings(client, index).then((function(mappings) {
    return prom.all(_.map(mappings, (function(data, type) {
      return backupType({
        client: client,
        index: index,
        type: type,
        filePath: filePath
      });
    })));
  })).then(_.flatten);
}
function indicesFromStatus(status) {
  return _.keys(status.indices);
}
function clusterStatus(client) {
  return client.get({path: '_status'});
}
function backupCluster(client, filePath) {
  return clusterStatus(client).then(indicesFromStatus).then((function(indices) {
    return prom.all(_.map(indices, (function(name) {
      return backupIndex(client, name, filePath);
    })));
  })).then(_.flatten);
}
function compress(filePath) {
  return prom((function(fulfill, reject) {
    fstream.Reader({
      path: filePath,
      type: 'Directory'
    }).pipe(tar.Pack()).pipe(zlib.Gzip()).pipe(fstream.Writer(filePath + '.tar.gz')).on('error', reject).on('close', (function() {
      process.stdout.write('\ncompressed to ' + filePath + '.tar.gz \n');
      fulfill(filePath);
    }));
  }));
}
function rmdirR(path) {
  if (fs.existsSync(path)) {
    fs.readdirSync(path).forEach(function(file) {
      var curPath = path + "/" + file;
      if (fs.lstatSync(curPath).isDirectory()) {
        rmdirR(curPath);
      } else {
        fs.unlinkSync(curPath);
      }
    });
    fs.rmdirSync(path);
  }
}
function backup($__3) {
  var host = "host"in $__3 ? $__3.host: 'localhost', port = "port"in $__3 ? $__3.port: 9200, index = $__3.index, type = $__3.type, filePath = "filePath"in $__3 ? $__3.filePath: 'temp';
  var client = new Client({
    host: host,
    port: port
  });
  filePath += '/' + new Date().getTime();
  return ((function() {
    if (index && type) {
      return backupType({
        client: client,
        index: index,
        type: type,
        filePath: filePath
      });
    } else if (index) {
      return backupIndex(client, index, filePath);
    } else {
      return backupCluster(client, filePath);
    }
  })()).then((function(files) {
    return (process.stdout.write('\n' + files.join('\n')), filePath);
  })).then(compress).then(rmdirR, (function(error) {
    return console.log(error);
  }));
}
