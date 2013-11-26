'use strict';
var fs = require('fs');
var Client = require('./client.js'), util = require('./util.js');
var prom = require('promiscuous-tool'), _ = require('lodash');
module.exports = pack;
function writeDocuments(fileStream) {
  return (function(data) {
    return util.promiseWriteToFileStream(fileStream, _.map(data.hits.hits, JSON.stringify).join('\n'));
  });
}
function documentGetter($__2) {
  var client = $__2.client, index = $__2.index, type = $__2.type, sortBy = "sortBy"in $__2 ? $__2.sortBy: '_Created';
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
function backupDocuments($__3) {
  var docGetter = $__3.docGetter, fileStream = $__3.fileStream, start = "start"in $__3 ? $__3.start: 0, size = "size"in $__3 ? $__3.size: 100;
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
        return util.promiseEndFile(fileStream);
      }
    })], data);
  }));
}
function writeMappingBackup(fileStream, mapping) {
  return util.promiseWriteToFileStream(fileStream, JSON.stringify(mapping)).then((function() {
    return util.promiseEndFile(fileStream);
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
function backupType($__4) {
  var client = $__4.client, index = $__4.index, type = $__4.type, filePath = $__4.filePath;
  var $__5 = filePaths(filePath, index, type), docFileName = $__5[0], mappingFileName = $__5[1];
  if (!fs.existsSync(filePath)) {
    fs.mkdirSync(filePath);
  }
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
    return _.keys(mappings).length ? prom.all(_.map(mappings, (function(data, type) {
      return backupType({
        client: client,
        index: index,
        type: type,
        filePath: filePath
      });
    }))): [];
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
function pack($__5) {
  var host = "host"in $__5 ? $__5.host: 'localhost', port = "port"in $__5 ? $__5.port: 9200, index = $__5.index, type = $__5.type, filePath = "filePath"in $__5 ? $__5.filePath: 'temp';
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
  })).then(util.compress).then(util.rmdirR, util.errorHandler);
}
