'use strict';
var fs = require('fs');
var Client = require('./client.js'), util = require('./util.js');
var prom = require('promiscuous-tool'), _ = require('lodash');
var THROTTLE = 50;
module.exports = pack;
function writeDocuments(fileStream) {
  return (function(data) {
    return util.promiseWriteToFileStream(fileStream, _.map(data.hits.hits, (function(data) {
      return JSON.stringify({index: {
          _index: data._index,
          _type: data._type,
          _id: data._id
        }}) + '\n' + JSON.stringify(data._source);
    })).join('\n'));
  });
}
function documentScroller(client, scrollID) {
  return (function() {
    return client.get({path: '_search/scroll?scroll=' + (THROTTLE + 100) + 'm&scroll_id=' + scrollID}).then((function(data) {
      scrollID = data['_scroll_id'];
      return data;
    }));
  });
}
function startScroll($__2) {
  var client = $__2.client, index = $__2.index, type = $__2.type, size = "size"in $__2 ? $__2.size: 100;
  return client.get({
    index: index,
    type: type,
    path: '_search?search_type=scan&scroll=' + (THROTTLE + 100) + 'm&size=' + size,
    body: {query: {"match_all": {}}}
  }).then((function(data) {
    return documentScroller(client, data['_scroll_id']);
  }));
}
function backupDocuments($__3) {
  var docScroller = $__3.docScroller, fileStream = $__3.fileStream;
  return docScroller().then((function(data) {
    return prom.sequence([writeDocuments(fileStream), (function(data) {
      if (data.hits.hits.length) {
        process.stdout.write('\rwriting ' + fileStream.path + ' : ' + fileStream.bytesWritten + '\r');
        return prom.delay(THROTTLE, (function() {
          return backupDocuments({
            docScroller: docScroller,
            fileStream: fileStream
          });
        }));
      } else {
        return util.promiseEndFile(fileStream);
      }
    })], data);
  }));
}
function writeMappingBackup(fileStream, mapping) {
  console.log(mapping);
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
    return type == null ? response[index]: response;
  }));
}
function typePath(path, type) {
  return path + '/' + type;
}
function backupType($__4) {
  var client = $__4.client, index = $__4.index, type = $__4.type, filePath = $__4.filePath;
  filePath = typePath(filePath, type);
  if (!fs.existsSync(filePath)) {
    fs.mkdirSync(filePath);
  }
  var docFileName = filePath + '/documents.json', mappingFileName = filePath + '/mapping.json';
  return mappings(client, index, type).then((function(mapping) {
    return prom.join(writeMappingBackup(fs.createWriteStream(mappingFileName, {flags: 'w'}), mapping), startScroll({
      client: client,
      index: index,
      type: type
    }).then((function(docScroller) {
      return backupDocuments({
        docScroller: docScroller,
        fileStream: fs.createWriteStream(docFileName, {flags: 'w'})
      });
    })));
  })).then((function() {
    return [docFileName, mappingFileName];
  }));
}
function indexSettings(client, index) {
  return client.get({
    index: index,
    path: '_settings'
  }).then((function(response) {
    return response[index];
  }));
}
function indexWriteSettings(filePath, index, data) {
  var innerSettings = {};
  innerSettings[index] = {
    'number_of_shards': data.settings['index.number_of_shards'],
    'number_of_replicas': data.settings['index.number_of_replicas']
  };
  return util.promiseWriteToFileStream(fs.createWriteStream(filePath + '/settings.json', {flags: 'w'}), JSON.stringify({settings: innerSettings}));
}
function createIndexDir(filePath, index) {
  filePath = filePath + '/' + index;
  if (!fs.existsSync(filePath)) {
    fs.mkdirSync(filePath);
  }
  return filePath;
}
function backupIndex(client, index, filePath) {
  filePath = createIndexDir(filePath, index);
  return indexSettings(client, index).then((function(data) {
    return indexWriteSettings(filePath, index, data);
  })).then((function() {
    return mappings(client, index);
  })).then((function(mappings) {
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
  if (!fs.existsSync(filePath)) {
    fs.mkdirSync(filePath);
  }
  return ((function() {
    if (index && type) {
      filePath = createIndexDir(filePath, index);
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
