'use strict';
var fs = require('fs');
var util = require('./util.js'), Client = require('./client.js');
var prom = require('promiscuous-tool'), _ = require('lodash'), byline = require('byline');
module.exports = unpack;
function latest(filePath) {
  return _.last(_.sortBy(fs.readdirSync(filePath)));
}
function tarExtension(file) {
  return file.substr(- 7) === '.tar.gz' ? file: file + '.tar.gz';
}
function version(filePath, name) {
  return tarExtension(name === 'latest' ? latest(filePath): name);
}
function getDirectories(filePath) {
  return prom((function(fulfill, reject) {
    return fs.readdir(filePath, (function(err, files) {
      if (err) {
        return reject(err);
      }
      fulfill(files.filter((function(file) {
        return file.indexOf('.') == - 1;
      })));
    }));
  }));
}
function getESPath(filePath) {
  return filePath.split('/').slice(2);
}
function putSettings(client, filePath) {
  var index = getESPath(filePath)[0];
  return client.put({
    index: index,
    body: fs.readFileSync(filePath + '/settings.json').toString()
  });
}
function buildTypes(client, filePath) {
  return getDirectories(filePath).then((function(types) {
    return prom.all(_.map(types, (function(type) {
      return prom.sequence([_.partial(putMapping, client, filePath + '/' + type), _.partial(bulkDocuments, client, filePath + '/' + type)]);
    })));
  }));
}
function putMapping(client, filePath) {
  var $__6 = getESPath(filePath), index = $__6[0], type = $__6[1];
  return client.put({
    index: index,
    type: type,
    path: '_mapping',
    body: fs.readFileSync(filePath + '/mapping.json').toString()
  });
}
function bulkDocuments(client, filePath) {
  filePath = filePath + '/documents.json';
  return prom((function(fulfill, reject) {
    var maxLines = 2000, buffer = [], promises = [], send = function() {
      if (buffer.length) {
        promises.push(client.post({
          path: '_bulk',
          body: buffer.join('\n')
        }).then((function(x) {
          return x;
        }), util.log));
        buffer = [];
      }
    };
    if (fs.existsSync(filePath)) {
      byline(fs.createReadStream(filePath)).on('data', (function(line) {
        if (buffer.length == maxLines) {
          send();
        }
        buffer.push(line.toString());
      })).on('end', (function() {
        send();
        fulfill(promises);
      }));
    } else {
      fulfill('no documents');
    }
  }));
}
function buildIndex(client, filePath, index) {
  filePath = filePath + '/' + index;
  return putSettings(client, filePath).then(_.partial(buildTypes, client, filePath));
}
function buildIndexes(client, filePath) {
  return getDirectories(filePath).then((function(indexes) {
    return prom.all(_.map(indexes, _.partial(buildIndex, client, filePath)));
  }));
}
function unpack($__6) {
  var host = "host"in $__6 ? $__6.host: 'localhost', port = "port"in $__6 ? $__6.port: 9200, filePath = "filePath"in $__6 ? $__6.filePath: 'temp', name = "name"in $__6 ? $__6.name: 'latest', rebuild = "rebuild"in $__6 ? $__6.rebuild: true;
  var client = new Client({
    host: host,
    port: port
  }), promise = util.extract(filePath + '/' + version(filePath, name));
  if (rebuild) {
    promise = promise.then(_.partial(buildIndexes, client));
  }
  return promise.then((function(data) {
    return console.log(data);
  }), util.errorHandler);
}
