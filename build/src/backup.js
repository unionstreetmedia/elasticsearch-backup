var http = require('http'), prom = require('promiscuous-tool'), _ = require('lodash'), fs = require('fs'), fstream = require('fstream'), zlib = require('zlib'), tar = require('tar');
exports.run = backup;
process.on('uncaughtException', function(e) {
  console.log("Caught unhandled exception: " + e);
  console.log(" ---> : " + e.stack);
});
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
function documents($__0) {
  var client = $__0.client, index = $__0.index, type = $__0.type, start = "start"in $__0 ? $__0.start: 0, size = "size"in $__0 ? $__0.size: 100, sortBy = "sortBy"in $__0 ? $__0.sortBy: '_Created', fileStream = $__0.fileStream;
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
  }).then((function(data) {
    return prom.sequence([writeDocuments(fileStream), (function(data) {
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
    })], data);
  }));
}
function writeMappingBackup($__1) {
  var fileStream = $__1.fileStream, index = $__1.index, name = $__1.name, mapping = $__1.mapping;
  return promiseWriteToFileStream(fileStream, JSON.stringify(mapping)).then((function() {
    return promiseEndFile(fileStream);
  }));
}
function mappings($__2) {
  var client = $__2.client, index = $__2.index, type = "type"in $__2 ? $__2.type: null;
  return client.get({
    index: index,
    type: type,
    path: '_mapping'
  }).then((function(response) {
    if (!type) {
      return response[index];
    }
    return response[type];
  }));
}
function backupType($__3) {
  var client = $__3.client, index = $__3.index, type = $__3.type, filePath = $__3.filePath;
  var fileBase = filePath + '/' + index + '_' + type + '_', docFileName = fileBase + 'documents.json', mappingFileName = fileBase + 'mapping.json';
  fs.mkdirSync(filePath);
  return mappings({
    client: client,
    index: index,
    type: type
  }).then((function(mapping) {
    return prom.join(writeMappingBackup({
      index: index,
      name: type,
      mapping: mapping,
      fileStream: fs.createWriteStream(mappingFileName, {flags: 'w'})
    }), documents({
      client: client,
      index: index,
      type: type,
      fileStream: fs.createWriteStream(docFileName, {flags: 'w'})
    }));
  })).then((function() {
    return [docFileName, mappingFileName];
  }));
}
function backupIndex(client, index, filePath) {
  return mappings({
    client: client,
    index: index
  }).then((function(mappings) {
    return prom.all(_.map(mappings, (function(data, name) {
      return backupType({
        client: client,
        index: index,
        type: name,
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
function backup($__4) {
  var host = "host"in $__4 ? $__4.host: 'localhost', port = "port"in $__4 ? $__4.port: 9200, index = $__4.index, type = $__4.type, filePath = "filePath"in $__4 ? $__4.filePath: 'temp';
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
    return prom((function(fulfill, reject) {
      process.stdout.write('\n' + files.join('\n'));
      fstream.Reader({
        path: filePath,
        type: 'Directory'
      }).pipe(tar.Pack()).pipe(zlib.Gzip()).pipe(fstream.Writer(filePath + '.tar.gz')).on('error', reject).on('close', (function() {
        rmdirR(filePath);
        process.stdout.write('\ncompressed to ' + filePath + '.tar.gz \n');
        fulfill();
      }));
    }));
  })).then((function() {}), (function(error) {
    return console.log(error);
  }));
}
function Client($__5) {
  var host = "host"in $__5 ? $__5.host: 'localhost', port = "port"in $__5 ? $__5.port: 9200;
  this.host = host;
  this.port = port;
}
Client.prototype.get = function($__6) {
  var index = "index"in $__6 ? $__6.index: null, type = "type"in $__6 ? $__6.type: null, path = $__6.path, body = "body"in $__6 ? $__6.body: null;
  var path = [index, type, path].filter((function(val) {
    return val;
  })).join('/');
  return prom((function(fulfill, reject) {
    var request = http.request({
      host: this.host,
      port: this.port,
      path: path,
      method: 'get',
      headers: {'Content-Type': 'application/json'}
    }, (function(response) {
      if (response.statusCode == 200) {
        var data = '';
        response.on('data', (function(chunk) {
          return data += chunk;
        })).once('error', (function(error) {
          return reject(error);
        })).once('end', (function() {
          return fulfill(JSON.parse(data));
        }));
      } else {
        fail(response);
      }
    }));
    if (body) {
      request.write(JSON.stringify(body));
    }
    request.once('error', (function(error) {
      return reject(error);
    }));
    request.end();
  }).bind(this));
};
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
