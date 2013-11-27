'use strict';
var fs = require('fs');
var util = require('./util.js'), Client = require('./client.js');
var prom = require('promiscuous-tool'), _ = require('lodash');
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
function getIndexDirectories(filePath) {
  return prom((function(fulfill, reject) {
    return fs.readdir(filePath, (function(err, files) {
      if (err) {
        return reject(err);
      }
      fulfill(files);
    }));
  }));
}
function postSettings(client, filePath) {
  return filePath;
  return client.post({
    index: filePath.substring(filePath.lastIndexOf('/')),
    body: fs.readFileSync(filePath + '/settings.json')
  });
}
function putMappings(client, filePath) {
  return filePath;
}
function bulkDocuments(client, filePath) {
  return filePath;
}
function buildIndex(client, filePath, index) {
  filePath = filePath + '/' + index;
  return postSettings(client, filePath);
}
function buildIndexes(client, filePath) {
  return getIndexDirectories(filePath).then(_.partialRight(_.map, _.partial(buildIndex, client, filePath)));
}
function unpack($__8) {
  var host = $__8.host, port = $__8.port, filePath = "filePath"in $__8 ? $__8.filePath: 'temp', name = "name"in $__8 ? $__8.name: 'latest';
  var client = new Client({
    host: host,
    port: port
  });
  return util.extract(filePath + '/' + version(filePath, name)).then(_.partial(buildIndexes, client)).then((function(data) {
    return console.log(data);
  }), util.errorHandler);
}
