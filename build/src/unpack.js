'use strict';
var fs = require('fs');
var util = require('./util.js');
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
function unpack($__6) {
  var host = $__6.host, port = $__6.port, filePath = "filePath"in $__6 ? $__6.filePath: 'temp', name = "name"in $__6 ? $__6.name: 'latest';
  return util.extract(filePath + '/' + version(filePath, name)).then((function() {
    return console.log('file extracted');
  }), util.errorHandler);
}
