'use strict';
var util = require('./util.js');
var prom = require('promiscuous-tool');
module.exports = unpack;
function unpack($__6) {
  var host = $__6.host, port = $__6.port, filePath = "filePath"in $__6 ? $__6.filePath: 'temp', version = $__6.version;
  return prom((function(fullfill, reject) {
    return util.extract(filePath, version);
  }));
}
