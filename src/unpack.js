'use strict';

//Internal
var util = require('./util.js');

//External
var prom = require('promiscuous-tool');

module.exports = unpack;

//Extract and populate cluster from tar.gz
function unpack ({host, port, filePath = 'temp', version}) {
    return prom((fullfill, reject) => util.extract(filePath, version));
}
