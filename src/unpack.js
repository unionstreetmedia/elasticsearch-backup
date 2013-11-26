'use strict';

//Standard Library
var fs = require('fs');

//Internal
var util = require('./util.js');

//External
var prom = require('promiscuous-tool'),
    _ = require('lodash');

module.exports = unpack;

//Find the latest tar
function latest (filePath) {
    return _.last(_.sortBy(fs.readdirSync(filePath)));
}

//Apply tar.gz if missing
function tarExtension (file) {
    return file.substr(-7) === '.tar.gz' ? file : file + '.tar.gz';
}

//Get correct version name
function version (filePath, name) {
    return tarExtension(name === 'latest' ? latest(filePath) : name);
}

//Extract and populate cluster from tar.gz
function unpack ({host, port, filePath = 'temp', name = 'latest'}) {
    return prom((fullfill, reject) => util.extract(filePath + '/' + version(filePath, name)));
}
