'use strict';
var zlib = require('zlib'), fs = require('fs');
var prom = require('promiscuous-tool'), _ = require('lodash'), fstream = require('fstream'), tar = require('tar');
module.exports = {
  promiseWriteToFileStream: promiseWriteToFileStream,
  promiseEndFile: promiseEndFile,
  compress: compress,
  extract: extract,
  rmdirR: rmdirR,
  errorHandler: errorHandler
};
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
function compress(filePath) {
  return prom((function(fulfill, reject) {
    if (fs.existsSync(filePath)) {
      fstream.Reader({
        path: filePath,
        type: 'Directory'
      }).pipe(tar.Pack()).pipe(zlib.Gzip()).pipe(fstream.Writer(filePath + '.tar.gz')).on('error', reject).on('close', (function() {
        process.stdout.write('\ncompressed to ' + filePath + '.tar.gz \n');
        fulfill(filePath);
      }));
    } else {
      process.stdout.write('\nNo file to compressed to ' + filePath + '.tar.gz \n');
      fulfill(filePath);
    }
  }));
}
function extract(file) {
  return prom((function(fulfill, reject) {
    var filePath = file.substring(0, file.lastIndexOf('/'));
    if (fs.existsSync(file)) {
      fstream.Reader({
        path: file,
        type: 'file'
      }).pipe(zlib.Gunzip()).pipe(tar.Extract({path: filePath})).on('error', reject).on('close', (function() {
        process.stdout.write('\nextracted to ' + filePath + '\n');
        fulfill(filePath);
      }));
    } else {
      process.stdout.write('\nNo file to extract\n');
      fulfill(filePath);
    }
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
function errorHandler(error) {
  if (error.errno === 34 && error.code === 'ENOENT') {
    console.log('Target directory does not exist');
  } else {
    console.log(error);
  }
  return error;
}
