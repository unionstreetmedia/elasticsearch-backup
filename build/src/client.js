'use strict';
var http = require('http');
var prom = require('promiscuous-tool');
module.exports = Client;
function Client($__0) {
  var host = "host"in $__0 ? $__0.host: 'localhost', port = "port"in $__0 ? $__0.port: 9200;
  this.host = host;
  this.port = port;
}
Client.prototype.get = function($__1) {
  var index = $__1.index, type = $__1.type, path = $__1.path, body = $__1.body;
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
      var data = '';
      response.on('data', (function(chunk) {
        return data += chunk;
      })).once('error', (function(error) {
        return reject(error);
      })).once('end', (function() {
        if (response.statusCode == 200) {
          fulfill(JSON.parse(data));
        } else {
          reject(response.statusCode + ':' + path + '\n\n' + data);
        }
      }));
    }));
    if (body) {
      if (typeof body === 'object') {
        body = JSON.stringify(body);
      }
      request.write(body);
    }
    request.once('error', (function(error) {
      return reject(error);
    }));
    request.end();
  }).bind(this));
};
