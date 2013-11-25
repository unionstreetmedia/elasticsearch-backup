'use strict';
var http = require('http');
var prom = require('promiscuous-tool');
module.exports = Client;
function Client($__5) {
  var host = "host"in $__5 ? $__5.host: 'localhost', port = "port"in $__5 ? $__5.port: 9200;
  this.host = host;
  this.port = port;
}
Client.prototype.get = function($__6) {
  var index = $__6.index, type = $__6.type, path = $__6.path, body = $__6.body;
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
        reject(response);
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
