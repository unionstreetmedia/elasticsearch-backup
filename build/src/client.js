var http = require('http'), prom = require('promiscuous-tool');
function Client($__4) {
  var host = "host"in $__4 ? $__4.host: 'localhost', port = "port"in $__4 ? $__4.port: 9200;
  this.host = host;
  this.port = port;
}
Client.prototype.get = function($__5) {
  var index = $__5.index, type = $__5.type, path = $__5.path, body = $__5.body;
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
module.exports = Client;
