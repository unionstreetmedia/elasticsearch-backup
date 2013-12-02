'use strict';

//Standard library
var http = require('http');

//External
var prom = require('promiscuous-tool'),
    _ = require('lodash');

module.exports = Client;

// Elasticsearch client
function Client ({host = 'localhost', port = 9200}) {
    this.host = host;
    this.port = port;
}

Client.prototype.request = function (method, {index, type, path, body}) {
    path = [index, type, path].filter(val => val).join('/');
    return prom((fulfill, reject) => {
        var request = http.request({
            method,
            path,
            host: this.host,
            port: this.port,
            headers: {
                'Content-Type': 'application/json'
            }
        }, response => {
            var data = '';
            response.on('data', chunk => data += chunk)
                .once('error', error => reject(error))
                .once('end', () => {
                    if (response.statusCode == 200) {
                        fulfill(JSON.parse(data));
                    } else {
                        reject(response.statusCode + ':' + path + '\n\n' + data);
                    }
                });
        });
        if (body) {
            if (typeof body === 'object') {
                body = JSON.stringify(body);
            }
            request.write(body);
        }
        request.once('error', error => reject(error));
        request.end();
    });
}

Client.prototype.put = _.partial(Client.prototype.request, 'put');

Client.prototype.post = _.partial(Client.prototype.request, 'post');

Client.prototype.get = _.partial(Client.prototype.request, 'get');
