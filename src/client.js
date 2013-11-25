var http = require('http'),
    prom = require('promiscuous-tool');

// Elasticsearch client
function Client ({host = 'localhost', port = 9200}) {
    this.host = host;
    this.port = port;
}

Client.prototype.get = function ({index, type, path, body}) {
    var path = [index, type, path].filter(val => val).join('/');
    return prom((fulfill, reject) => {
        var request = http.request({
            host: this.host,
            port: this.port,
            path: path,
            method: 'get',
            headers: {
                'Content-Type': 'application/json'
            }
        }, response => {
            if (response.statusCode == 200) {
                var data = '';
                response.on('data', chunk => data += chunk)
                    .once('error', error => reject(error))
                    .once('end', () => fulfill(JSON.parse(data)));
            } else {
                fail(response);
            }
        });
        if (body) {
            request.write(JSON.stringify(body));
        }
        request.once('error', error => reject(error));
        request.end();
    });
}

module.exports = Client;
